//go:build meta_internal

package omebot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Actor is the human behind a command, as resolved by the relay.
type Actor struct {
	EmployeeFBID int64  `json:"employeeFbid,omitempty"`
	SlackUserID  string `json:"slackUserId,omitempty"`
}

// CommandRequest is the body of POST /v1/command.
type CommandRequest struct {
	Actor     Actor  `json:"actor"`
	ChannelID string `json:"channelId,omitempty"`
	// RequestID correlates the audit line with the caller's own logs.
	RequestID string `json:"requestId,omitempty"`
	// Text is everything the user typed after "/ome".
	Text string `json:"text"`
}

// CommandResponse is the reply the relay renders into Slack.
type CommandResponse struct {
	OK bool `json:"ok"`
	// Text is Slack mrkdwn ready to post.
	Text     string          `json:"text"`
	Sections []Result        `json:"sections,omitempty"`
	Clusters []ClusterStatus `json:"clusters,omitempty"`
	Error    string          `json:"error,omitempty"`
	// ErrorClass is empty on success; see the Err* constants.
	ErrorClass string `json:"errorClass,omitempty"`
}

// ToolRequest is the body of POST /v1/tool.
type ToolRequest struct {
	Actor     Actor    `json:"actor"`
	ChannelID string   `json:"channelId,omitempty"`
	RequestID string   `json:"requestId,omitempty"`
	Tool      string   `json:"tool"`
	Args      []string `json:"args"`
}

// ToolResponse is the reply to POST /v1/tool. Result is set whenever the
// tool ran, including when it failed.
type ToolResponse struct {
	OK         bool        `json:"ok"`
	Text       string      `json:"text"`
	Result     *ToolResult `json:"result,omitempty"`
	Error      string      `json:"error,omitempty"`
	ErrorClass string      `json:"errorClass,omitempty"`
}

// ReleaseResponse is the reply to GET /v1/release.
type ReleaseResponse struct {
	OK         bool           `json:"ok"`
	Text       string         `json:"text"`
	Release    *ReleaseReport `json:"release,omitempty"`
	Error      string         `json:"error,omitempty"`
	ErrorClass string         `json:"errorClass,omitempty"`
}

// Server wires the HTTP API to the runner, prober, tools and release reader.
type Server struct {
	Config   *Config
	Paths    map[string]string
	Runner   *Runner
	Prober   *Prober
	Verifier *Verifier
	Log      *slog.Logger
	// Tools serves POST /v1/tool; nil refuses every tool.
	Tools *Tools
	// Release serves GET /v1/release; nil answers not_configured.
	Release *Release

	ready atomic.Bool
	// slots bounds requests that run a process or probe clusters.
	slots chan struct{}
}

// auditEntry is one audit line. It never carries tool or command output.
type auditEntry struct {
	kind      string
	requestID string
	actor     Actor
	channel   string
	text      string
	tool      string
	args      []string
	ok        bool
	errClass  string
	err       string
	clusters  []string
	exits     []int
}

// SetReady flips the readiness probe once startup work is complete.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler {
	if s.slots == nil {
		s.slots = make(chan struct{}, s.Config.MaxInFlight)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /v1/clusters", s.handleClusters)
	mux.HandleFunc("POST /v1/command", s.handleCommand)
	mux.HandleFunc("POST /v1/tool", s.handleTool)
	mux.HandleFunc("GET /v1/release", s.handleRelease)
	return mux
}

// acquire takes a concurrency slot without waiting; the caller must call
// release when it returns true.
func (s *Server) acquire() bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) release() { <-s.slots }

const busyMsg = "the bot is busy; retry in a moment"

// readBody reads and verifies a request body, writing the error reply itself
// when it returns false.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	limit := int64(s.Config.MaxRequestBytes)
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil || int64(len(body)) > limit {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	if err := s.Verifier.Verify(r, body); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return nil, false
	}
	return body, true
}

func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	if err := s.Verifier.Verify(r, nil); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if !s.acquire() {
		writeJSON(w, http.StatusTooManyRequests, refusal(ErrBusy, busyMsg))
		return
	}
	defer s.release()
	start := time.Now()
	statuses := s.Prober.ProbeAll(r.Context())
	s.audit(auditEntry{kind: "clusters", ok: true}, time.Since(start))
	writeJSON(w, http.StatusOK, CommandResponse{OK: true, Text: RenderClusters(statuses), Clusters: statuses})
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	var req CommandRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, CommandResponse{Error: "malformed request: " + err.Error(), ErrorClass: ErrMalformed})
		return
	}
	start := time.Now()
	resp, status := s.execute(r.Context(), req)
	e := auditEntry{kind: "command", requestID: req.RequestID, actor: req.Actor, channel: req.ChannelID, text: req.Text,
		ok: resp.OK, errClass: resp.ErrorClass, err: resp.Error}
	for _, sec := range resp.Sections {
		e.clusters = append(e.clusters, sec.Cluster)
		e.exits = append(e.exits, sec.ExitCode)
	}
	s.audit(e, time.Since(start))
	writeJSON(w, status, resp)
}

func (s *Server) handleTool(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	var req ToolRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, ToolResponse{Error: "malformed request: " + err.Error(), ErrorClass: ErrMalformed})
		return
	}
	if !s.acquire() {
		writeJSON(w, http.StatusTooManyRequests, ToolResponse{Error: busyMsg, ErrorClass: ErrBusy, Text: ":no_entry: " + busyMsg})
		return
	}
	defer s.release()
	start := time.Now()
	resp := s.runTool(r.Context(), req)
	e := auditEntry{kind: "tool", requestID: req.RequestID, actor: req.Actor, channel: req.ChannelID, tool: req.Tool, args: req.Args,
		ok: resp.OK, errClass: resp.ErrorClass}
	if resp.Result != nil {
		e.exits = []int{resp.Result.ExitCode}
	}
	if resp.ErrorClass != "" {
		e.err = resp.Error
	}
	s.audit(e, time.Since(start))
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) runTool(ctx context.Context, req ToolRequest) ToolResponse {
	if s.Tools == nil {
		msg := "no tools are configured"
		return ToolResponse{Error: msg, ErrorClass: ErrUnknownTool, Text: ":no_entry: " + msg}
	}
	res, class := s.Tools.Run(ctx, req.Tool, req.Args)
	if class != "" && class != ErrTimeout {
		return ToolResponse{Error: res.Error, ErrorClass: class, Text: ":no_entry: " + res.Error}
	}
	return ToolResponse{
		OK:         class == "" && res.ExitCode == 0,
		Text:       RenderTool(req.Tool, req.Args, res),
		Result:     &res,
		Error:      res.Error,
		ErrorClass: class,
	}
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	if err := s.Verifier.Verify(r, nil); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if s.Release == nil {
		msg := "no Kargo project is configured for release reports"
		writeJSON(w, http.StatusNotFound, ReleaseResponse{Error: msg, ErrorClass: ErrNotConfigured, Text: ":no_entry: " + msg})
		return
	}
	if !s.acquire() {
		writeJSON(w, http.StatusTooManyRequests, ReleaseResponse{Error: busyMsg, ErrorClass: ErrBusy, Text: ":no_entry: " + busyMsg})
		return
	}
	defer s.release()
	start := time.Now()
	rep, err := s.Release.Report(r.Context(), s.Prober.ProbeAll(r.Context()))
	if err != nil {
		s.audit(auditEntry{kind: "release", err: err.Error()}, time.Since(start))
		writeJSON(w, http.StatusBadGateway, ReleaseResponse{Error: "read Kargo: " + err.Error(), Text: ":warning: read Kargo: " + err.Error()})
		return
	}
	s.audit(auditEntry{kind: "release", ok: true}, time.Since(start))
	writeJSON(w, http.StatusOK, ReleaseResponse{OK: true, Text: RenderRelease(rep), Release: &rep})
}

// execute runs one command and never panics the request on CLI failure.
func (s *Server) execute(ctx context.Context, req CommandRequest) (CommandResponse, int) {
	parsed, err := ParseCommand(req.Text)
	if err != nil {
		class := ErrMalformed
		var pe *ParseError
		if errors.As(err, &pe) {
			class = pe.Class
		}
		return refusal(class, err.Error()), http.StatusOK
	}
	if parsed.Kind == KindHelp {
		return CommandResponse{OK: true, Text: HelpText(s.Config.ClusterNames())}, http.StatusOK
	}
	if !s.acquire() {
		return refusal(ErrBusy, busyMsg), http.StatusTooManyRequests
	}
	defer s.release()
	if parsed.Kind == KindClusters {
		statuses := s.Prober.ProbeAll(ctx)
		return CommandResponse{OK: true, Text: RenderClusters(statuses), Clusters: statuses}, http.StatusOK
	}

	args := append(append([]string(nil), parsed.Verb...), parsed.Args...)
	var targets []Cluster
	if parsed.Cluster != "" {
		cl, ok := s.Config.Cluster(parsed.Cluster)
		if !ok {
			return refusal(ErrUnknownCluster, fmt.Sprintf("unknown cluster %q; `clusters` lists them", parsed.Cluster)), http.StatusOK
		}
		targets = []Cluster{cl}
	} else {
		targets = s.Config.Clusters
	}

	fanCtx := ctx
	if len(targets) > 1 {
		var cancel context.CancelFunc
		fanCtx, cancel = context.WithTimeout(ctx, time.Duration(s.Config.Timeouts.FanoutSeconds)*time.Second)
		defer cancel()
	}
	results := make([]Result, len(targets))
	var wg sync.WaitGroup
	for i, cl := range targets {
		wg.Add(1)
		go func(i int, cl Cluster) {
			defer wg.Done()
			results[i] = s.Runner.Run(fanCtx, cl.Name, s.Paths[cl.Name], args)
		}(i, cl)
	}
	wg.Wait()

	ok := true
	for _, r := range results {
		if r.ExitCode != 0 {
			ok = false
		}
	}
	return CommandResponse{OK: ok, Text: RenderResults(strings.Join(args, " "), results), Sections: results}, http.StatusOK
}

func refusal(class, msg string) CommandResponse {
	return CommandResponse{Error: msg, ErrorClass: class, Text: ":no_entry: " + msg}
}

func (s *Server) audit(e auditEntry, d time.Duration) {
	s.Log.Info("request",
		"kind", e.kind,
		"request_id", e.requestID,
		"actor_fbid", e.actor.EmployeeFBID,
		"slack_user", e.actor.SlackUserID,
		"channel", e.channel,
		"text", e.text,
		"tool", e.tool,
		"args", e.args,
		"ok", e.ok,
		"error_class", e.errClass,
		"error", e.err,
		"clusters", e.clusters,
		"exit_codes", e.exits,
		"duration_ms", d.Milliseconds(),
	)
}

// RenderTool formats one tool call as Slack mrkdwn.
func RenderTool(tool string, args []string, res ToolResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "`%s %s`", tool, strings.Join(args, " "))
	if res.ExitCode != 0 {
		fmt.Fprintf(&b, " — exit %d", res.ExitCode)
	}
	b.WriteString("\n```\n")
	out := strings.TrimRight(res.Output, "\n")
	if res.Error != "" {
		if out != "" {
			out += "\n"
		}
		out += res.Error
	}
	if out == "" {
		out = "(no output)"
	}
	b.WriteString(out)
	if res.Truncated {
		b.WriteString("\n… output truncated")
	}
	b.WriteString("\n```")
	return b.String()
}

// RenderClusters formats the fleet table as Slack mrkdwn.
func RenderClusters(statuses []ClusterStatus) string {
	var b strings.Builder
	b.WriteString("*Installed clusters*\n```\n")
	fmt.Fprintf(&b, "%-26s %-5s %-10s %-8s %-10s %-11s %s\n", "CLUSTER", "PROV", "ROLE", "REACH", "K8S", "OME", "NOTE")
	for _, st := range statuses {
		reach, ome := "no", "-"
		if st.Reachable {
			reach = "yes"
			ome = "absent"
			if st.OMEInstalled {
				ome = "installed"
				if st.OMEVersion != "" {
					ome = st.OMEVersion
				}
			}
		}
		note := st.Error
		if note == "" && st.KargoStage != "" {
			note = "kargo:" + st.KargoStage
		}
		fmt.Fprintf(&b, "%-26s %-5s %-10s %-8s %-10s %-11s %s\n", st.Name, st.Provider, orDash(st.Role), reach, orDash(st.K8sVersion), ome, note)
	}
	b.WriteString("```")
	return b.String()
}

// RenderResults formats CLI output per cluster as Slack mrkdwn.
func RenderResults(command string, results []Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "`kubectl ome %s`\n", command)
	for _, r := range results {
		fmt.Fprintf(&b, "*%s*", r.Cluster)
		if r.ExitCode != 0 {
			fmt.Fprintf(&b, " — exit %d", r.ExitCode)
		}
		b.WriteString("\n```\n")
		out := strings.TrimRight(r.Output, "\n")
		if out == "" && r.Error != "" {
			out = r.Error
		} else if r.Error != "" {
			out += "\n" + r.Error
		}
		if out == "" {
			out = "(no output)"
		}
		b.WriteString(out)
		if r.Truncated {
			b.WriteString("\n… output truncated")
		}
		b.WriteString("\n```\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ErrNotReady is returned by Start when the kubeconfig set cannot be built.
var ErrNotReady = errors.New("service not ready")
