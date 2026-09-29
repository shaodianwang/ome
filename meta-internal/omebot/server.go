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

// maxRequestBytes bounds a command request body; a slash command is a line
// of text and a little metadata.
const maxRequestBytes = 64 << 10

// Actor is the human behind a command, as resolved by the relay.
type Actor struct {
	EmployeeFBID int64  `json:"employeeFbid,omitempty"`
	SlackUserID  string `json:"slackUserId,omitempty"`
}

// CommandRequest is the body of POST /v1/command.
type CommandRequest struct {
	Actor     Actor  `json:"actor"`
	ChannelID string `json:"channelId,omitempty"`
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
}

// Server wires the HTTP API to the runner and prober.
type Server struct {
	Config   *Config
	Paths    map[string]string
	Runner   *Runner
	Prober   *Prober
	Verifier *Verifier
	Log      *slog.Logger

	ready atomic.Bool
}

// SetReady flips the readiness probe once startup work is complete.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler {
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
	return mux
}

func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	if err := s.Verifier.Verify(r, nil); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	statuses := s.Prober.ProbeAll(r.Context())
	writeJSON(w, http.StatusOK, CommandResponse{OK: true, Text: RenderClusters(statuses), Clusters: statuses})
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil || len(body) > maxRequestBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := s.Verifier.Verify(r, body); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var req CommandRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, CommandResponse{Error: "malformed request: " + err.Error()})
		return
	}
	start := time.Now()
	resp, status := s.execute(r.Context(), req)
	s.audit(req, resp, time.Since(start))
	writeJSON(w, status, resp)
}

// execute runs one command and never panics the request on CLI failure.
func (s *Server) execute(ctx context.Context, req CommandRequest) (CommandResponse, int) {
	parsed, err := ParseCommand(req.Text)
	if err != nil {
		return CommandResponse{Error: err.Error(), Text: ":no_entry: " + err.Error()}, http.StatusOK
	}
	switch parsed.Kind {
	case KindHelp:
		return CommandResponse{OK: true, Text: HelpText(s.Config.ClusterNames())}, http.StatusOK
	case KindClusters:
		statuses := s.Prober.ProbeAll(ctx)
		return CommandResponse{OK: true, Text: RenderClusters(statuses), Clusters: statuses}, http.StatusOK
	}

	args := append(append([]string(nil), parsed.Verb...), parsed.Args...)
	var targets []Cluster
	if parsed.Cluster != "" {
		cl, ok := s.Config.Cluster(parsed.Cluster)
		if !ok {
			msg := fmt.Sprintf("unknown cluster %q; `clusters` lists them", parsed.Cluster)
			return CommandResponse{Error: msg, Text: ":no_entry: " + msg}, http.StatusOK
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

func (s *Server) audit(req CommandRequest, resp CommandResponse, d time.Duration) {
	clusters := make([]string, 0, len(resp.Sections))
	exits := make([]int, 0, len(resp.Sections))
	for _, r := range resp.Sections {
		clusters = append(clusters, r.Cluster)
		exits = append(exits, r.ExitCode)
	}
	s.Log.Info("command",
		"actor_fbid", req.Actor.EmployeeFBID,
		"slack_user", req.Actor.SlackUserID,
		"channel", req.ChannelID,
		"text", req.Text,
		"ok", resp.OK,
		"error", resp.Error,
		"clusters", clusters,
		"exit_codes", exits,
		"duration_ms", d.Milliseconds(),
	)
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
