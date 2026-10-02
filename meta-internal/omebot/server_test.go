//go:build meta_internal

package omebot

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testServer(t *testing.T, mode string) *Server {
	t.Helper()
	cfg, err := Parse([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Clusters = cfg.Clusters[:2] // drop the in-cluster entry; no ServiceAccount in tests
	paths := map[string]string{"member-a": "/tmp/member-a.yaml", "hub-a": "/etc/kubeconfigs/hub-a"}
	return &Server{
		Config:   cfg,
		Paths:    paths,
		Runner:   &Runner{Bin: fakeCLI(t), Timeout: 5 * time.Second, Limit: 4096},
		Prober:   &Prober{Config: cfg, Paths: paths, Timeout: time.Second},
		Verifier: &Verifier{Mode: mode, Secret: []byte("k"), MaxSkew: time.Minute},
		Log:      slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	}
}

func post(t *testing.T, h http.Handler, body string, signed bool) (*httptest.ResponseRecorder, CommandResponse) {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/command", strings.NewReader(body))
	if signed {
		ts := time.Now().Unix()
		req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
		req.Header.Set(HeaderSignature, Sign([]byte("k"), ts, []byte(body)))
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var resp CommandResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec, resp
}

func TestServerHelpAndAuth(t *testing.T) {
	h := testServer(t, AuthModeHMAC).Handler()
	if rec, _ := post(t, h, `{"text":"help"}`, false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned request: code %d", rec.Code)
	}
	rec, resp := post(t, h, `{"text":"help"}`, true)
	if rec.Code != 200 || !resp.OK || !strings.Contains(resp.Text, "installed clusters") {
		t.Fatalf("help: code %d resp %+v", rec.Code, resp)
	}
}

func TestServerFanoutAndSingleCluster(t *testing.T) {
	h := testServer(t, AuthModeNone).Handler()
	_, resp := post(t, h, `{"actor":{"employeeFbid":1},"text":"get isvc -A"}`, false)
	if !resp.OK || len(resp.Sections) != 2 {
		t.Fatalf("fanout: %+v", resp)
	}
	if !strings.Contains(resp.Text, "*member-a*") || !strings.Contains(resp.Text, "*hub-a*") {
		t.Fatalf("fanout text: %s", resp.Text)
	}
	_, resp = post(t, h, `{"text":"rollout status chat --cluster hub-a -n team-a"}`, false)
	if !resp.OK || len(resp.Sections) != 1 || resp.Sections[0].Cluster != "hub-a" {
		t.Fatalf("single: %+v", resp)
	}
	if !strings.Contains(resp.Sections[0].Output, "args: rollout status chat -n team-a") {
		t.Fatalf("args not forwarded: %q", resp.Sections[0].Output)
	}
	_, resp = post(t, h, `{"text":"get isvc --cluster nope"}`, false)
	if resp.OK || !strings.Contains(resp.Error, "unknown cluster") || resp.ErrorClass != ErrUnknownCluster {
		t.Fatalf("unknown cluster: %+v", resp)
	}
	_, resp = post(t, h, `{"actor":{"canMutate":true},"text":"rollout pause chat --cluster hub-a --yes"}`, false)
	if resp.OK || !strings.Contains(resp.Error, "switched off") || resp.ErrorClass != ErrMutationsOff {
		t.Fatalf("guarded verb with mutations off: %+v", resp)
	}
}

func TestServerGuardedVerbs(t *testing.T) {
	s, logs := toolServer(t)
	s.Config.Mutations.Enabled = true
	s.Config.Clusters[0].Writable = true // member-a
	h := s.Handler()

	for body, want := range map[string]string{
		`{"actor":{"employeeFbid":7,"canMutate":true},"text":"rollout pause chat --cluster hub-a --yes"}`:   ErrClusterNotWritable,
		`{"actor":{"employeeFbid":7,"canMutate":true},"text":"rollout pause chat --cluster nope --yes"}`:    ErrUnknownCluster,
		`{"actor":{"employeeFbid":7},"text":"rollout pause chat --cluster member-a --yes"}`:                 ErrActorNotAllowed,
		`{"actor":{"employeeFbid":7,"canMutate":true},"text":"rollout pause chat --cluster member-a"}`:      ErrNeedsConfirmation,
		`{"actor":{"employeeFbid":7,"canMutate":true},"text":"get isvc --cluster member-a --yes"}`:          ErrForbiddenFlag,
		`{"actor":{"employeeFbid":7,"canMutate":true},"text":"scale chat --component engine --replicas 2"}`: ErrNeedsCluster,
	} {
		rec, resp := post(t, h, body, true)
		if rec.Code != 200 || resp.OK || resp.ErrorClass != want || resp.Mutation {
			t.Errorf("%s: code %d resp %+v", body, rec.Code, resp)
		}
		if len(resp.Sections) != 0 {
			t.Errorf("%s: a refused guarded verb ran something: %+v", body, resp.Sections)
		}
	}

	rec, resp := post(t, h, `{"requestId":"r-m","actor":{"employeeFbid":7,"slackUserId":"U1","canMutate":true},"text":"scale chat --component engine --replicas 2 --cluster member-a --yes"}`, true)
	if rec.Code != 200 || !resp.OK || !resp.Mutation || len(resp.Sections) != 1 || resp.Sections[0].Cluster != "member-a" {
		t.Fatalf("guarded verb: code %d resp %+v", rec.Code, resp)
	}
	if !strings.Contains(resp.Sections[0].Output, "args: scale chat --component engine --replicas 2 --yes") {
		t.Fatalf("args not forwarded: %q", resp.Sections[0].Output)
	}
	if !strings.HasPrefix(resp.Text, ":warning: guarded action on member-a") {
		t.Errorf("text: %s", resp.Text)
	}
	audit := logs.String()
	for _, want := range []string{`"kind":"command"`, `"request_id":"r-m"`, `"mutation":true`, `"actor_fbid":7`} {
		if !strings.Contains(audit, want) {
			t.Errorf("audit lacks %s: %s", want, audit)
		}
	}

	_, resp = post(t, h, `{"text":"help"}`, true)
	if !strings.Contains(resp.Text, "run only on member-a") {
		t.Errorf("help does not name the writable cluster: %s", resp.Text)
	}
}

func TestServerQuestion(t *testing.T) {
	s, logs := toolServer(t)
	h := s.Handler()
	rec, resp := post(t, h, `{"requestId":"r-q","surface":"mention","actor":{"employeeFbid":7},"text":"ask what's wrong with chat?"}`, true)
	if rec.Code != 200 || resp.OK || resp.ErrorClass != ErrQuestion {
		t.Fatalf("question: code %d resp %+v", rec.Code, resp)
	}
	if resp.Question != "what's wrong with chat?" || resp.QuestionMode != "ask" || !strings.Contains(resp.Text, "question") {
		t.Errorf("question fields: %+v", resp)
	}
	audit := logs.String()
	for _, want := range []string{`"kind":"command"`, `"request_id":"r-q"`, `"surface":"mention"`, `"error_class":"question"`} {
		if !strings.Contains(audit, want) {
			t.Errorf("audit lacks %s: %s", want, audit)
		}
	}
	if strings.Contains(audit, "wrong with chat") {
		t.Error("audit must not carry the question")
	}

	s.slots = make(chan struct{}, 1)
	s.slots <- struct{}{}
	if rec, _ := post(t, h, `{"text":"why is chat slow?"}`, true); rec.Code != http.StatusOK {
		t.Errorf("a question needs no slot: %d", rec.Code)
	}
}

func TestRenderClusters(t *testing.T) {
	text := RenderClusters([]ClusterStatus{
		{Name: "member-a", Provider: "gke", Role: "member", Reachable: true, K8sVersion: "v1.33.1", OMEInstalled: true, OMEVersion: "abc1234", KargoStage: "stage-1"},
		{Name: "hub-a", Provider: "mks", Error: "dial tcp: timeout"},
	})
	for _, needle := range []string{"member-a", "abc1234", "kargo:stage-1", "hub-a", "dial tcp: timeout"} {
		if !strings.Contains(text, needle) {
			t.Errorf("render lacks %q:\n%s", needle, text)
		}
	}
}

func signedRequest(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	ts := time.Now().Unix()
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, Sign([]byte("k"), ts, []byte(body)))
	return req
}

func toolServer(t *testing.T) (*Server, *bytes.Buffer) {
	t.Helper()
	s := testServer(t, AuthModeHMAC)
	tools, _ := testTools(t)
	s.Tools = tools
	logs := &bytes.Buffer{}
	s.Log = slog.New(slog.NewJSONHandler(logs, nil))
	return s, logs
}

func postTool(t *testing.T, h http.Handler, body string) (*httptest.ResponseRecorder, ToolResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, "POST", "/v1/tool", body))
	var resp ToolResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec, resp
}

func TestServerTool(t *testing.T) {
	s, logs := toolServer(t)
	h := s.Handler()
	rec, resp := postTool(t, h, `{"requestId":"r-1","actor":{"employeeFbid":7},"tool":"ci","args":["status","42"]}`)
	if rec.Code != 200 || !resp.OK || resp.Result == nil || !strings.Contains(resp.Result.Output, "args: --prefix status 42") {
		t.Fatalf("tool: code %d resp %+v", rec.Code, resp)
	}
	if !strings.Contains(resp.Text, "status 42") {
		t.Errorf("text: %s", resp.Text)
	}
	audit := logs.String()
	for _, want := range []string{`"kind":"tool"`, `"request_id":"r-1"`, `"tool":"ci"`, `"actor_fbid":7`} {
		if !strings.Contains(audit, want) {
			t.Errorf("audit lacks %s: %s", want, audit)
		}
	}
	if strings.Contains(audit, "scratch: /tmp/scratch") {
		t.Error("audit must not carry tool output")
	}

	for body, want := range map[string]string{
		`{"tool":"nope","args":["x"]}`:                 ErrUnknownTool,
		`{"tool":"ci","args":["retry"]}`:               ErrSubcommandNotAllowed,
		`{"tool":"ci","args":["status","--download"]}`: ErrForbiddenFlag,
	} {
		rec, resp := postTool(t, h, body)
		if rec.Code != 200 || resp.OK || resp.ErrorClass != want {
			t.Errorf("%s: code %d resp %+v", body, rec.Code, resp)
		}
	}
	rec, resp = postTool(t, h, `{"tool":"ci","args":["fail"]}`)
	if rec.Code != 200 || resp.OK || resp.ErrorClass != "" || resp.Result.ExitCode != 4 {
		t.Errorf("failing tool: code %d resp %+v", rec.Code, resp)
	}

	unsigned := httptest.NewRequest("POST", "/v1/tool", strings.NewReader(`{"tool":"ci","args":["status"]}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, unsigned)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unsigned tool: %d", rec.Code)
	}
}

func TestServerRelease(t *testing.T) {
	s, logs := toolServer(t)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, signedRequest(t, "GET", "/v1/release", ""))
	var resp ReleaseResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if rec.Code != http.StatusNotFound || resp.ErrorClass != ErrNotConfigured {
		t.Fatalf("unconfigured: code %d resp %+v", rec.Code, resp)
	}

	s.Release = fakeRelease(t)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, signedRequest(t, "GET", "/v1/release", ""))
	resp = ReleaseResponse{}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if rec.Code != 200 || !resp.OK || resp.Release == nil || !strings.Contains(resp.Text, "stage-rc") {
		t.Fatalf("release: code %d resp %+v", rec.Code, resp)
	}
	if !strings.Contains(logs.String(), `"kind":"release"`) {
		t.Errorf("audit lacks release kind: %s", logs.String())
	}
}

func TestServerBusyAndBodyCap(t *testing.T) {
	s, _ := toolServer(t)
	s.slots = make(chan struct{}, 1)
	s.slots <- struct{}{}
	h := s.Handler()

	rec, resp := postTool(t, h, `{"tool":"ci","args":["status"]}`)
	if rec.Code != http.StatusTooManyRequests || resp.ErrorClass != ErrBusy {
		t.Errorf("tool busy: code %d resp %+v", rec.Code, resp)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, "POST", "/v1/command", `{"text":"get isvc -A"}`))
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), ErrBusy) {
		t.Errorf("command busy: code %d body %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, "GET", "/v1/release", ""))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unconfigured release is refused before taking a slot: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, "POST", "/v1/command", `{"text":"help"}`))
	if rec.Code != http.StatusOK {
		t.Errorf("help needs no slot: %d", rec.Code)
	}

	s.Config.MaxRequestBytes = 16
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, "POST", "/v1/tool", `{"tool":"ci","args":["status","long"]}`))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("body cap: %d", rec.Code)
	}
}
