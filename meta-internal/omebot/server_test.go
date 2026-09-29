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
	if resp.OK || !strings.Contains(resp.Error, "unknown cluster") {
		t.Fatalf("unknown cluster: %+v", resp)
	}
	_, resp = post(t, h, `{"text":"rollout pause chat --cluster hub-a"}`, false)
	if resp.OK || !strings.Contains(resp.Error, "not a read-only") {
		t.Fatalf("guarded verb: %+v", resp)
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
