//go:build meta_internal

package omebot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeCLI writes a shell script that echoes its arguments and KUBECONFIG,
// then behaves per the FAKE_MODE environment variable.
func fakeCLI(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubectl-ome")
	script := `#!/bin/sh
echo "args: $*"
echo "kubeconfig: ${KUBECONFIG:-<none>}"
echo "Authorization: Bearer abcdefghijklmnop"
case "${FAKE_MODE:-}" in
  fail) echo "boom" >&2; exit 3;;
  big) i=0; while [ $i -lt 2000 ]; do echo "line $i"; i=$((i+1)); done;;
  slow) sleep 5;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunnerRuns(t *testing.T) {
	r := &Runner{Bin: fakeCLI(t), Timeout: 5 * time.Second, Limit: 4096}
	res := r.Run(context.Background(), "member-a", "/tmp/member-a.yaml", []string{"get", "isvc", "-A"})
	if res.ExitCode != 0 || res.Error != "" {
		t.Fatalf("unexpected failure: %+v", res)
	}
	if !strings.Contains(res.Output, "args: get isvc -A") || !strings.Contains(res.Output, "kubeconfig: /tmp/member-a.yaml") {
		t.Fatalf("output = %q", res.Output)
	}
	if strings.Contains(res.Output, "abcdefghijklmnop") {
		t.Fatal("bearer token leaked through redaction")
	}
}

func TestRunnerFailureTruncationTimeout(t *testing.T) {
	r := &Runner{Bin: fakeCLI(t), Timeout: 5 * time.Second, Limit: 512}

	r.Env = []string{"FAKE_MODE=fail"}
	if res := r.Run(context.Background(), "c", "", nil); res.ExitCode != 3 || !strings.Contains(res.Error, "boom") {
		t.Fatalf("fail mode: %+v", res)
	}

	r.Env = []string{"FAKE_MODE=big"}
	if res := r.Run(context.Background(), "c", "", nil); !res.Truncated || len(res.Output) > 512 {
		t.Fatalf("big mode: truncated=%v len=%d", res.Truncated, len(res.Output))
	}

	r.Env = []string{"FAKE_MODE=slow"}
	r.Timeout = 300 * time.Millisecond
	if res := r.Run(context.Background(), "c", "", nil); res.ExitCode != -1 || !strings.Contains(res.Error, "timed out") {
		t.Fatalf("slow mode: %+v", res)
	}
}
