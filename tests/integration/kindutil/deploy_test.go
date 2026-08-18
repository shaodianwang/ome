package kindutil

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDeployOMENoop(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		opts *DeployedOME
	}{
		{name: "nil options"},
		{name: "empty script", opts: &DeployedOME{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := deployOME(cancelled, "test-cluster", tt.opts); err != nil {
				t.Fatalf("deployOME() error = %v, want nil", err)
			}
		})
	}
}

func TestDeployOMEPassesInvocationConfiguration(t *testing.T) {
	output := filepath.Join(t.TempDir(), "invocation")
	script := writeDeployTestScript(t, `
output=${DEPLOY_TEST_OUTPUT:?}
printf '%s\n' "$@" > "${output}.args"
printf '%s\n' "${DEPLOY_TEST_VALUE:?}" > "${output}.env"
pwd > "${output}.pwd"
printf 'deployment stdout\n'
printf 'deployment stderr\n' >&2
`)

	err := deployOME(context.Background(), "test-cluster", &DeployedOME{
		Script: script,
		Args:   []string{"--flag", "two words"},
		Env: []string{
			"DEPLOY_TEST_OUTPUT=" + output,
			"DEPLOY_TEST_VALUE=expected-value",
		},
	})
	if err != nil {
		t.Fatalf("deployOME() error = %v", err)
	}

	assertFileContents(t, output+".args", "--cluster\ntest-cluster\n--flag\ntwo words\n")
	assertFileContents(t, output+".env", "expected-value\n")
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot() error = %v", err)
	}
	assertFileContents(t, output+".pwd", root+"\n")
}

func TestDeployOMENonPositiveTimeoutAddsNoDeadline(t *testing.T) {
	script := writeDeployTestScript(t, "exit 0")
	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			err := deployOME(context.Background(), "test-cluster", &DeployedOME{
				Script:  script,
				Timeout: timeout,
			})
			if err != nil {
				t.Fatalf("deployOME() error = %v, want nil", err)
			}
		})
	}
}

func TestDeployOMEMissingScript(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-deploy-script")
	err := deployOME(context.Background(), "test-cluster", &DeployedOME{Script: missing})
	if err == nil {
		t.Fatal("deployOME() error = nil, want missing-script error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deployOME() error = %v, want os.ErrNotExist", err)
	}
}

func TestDeployOMENonzeroExit(t *testing.T) {
	script := writeDeployTestScript(t, "exit 23")
	err := deployOME(context.Background(), "test-cluster", &DeployedOME{Script: script})
	if err == nil {
		t.Fatal("deployOME() error = nil, want command failure")
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("deployOME() error = %v, want *exec.ExitError", err)
	}
	if got := exitErr.ExitCode(); got != 23 {
		t.Fatalf("exit code = %d, want 23", got)
	}
}

func TestDeployOMECancelsWithCallerContext(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	pgidFile := filepath.Join(t.TempDir(), "process-group")
	script := writeDeployTestScript(t, `
	printf '%s\n' "$$" > "${DEPLOY_TEST_PGID:?}"
	(
		: > "${DEPLOY_TEST_READY:?}"
		sleep 30
	) &
	wait
	`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- deployOME(ctx, "test-cluster", &DeployedOME{
			Script: script,
			Env: []string{
				"DEPLOY_TEST_READY=" + ready,
				"DEPLOY_TEST_PGID=" + pgidFile,
			},
		})
	}()

	waitForFile(t, ready)
	pgid := readProcessID(t, pgidFile)
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("deployOME() error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deployOME() did not stop after caller context cancellation")
	}

	// The deploy shell waits for a child rather than replacing itself with it.
	// The process group disappears only when cancellation terminates both.
	waitForProcessGroupGone(t, pgid)
}

func TestDeployOMETimeout(t *testing.T) {
	script := writeDeployTestScript(t, "exec sleep 30")
	started := time.Now()
	err := deployOME(context.Background(), "test-cluster", &DeployedOME{
		Script:  script,
		Timeout: 100 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deployOME() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("deployOME() returned after %v, want prompt timeout cancellation", elapsed)
	}
}

func writeDeployTestScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "deploy.sh")
	contents := "#!/bin/sh\nset -eu\n" + body + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatalf("write test deploy script: %v", err)
	}
	return path
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("contents of %s = %q, want %q", path, got, want)
	}
}

func readProcessID(t *testing.T, path string) int {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read process ID from %s: %v", path, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	if err != nil || pid <= 0 {
		t.Fatalf("parse process ID from %s: %q: %v", path, contents, err)
	}
	return pid
}

func waitForProcessGroupGone(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("probe process group %d: %v", pgid, err)
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			t.Fatalf("process group %d still exists after deploy cancellation", pgid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(path)
		if err == nil {
			return
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat %s: %v", path, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
