//go:build meta_internal

package omebot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeTool writes a script that prints its arguments and selected env, then
// behaves per its first argument.
func fakeTool(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tool")
	script := `#!/bin/sh
echo "args: $*"
echo "token-set: ${API_TOKEN:+yes}"
echo "scratch: ${SCRATCH:-<none>}"
echo "token=abcdefgh123456"
case "$2" in
  fail) echo "tool broke" >&2; exit 4;;
  slow) sleep 30;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func testTools(t *testing.T) (*Tools, string) {
	t.Helper()
	secret := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(secret, []byte("s3cr3t-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return NewTools([]ToolConfig{{
		Name:           "ci",
		Command:        fakeTool(t),
		ArgsPrefix:     []string{"--prefix"},
		Subcommands:    []string{"status", "fail", "slow"},
		ForbiddenFlags: []string{"--download"},
		TimeoutSeconds: 1,
		EnvFromFiles:   map[string]string{"API_TOKEN": secret},
		Env:            []string{"SCRATCH=/tmp/scratch"},
	}}, nil, 4096), secret
}

func TestToolRuns(t *testing.T) {
	tools, _ := testTools(t)
	res, class := tools.Run(context.Background(), "ci", []string{"status", "123", "--job", "a b"})
	if class != "" || res.ExitCode != 0 {
		t.Fatalf("class %q res %+v", class, res)
	}
	for _, want := range []string{"args: --prefix status 123 --job a b", "token-set: yes", "scratch: /tmp/scratch", "token=<redacted>"} {
		if !strings.Contains(res.Output, want) {
			t.Errorf("output lacks %q:\n%s", want, res.Output)
		}
	}
	if strings.Contains(res.Output, "s3cr3t-value") || strings.Contains(res.Output, "abcdefgh123456") {
		t.Fatal("secret leaked")
	}
}

func TestToolRefusals(t *testing.T) {
	tools, _ := testTools(t)
	cases := []struct {
		name string
		tool string
		args []string
		want string
	}{
		{"unknown tool", "nope", []string{"status"}, ErrUnknownTool},
		{"no subcommand", "ci", nil, ErrSubcommandNotAllowed},
		{"bad subcommand", "ci", []string{"retry", "1"}, ErrSubcommandNotAllowed},
		{"forbidden flag", "ci", []string{"status", "--download"}, ErrForbiddenFlag},
		{"forbidden flag value", "ci", []string{"status", "--download=/tmp/x"}, ErrForbiddenFlag},
		{"control character", "ci", []string{"status", "a\x00b"}, ErrBadArgs},
	}
	for _, tc := range cases {
		if _, class := tools.Run(context.Background(), tc.tool, tc.args); class != tc.want {
			t.Errorf("%s: class %q, want %q", tc.name, class, tc.want)
		}
	}
}

func TestToolFailureTimeoutMissingSecret(t *testing.T) {
	tools, secret := testTools(t)
	res, class := tools.Run(context.Background(), "ci", []string{"fail"})
	if class != "" || res.ExitCode != 4 || !strings.Contains(res.Error, "tool broke") {
		t.Fatalf("fail: class %q res %+v", class, res)
	}
	res, class = tools.Run(context.Background(), "ci", []string{"slow"})
	if class != ErrTimeout || res.ExitCode != -1 {
		t.Fatalf("slow: class %q res %+v", class, res)
	}
	if err := os.Remove(secret); err != nil {
		t.Fatal(err)
	}
	res, class = tools.Run(context.Background(), "ci", []string{"status"})
	if class != "" || res.ExitCode != -1 || !strings.Contains(res.Error, "API_TOKEN") {
		t.Fatalf("missing secret: class %q res %+v", class, res)
	}
}
