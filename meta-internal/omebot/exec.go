//go:build meta_internal

package omebot

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// Runner executes kubectl-ome with a bounded runtime and bounded output.
type Runner struct {
	Bin     string
	Timeout time.Duration
	Limit   int
	// Env is appended to the inherited environment; KUBECONFIG is set per run.
	Env []string
}

// Result is one CLI invocation against one cluster.
type Result struct {
	Cluster   string        `json:"cluster"`
	ExitCode  int           `json:"exitCode"`
	Output    string        `json:"output"`
	Truncated bool          `json:"truncated"`
	Error     string        `json:"error,omitempty"`
	Duration  time.Duration `json:"-"`
}

var secretRE = regexp.MustCompile(`(?i)(bearer\s+|token[=:]\s*|authorization[=:]\s*)[A-Za-z0-9._~+/=-]{8,}`)

// Redact masks anything shaped like a credential before output leaves the
// process.
func Redact(s string) string {
	return secretRE.ReplaceAllString(s, "${1}<redacted>")
}

// Run invokes the CLI for one cluster. An empty kubeconfig path leaves the
// CLI on its in-cluster credentials.
func (r *Runner) Run(ctx context.Context, cluster, kubeconfig string, args []string) Result {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, r.Bin, args...)
	env := append([]string(nil), os.Environ()...)
	env = append(env, r.Env...)
	if kubeconfig != "" {
		env = append(env, "KUBECONFIG="+kubeconfig)
	}
	cmd.Env = env
	stdout := &cappedBuffer{limit: r.Limit}
	stderr := &cappedBuffer{limit: r.Limit}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	res := Result{Cluster: cluster}
	err := cmd.Run()
	res.Duration = time.Since(start)
	res.Output = Redact(stdout.String())
	res.Truncated = stdout.truncated || stderr.truncated
	if err != nil {
		var exitErr *exec.ExitError
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			res.ExitCode = -1
			res.Error = "timed out after " + r.Timeout.String()
		case errors.As(err, &exitErr):
			res.ExitCode = exitErr.ExitCode()
			res.Error = strings.TrimSpace(Redact(stderr.String()))
		default:
			res.ExitCode = -1
			res.Error = Redact(err.Error())
		}
	}
	return res
}

// cappedBuffer keeps the first limit bytes and remembers that more arrived.
type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	room := c.limit - c.buf.Len()
	if room <= 0 {
		c.truncated = true
		return len(p), nil
	}
	if len(p) > room {
		c.truncated = true
		c.buf.Write(p[:room])
		return len(p), nil
	}
	return c.buf.Write(p)
}

func (c *cappedBuffer) String() string { return c.buf.String() }
