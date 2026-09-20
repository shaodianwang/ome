package kindutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestIsRetriableCreateFailure(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		stderr string
		want   bool
	}{
		{
			name: "no error is never retried",
		},
		{
			// The per-attempt context expiring shows up as a killed
			// child process, not as the context error.
			name: "per-attempt timeout kills the child",
			err:  errors.New("createCluster: kind create cluster --name ome-coord-kind: signal: killed (stderr: )"),
			want: true,
		},
		{
			name: "context deadline surfaces directly",
			err:  fmt.Errorf("createCluster: %w", context.DeadlineExceeded),
			want: true,
		},
		{
			name: "context deadline reported only as text",
			err:  errors.New("createCluster: context deadline exceeded"),
			want: true,
		},
		{
			name: "kind's own node-boot deadline",
			err:  errors.New("createCluster: kind create cluster --name ome-coord-kind: exit status 1"),
			stderr: `ERROR: failed to create cluster: could not find a log line that ` +
				`matches "Reached target .*Multi-User System.*|detected cgroup v1"`,
			want: true,
		},
		{
			name:   "leftover cluster is deterministic",
			err:    errors.New("createCluster: kind create cluster --name ome-coord-kind: exit status 1"),
			stderr: "ERROR: failed to create cluster: node(s) already exist for a cluster with the name \"ome-coord-kind\"",
		},
		{
			name:   "unreachable node image is deterministic",
			err:    errors.New("createCluster: kind create cluster --name ome-coord-kind: exit status 1"),
			stderr: "ERROR: failed to pull image: no such host",
		},
		{
			name: "malformed config is deterministic",
			err:  errors.New("createCluster: write cluster config: permission denied"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetriableCreateFailure(tc.err, tc.stderr); got != tc.want {
				t.Fatalf("isRetriableCreateFailure() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClusterCreateTimeout(t *testing.T) {
	tests := []struct {
		name string
		set  bool
		raw  string
		want time.Duration
	}{
		{name: "unset uses the default", want: defaultClusterCreateTimeout},
		{name: "empty uses the default", set: true, want: defaultClusterCreateTimeout},
		{name: "whitespace uses the default", set: true, raw: "   ", want: defaultClusterCreateTimeout},
		{name: "minutes", set: true, raw: "12m", want: 12 * time.Minute},
		{name: "seconds", set: true, raw: "90s", want: 90 * time.Second},
		{name: "surrounding whitespace is trimmed", set: true, raw: " 4m ", want: 4 * time.Minute},
		{name: "unparsable falls back", set: true, raw: "soon", want: defaultClusterCreateTimeout},
		{name: "bare integer falls back", set: true, raw: "300", want: defaultClusterCreateTimeout},
		{name: "zero falls back", set: true, raw: "0s", want: defaultClusterCreateTimeout},
		{name: "negative falls back", set: true, raw: "-5m", want: defaultClusterCreateTimeout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// t.Setenv unconditionally so the ambient value is
			// restored either way; then clear it for the unset case.
			t.Setenv(envClusterCreateTimeout, tc.raw)
			if !tc.set {
				if err := os.Unsetenv(envClusterCreateTimeout); err != nil {
					t.Fatalf("unset %s: %v", envClusterCreateTimeout, err)
				}
			}
			if got := clusterCreateTimeout(); got != tc.want {
				t.Fatalf("clusterCreateTimeout() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The default has to clear the worst observed cluster-create wall time
// on a contended container runtime, where `Preparing nodes` alone can
// run close to three minutes.
func TestDefaultClusterCreateTimeoutAbsorbsAStarvedRuntime(t *testing.T) {
	if defaultClusterCreateTimeout < 5*time.Minute {
		t.Fatalf("defaultClusterCreateTimeout = %v, too tight for a contended runtime",
			defaultClusterCreateTimeout)
	}
}
