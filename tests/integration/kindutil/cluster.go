package kindutil

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// envKeepCluster, when set to a truthy value (see envEnabled), keeps
// the KIND cluster running across test sessions for fast iteration. Set
// this in your shell while iterating; the spec contract is "cluster
// reuse drops the per-run overhead from ~25s to <5s."
const envKeepCluster = "KIND_KEEP_CLUSTER"

// envRecreateCluster, when set to a truthy value (see envEnabled),
// forces RunKindSuite to delete and recreate any pre-existing cluster
// with the same name. Useful after a partial-state corruption or a CRD
// change that requires a fresh apiserver.
const envRecreateCluster = "KIND_RECREATE"

// envEnabled reports whether the named env var is set to a truthy
// value. Accepts "1", "true", "yes", "on" (case-insensitive) so the
// flag behaves the same whether it's set by `make test-kind`
// (KIND_KEEP_CLUSTER=true) or by hand (KIND_KEEP_CLUSTER=1). Before
// this, the code checked only == "1" while the Makefile and READMEs
// documented =true, so `make test-kind` silently never reused the
// cluster — every run paid the full ~25s recreate.
func envEnabled(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// clusterCreateTimeout caps how long `kind create cluster --wait`
// may take before we give up. KIND's own --wait defaults to 60s; we
// give it 3min to absorb cold image pulls on first-ever runs.
const clusterCreateTimeout = 3 * time.Minute

// EnsureCluster brings the named cluster into a Ready state. If the
// cluster already exists, it's reused (modulo KIND_RECREATE=1). The
// returned restConfig points at the cluster's apiserver.
//
// Failure modes are explicit:
//   - `kind` binary missing on $PATH → ErrKindMissing
//   - docker / container-runtime backend not responding → ErrDockerMissing
//   - `kind create cluster` non-zero exit → wrapped error with stderr
func EnsureCluster(ctx context.Context, name string, workers int) (*rest.Config, error) {
	if _, err := exec.LookPath("kind"); err != nil {
		return nil, fmt.Errorf("%w: install kind via "+
			"`curl -Lo ~/.local/bin/kind https://kind.sigs.k8s.io/dl/v0.31.0/kind-darwin-arm64 && chmod +x ~/.local/bin/kind`",
			ErrKindMissing)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("%w: docker CLI not on $PATH. "+
			"Install Colima (`brew install colima docker && colima start --cpu 4 --memory 8`) or another runtime per kind-README.md §5.1",
			ErrDockerMissing)
	}

	exists, err := clusterExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("EnsureCluster: list clusters: %w", err)
	}
	if exists && envEnabled(envRecreateCluster) {
		// KIND_RECREATE forces a fresh cluster even when KIND_KEEP_CLUSTER
		// is also set — an explicit recreate request wins over reuse. Use
		// the unconditional delete; DeleteCluster would no-op on
		// KIND_KEEP_CLUSTER, leaving the stale cluster in place so the
		// subsequent createCluster fails with "node(s) already exist".
		if err := deleteClusterNow(ctx, name); err != nil {
			return nil, fmt.Errorf("EnsureCluster: recreate-requested delete: %w", err)
		}
		exists = false
	}
	if !exists {
		if err := createCluster(ctx, name, workers); err != nil {
			return nil, err
		}
	}
	return kubeconfigForCluster(ctx, name)
}

// DeleteCluster tears down the named KIND cluster. No-op when the
// cluster doesn't exist. Honors KIND_KEEP_CLUSTER by skipping the
// delete entirely so iterative `go test` runs reuse the cluster.
// KIND_RECREATE callers want a fresh cluster regardless and go through
// deleteClusterNow instead.
func DeleteCluster(ctx context.Context, name string) error {
	if envEnabled(envKeepCluster) {
		return nil
	}
	return deleteClusterNow(ctx, name)
}

// deleteClusterNow deletes the named cluster unconditionally — it does
// NOT honor KIND_KEEP_CLUSTER. No-op when the cluster doesn't exist.
// Used by the KIND_RECREATE path (which must win over KIND_KEEP_CLUSTER)
// and by DeleteCluster once the keep guard has passed.
func deleteClusterNow(ctx context.Context, name string) error {
	exists, err := clusterExists(ctx, name)
	if err != nil {
		return fmt.Errorf("deleteClusterNow: list clusters: %w", err)
	}
	if !exists {
		return nil
	}
	cmd := exec.CommandContext(ctx, "kind", "delete", "cluster", "--name", name)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("deleteClusterNow: kind delete cluster --name %s: %w (stderr: %s)",
			name, err, stderr.String())
	}
	return nil
}

// clusterExists reports whether `kind get clusters` lists name.
func clusterExists(ctx context.Context, name string) (bool, error) {
	cmd := exec.CommandContext(ctx, "kind", "get", "clusters")
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return false, fmt.Errorf("kind get clusters: %w (stderr: %s)", err, stderr.String())
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if strings.TrimSpace(line) == name {
			return true, nil
		}
	}
	return false, nil
}

// createCluster runs `kind create cluster --wait` for the named
// cluster with the requested worker count. Single-node clusters
// (workers=0) skip the multi-node config-file dance for fastest
// spinup.
func createCluster(ctx context.Context, name string, workers int) error {
	args := []string{"create", "cluster", "--name", name, "--wait", "60s"}
	if workers > 0 {
		cfg, cleanup, err := writeMultiNodeConfig(workers)
		if err != nil {
			return fmt.Errorf("createCluster: write multi-node config: %w", err)
		}
		defer cleanup()
		args = append(args, "--config", cfg)
	}
	createCtx, cancel := context.WithTimeout(ctx, clusterCreateTimeout)
	defer cancel()

	cmd := exec.CommandContext(createCtx, "kind", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("createCluster: kind create cluster --name %s: %w (stderr: %s)",
			name, err, stderr.String())
	}
	return nil
}

// writeMultiNodeConfig emits a temp KIND config-file with N worker
// nodes and returns its path. cleanup deletes the temp file.
func writeMultiNodeConfig(workers int) (string, func(), error) {
	var b bytes.Buffer
	b.WriteString("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n")
	b.WriteString("- role: control-plane\n")
	for i := 0; i < workers; i++ {
		b.WriteString("- role: worker\n")
	}
	f, err := os.CreateTemp("", "kind-multinode-*.yaml")
	if err != nil {
		return "", func() {}, err
	}
	if _, err := f.Write(b.Bytes()); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", func() {}, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", func() {}, err
	}
	cleanup := func() { _ = os.Remove(f.Name()) }
	return f.Name(), cleanup, nil
}

// kubeconfigForCluster runs `kind get kubeconfig --name <cluster>`
// and parses its output into a *rest.Config the manager can drive.
func kubeconfigForCluster(ctx context.Context, name string) (*rest.Config, error) {
	cmd := exec.CommandContext(ctx, "kind", "get", "kubeconfig", "--name", name)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("kind get kubeconfig --name %s: %w (stderr: %s)",
			name, err, stderr.String())
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(out.Bytes())
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	return cfg, nil
}

// ErrKindMissing is returned by EnsureCluster when `kind` isn't on
// $PATH. Distinct error so callers can produce a targeted install
// hint.
var ErrKindMissing = errSentinel("kind binary not found on $PATH")

// ErrDockerMissing is returned by EnsureCluster when the docker CLI
// (or compatible client) isn't on $PATH. Container-runtime backends
// vary on macOS — see kind-README.md §5.1.
var ErrDockerMissing = errSentinel("docker CLI not found on $PATH")

// errSentinel is a string-typed error so package-level errors satisfy
// errors.Is via value equality without exposing a struct type.
type errSentinel string

func (e errSentinel) Error() string { return string(e) }
