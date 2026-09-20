package kindutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

// envNodeImage, when non-empty, is passed to `kind create cluster
// --image` so the node image can be pre-pulled by CI (keeping the
// pre-pull and the booted cluster in lockstep) or redirected to a
// registry mirror when Docker Hub isn't reachable from the runner.
// Unset means kind's bundled default node image for the installed
// kind release.
const envNodeImage = "KIND_NODE_IMAGE"

// envEnabled reports whether the named env var is set to a truthy
// value. Accepts "1", "true", "yes", "on" (case-insensitive) so the
// flag behaves the same whether it's set by `make test-kind`
// (KIND_KEEP_CLUSTER=true) or by hand (KIND_KEEP_CLUSTER=1).
func envEnabled(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// envClusterCreateTimeout overrides how long `kind create cluster
// --wait` may take before the attempt is abandoned. Parsed with
// time.ParseDuration ("10m", "90s"); an empty, unparsable, or
// non-positive value falls back to defaultClusterCreateTimeout.
const envClusterCreateTimeout = "KIND_CLUSTER_CREATE_TIMEOUT"

// defaultClusterCreateTimeout caps a single `kind create cluster
// --wait` attempt. A default is acceptable here because this is test
// scaffolding, not product behavior: no binary shipped to a user reads
// it, and a suite that cannot name its own patience has no cluster.
//
// Sized for the slowest environment rather than the common one. An
// uncontended runner boots three node containers in under 10s and
// finishes the whole create in ~35s, so a generous cap costs nothing
// there; a contended container runtime spends 2-3min in `Preparing
// nodes` alone, and killing it at the 3min mark destroys a cluster
// that was seconds from Ready. Override with KIND_CLUSTER_CREATE_TIMEOUT.
const defaultClusterCreateTimeout = 8 * time.Minute

// clusterCreateTimeout resolves the per-attempt cluster-create cap from
// envClusterCreateTimeout, falling back to defaultClusterCreateTimeout.
func clusterCreateTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envClusterCreateTimeout))
	if raw == "" {
		return defaultClusterCreateTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		fmt.Printf("createCluster: ignoring %s=%q (%v), using %s\n",
			envClusterCreateTimeout, raw, err, defaultClusterCreateTimeout)
		return defaultClusterCreateTimeout
	}
	return d
}

// KindExtraMount mirrors one entry of a KIND node's extraMounts: a host
// directory bind-mounted into a node container at the same (or a
// caller-chosen) path, applied identically to every node in the cluster.
// Needed by any fixture that assumes a real, node-independent filesystem —
// a static hostPath PersistentVolume, for instance — since without it
// kubelet's own DirectoryOrCreate handling only ever sees the individual
// node container's own throwaway filesystem.
type KindExtraMount struct {
	HostPath      string
	ContainerPath string
}

// EnsureCluster brings the named cluster into a Ready state. If the
// cluster already exists, it's reused (modulo KIND_RECREATE=1). The
// returned restConfig points at the cluster's apiserver.
//
// extraMounts, if given, is applied to every node (control-plane and
// workers alike) so a bind-mounted host path is available regardless of
// which node a pod lands on. Ignored when reusing an existing cluster —
// only cluster creation can set extraMounts.
//
// Failure modes are explicit:
//   - `kind` binary missing on $PATH → ErrKindMissing
//   - docker / container-runtime backend not responding → ErrDockerMissing
//   - `kind create cluster` non-zero exit → wrapped error with stderr
func EnsureCluster(ctx context.Context, name string, workers int, extraMounts ...KindExtraMount) (*rest.Config, error) {
	if _, err := exec.LookPath("kind"); err != nil {
		return nil, fmt.Errorf("%w: install kind via "+
			"`curl -Lo ~/.local/bin/kind https://kind.sigs.k8s.io/dl/v0.31.0/kind-darwin-arm64 && chmod +x ~/.local/bin/kind`",
			ErrKindMissing)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("%w: docker CLI not on $PATH. "+
			"Install Colima (`brew install colima docker && colima start --cpu 4 --memory 8`) or another Docker-compatible runtime",
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
		if err := createCluster(ctx, name, workers, extraMounts); err != nil {
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

// createCluster brings up the named cluster, retrying at most once when
// the first attempt fails the way a starved container runtime fails
// (see isRetriableCreateFailure). The partial cluster is deleted before
// the second attempt, because kind refuses to create over the leftover
// node containers with "node(s) already exist".
//
// Exactly one retry, and only for those signatures: any other create
// failure — a bad config, an unreachable node image, a name collision —
// is deterministic, and retrying it only doubles the time to the same
// red.
func createCluster(ctx context.Context, name string, workers int, extraMounts []KindExtraMount) error {
	const attempts = 2
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		var stderr string
		stderr, err = createClusterOnce(ctx, name, workers, extraMounts)
		if err == nil {
			if attempt > 1 {
				fmt.Printf("createCluster: attempt %d/%d for %q succeeded\n", attempt, attempts, name)
			}
			return nil
		}
		if attempt == attempts || !isRetriableCreateFailure(err, stderr) {
			return err
		}
		fmt.Printf("createCluster: attempt %d/%d for %q failed with a starved-runtime signature, "+
			"deleting the partial cluster and retrying once: %v\n", attempt, attempts, name, err)
		if delErr := deleteClusterNow(ctx, name); delErr != nil {
			return fmt.Errorf("%w (cleanup before retry also failed: %v)", err, delErr)
		}
	}
	return err
}

// isRetriableCreateFailure reports whether a `kind create cluster`
// failure is the resource-starvation kind that a second attempt can
// plausibly clear. Two signatures, both observed on a contended CI
// container runtime and neither implying anything about the code under
// test:
//
//   - the attempt hit its own deadline and the process was killed
//     mid-boot, typically seconds from a Ready control plane;
//   - kind's internal node-boot deadline expired first, which it
//     reports as not finding the systemd "Reached target ... Multi-User
//     System" log line.
//
// Pure over the error and the captured stderr so the decision is
// testable without a container runtime.
func isRetriableCreateFailure(err error, stderr string) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// exec kills the child when the per-attempt context expires, which
	// surfaces as an ExitError carrying the signal rather than as the
	// context error itself.
	haystack := err.Error() + "\n" + stderr
	for _, signature := range []string{
		"signal: killed",
		"context deadline exceeded",
		"could not find a log line that matches",
	} {
		if strings.Contains(haystack, signature) {
			return true
		}
	}
	return false
}

// createClusterOnce runs a single `kind create cluster --wait` for the
// named cluster with the requested worker count and extraMounts, and
// returns kind's stderr alongside any error so the caller can classify
// the failure. A single-node cluster with no extraMounts skips the
// config-file dance entirely for fastest spinup; either a worker count
// or extraMounts forces a config file since both require settings
// `kind create cluster`'s flags cannot express.
func createClusterOnce(ctx context.Context, name string, workers int, extraMounts []KindExtraMount) (string, error) {
	args := []string{"create", "cluster", "--name", name, "--wait", "60s"}
	if img := strings.TrimSpace(os.Getenv(envNodeImage)); img != "" {
		args = append(args, "--image", img)
	}
	if workers > 0 || len(extraMounts) > 0 {
		cfg, content, cleanup, err := writeClusterConfig(workers, extraMounts)
		if err != nil {
			return "", fmt.Errorf("createCluster: write cluster config: %w", err)
		}
		defer cleanup()
		args = append(args, "--config", cfg)
		// Printed unconditionally (not just on error): whether a --config
		// was even passed, and what it contained, is exactly what the
		// caller cannot otherwise see once `kind create cluster` succeeds.
		fmt.Printf("createCluster: %s --config %s:\n%s", strings.Join(args, " "), cfg, content)
	}
	createCtx, cancel := context.WithTimeout(ctx, clusterCreateTimeout())
	defer cancel()

	cmd := exec.CommandContext(createCtx, "kind", args...)
	var stderr bytes.Buffer
	cmd.Stdout = os.Stdout
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	if err := cmd.Run(); err != nil {
		return stderr.String(), fmt.Errorf("createCluster: kind create cluster --name %s: %w (stderr: %s)",
			name, err, stderr.String())
	}
	return stderr.String(), nil
}

// writeClusterConfig emits a temp KIND config-file with N worker nodes and
// returns its path and content. extraMounts, when non-empty, is repeated
// identically on every node (control-plane and workers) — a static
// hostPath PersistentVolume needs the same content regardless of which
// node a pod mounting it lands on. cleanup deletes the temp file.
func writeClusterConfig(workers int, extraMounts []KindExtraMount) (string, string, func(), error) {
	var b bytes.Buffer
	b.WriteString("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n")
	writeNode := func(role string) {
		b.WriteString("- role: " + role + "\n")
		if len(extraMounts) == 0 {
			return
		}
		b.WriteString("  extraMounts:\n")
		for _, m := range extraMounts {
			b.WriteString("  - hostPath: " + m.HostPath + "\n")
			b.WriteString("    containerPath: " + m.ContainerPath + "\n")
		}
	}
	writeNode("control-plane")
	for i := 0; i < workers; i++ {
		writeNode("worker")
	}
	content := b.String()
	f, err := os.CreateTemp("", "kind-cluster-config-*.yaml")
	if err != nil {
		return "", "", func() {}, err
	}
	if _, err := f.Write(b.Bytes()); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", "", func() {}, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", "", func() {}, err
	}
	cleanup := func() { _ = os.Remove(f.Name()) }
	return f.Name(), content, cleanup, nil
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
// vary on macOS.
var ErrDockerMissing = errSentinel("docker CLI not found on $PATH")

// errSentinel is a string-typed error so package-level errors satisfy
// errors.Is via value equality without exposing a struct type.
type errSentinel string

func (e errSentinel) Error() string { return string(e) }
