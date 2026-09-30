package kindutil

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// SchedulerInstall puts the gang scheduler into the suite's KIND cluster so
// the fixtures' pods are placed by it instead of kube-scheduler. The chart is
// installed with small test-cluster values (one replica, no PodDisruptionBudget,
// small requests); the PodGroup CRD it consumes is added
// to the suite's CRD paths by RunKindSuite.
type SchedulerInstall struct {
	// Name is the scheduler profile pods select through spec.schedulerName.
	Name string
	// Image is the local docker tag to load into the cluster, `repository:tag`
	// with no registry. When docker does not have it, it is built from the
	// scheduler module the way deploy-ome-kind.sh builds it offline: compile,
	// then package with dockerfiles/kind-prebuilt.Dockerfile.
	Image string
	// ChartPath is the repo-relative chart directory; empty means
	// charts/ome-scheduler.
	ChartPath string
	// Namespace receives the release; empty means "ome".
	Namespace string
	// TopologyKey is the node label the scheduler pins gangs to. The gang
	// scheduler treats a gang without a topology domain as unresolvable, and
	// KIND nodes carry no domain label, so every node is labeled with this key
	// and the chart gets it as the plugin's fallback. Empty means
	// topology.kubernetes.io/zone.
	TopologyKey string
	// TopologyDomain is the single domain value stamped on every node; empty
	// means "ome-kind".
	TopologyDomain string
	// MinWorkers raises the suite's worker count: the gang scheduler places one
	// gang member per node, so gang specs need as many workers as their largest
	// gang plus its surge. Zero means 6.
	MinWorkers int
}

const schedulerCRDPath = "tests/integration/dep-crds/scheduler-plugins"

func (s *SchedulerInstall) chartPath() string {
	if s.ChartPath != "" {
		return s.ChartPath
	}
	return "charts/ome-scheduler"
}

func (s *SchedulerInstall) namespace() string {
	if s.Namespace != "" {
		return s.Namespace
	}
	return "ome"
}

func (s *SchedulerInstall) topologyKey() string {
	if s.TopologyKey != "" {
		return s.TopologyKey
	}
	return "topology.kubernetes.io/zone"
}

// WorkerCount is the minimum number of worker nodes the scheduler fixtures need.
func (s *SchedulerInstall) WorkerCount() int {
	if s.MinWorkers > 0 {
		return s.MinWorkers
	}
	return 6
}

func (s *SchedulerInstall) topologyDomain() string {
	if s.TopologyDomain != "" {
		return s.TopologyDomain
	}
	return "ome-kind"
}

// withSchedulerCRDs returns paths plus the scheduler-plugins CRD directory
// when a scheduler is requested and the directory is not already listed.
func withSchedulerCRDs(paths []string, s *SchedulerInstall) []string {
	if s == nil {
		return paths
	}
	for _, p := range paths {
		if strings.TrimSuffix(p, "/") == schedulerCRDPath {
			return paths
		}
	}
	return append(append([]string{}, paths...), schedulerCRDPath)
}

// InstallScheduler loads the scheduler image into clusterName and installs
// the chart under the given profile name, waiting for the Deployment.
func InstallScheduler(ctx context.Context, clusterName string, s *SchedulerInstall) error {
	if s == nil {
		return nil
	}
	if s.Name == "" || s.Image == "" {
		return fmt.Errorf("installScheduler: Name and Image are required (got %q, %q)", s.Name, s.Image)
	}
	repository, tag, ok := strings.Cut(s.Image, ":")
	if !ok || strings.Contains(tag, "/") {
		return fmt.Errorf("installScheduler: Image must be repository:tag, got %q", s.Image)
	}
	if err := ensureSchedulerImage(ctx, s.Image); err != nil {
		return err
	}
	if err := kindLoad(ctx, clusterName, []string{s.Image}); err != nil {
		return fmt.Errorf("installScheduler: %w", err)
	}
	root, err := RepoRoot()
	if err != nil {
		return err
	}
	kubeconfig, err := writeClusterKubeconfig(ctx, clusterName)
	if err != nil {
		return err
	}
	defer os.Remove(kubeconfig)
	// The first API calls into a freshly created cluster can hit an apiserver
	// or etcd that is still settling; that is bring-up, not a suite result.
	if err := retryTransient(ctx, 24, func() error {
		label := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "label", "nodes", "--all", "--overwrite",
			s.topologyKey()+"="+s.topologyDomain())
		var labelErr bytes.Buffer
		label.Stderr = &labelErr
		if err := label.Run(); err != nil {
			return fmt.Errorf("installScheduler: label nodes with the topology domain: %w (stderr: %s)", err, labelErr.String())
		}
		return nil
	}); err != nil {
		return err
	}
	args := []string{
		"upgrade", "--install", "ome-scheduler", filepath.Join(root, s.chartPath()),
		"--kubeconfig", kubeconfig,
		"--namespace", s.namespace(), "--create-namespace",
		"--set", "global.hub=",
		"--set", "scheduler.name=" + s.Name,
		"--set", "scheduler.plugin.topologyKey=" + s.topologyKey(),
		"--set", "scheduler.image.repository=" + repository,
		"--set", "scheduler.image.tag=" + tag,
		"--set", "scheduler.replicaCount=1",
		"--set", "scheduler.podDisruptionBudget.enabled=false",
		"--set", "scheduler.resources.requests.cpu=100m",
		"--set", "scheduler.resources.requests.memory=256Mi",
		"--set", "scheduler.resources.limits.cpu=1",
		"--set", "scheduler.resources.limits.memory=1Gi",
		"--wait", "--timeout", "300s",
	}
	// Idempotent, so a second attempt after an apiserver hiccup is safe.
	return retryTransient(ctx, 2, func() error {
		cmd := exec.CommandContext(ctx, "helm", args...)
		cmd.Dir = root
		cmd.Env = os.Environ()
		var stderr bytes.Buffer
		cmd.Stdout = os.Stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("installScheduler: helm upgrade --install ome-scheduler: %w (stderr: %s)", err, stderr.String())
		}
		return nil
	})
}

// retryTransient runs fn up to attempts times, pausing between attempts, and
// returns the last error. Bring-up commands against a cluster that is still
// settling are retried this way instead of failing BeforeSuite on the first
// timeout.
func retryTransient(ctx context.Context, attempts int, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(5 * time.Second):
		}
	}
	return err
}

// writeClusterKubeconfig captures `kind get kubeconfig` into a private temp
// file for tools that take a --kubeconfig flag. Read-only against kind; the
// ambient KUBECONFIG is never touched.
func writeClusterKubeconfig(ctx context.Context, clusterName string) (string, error) {
	cmd := exec.CommandContext(ctx, "kind", "get", "kubeconfig", "--name", clusterName)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("kind get kubeconfig --name %s: %w (stderr: %s)", clusterName, err, stderr.String())
	}
	f, err := os.CreateTemp("", "ome-kind-scheduler-*.kubeconfig")
	if err != nil {
		return "", err
	}
	if _, err := f.Write(out.Bytes()); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", err
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), f.Close()
}

// ensureSchedulerImage leaves `tag` present in the local docker daemon:
// either it already is (CI loads a baked archive and tags it), or it is built
// from the scheduler module.
func ensureSchedulerImage(ctx context.Context, tag string) error {
	if exec.CommandContext(ctx, "docker", "image", "inspect", tag).Run() == nil {
		return nil
	}
	root, err := RepoRoot()
	if err != nil {
		return err
	}
	moduleDir := filepath.Join(root, "scheduler")
	hash, err := contextHash(moduleDir)
	if err != nil {
		return fmt.Errorf("ensureSchedulerImage: hash %s: %w", moduleDir, err)
	}
	buildCacheMu.Lock()
	cached := buildCache["scheduler:"+tag] == hash
	buildCacheMu.Unlock()
	if cached {
		return nil
	}
	stage, err := os.MkdirTemp("", "ome-scheduler-image-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)

	build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", filepath.Join(stage, "ome-scheduler"), "./cmd/ome-scheduler")
	build.Dir = moduleDir
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	var stderr bytes.Buffer
	build.Stderr = &stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("ensureSchedulerImage: go build ./cmd/ome-scheduler: %w (stderr: %s)", err, stderr.String())
	}
	if err := os.Symlink("ome-scheduler", filepath.Join(stage, "entrypoint")); err != nil {
		return err
	}
	pack := exec.CommandContext(ctx, "docker", "build", "--pull=false",
		"-f", filepath.Join(root, "dockerfiles", "kind-prebuilt.Dockerfile"),
		"--build-arg", "BASE_IMAGE=scratch",
		"--build-arg", "RUN_USER=65532:65532",
		"-t", tag, stage)
	pack.Env = append(os.Environ(), "DOCKER_BUILDKIT=0")
	stderr.Reset()
	pack.Stderr = &stderr
	if err := pack.Run(); err != nil {
		return fmt.Errorf("ensureSchedulerImage: docker build %s: %w (stderr: %s)", tag, err, stderr.String())
	}
	buildCacheMu.Lock()
	buildCache["scheduler:"+tag] = hash
	buildCacheMu.Unlock()
	return nil
}
