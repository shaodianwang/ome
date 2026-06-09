package kindutil

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SuiteOpts configures a RunKindSuite invocation.
type SuiteOpts struct {
	// ClusterName is the KIND cluster name to create (or reuse).
	// Lower-case, hyphenated; one suite per cluster. Cluster reuse
	// across `go test` runs via KIND_KEEP_CLUSTER=1.
	ClusterName string

	// Workers is the number of worker nodes in the KIND cluster.
	// 0 (default) means single-node (control-plane only). 3 is the
	// recommended value for tests that need scheduling pressure.
	Workers int

	// BuildImages declares Docker images to build + kind-load before
	// the manager starts. The build step is content-hashed so re-runs
	// skip the work when the source files haven't changed.
	BuildImages []ImageBuildSpec

	// CRDPaths lists relative-to-repo-root paths to CRD YAML
	// directories (or single files) to apply at suite startup.
	// Empty == use the default config/crd/full.
	CRDPaths []string

	// Scheme is the runtime.Scheme the in-process manager registers.
	// nil == BaseScheme() (core k8s + OME v1beta1). Suites that need
	// extra schemes (KEDA, scheduler-plugins, Gateway API, etc.)
	// build their own via BaseScheme() then AddToScheme(s).
	Scheme *runtime.Scheme

	// Setup wires the OME reconcilers onto the manager. Identical
	// signature to framework.ManagerSetup so a callback can be
	// shared between envtest and KIND suites without modification.
	// nil == skip controller startup (smoke-test pattern).
	Setup ManagerSetup
}

// SuiteState is the shared, cross-spec state every KIND-backed
// integration suite reads. Populated by RunKindSuite before any
// Describe/It runs; nil before then.
//
// Tests access fields directly (Client, RestConfig, Manager) to mint
// per-spec namespaces and assert on cluster state.
type SuiteState struct {
	// Ctx is the suite's root context. Test code passes it to client
	// calls and Eventually blocks. Cancelled at suite teardown.
	Ctx context.Context

	// RestConfig points at the suite's KIND cluster apiserver.
	RestConfig *rest.Config

	// Client is a controller-runtime client wired against RestConfig
	// + the suite's Scheme.
	Client client.Client

	// Manager is the in-process controller-runtime manager. nil when
	// SuiteOpts.Setup was nil (smoke-test pattern).
	Manager ctrl.Manager

	// ClusterName mirrors SuiteOpts.ClusterName for cleanup helpers
	// that need to dispatch `kind` subcommands.
	ClusterName string
}

// activeSuite is the currently-running suite's state. Tests read it
// via the Suite() accessor below. Set by RunKindSuite at the start
// of each TestXxx; nil between suites.
var (
	activeSuiteMu sync.RWMutex
	activeSuite   *SuiteState
)

// Suite returns the currently-active SuiteState. Panics if called
// outside a RunKindSuite invocation — that's almost always a test
// authoring mistake (missing TestMain, BeforeSuite ordering wrong).
func Suite() *SuiteState {
	activeSuiteMu.RLock()
	defer activeSuiteMu.RUnlock()
	if activeSuite == nil {
		panic("kindutil.Suite: no active suite; call RunKindSuite from TestXxx")
	}
	return activeSuite
}

// RunKindSuite is the TestXxx entry point each KIND-backed
// integration suite calls. It:
//
//  1. Validates kind + docker are on $PATH
//  2. Creates (or reuses) the KIND cluster
//  3. Builds + kind-loads the requested images
//  4. Applies CRDs
//  5. Starts the in-process controller manager (if Setup is set)
//  6. Stores SuiteState so test code can call Suite()
//  7. Tears down on TestMain exit (cluster delete honors KIND_KEEP_CLUSTER=1)
//
// Caller wires t.Run / ginkgo.RunSpecs themselves — RunKindSuite
// only sets up shared state. Mirrors framework.RunSuite's contract
// so the porting delta is minimal.
func RunKindSuite(t *testing.T, opts SuiteOpts) *SuiteState {
	t.Helper()
	if opts.ClusterName == "" {
		t.Fatal("RunKindSuite: ClusterName is required")
	}
	if opts.Scheme == nil {
		opts.Scheme = BaseScheme()
	}

	SetupLogger()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg, err := EnsureCluster(ctx, opts.ClusterName, opts.Workers)
	if err != nil {
		t.Fatalf("RunKindSuite: EnsureCluster(%s): %v", opts.ClusterName, err)
	}
	t.Cleanup(func() {
		if err := DeleteCluster(context.Background(), opts.ClusterName); err != nil {
			t.Logf("RunKindSuite: DeleteCluster(%s) on cleanup: %v", opts.ClusterName, err)
		}
	})

	if err := BuildAndLoadImages(ctx, opts.ClusterName, opts.BuildImages); err != nil {
		t.Fatalf("RunKindSuite: BuildAndLoadImages: %v", err)
	}

	if err := ApplyCRDs(ctx, cfg, opts.CRDPaths); err != nil {
		t.Fatalf("RunKindSuite: ApplyCRDs: %v", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: opts.Scheme})
	if err != nil {
		t.Fatalf("RunKindSuite: client.New: %v", err)
	}

	state := &SuiteState{
		Ctx:         ctx,
		RestConfig:  cfg,
		Client:      c,
		ClusterName: opts.ClusterName,
	}

	if opts.Setup != nil {
		mgr, stop, err := StartManager(ctx, cfg, opts.Scheme, opts.Setup)
		if err != nil {
			t.Fatalf("RunKindSuite: StartManager: %v", err)
		}
		t.Cleanup(stop)
		state.Manager = mgr
	}

	setActiveSuite(state)
	t.Cleanup(func() { setActiveSuite(nil) })

	return state
}

// setActiveSuite swaps the package-global activeSuite under a write
// lock so Suite() (read lock) sees a consistent value.
func setActiveSuite(s *SuiteState) {
	activeSuiteMu.Lock()
	activeSuite = s
	activeSuiteMu.Unlock()
}

// AssertResources sanity-checks that a suite's Setup callback gave
// us a usable Client + Manager. Call from BeforeSuite when a missing
// Setup would only show up as a deep-stack nil-deref later. Returns
// a wrapped error rather than panicking so the test reports cleanly.
func (s *SuiteState) AssertResources() error {
	if s == nil {
		return fmt.Errorf("SuiteState is nil")
	}
	if s.Client == nil {
		return fmt.Errorf("SuiteState.Client is nil")
	}
	if s.RestConfig == nil {
		return fmt.Errorf("SuiteState.RestConfig is nil")
	}
	return nil
}
