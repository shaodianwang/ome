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

	// ExtraMounts, if given, bind-mounts each host path into every node
	// (control-plane and workers alike) at cluster-creation time. Only
	// takes effect when the cluster doesn't already exist — see
	// EnsureCluster. A fixture relying on a static hostPath
	// PersistentVolume needs this so the same directory backs the volume
	// regardless of which node a consuming pod lands on.
	ExtraMounts []KindExtraMount

	// BuildImages declares Docker images to build + kind-load before
	// the manager starts. The build step is content-hashed so re-runs
	// skip the work when the source files haven't changed.
	BuildImages []ImageBuildSpec

	// CRDPaths lists relative-to-repo-root paths to CRD YAML
	// directories (or single files) to apply at suite startup.
	// For an in-process suite, empty uses config/crd/full. For a deployed
	// suite, empty skips CRD installation because the OME chart owns its CRDs.
	CRDPaths []string

	// Scheme is the runtime.Scheme the in-process manager registers.
	// nil == BaseScheme() (core k8s + OME v1beta1). Suites that need
	// extra schemes (LWS, KEDA, scheduler-plugins, Gateway API, etc.)
	// build their own via BaseScheme() then AddToScheme(s).
	Scheme *runtime.Scheme

	// Setup wires the OME reconcilers onto the manager. Identical
	// signature to framework.ManagerSetup so a callback can be
	// shared between envtest and KIND suites without modification.
	// nil == skip controller startup (smoke-test pattern).
	Setup ManagerSetup

	// Deployed runs the suite against a real OME install instead of an
	// in-process manager. Mutually exclusive with Setup: the two would
	// otherwise reconcile the same objects and fight.
	//
	// CRDPaths carries only optional dependency CRDs in this mode. The chart
	// owns the OME CRDs.
	Deployed *DeployedOME
	// Scheduler, when set, installs the gang scheduler into the cluster after
	// the CRDs and before the manager, and adds the PodGroup CRD to CRDPaths.
	// Fixture runtimes select it through spec.schedulerName (see kindfix).
	Scheduler *SchedulerInstall
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

	managerMu     sync.Mutex
	managerStop   func()
	managerSetup  ManagerSetup
	managerScheme *runtime.Scheme

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
//  5. Installs OME (if Deployed is set)
//  6. Starts the in-process controller manager (if Setup is set)
//  7. Stores SuiteState so test code can call Suite()
//  8. Tears down on TestMain exit (cluster delete honors KIND_KEEP_CLUSTER=1)
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
	if opts.Setup != nil && opts.Deployed != nil {
		t.Fatal("RunKindSuite: Setup and Deployed are mutually exclusive — " +
			"an in-process manager and a deployed one would reconcile the same objects")
	}

	SetupLogger()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if opts.Scheduler != nil && opts.Workers < opts.Scheduler.minWorkers() {
		opts.Workers = opts.Scheduler.minWorkers()
	}
	cfg, err := EnsureCluster(ctx, opts.ClusterName, opts.Workers, opts.ExtraMounts...)
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

	if opts.Scheduler != nil {
		opts.CRDPaths = withSchedulerCRDs(opts.CRDPaths, opts.Scheduler)
	}
	if shouldApplyCRDs(opts) {
		if err := ApplyCRDs(ctx, cfg, opts.CRDPaths); err != nil {
			t.Fatalf("RunKindSuite: ApplyCRDs: %v", err)
		}
	}
	// The scheduler lists PodGroups at startup, so it follows the CRDs; the
	// manager and a deployed OME follow it so their first gang finds it.
	if err := installScheduler(ctx, opts.ClusterName, opts.Scheduler); err != nil {
		t.Fatalf("RunKindSuite: %v", err)
	}

	// After the optional CRDs: a deployed controller discovers them at startup,
	// so installing OME first would have it decide they are absent.
	if err := deployOME(ctx, opts.ClusterName, opts.Deployed); err != nil {
		t.Fatalf("RunKindSuite: %v", err)
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
		state.Manager = mgr
		state.managerStop = stop
		state.managerSetup = opts.Setup
		state.managerScheme = opts.Scheme
		t.Cleanup(state.stopManager)
	}

	setActiveSuite(state)
	t.Cleanup(func() { setActiveSuite(nil) })

	return state
}

func shouldApplyCRDs(opts SuiteOpts) bool {
	return opts.Deployed == nil || len(opts.CRDPaths) > 0
}

// RestartManager stops the suite's in-process manager, waits for it to exit,
// and starts a fresh manager against the same cluster and setup callback. The
// Kubernetes client and suite context remain valid, so specs can verify that
// durable controller state resumes after process-local caches disappear.
func (s *SuiteState) RestartManager() error {
	if s == nil {
		return fmt.Errorf("SuiteState is nil")
	}
	s.managerMu.Lock()
	defer s.managerMu.Unlock()
	if s.managerSetup == nil {
		return fmt.Errorf("SuiteState has no manager setup callback")
	}
	if s.managerStop != nil {
		s.managerStop()
	}
	s.Manager = nil
	s.managerStop = nil
	mgr, stop, err := StartManager(s.Ctx, s.RestConfig, s.managerScheme, s.managerSetup)
	if err != nil {
		return fmt.Errorf("restart manager: %w", err)
	}
	s.Manager = mgr
	s.managerStop = stop
	return nil
}

func (s *SuiteState) stopManager() {
	s.managerMu.Lock()
	defer s.managerMu.Unlock()
	if s.managerStop != nil {
		s.managerStop()
		s.managerStop = nil
	}
	s.Manager = nil
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
