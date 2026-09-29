// Package framework provides a shared envtest + controller-runtime
// manager harness for OME's integration test suites.
//
// A typical suite builds a Framework, calls Init() to start envtest in
// BeforeSuite, SetupClient() to obtain a client, optionally
// StartManager() to bring up reconcilers, and Teardown() in AfterSuite.
package framework

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap/zaptest/observer"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// defaultControlPlaneStopTimeout is the upper bound on how long
// envtest.Environment.Stop() may block. envtest sometimes needs a
// little extra time to shut down etcd cleanly.
const defaultControlPlaneStopTimeout = 90 * time.Second

// defaultControlPlaneStartTimeout overrides envtest's 20s default for
// apiserver / etcd boot. CI runners have shown 3–4s per CRD Update and
// the OME suite installs ~10 CRDs (OME + LWS/KEDA/etc) before
// BeforeSuite can return — 2m gives ample headroom without slowing
// healthy local runs.
const defaultControlPlaneStartTimeout = 2 * time.Minute

// defaultCRDInstallTimeout overrides envtest's 10s default. Same
// reasoning as defaultControlPlaneStartTimeout — we observed CRD
// installs taking >18s on overloaded CI runners, blowing the 10s
// budget and failing BeforeSuite with "context deadline exceeded".
const defaultCRDInstallTimeout = 2 * time.Minute

// Framework owns one envtest.Environment and (optionally) one
// controller-runtime manager goroutine. Construct one per Go test
// package — Ginkgo runs each package in its own process, so cross-suite
// envtest isolation is automatic.
type Framework struct {
	// ProjectRoot is the absolute path to the OME repo root. If empty,
	// Init() walks up from the calling file until it finds go.mod.
	ProjectRoot string

	// OptionalCRDs declares which optional-dep CRD bundles to install.
	// Each bundle resolves to a directory under
	// ProjectRoot/tests/integration/dep-crds/<dir>.
	OptionalCRDs []OptionalCRDBundle

	// WebhookPath optionally points at an OME webhook YAML directory
	// (relative to ProjectRoot). Empty disables webhook installation —
	// suites that test only reconcile-loop behavior should leave this
	// empty since webhooks add latency and require the cert dance.
	WebhookPath string

	// ExtraWebhookPaths optionally points at ADDITIONAL webhook YAML
	// directories given as ABSOLUTE paths (unlike WebhookPath, which is
	// ProjectRoot-relative). Used for dependency-supplied webhook
	// manifests that live outside the repo — notably Kueue's pod webhook
	// in the read-only module cache (see framework/kueue.go: StartKueue
	// needs the Kueue pod webhook installed so it can gate pods). When
	// either WebhookPath or ExtraWebhookPaths is non-empty, StartManager
	// wires a webhook server.
	ExtraWebhookPaths []string

	// APIServerFeatureGates / APIServerRuntimeConfig are passed through
	// to envtest's apiserver --feature-gates / --runtime-config flags.
	// Use for tests that depend on alpha APIs.
	APIServerFeatureGates  []string
	APIServerRuntimeConfig []string

	// ControlPlaneStopTimeout caps envtest teardown wait. Defaults to
	// defaultControlPlaneStopTimeout if zero.
	ControlPlaneStopTimeout time.Duration

	// SkipWriteConflictCheck disables the Teardown write-conflict gate
	// for this Framework. The gate fails the suite when a controller
	// logs an apiserver optimistic-lock rejection at error level (a
	// missing conflict-retry path). Leave false for normal suites; set
	// true only for suites that deliberately provoke conflicts.
	//
	// Prefer AllowedWriteConflicts when only some conflicts are
	// tolerable — this switch gives up the gate for the whole suite.
	SkipWriteConflictCheck bool

	// AllowedWriteConflicts names conflicts the Teardown gate must not
	// fail on, leaving it armed for every other reconciler in the
	// suite. Use it for a dependency's controller that retries a
	// conflict and converges but logs the rejection at error level
	// first — the gate cannot tell that apart from an OME reconciler
	// that loses the race and gives up.
	AllowedWriteConflicts []WriteConflictExemption

	// scheme is the *client-side* runtime.Scheme assembled in
	// SetupClient. Distinct from f.testEnv.Scheme (apiserver-side); the
	// split lets us register controller-only types (e.g. LWS, KEDA)
	// without polluting the apiserver scheme.
	scheme *apiruntime.Scheme

	// observedLogs captures controller-runtime log entries for the
	// Teardown write-conflict gate. Set by Init (via the OnceValue
	// logger), read by Teardown. Nil if Init was never called.
	observedLogs *observer.ObservedLogs

	// observedFrom is the observer's length when this Framework
	// started. The observer is shared by every Framework in the test
	// binary and is never drained, so Teardown asserts from this index
	// rather than over the binary's whole history. Frameworks whose
	// lifetimes overlap still share the tail.
	observedFrom int

	// unexported state — set by Init/StartManager, consumed by Teardown.
	testEnv        *envtest.Environment
	manager        ctrlManager
	managerCancel  context.CancelFunc
	managerDone    <-chan struct{}
	lastManagerErr error
}

// ctrlManager is a tiny interface alias so framework.go doesn't need
// to import sigs.k8s.io/controller-runtime just for the Manager type
// (manager.go has the import). Kept private.
type ctrlManager = ctrl.Manager

// OptionalCRDBundle declares an optional-dep CRD set installed by
// envtest plus the schemes registered on the client-side scheme by
// SetupClient. Bundles without schemes are install-only (rare).
type OptionalCRDBundle struct {
	Name    string
	CRDDir  string                           // relative to ProjectRoot/tests/integration/dep-crds/
	Schemes []func(*apiruntime.Scheme) error // client-side scheme contributors
}

// Init starts envtest with OME's CRDs (and any declared optional
// bundles) installed. Must be called before SetupClient/StartManager;
// must be paired with Teardown.
//
// Returns *rest.Config so the suite can create its own clients before
// any controller is running — useful for seeding baseline state and
// for webhook-only suites.
func (f *Framework) Init() (*rest.Config, error) {
	// Wire the controller-runtime logger to an observing tee before
	// anything starts so the whole suite's >=Error logs are captured
	// for the Teardown write-conflict gate. Idempotent across multiple
	// Frameworks in one test binary (sync.OnceValue).
	f.observedLogs = setupObservedLogger()
	f.observedFrom = f.observedLogs.Len()

	if f.ProjectRoot == "" {
		root, err := findProjectRoot()
		if err != nil {
			return nil, fmt.Errorf("framework.Init: resolve ProjectRoot: %w", err)
		}
		f.ProjectRoot = root
	}

	crdPaths := []string{filepath.Join(f.ProjectRoot, "config", "crd", "full")}
	for _, b := range f.OptionalCRDs {
		crdPaths = append(crdPaths, filepath.Join(f.ProjectRoot, "tests", "integration", "dep-crds", b.CRDDir))
	}

	stopTimeout := f.ControlPlaneStopTimeout
	if stopTimeout == 0 {
		stopTimeout = defaultControlPlaneStopTimeout
	}

	f.testEnv = &envtest.Environment{
		CRDDirectoryPaths:        crdPaths,
		ErrorIfCRDPathMissing:    true,
		ControlPlaneStopTimeout:  stopTimeout,
		ControlPlaneStartTimeout: defaultControlPlaneStartTimeout,
		CRDInstallOptions: envtest.CRDInstallOptions{
			MaxTime: defaultCRDInstallTimeout,
		},
	}
	if f.webhooksEnabled() {
		var paths []string
		if f.WebhookPath != "" {
			paths = append(paths, filepath.Join(f.ProjectRoot, f.WebhookPath))
		}
		paths = append(paths, f.ExtraWebhookPaths...)
		f.testEnv.WebhookInstallOptions.Paths = paths
	}
	if len(f.APIServerFeatureGates) > 0 {
		f.testEnv.ControlPlane.GetAPIServer().Configure().Append("feature-gates", joinCommas(f.APIServerFeatureGates))
	}
	if len(f.APIServerRuntimeConfig) > 0 {
		f.testEnv.ControlPlane.GetAPIServer().Configure().Append("runtime-config", joinCommas(f.APIServerRuntimeConfig))
	}

	// Register the OME scheme on envtest's apiserver scheme so the
	// apiserver can decode CR creates/updates immediately.
	utilruntime.Must(clientgoscheme.AddToScheme(envtestScheme()))
	utilruntime.Must(v1beta1.AddToScheme(envtestScheme()))
	f.testEnv.Scheme = envtestScheme()

	cfg, err := startControlPlane(f.testEnv)
	if err != nil {
		return nil, fmt.Errorf("framework.Init: envtest.Start: %w", err)
	}
	return cfg, nil
}

// controlPlaneStartAttempts bounds the restarts of an envtest control plane
// whose apiserver stops answering while envtest is still installing CRDs.
const controlPlaneStartAttempts = 3

// startControlPlane starts the envtest control plane, restarting it when its
// apiserver drops the connection during bring-up. On a loaded agent several
// suites start apiservers at once and one can lose its client connection or
// refuse it before envtest finishes installing CRDs; that is the process
// dying under the load, not a test result, and Ginkgo would otherwise fail
// the whole suite in BeforeSuite. Any other error returns as is.
func startControlPlane(env *envtest.Environment) (*rest.Config, error) {
	var err error
	for attempt := 1; attempt <= controlPlaneStartAttempts; attempt++ {
		var cfg *rest.Config
		cfg, err = env.Start()
		if err == nil {
			return cfg, nil
		}
		if !controlPlaneDropped(err) || attempt == controlPlaneStartAttempts {
			break
		}
		_ = env.Stop()
		time.Sleep(time.Duration(attempt) * 5 * time.Second)
	}
	return nil, err
}

// controlPlaneDropped reports the bring-up failures that mean the apiserver
// process stopped answering rather than rejected a request.
func controlPlaneDropped(err error) bool {
	msg := err.Error()
	for _, sig := range []string{
		"http2: client connection lost",
		"connection refused",
		"connection reset by peer",
		"context deadline exceeded",
		"unexpected EOF",
	} {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

// SetupClient assembles a client-side runtime.Scheme (OME + core/batch
// + every declared OptionalCRDBundle's schemes) and returns a context
// plus a controller-runtime client. The context lives until the
// caller's t.Cleanup / AfterSuite runs Teardown — it is NOT cancelled
// here.
//
// Suites typically write:
//
//	ctx, k8sClient := f.SetupClient(cfg)
//
// and then either drive the apiserver directly (webhook-only suites)
// or pass `ctx` and `cfg` to StartManager.
func (f *Framework) SetupClient(cfg *rest.Config) (context.Context, client.Client, error) {
	if cfg == nil {
		return nil, nil, fmt.Errorf("framework.SetupClient: nil rest.Config (call Init first)")
	}

	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))

	// Register schemes from any declared optional bundles. Bundles
	// without scheme contributors are ignored.
	for _, b := range f.OptionalCRDs {
		for _, addToScheme := range b.Schemes {
			if err := addToScheme(scheme); err != nil {
				return nil, nil, fmt.Errorf("framework.SetupClient: register scheme for bundle %q: %w", b.Name, err)
			}
		}
	}
	f.scheme = scheme

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, fmt.Errorf("framework.SetupClient: client.New: %w", err)
	}
	// Background ctx; cancellation is the caller's responsibility (test
	// uses t.Cleanup or Ginkgo's AfterSuite).
	return context.Background(), c, nil
}

// Scheme returns the client-side scheme assembled by SetupClient. Nil
// before SetupClient is called. Suites pass it into the Manager
// constructor in StartManager.
func (f *Framework) Scheme() *apiruntime.Scheme {
	return f.scheme
}

// ObservedLogs returns the in-memory log observer wired by Init. Nil
// before Init. Suites rarely need this — Teardown runs the write-
// conflict gate automatically — but it is exposed for suites that want
// to assert on captured controller logs directly.
func (f *Framework) ObservedLogs() *observer.ObservedLogs {
	return f.observedLogs
}

// Teardown stops envtest and runs the write-conflict gate. Idempotent:
// safe to call from AfterSuite even if Init failed.
//
// The gate (VerifyNoWriteConflicts) runs after envtest is stopped so
// the observer holds the suite's complete log history. It fails the
// suite via Gomega if any controller logged an apiserver optimistic-
// lock rejection at error level, over the entries logged after this
// Framework started, minus the conflicts AllowedWriteConflicts names.
// Disable it entirely with SkipWriteConflictCheck for suites that
// provoke conflicts on purpose.
func (f *Framework) Teardown() error {
	if f.testEnv == nil {
		return nil
	}
	stopErr := f.testEnv.Stop()
	if !f.SkipWriteConflictCheck {
		VerifyNoWriteConflicts(f.observedLogs, f.observedFrom, f.AllowedWriteConflicts)
	}
	return stopErr
}

// webhooksEnabled reports whether any webhook manifests are configured —
// either OME's (WebhookPath) or a dependency's (ExtraWebhookPaths). When
// true, Init installs the manifests on envtest and StartManager wires a
// webhook server + readiness poll.
func (f *Framework) webhooksEnabled() bool {
	return f.WebhookPath != "" || len(f.ExtraWebhookPaths) > 0
}

// findProjectRoot walks up from the file that called the public
// Framework method until it finds a directory containing go.mod.
// Returns an absolute path. The walk ceiling is os.Getwd's root.
func findProjectRoot() (string, error) {
	// runtime.Caller(2) skips findProjectRoot + the public Framework
	// method that invoked it (e.g., Init).
	_, file, _, ok := runtime.Caller(2)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above %s", file)
		}
		dir = parent
	}
}

// envtestScheme returns the scheme installed on the apiserver. Suites
// build their own client-side scheme via SetupClient.
func envtestScheme() *apiruntime.Scheme {
	// envtest's apiserver uses the global client-go scheme by default;
	// using the same one keeps codecs consistent.
	return clientgoscheme.Scheme
}

// joinCommas is a tiny helper kept in this file so the package has
// zero dependencies on strings.
func joinCommas(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}
