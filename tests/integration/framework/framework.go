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
	"time"

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

	// APIServerFeatureGates / APIServerRuntimeConfig are passed through
	// to envtest's apiserver --feature-gates / --runtime-config flags.
	// Use for tests that depend on alpha APIs.
	APIServerFeatureGates  []string
	APIServerRuntimeConfig []string

	// ControlPlaneStopTimeout caps envtest teardown wait. Defaults to
	// defaultControlPlaneStopTimeout if zero.
	ControlPlaneStopTimeout time.Duration

	// scheme is the *client-side* runtime.Scheme assembled in
	// SetupClient. Distinct from f.testEnv.Scheme (apiserver-side); the
	// split lets us register controller-only types (e.g. LWS, KEDA)
	// without polluting the apiserver scheme.
	scheme *apiruntime.Scheme

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
	if f.WebhookPath != "" {
		f.testEnv.WebhookInstallOptions.Paths = []string{filepath.Join(f.ProjectRoot, f.WebhookPath)}
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

	cfg, err := f.testEnv.Start()
	if err != nil {
		return nil, fmt.Errorf("framework.Init: envtest.Start: %w", err)
	}
	return cfg, nil
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

// Teardown stops envtest. Idempotent: safe to call from AfterSuite even
// if Init failed.
func (f *Framework) Teardown() error {
	if f.testEnv == nil {
		return nil
	}
	return f.testEnv.Stop()
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
