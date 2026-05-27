package kindutil

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ManagerSetup mirrors framework.ManagerSetup so KIND-backed test
// suites can reuse the exact same reconciler-wiring callbacks the
// envtest suites already have. The signature must stay byte-identical
// to framework.ManagerSetup (we copy rather than import to avoid a
// circular dep — framework imports util, and we sit under tests/
// peer to both).
type ManagerSetup func(ctx context.Context, mgr ctrl.Manager) error

// StartManager brings up an in-process controller-runtime manager
// against cfg, runs setup synchronously (the suite registers its
// reconcilers there), then starts the manager in a background
// goroutine. Returns the manager + a stop function the caller must
// call (typically via t.Cleanup).
//
// Behavior chosen to match the envtest framework as closely as
// possible so the test-author iteration loop is identical:
//
//   - metrics server bind address "0" (disabled) — Ginkgo -procs=N
//     would collide otherwise.
//   - SkipNameValidation=true — a single test binary that restarts
//     the manager between specs (some PD specs do this) won't trip
//     controller-runtime's dup-name guard.
//   - leader election off — single instance per test process.
//   - no webhook server — webhook coverage stays on envtest.
//
// Returns after the manager's cache is in sync. Failure modes
// (cache won't sync, setup returns an error, mgr.Start errors before
// cache sync) come back as the wrapped error; ctx cancellation
// signals shutdown to the caller-owned goroutine.
func StartManager(ctx context.Context, cfg *rest.Config,
	scheme *runtime.Scheme, setup ManagerSetup) (ctrl.Manager, func(), error) {

	mgr, err := manager.New(cfg, manager.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
		Controller:     config.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		return nil, func() {}, fmt.Errorf("StartManager: manager.New: %w", err)
	}

	if err := setup(ctx, mgr); err != nil {
		return nil, func() {}, fmt.Errorf("StartManager: setup callback: %w", err)
	}

	mgrCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var startErr error
	go func() {
		defer close(done)
		startErr = mgr.Start(mgrCtx)
	}()

	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		cancel()
		<-done
		if startErr != nil {
			return nil, func() {}, fmt.Errorf("StartManager: cache sync failed (start error: %w)", startErr)
		}
		return nil, func() {}, fmt.Errorf("StartManager: cache sync failed")
	}

	stop := stopOnce(func() {
		cancel()
		<-done
	})
	return mgr, stop, nil
}

// stopOnce wraps fn so multiple Stop calls are safe — t.Cleanup may
// race with explicit shutdown when a panicking test triggers both.
func stopOnce(fn func()) func() {
	var once sync.Once
	return func() { once.Do(fn) }
}

// BaseScheme returns a runtime.Scheme pre-populated with the core
// types every OME test needs:
//
//   - k8s built-ins (pods, services, deployments, endpointslices,
//     events, configmaps, secrets, leases, ...) via clientgoscheme
//   - the OME v1beta1 group (InferenceService, BaseModel,
//     ServingRuntime, etc.)
//
// Suites that need extra schemes (LWS, KEDA, scheduler-plugins,
// Gateway API, etc.) add them in their ManagerSetup callback before
// constructing their reconcilers.
func BaseScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(v1beta1.AddToScheme(s))
	return s
}

// SetupLogger sets the controller-runtime global logger to a zap
// dev-mode logger so reconciler stdout is human-readable during
// test runs. Safe to call multiple times; the second call wins.
//
// Idempotent because Go tests sometimes run a suite multiple times
// in one process when using -count > 1, and ctrl.SetLogger isn't
// re-entrant-safe with a strict sync.Once.
var setLoggerOnce sync.Once

func SetupLogger() {
	setLoggerOnce.Do(func() {
		ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	})
}
