package framework

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// ManagerSetup is the seam between the generic framework and OME's
// controllers. Each suite passes a function that registers exactly the
// reconcilers, indexers, and webhooks it needs onto the supplied
// manager. Mirrors Kueue's framework.ManagerSetup design — the
// framework intentionally knows nothing about specific OME controllers.
//
// Returning an error fails the test; the framework calls
// gomega/testing.Fatalf with the wrapped message.
type ManagerSetup func(ctx context.Context, mgr ctrl.Manager) error

// ManagerOption mutates manager.Options before construction. Useful for
// fault-injecting clients, custom caches, or extra readiness probes.
type ManagerOption func(*manager.Options)

// webhookReadyTimeout caps how long StartManager waits for the webhook
// server to accept TLS handshakes. Hit only when the cert dance or
// port-binding goes wrong; should never be approached on a healthy
// machine.
const webhookReadyTimeout = 30 * time.Second

// StartManager brings up a controller-runtime manager configured for
// envtest:
//
//   - metrics server bind address "0" (disabled) to avoid port
//     collisions when Ginkgo runs suites with -procs=N
//   - SkipNameValidation=true so a single test binary can restart the
//     manager between specs without controller-runtime barfing on
//     duplicate controller names
//   - webhook server wired to the envtest-installed webhook port + cert
//     dir (only when WebhookPath was set on Init)
//   - the suite's ManagerSetup callback runs synchronously *before*
//     mgr.Start so reconcilers/indexers/webhooks can register without
//     racing the cache
//
// The manager runs in a background goroutine. StartManager blocks
// until the cache is ready (and the webhook port answers TLS, if a
// webhook was installed) or until the context is cancelled.
//
// Pair with StopManager. Safe to start/stop multiple times within a
// single envtest lifecycle (e.g., per-Describe restart for tests that
// need different controller config).
func (f *Framework) StartManager(ctx context.Context, cfg *rest.Config,
	setup ManagerSetup, opts ...ManagerOption) error {
	if f.testEnv == nil {
		return fmt.Errorf("framework.StartManager: Init must be called first")
	}
	if f.scheme == nil {
		return fmt.Errorf("framework.StartManager: SetupClient must be called first")
	}

	mgrOpts := manager.Options{
		Scheme:         f.scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
		Controller:     config.Controller{SkipNameValidation: ptr.To(true)},
	}
	if f.WebhookPath != "" {
		whOpts := &f.testEnv.WebhookInstallOptions
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Host:    whOpts.LocalServingHost,
			Port:    whOpts.LocalServingPort,
			CertDir: whOpts.LocalServingCertDir,
		})
	}
	for _, opt := range opts {
		opt(&mgrOpts)
	}

	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		return fmt.Errorf("framework.StartManager: ctrl.NewManager: %w", err)
	}

	if err := setup(ctx, mgr); err != nil {
		return fmt.Errorf("framework.StartManager: ManagerSetup: %w", err)
	}

	mgrCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := mgr.Start(mgrCtx); err != nil {
			// Stash the error on the framework so a later Teardown can
			// surface it; we can't fail t directly from this goroutine.
			f.lastManagerErr = err
		}
	}()

	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		cancel()
		<-done
		return fmt.Errorf("framework.StartManager: cache failed to sync")
	}

	if f.WebhookPath != "" {
		if err := waitForWebhookReady(&f.testEnv.WebhookInstallOptions); err != nil {
			cancel()
			<-done
			return fmt.Errorf("framework.StartManager: webhook not ready: %w", err)
		}
	}

	f.manager = mgr
	f.managerCancel = cancel
	f.managerDone = done
	return nil
}

// StopManager cancels the manager goroutine and waits for it to exit.
// Idempotent. Safe to call from AfterAll/AfterSuite even if
// StartManager was not called.
func (f *Framework) StopManager(ctx context.Context) error {
	if f.managerCancel == nil {
		return nil
	}
	f.managerCancel()
	select {
	case <-f.managerDone:
	case <-ctx.Done():
		return fmt.Errorf("framework.StopManager: context cancelled before manager exit")
	}
	f.manager = nil
	f.managerCancel = nil
	f.managerDone = nil
	return f.lastManagerErr
}

// Manager returns the running manager. Nil before StartManager / after
// StopManager. Useful for tests that want to inspect mgr.GetClient()
// (which uses the cache, unlike the SetupClient client which doesn't).
func (f *Framework) Manager() ctrl.Manager {
	return f.manager
}

// waitForWebhookReady polls the envtest-installed webhook port with a
// dummy TLS handshake (insecure-skip because envtest's CA is local).
// Without this poll, immediate Create calls may fail with "connection
// refused" because mgr.Start returns before the webhook listener is
// bound — a well-known controller-runtime envtest race.
func waitForWebhookReady(whOpts *envtest.WebhookInstallOptions) error {
	addr := fmt.Sprintf("%s:%d", whOpts.LocalServingHost, whOpts.LocalServingPort)
	deadline := time.Now().Add(webhookReadyTimeout)
	for time.Now().Before(deadline) {
		conn, err := tls.DialWithDialer(
			&net.Dialer{Timeout: time.Second}, "tcp", addr,
			&tls.Config{InsecureSkipVerify: true}, //nolint:gosec // envtest local cert
		)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("webhook port %s not reachable after %s", addr, webhookReadyTimeout)
}
