// Command runner drives the InferenceService performance/scale harness.
//
// It boots an envtest control plane with raised apiserver request limits,
// installs OME's CRDs (plus the PodMonitor CRD the RawDeployment path
// reconciles), wires ONLY the InferenceService reconciler, then generates
// a data-driven load (N ServingRuntimes x M BaseModels x K
// InferenceServices across small/medium/large classes). A fake executor
// flips each produced Deployment to Ready after a per-class simulated
// delay — no kubelet is involved — and a recorder measures time-to-Ready
// per class. On completion it writes summary.yaml + cmd.stats.yaml into
// the output directory, which the checker test asserts against.
//
// Everything behavioral is a flag or comes from the generator YAML; there
// are no magic in-code defaults for replica counts, intervals, image
// refs, or limits.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	zaplog "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/util"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/config"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/executor"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/generator"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/observer"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/recorder"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/stats"
)

var (
	outputDir       = flag.String("o", "", "output directory for summary.yaml + cmd.stats.yaml (required)")
	generatorConfig = flag.String("generator-config", "", "path to the generator config YAML (required)")
	projectRoot     = flag.String("project-root", "", "absolute path to the OME repo root (for CRD discovery); defaults to the cwd")
	loadNamespace   = flag.String("namespace", "perf-isvc", "namespace the harness creates InferenceServices in")

	timeout = flag.Duration("timeout", 10*time.Minute, "max time to wait for the run to converge")

	// Client / apiserver throughput knobs. The defaults are intentionally
	// modest; the make target / operator raises them per scenario. These
	// govern how hard the runner can push the apiserver, not OME behavior.
	qps   = flag.Float64("qps", 0, "rest client QPS for the runner's clients; 0 keeps the client-go default")
	burst = flag.Int("burst", 0, "rest client burst for the runner's clients; 0 keeps the client-go default")

	// Apiserver request-limit overrides. Under a large run the default
	// kube-apiserver max-requests-inflight (400) / max-mutating (200) can
	// saturate and surface as "http2: client connection lost"; raise them.
	maxRequestsInflight         = flag.Int("max-requests-inflight", 3000, "envtest apiserver --max-requests-inflight")
	maxMutatingRequestsInflight = flag.Int("max-mutating-requests-inflight", 1500, "envtest apiserver --max-mutating-requests-inflight")

	// Concurrency of the harness executor. The InferenceService reconciler's
	// own concurrency is fixed by its SetupWithManager (the harness imports that
	// wiring verbatim and must not edit it).
	executorConcurrency = flag.Int("executor-concurrency", 10, "MaxConcurrentReconciles for the fake executor")

	// Recorder intake buffer. Bounded so a saturated recorder drops (and
	// counts) events rather than back-pressuring the observer.
	recorderBuffer = flag.Int("recorder-buffer", 4096, "bounded intake-channel depth for the recorder")
)

var scheme = apiruntime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	// PodMonitor: the ISVC RawDeployment path unconditionally reconciles
	// one, so its type must be in the scheme or SetControllerReference
	// fails.
	utilruntime.Must(monitoringv1.AddToScheme(scheme))
	// KEDA ScaledObject: the ISVC reconciler Owns it unconditionally
	// (KEDA is a hard cluster-side dependency for OME), so the scheme must
	// know the type or the controller's watch fails to start.
	utilruntime.Must(kedav1.AddToScheme(scheme))
}

func main() {
	opts := zap.Options{
		TimeEncoder: zapcore.RFC3339NanoTimeEncoder,
		ZapOpts:     []zaplog.Option{zaplog.AddCaller()},
		Development: true,
		Level:       zaplog.NewAtomicLevelAt(zapcore.InfoLevel),
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	log := zap.New(zap.UseFlagOptions(&opts))
	ctrl.SetLogger(log)

	if err := run(ctrl.LoggerInto(context.Background(), log)); err != nil {
		log.Error(err, "runner failed")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx)

	if *outputDir == "" || *generatorConfig == "" {
		return fmt.Errorf("both -o and -generator-config are required")
	}
	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		return fmt.Errorf("creating output dir: %w", err)
	}

	cfg, err := config.LoadConfig(*generatorConfig)
	if err != nil {
		return err
	}
	log.Info("loaded generator config", "classes", len(cfg.Classes), "total", cfg.Total())

	root := *projectRoot
	if root == "" {
		root, err = os.Getwd()
		if err != nil {
			return fmt.Errorf("resolving project root: %w", err)
		}
	}

	testEnv, restCfg, err := startEnvtest(ctx, root)
	if err != nil {
		return err
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			log.Error(err, "stopping envtest")
		}
	}()

	if *qps > 0 {
		restCfg.QPS = float32(*qps)
	}
	if *burst > 0 {
		restCfg.Burst = *burst
	}

	// Direct (non-cached) client for seeding + generation, mirroring how
	// the integration suites drive the apiserver.
	directClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("building direct client: %w", err)
	}

	if err := util.SeedDefaultConfigMaps(ctx, directClient); err != nil {
		return fmt.Errorf("seeding config maps: %w", err)
	}
	if err := ensureNamespace(ctx, directClient, *loadNamespace); err != nil {
		return fmt.Errorf("creating load namespace: %w", err)
	}

	rec := recorder.New(*timeout, cfg.Total(), *recorderBuffer)

	mgr, err := startManager(ctx, restCfg, rec)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	mgrCtx, cancelMgr := context.WithCancel(ctx)
	defer cancelMgr()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := mgr.Start(mgrCtx); err != nil {
			log.Error(err, "manager exited with error")
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		cancelMgr()
		wg.Wait()
		return fmt.Errorf("manager cache failed to sync")
	}

	// Recorder loop. Run returns when every InferenceService is Ready or
	// the timeout trips.
	recDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		recDone <- rec.Run(ctx)
	}()

	// Generate the load. The generator stamps creation times into the
	// recorder as it goes.
	startWall := time.Now()
	gen := generator.New(directClient, cfg, *loadNamespace, rec)
	if err := gen.Generate(ctx); err != nil {
		cancelMgr()
		wg.Wait()
		return fmt.Errorf("generating load: %w", err)
	}

	runErr := <-recDone
	wallMs := time.Since(startWall).Milliseconds()

	// Tear the manager down before writing artifacts so no late status
	// flip races the summary fold.
	cancelMgr()
	wg.Wait()

	if err := rec.WriteSummary(filepath.Join(*outputDir, "summary.yaml")); err != nil {
		return fmt.Errorf("writing summary: %w", err)
	}
	if err := writeCmdStats(filepath.Join(*outputDir, "cmd.stats.yaml"), wallMs); err != nil {
		return fmt.Errorf("writing cmd stats: %w", err)
	}

	// Best-effort catalog cleanup (namespaced ISVCs go with the env).
	if err := generator.Cleanup(context.Background(), directClient); err != nil {
		log.Error(err, "cleanup (non-fatal)")
	}

	if runErr != nil {
		return fmt.Errorf("run did not converge: %w", runErr)
	}
	log.Info("run complete", "wallMs", wallMs, "summary", filepath.Join(*outputDir, "summary.yaml"))
	return nil
}

// startEnvtest brings up the control plane with raised request limits and
// OME + PodMonitor CRDs installed.
func startEnvtest(ctx context.Context, root string) (*envtest.Environment, *rest.Config, error) {
	log := ctrl.LoggerFrom(ctx)

	depCRDs := filepath.Join(root, "tests", "integration", "dep-crds")
	crdPaths := []string{
		filepath.Join(root, "config", "crd", "full"),
		// PodMonitor CRD: vendored under dep-crds (see crd_bundles.go's
		// PrometheusOperator bundle). The RawDeployment reconcile path
		// always creates a PodMonitor.
		filepath.Join(depCRDs, "prometheus-operator"),
		// KEDA CRDs (ScaledObject et al.): the ISVC reconciler Owns
		// ScaledObject unconditionally, so the informer needs the CRD
		// installed or cache sync wedges. Vendored by `make dep-crds` (gitignored).
		filepath.Join(depCRDs, "keda"),
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:        crdPaths,
		ErrorIfCRDPathMissing:    true,
		ControlPlaneStartTimeout: 2 * time.Minute,
		ControlPlaneStopTimeout:  90 * time.Second,
		CRDInstallOptions: envtest.CRDInstallOptions{
			MaxTime: 2 * time.Minute,
		},
	}
	ometesting.EtcdForBusyHosts(testEnv)
	// Raise apiserver request limits to survive a high-load run.
	testEnv.ControlPlane.GetAPIServer().Configure().
		Append("max-requests-inflight", fmt.Sprintf("%d", *maxRequestsInflight)).
		Append("max-mutating-requests-inflight", fmt.Sprintf("%d", *maxMutatingRequestsInflight))

	// Match the integration framework: install the OME scheme on the
	// apiserver-side (client-go) scheme.
	utilruntime.Must(clientgoscheme.AddToScheme(clientgoscheme.Scheme))
	utilruntime.Must(v1beta1.AddToScheme(clientgoscheme.Scheme))
	testEnv.Scheme = clientgoscheme.Scheme

	restCfg, err := testEnv.Start()
	if err != nil {
		return nil, nil, fmt.Errorf("starting envtest: %w", err)
	}
	log.Info("envtest started",
		"maxRequestsInflight", *maxRequestsInflight,
		"maxMutatingRequestsInflight", *maxMutatingRequestsInflight)
	return testEnv, restCfg, nil
}

// startManager builds the controller-runtime manager and wires exactly
// three controllers: the real ISVC reconciler (the subject under test),
// the fake executor, and the readiness observer.
func startManager(ctx context.Context, restCfg *rest.Config, rec *recorder.Recorder) (ctrl.Manager, error) {
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
		Controller: crconfig.Controller{
			SkipNameValidation: ptr.To(true),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating manager: %w", err)
	}

	// The InferenceService reconciler — the system under test. A Noop
	// traffic translator pins behavior independent of which Gateway CRDs
	// happen to be installed, so the harness measures the core reconcile
	// path, not traffic translation. The reconciler's concurrency is
	// whatever its own SetupWithManager configures; the harness imports
	// that wiring verbatim per scope and cannot tune it here.
	if err := components.SetupInferenceService(ctx, mgr,
		components.WithTrafficTranslator(translators.NewNoop())); err != nil {
		return nil, fmt.Errorf("wiring InferenceService reconciler: %w", err)
	}

	if err := executor.New(mgr.GetClient(), *executorConcurrency, time.Now).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("wiring executor: %w", err)
	}
	if err := observer.New(rec).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("wiring observer: %w", err)
	}
	return mgr, nil
}

func ensureNamespace(ctx context.Context, c client.Client, name string) error {
	ns := &corev1.Namespace{}
	ns.Name = name
	err := c.Create(ctx, ns)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// writeCmdStats records the run's wall time plus best-effort user/sys CPU
// and peak RSS (via getrusage, in rusage_*.go). cmdResourceUsage returns
// zeroes + Maxrss=-1 on platforms where getrusage is unavailable.
func writeCmdStats(path string, wallMs int64) error {
	cs := cmdResourceUsage()
	cs.WallMs = wallMs
	return stats.Write(path, cs)
}
