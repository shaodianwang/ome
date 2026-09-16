// Command runner drives the OMENative / InferenceReplica (IR) pod-reconcile
// performance/scale harness.
//
// It boots an envtest control plane with raised apiserver request limits,
// installs OME's CRDs (plus PodMonitor, KEDA, and scheduler-plugins
// PodGroup — all reconciled or discovered by the OMENative path), wires the
// real InferenceService reconciler AND the real InferenceReplica reconciler
// (the two halves of the IR-managed OMENative path), then generates a
// data-driven load of OMENative InferenceServices across one or more size
// classes (single-pod replicas + leader/worker gangs).
//
// Unlike the InferenceService harness — whose fake executor sets DEPLOYMENT
// status at a constant per-ISVC cost — OMENative manages PODS directly, so
// the harness drives REAL pods to Ready with a fake kubelet: the
// integration PodKubeletSimulator (binds + stamps ContainersReady) +
// EndpointSliceSimulator (synthesizes Ready endpoints), which together let
// the serving-gate pipeline converge and the IR report ReadyReplicas ==
// Replicas. The recorder measures time-to-Ready per class; a metrics reader
// folds the controller-runtime / OME prometheus collectors into the summary
// (apiserver op counts, 409 status-conflict rate, reconcile/workqueue
// churn, post-convergence idle reconcile rate). On completion it writes
// summary.yaml + cmd.stats.yaml into the output directory, which the checker
// test asserts against.
//
// Everything behavioral is a flag or comes from the generator YAML; there
// are no magic in-code defaults for replica counts, intervals, image refs,
// or limits.
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
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	inferencereplica "github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	v1beta1isvc "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
	"github.com/ome-projects/ome/tests/perf/internal/opcount"
	"github.com/ome-projects/ome/tests/perf/omenative/runner/config"
	"github.com/ome-projects/ome/tests/perf/omenative/runner/generator"
	"github.com/ome-projects/ome/tests/perf/omenative/runner/metricsobs"
	"github.com/ome-projects/ome/tests/perf/omenative/runner/observer"
	"github.com/ome-projects/ome/tests/perf/omenative/runner/recorder"
	"github.com/ome-projects/ome/tests/perf/omenative/runner/stats"
)

var (
	outputDir       = flag.String("o", "", "output directory for summary.yaml + cmd.stats.yaml (required)")
	generatorConfig = flag.String("generator-config", "", "path to the generator config YAML (required)")
	projectRoot     = flag.String("project-root", "", "absolute path to the OME repo root (for CRD discovery); defaults to the cwd")
	loadNamespace   = flag.String("namespace", "perf-omenative", "namespace the harness creates InferenceServices in")

	timeout = flag.Duration("timeout", 10*time.Minute, "max time to wait for the run to converge")

	// Client / apiserver throughput knobs. The defaults are intentionally
	// modest; the make target raises them per scenario. These govern how
	// hard the runner can push the apiserver, not OME behavior.
	qps   = flag.Float64("qps", 0, "rest client QPS for the runner's clients; 0 keeps the client-go default")
	burst = flag.Int("burst", 0, "rest client burst for the runner's clients; 0 keeps the client-go default")

	// Apiserver request-limit overrides. Under a large run the default
	// kube-apiserver max-requests-inflight (400) / max-mutating (200) can
	// saturate and surface as "http2: client connection lost"; raise them.
	maxRequestsInflight         = flag.Int("max-requests-inflight", 3000, "envtest apiserver --max-requests-inflight")
	maxMutatingRequestsInflight = flag.Int("max-mutating-requests-inflight", 1500, "envtest apiserver --max-mutating-requests-inflight")

	// Reconciler concurrency. Both the ISVC and IR reconcilers serialize
	// per object key, so distinct objects reconcile in parallel safely.
	// Config-driven (no in-code default in the reconcilers); the harness
	// sets it so the controllers keep up with the generated fan-out.
	isvcConcurrency = flag.Int("isvc-concurrency", 10, "MaxConcurrentReconciles for the InferenceService reconciler")
	irConcurrency   = flag.Int("ir-concurrency", 10, "MaxConcurrentReconciles for the InferenceReplica reconciler")
	configCacheTTL  = flag.Duration("config-cache-ttl", 0, "InferenceReplica operator-config cache TTL; 0 disables caching")

	// Recorder intake buffer. Bounded so a saturated recorder drops (and
	// counts) events rather than back-pressuring the observer.
	recorderBuffer = flag.Int("recorder-buffer", 8192, "bounded intake-channel depth for the recorder")

	// Steady-state idle-rate sampling. After convergence the harness waits
	// idleSettleDelay for the convergence tail (status roll-ups, HPA mirror)
	// to drain, then samples controller_runtime_reconcile_total twice
	// idleSampleWindow apart and reports the delta/sec — the storm guard.
	// Both config-driven (no magic in-code value).
	idleSettleDelay  = flag.Duration("idle-settle-delay", 4*time.Second, "post-convergence settle wait before sampling the steady-state idle reconcile rate")
	idleSampleWindow = flag.Duration("idle-sample-window", 3*time.Second, "window over which the steady-state idle reconcile rate is sampled")
)

var scheme = apiruntime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(autoscalingv2.AddToScheme(scheme))
	utilruntime.Must(discoveryv1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	// PodMonitor: the ISVC reconcile path creates one, so its type must be
	// in the scheme or SetControllerReference fails.
	utilruntime.Must(monitoringv1.AddToScheme(scheme))
	// KEDA ScaledObject: both the ISVC and IR reconcilers Owns it
	// unconditionally, so the scheme must know the type or the controllers'
	// watches fail to start.
	utilruntime.Must(kedav1.AddToScheme(scheme))
	// scheduler-plugins PodGroup: the IR reconciler creates a PodGroup per
	// multi-pod (gang) Instance once the CRD is discovered. Registering the
	// type lets gang classes co-schedule; absent it, gangs would stamp
	// GangSchedulingUnavailable.
	utilruntime.Must(schedulingv1alpha1.AddToScheme(scheme))
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
	log.Info("loaded generator config",
		"classes", len(cfg.Classes), "isvcs", cfg.Total(), "pods", cfg.TotalPods())

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
	// Typed clientset for the PodBinder (the binding subresource the typed
	// client.Client does not expose).
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building clientset: %w", err)
	}

	if err := util.SeedDefaultConfigMaps(ctx, directClient); err != nil {
		return fmt.Errorf("seeding config maps: %w", err)
	}
	if err := ensureNamespace(ctx, directClient, *loadNamespace); err != nil {
		return fmt.Errorf("creating load namespace: %w", err)
	}

	podsPerClass := map[string]int32{}
	for _, cl := range cfg.Classes {
		podsPerClass[cl.ClassName] = int32(cl.PodsPerISVC())
	}
	rec := recorder.New(*timeout, cfg.Total(), cfg.TotalPods(), *recorderBuffer, podsPerClass)
	gen := generator.New(directClient, cfg, *loadNamespace, rec)

	// Shared op-count accumulator. The IR reconciler — the pod-churn path
	// the OMENative harness targets — is constructed with opcount-wrapped
	// client + reader, so every pod LIST / create / status-update it issues
	// is counted. (The ISVC reconciler's reads via its internal
	// RuntimeSelector and its APIReader, which its SetupWithManager rebinds
	// to the manager's, are not all wrapped — see startManager.)
	counts := opcount.NewCounts()

	mgr, err := startManager(ctx, restCfg, rec, gen, counts)
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

	// Fake kubelet: bind pods, stamp ContainersReady, synthesize Ready
	// EndpointSlices — the integration simulators, run here against the
	// envtest cluster so OMENative pods reach Ready and each IR converges
	// ReadyReplicas. Driven off the manager's cached client (same client
	// the controllers see), started after cache sync.
	simCtx, cancelSims := context.WithCancel(ctx)
	defer cancelSims()
	binder := util.NewPodBinder(clientset)
	kubeletSim := util.NewPodKubeletSimulator(mgr.GetClient(), binder)
	// Mirror Spec->Status container images so any in-place convergence the
	// IR path waits on can complete (harmless for cold-create; matches the
	// coordination suites' BeforeSuite).
	kubeletSim.SetImageStatusMirror(true)
	sliceSim := util.NewEndpointSliceSimulator(mgr.GetClient())
	kubeletSim.Start(simCtx)
	sliceSim.Start(simCtx)
	defer func() {
		kubeletSim.Stop()
		sliceSim.Stop()
	}()

	// Recorder loop. Run returns when every InferenceService is Ready (its
	// IR reports ReadyReplicas == Replicas) or the timeout trips.
	recDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		recDone <- rec.Run(ctx)
	}()

	// Generate the load. The generator stamps creation times into the
	// recorder as it goes.
	startWall := time.Now()
	if err := gen.Generate(ctx); err != nil {
		cancelMgr()
		wg.Wait()
		return fmt.Errorf("generating load: %w", err)
	}

	runErr := <-recDone
	wallMs := time.Since(startWall).Milliseconds()

	// Stop the fake kubelet BEFORE sampling the idle reconcile rate. The
	// simulators poll (the EndpointSlice sim re-writes every slice each tick),
	// which fans out watch-driven re-reconciles that would mask OME's own
	// steady-state rate. Converged pods keep their Ready conditions once the
	// sims stop, so this does not regress readiness — it isolates the storm
	// guard to the controllers' own behavior.
	cancelSims()
	kubeletSim.Stop()
	sliceSim.Stop()

	// Steady-state idle reconcile rate: with the manager still running but the
	// simulators stopped, wait out the convergence tail then sample the
	// reconcile counter across the idle window. A hot-loop requeue bug shows
	// up as a non-trivial rate; a healthy converged fleet settles to ~0.
	// Skipped (left 0) if the run did not converge.
	if runErr == nil {
		if rate, err := sampleIdleReconcileRate(ctx, *idleSettleDelay, *idleSampleWindow); err != nil {
			log.Error(err, "sampling idle reconcile rate (non-fatal)")
		} else {
			rec.WithIdleReconcileRate(rate)
		}
	}

	// Read the prometheus-registry-derived metrics once, after the idle
	// sample, and fold them into the summary.
	if su, err := metricsobs.StatusUpdates(); err != nil {
		log.Error(err, "reading status-update metrics (non-fatal)")
	} else {
		rec.WithStatusUpdates(su)
	}
	if ch, err := metricsobs.Churn(); err != nil {
		log.Error(err, "reading churn metrics (non-fatal)")
	} else {
		rec.WithChurn(ch)
	}
	rec.WithAPIOps(counts.Flat(), counts.Total())

	// Tear the manager down before writing artifacts so no late status flip
	// races the summary fold. (Simulators already stopped above.)
	cancelMgr()
	wg.Wait()

	// Producers are now stopped; fold any pod-lifecycle events still
	// buffered when Run exited on ISVC convergence into the summary.
	rec.Drain()

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
	log.Info("run complete",
		"wallMs", wallMs,
		"apiOps", counts.Total(),
		"summary", filepath.Join(*outputDir, "summary.yaml"))
	return nil
}

// startEnvtest brings up the control plane with raised request limits and
// OME + PodMonitor + KEDA + PodGroup CRDs installed.
func startEnvtest(ctx context.Context, root string) (*envtest.Environment, *rest.Config, error) {
	log := ctrl.LoggerFrom(ctx)

	depCRDs := filepath.Join(root, "tests", "integration", "dep-crds")
	crdPaths := []string{
		filepath.Join(root, "config", "crd", "full"),
		// PodMonitor CRD: the OMENative ISVC path creates one. Vendored
		// under dep-crds (gitignored; re-vendored by `make dep-crds`).
		filepath.Join(depCRDs, "prometheus-operator"),
		// KEDA CRDs (ScaledObject et al.): both reconcilers Owns
		// ScaledObject unconditionally, so the informer needs the CRD
		// installed or cache sync wedges.
		filepath.Join(depCRDs, "keda"),
		// scheduler-plugins PodGroup CRD: the IR reconciler discovers it at
		// setup and creates a PodGroup per gang Instance. Required so gang
		// classes co-schedule instead of stamping GangSchedulingUnavailable.
		filepath.Join(depCRDs, "scheduler-plugins"),
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

// startManager builds the controller-runtime manager and wires the real
// InferenceService + InferenceReplica reconcilers (the two halves of the
// OMENative IR-managed path), plus the watch-only observer. The OMENative
// pod field index is registered before Start so the IR reconciler's pod
// LISTs resolve through the index.
func startManager(ctx context.Context, restCfg *rest.Config, rec *recorder.Recorder, gen *generator.Generator, counts *opcount.Counts) (ctrl.Manager, error) {
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

	// OMENative pod field index — the IR reconciler's cache fast path for
	// resolving an Instance's pods. Registered exactly as cmd/manager does.
	if err := query.RegisterOMENativePodIndex(context.Background(), mgr.GetFieldIndexer()); err != nil {
		return nil, fmt.Errorf("registering OMENative pod index: %w", err)
	}

	// opcount-wrapped client + reader, shared by both reconcilers' struct
	// fields so the ops they issue through those handles are counted.
	countingClient := opcount.NewClient(mgr.GetClient(), counts)
	countingReader := opcount.NewReader(mgr.GetAPIReader(), counts)

	if err := setupInferenceService(mgr, countingClient); err != nil {
		return nil, fmt.Errorf("wiring InferenceService reconciler: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("creating InferenceReplica clientset: %w", err)
	}
	podBatchSizes, err := controllerconfig.LoadPodBatchSizes(clientset)
	if err != nil {
		return nil, fmt.Errorf("loading lifecycle scale settings: %w", err)
	}
	statusConfig, err := controllerconfig.NewOMENativeStatusConfig(clientset)
	if err != nil {
		return nil, fmt.Errorf("loading omenativeStatus configuration: %w", err)
	}

	// InferenceReplica reconciler — the pod lifecycle controller, the
	// subject the OMENative harness measures. Constructed directly (not via
	// a components helper) so its Client + APIReader are the opcount-wrapped
	// handles; its SetupWithManager does not rebind either field, so every
	// pod op it issues is counted.
	ir := &inferencereplica.Reconciler{
		Client:                   countingClient,
		Clientset:                clientset,
		Log:                      ctrl.Log.WithName("InferenceReplica"),
		APIReader:                countingReader,
		InstanceStatusDecoder:    irstatus.NewDecoder(statusConfig.DecodeBound()),
		InstanceStatusTarget:     statusConfig.InstanceStatusEncoding,
		Recorder:                 mgr.GetEventRecorderFor("InferenceReplica"),
		Expectations:             workload.NewExpectations(),
		MaxConcurrentReconciles:  *irConcurrency,
		ConfigCacheTTL:           *configCacheTTL,
		ScaleUpPodBatchSize:      podBatchSizes.ScaleUp,
		ScaleDownPodBatchSize:    podBatchSizes.ScaleDown,
		ScaleDownRequeueInterval: podBatchSizes.ScaleDownRequeueInterval,
	}
	if err := ir.SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("wiring InferenceReplica reconciler: %w", err)
	}

	if err := observer.New(rec, gen).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("wiring observer: %w", err)
	}
	return mgr, nil
}

// setupInferenceService constructs the InferenceService reconciler with the
// opcount-wrapped client and registers it. Mirrors
// tests/integration/components.SetupInferenceService, but builds the
// reconciler inline so the wrapped client lands on the struct (the helper
// hardcodes mgr.GetClient()). A Noop traffic translator pins behavior
// independent of which Gateway CRDs are installed, so the harness measures
// the core reconcile path, not traffic translation.
//
// Caveat: the reconciler's SetupWithManager rebinds APIReader to
// mgr.GetAPIReader() and builds its RuntimeSelector on mgr.GetClient(), so
// those two read paths bypass the op counter. The IR reconciler — the pod
// path the harness targets — is fully wrapped; the ISVC op counts are a
// lower bound.
func setupInferenceService(mgr ctrl.Manager, countingClient client.Client) error {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}
	translator := traffic.Translator(translators.NewNoop())
	trafficReconciler := traffic.NewReconciler(countingClient, mgr.GetScheme(), translator)

	statusConfig, err := controllerconfig.NewOMENativeStatusConfig(clientset)
	if err != nil {
		return fmt.Errorf("load omenativeStatus configuration: %w", err)
	}
	r := &v1beta1isvc.InferenceServiceReconciler{
		Client:                  countingClient,
		InstanceStatusDecoder:   irstatus.NewDecoder(statusConfig.DecodeBound()),
		Clientset:               clientset,
		Log:                     ctrl.Log.WithName("InferenceService"),
		Scheme:                  mgr.GetScheme(),
		Recorder:                mgr.GetEventRecorderFor("InferenceService"),
		Expectations:            omenative.NewExpectations(),
		TrafficReconciler:       trafficReconciler,
		MaxConcurrentReconciles: *isvcConcurrency,
	}
	return r.SetupWithManager(mgr)
}

// sampleIdleReconcileRate measures reconciles/sec over window by reading the
// controller_runtime_reconcile_total counter at the window's start and end,
// after first waiting settle for the convergence tail to drain. Called with
// the manager + simulators still running, so a converged fleet should report
// near-zero and a requeue-storm bug a non-trivial rate. The simulators poll
// (kubelet + endpointslice) so a small non-zero floor from watch-driven
// re-reconciles is expected — the storm guard catches orders-of-magnitude
// excess, not that floor.
func sampleIdleReconcileRate(ctx context.Context, settle, window time.Duration) (float64, error) {
	if err := sleepCtx(ctx, settle); err != nil {
		return 0, err
	}
	start, err := metricsobs.TotalReconciles()
	if err != nil {
		return 0, err
	}
	if err := sleepCtx(ctx, window); err != nil {
		return 0, err
	}
	end, err := metricsobs.TotalReconciles()
	if err != nil {
		return 0, err
	}
	delta := end - start
	if delta < 0 {
		delta = 0
	}
	return float64(delta) / window.Seconds(), nil
}

// sleepCtx waits d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
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
