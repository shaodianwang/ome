package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	zaplog "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
	schedulerpluginsv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	volcanobatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	volcano "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1basemodelcontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	v1beta1inferencereplicacontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	v1beta1isvccontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	trafficfactory "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/factory"
	v1beta1runtimerevisioncontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/runtimerevision"
	v1beta1servingruntimecontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/servingruntime"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	"github.com/ome-projects/ome/pkg/utils"
	"github.com/ome-projects/ome/pkg/version"
	basemodelwebhook "github.com/ome-projects/ome/pkg/webhook/admission/basemodel"
	inferencereplicawebhook "github.com/ome-projects/ome/pkg/webhook/admission/inferencereplica"
	"github.com/ome-projects/ome/pkg/webhook/admission/isvc"
	"github.com/ome-projects/ome/pkg/webhook/admission/pod"
	"github.com/ome-projects/ome/pkg/webhook/admission/runtimepreset"
	runtimerevisionwebhook "github.com/ome-projects/ome/pkg/webhook/admission/runtimerevision"
	"github.com/ome-projects/ome/pkg/webhook/admission/servingruntime"
)

const (
	LeaderLockName          = "ome-controller-manager-leader-lock"
	LeaderElectionNamespace = "ome"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	tlsOpts  []func(*tls.Config)
)

// registerOptionalScheme attempts to register a scheme if its CRD is available
func registerOptionalScheme(cfg *rest.Config, s *runtime.Scheme, groupVersion schema.GroupVersion, kind string, addToScheme func(*runtime.Scheme) error) error {
	found, err := utils.IsCrdAvailable(cfg, groupVersion.String(), kind)
	if err != nil {
		return fmt.Errorf("error checking if %s kind is available: %w", kind, err)
	}
	if found {
		setupLog.Info("Setting up scheme", "groupVersion", groupVersion.String(), "kind", kind)
		if err := addToScheme(s); err != nil {
			return fmt.Errorf("unable to add %s APIs to scheme: %w", kind, err)
		}
	}
	return nil
}

func init() {
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(schedulerpluginsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kueuev1beta1.AddToScheme(scheme))
}

// Options defines the program-configurable options that may be passed on the command line.
type Options struct {
	metricsAddr                string
	secureMetrics              bool
	enableHTTP2                bool
	webhookPort                int
	enableLeaderElection       bool
	enableWebhook              bool
	probeAddr                  string
	leaderElectionNamespace    string
	runtimeRevisionRetention   int
	runtimeRevisionGracePeriod time.Duration
	enableInferenceReplicaCtrl bool
	// disableIRManagedOmenative is the cluster-operator kill switch
	// for the IR-managed OMENative dispatch path. When true, the ISVC
	// controller routes every OMENative-mode Component through the
	// legacy omenative direct path instead of the InferenceReplica
	// projector. Defaults false: OMENative-mode Components flow through
	// the IR-managed path automatically.
	disableIRManagedOmenative bool
	zapOpts                   zap.Options
}

// DefaultOptions returns the default values for the program options.
func DefaultOptions() Options {
	return Options{
		metricsAddr:                ":8080",
		webhookPort:                9443,
		enableLeaderElection:       false,
		enableWebhook:              false,
		enableHTTP2:                false,
		secureMetrics:              false,
		probeAddr:                  ":8081",
		leaderElectionNamespace:    LeaderElectionNamespace,
		runtimeRevisionRetention:   10,
		runtimeRevisionGracePeriod: 24 * time.Hour,
		// The InferenceReplica controller defaults ON. The reconciler
		// drives the per-Component pipeline via workload.Reconcile when
		// an IR is created directly (PR 3 wires the ISVC controller to
		// project IRs). The flag exists so operators can disable the
		// controller if a regression lands.
		enableInferenceReplicaCtrl: true,
		// OMENative-mode Components route through the IR-managed
		// path by default. The kill switch is opt-out (flip to true
		// to force every OMENative Component back to the omenative
		// direct path).
		disableIRManagedOmenative: false,
		zapOpts: zap.Options{
			TimeEncoder: zapcore.RFC3339TimeEncoder,
			ZapOpts:     []zaplog.Option{zaplog.AddCaller()},
		},
	}
}

// GetOptions parses the program flags and returns them as Options.
func GetOptions() Options {
	opts := DefaultOptions()
	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", opts.metricsAddr, "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.BoolVar(&opts.secureMetrics, "metrics-secure", opts.secureMetrics,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.BoolVar(&opts.enableHTTP2, "enable-http2", opts.enableHTTP2,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.IntVar(&opts.webhookPort, "webhook-port", opts.webhookPort, "The port that the webhook server binds to.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", opts.enableLeaderElection,
		"Enable leader election for ome controller manager. "+
			"Enabling this will ensure there is only one active ome controller manager.")
	flag.StringVar(&opts.leaderElectionNamespace, "leader-election-namespace", opts.leaderElectionNamespace, "The namespace in which the leader election configmap will be created.")
	flag.BoolVar(&opts.enableWebhook, "webhook", opts.enableWebhook, "Enable the webhook server.")
	flag.StringVar(&opts.probeAddr, "health-probe-addr", opts.probeAddr, "The address the probe endpoint binds to.")
	flag.IntVar(&opts.runtimeRevisionRetention, "runtime-revision-retention", opts.runtimeRevisionRetention,
		"Max OME-owned ControllerRevisions to keep per source runtime (older ones GC after the grace period if unreferenced).")
	flag.DurationVar(&opts.runtimeRevisionGracePeriod, "runtime-revision-grace-period", opts.runtimeRevisionGracePeriod,
		"How long an OME-owned ControllerRevision stays after first being observed unreferenced + over retention.")
	flag.BoolVar(&opts.enableInferenceReplicaCtrl, "enable-inferencereplica-controller", opts.enableInferenceReplicaCtrl,
		"Run the InferenceReplica controller. Defaults true; flip to false to disable if the per-IR lifecycle code ships a regression.")
	flag.BoolVar(&opts.disableIRManagedOmenative, "disable-ir-managed-omenative", opts.disableIRManagedOmenative,
		"Cluster-wide kill switch for the IR-managed OMENative dispatch path. When true, the ISVC controller routes every OMENative-mode Component through the legacy omenative direct path instead of projecting an InferenceReplica. Defaults false (OMENative-mode Components flow through the IR-managed path automatically). Operator escape hatch for an IR-path regression — flip to true to immediately route every workload back to the legacy path without touching per-ISVC specs.")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}

func main() {
	options := GetOptions()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options.zapOpts)))

	setupLog.Info("Initializing", "gitVersion", version.GitVersion, "gitCommit", version.GitCommit)

	// Wire the IR-managed kill switch into the irprojector package's
	// process-wide flag. Single load per dispatch (atomic int32) so the
	// per-Component predicate check stays cheap. Logged at startup so
	// the operator-visible state is obvious from the manager's startup
	// banner.
	irprojector.SetGlobalDisableIRManaged(options.disableIRManagedOmenative)
	if options.disableIRManagedOmenative {
		setupLog.Info("IR-managed OMENative path DISABLED via --disable-ir-managed-omenative; OMENative-mode Components will route through the omenative direct path")
	}

	// Get a config to talk to the apiserver
	setupLog.Info("Configuring API client connection")
	cfg := ctrl.GetConfigOrDie()

	// Setup clientset to directly talk to the api server
	setupLog.Info("Creating Kubernetes client set")
	clientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "Failed to create Kubernetes client set")
		os.Exit(1)
	}

	if !options.enableHTTP2 {
		// if the enable-http2 flag is false (the default), http/2 should be disabled
		// due to its vulnerabilities. More specifically, disabling http/2 will
		// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
		// Rapid Reset CVEs. For more information see:
		// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
		// - https://github.com/advisories/GHSA-4374-p667-p6c8
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			setupLog.Info("disabling http/2")
			c.NextProtos = []string{"http/1.1"}
		})
	}

	// Create a new Cmd to provide shared dependencies and start components
	setupLog.Info("Initializing controller manager",
		"metricsAddr", options.metricsAddr,
		"webhookPort", options.webhookPort,
		"leaderElection", options.enableLeaderElection)
	mgr, err := manager.New(cfg, manager.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   options.metricsAddr,
			TLSOpts:       tlsOpts,
			SecureServing: options.secureMetrics,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    options.webhookPort,
			TLSOpts: tlsOpts,
		}),
		LeaderElection:          options.enableLeaderElection,
		LeaderElectionID:        LeaderLockName,
		LeaderElectionNamespace: options.leaderElectionNamespace,
		HealthProbeBindAddress:  options.probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "Failed to initialize controller manager")
		os.Exit(1)
	}

	deployConfig, err := controllerconfig.NewDeployConfig(clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to initialize deployment configuration")
		os.Exit(1)
	}
	ingressConfig, err := controllerconfig.NewIngressConfig(clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to initialize ingress configuration")
		os.Exit(1)
	}
	modelCacheConfig, err := controllerconfig.NewModelCacheConfig(clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to initialize model cache configuration")
		os.Exit(1)
	}
	modelCacheProvider, err := sharded.BuildProvider(*modelCacheConfig)
	if err != nil {
		setupLog.Error(err, "Failed to initialize model cache provider")
		os.Exit(1)
	}
	if modelCacheProvider.Configured() {
		setupLog.Info("Configured model cache provider", "provider", modelCacheProvider.Name)
	} else {
		setupLog.Info("Model cache provider is not configured")
	}
	omeAgentConfig, err := controllerconfig.NewOmeAgentConfig(clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to initialize ome-agent configuration")
		os.Exit(1)
	}

	// Register optional schemes based on CRD availability
	setupLog.Info("Registering optional CRD schemes")
	optionalSchemes := []struct {
		groupVersion schema.GroupVersion
		kind         string
		addToScheme  func(*runtime.Scheme) error
	}{
		{lws.SchemeGroupVersion, constants.LWSKind, lws.AddToScheme},
		{volcano.SchemeGroupVersion, constants.VolcanoQueueKind, volcano.AddToScheme},
		{volcanobatch.SchemeGroupVersion, constants.VolcanoJobKind, volcanobatch.AddToScheme},
		{kedav1.SchemeGroupVersion, constants.KEDAScaledObjectKind, kedav1.AddToScheme},
		{monitoringv1.SchemeGroupVersion, constants.PodMonitorKind, monitoringv1.AddToScheme},
	}

	for _, s := range optionalSchemes {
		if err := registerOptionalScheme(cfg, mgr.GetScheme(), s.groupVersion, s.kind, s.addToScheme); err != nil {
			setupLog.Error(err, "Failed to register optional scheme",
				"groupVersion", s.groupVersion.String(),
				"kind", s.kind)
			os.Exit(1)
		}
	}

	if ingressConfig.EnableGatewayAPI {
		setupLog.Info("Registering Gateway API scheme")
		utilruntime.Must(gatewayapiv1.Install(mgr.GetScheme()))
	}

	// OEP-0010: select the active traffic translator based on
	// installed backend-policy CRDs (Envoy Gateway BackendTrafficPolicy,
	// Istio DestinationRule, else Noop). The selection is process-
	// level — the translator stays stable for the controller's
	// lifetime — so we build it once at startup.
	setupLog.Info("Selecting traffic translator")
	trafficTranslator, err := trafficfactory.New(cfg)
	if err != nil {
		setupLog.Error(err, "Failed to select traffic translator")
		os.Exit(1)
	}
	setupLog.Info("Selected traffic translator", "translator", trafficTranslator.Name())
	trafficReconciler := traffic.NewReconciler(mgr.GetClient(), mgr.GetScheme(), trafficTranslator)

	// Setup Event Broadcaster
	setupLog.Info("Configuring event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	setupLog.Info("Setting up InferenceService controller")
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
	if err = (&v1beta1isvccontroller.InferenceServiceReconciler{
		Client:            mgr.GetClient(),
		Clientset:         clientSet,
		Log:               ctrl.Log.WithName("InferenceService"),
		Scheme:            mgr.GetScheme(),
		Recorder:          eventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"}),
		ModelCache:        modelCacheProvider,
		TrafficReconciler: trafficReconciler,
	}).SetupWithManager(mgr, deployConfig, ingressConfig); err != nil {
		setupLog.Error(err, "Failed to create InferenceService controller")
		os.Exit(1)
	}

	setupLog.Info("Setting up BaseModel controller")
	if err = (&v1beta1basemodelcontroller.BaseModelReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("BaseModel"),
		Scheme:         mgr.GetScheme(),
		ModelCache:     modelCacheProvider,
		OmeAgentConfig: omeAgentConfig,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create BaseModel controller")
		os.Exit(1)
	}

	setupLog.Info("Setting up ClusterBaseModel controller")
	if err = (&v1beta1basemodelcontroller.ClusterBaseModelReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("ClusterBaseModel"),
		Scheme:         mgr.GetScheme(),
		ModelCache:     modelCacheProvider,
		OmeAgentConfig: omeAgentConfig,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create ClusterBaseModel controller")
		os.Exit(1)
	}

	// OEP-0012: single inheritance reconciler that handles both CSR and
	// SR via one source per GVK. Reconcile branches on req.Namespace.
	servingRuntimeEventBroadcaster := record.NewBroadcaster()
	servingRuntimeEventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
	setupLog.Info("Setting up ServingRuntime inheritance controller")
	if err = (&v1beta1servingruntimecontroller.InheritanceReconciler{
		Client:   mgr.GetClient(),
		Log:      ctrl.Log.WithName("ServingRuntimeInheritance"),
		Scheme:   mgr.GetScheme(),
		Recorder: servingRuntimeEventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"}),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create ServingRuntime inheritance controller")
		os.Exit(1)
	}

	setupLog.Info("Setting up runtime-revision GC controller",
		"retention", options.runtimeRevisionRetention,
		"gracePeriod", options.runtimeRevisionGracePeriod)
	if err = (&v1beta1runtimerevisioncontroller.GCReconciler{
		Client:              mgr.GetClient(),
		Log:                 ctrl.Log.WithName("RuntimeRevisionGC"),
		Scheme:              mgr.GetScheme(),
		OMENamespace:        constants.OMENamespace,
		RetentionPerRuntime: options.runtimeRevisionRetention,
		GracePeriod:         options.runtimeRevisionGracePeriod,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create runtime-revision GC controller")
		os.Exit(1)
	}

	if options.enableInferenceReplicaCtrl {
		setupLog.Info("Setting up InferenceReplica controller")
		if err = (&v1beta1inferencereplicacontroller.Reconciler{
			Client:    mgr.GetClient(),
			Log:       ctrl.Log.WithName("InferenceReplica"),
			Scheme:    mgr.GetScheme(),
			APIReader: mgr.GetAPIReader(),
			Recorder:  eventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"}),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create InferenceReplica controller")
			os.Exit(1)
		}
	} else {
		setupLog.Info("InferenceReplica controller disabled via --enable-inferencereplica-controller=false")
	}

	if options.enableWebhook {
		setupLog.Info("Configuring webhook server", "port", options.webhookPort)
		hookServer := mgr.GetWebhookServer()

		setupLog.Info("Registering InferenceService webhook to the webhook server")
		hookServer.Register("/mutate-pods", &webhook.Admission{
			Handler: &pod.Mutator{Client: mgr.GetClient(), Clientset: clientSet, Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering cluster serving runtime validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-clusterservingruntime", &webhook.Admission{
			Handler: &servingruntime.ClusterServingRuntimeValidator{Client: mgr.GetClient(), Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering serving runtime validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-servingruntime", &webhook.Admission{
			Handler: &servingruntime.ServingRuntimeValidator{Client: mgr.GetClient(), Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering serving runtime engine-preset mutator webhooks to the webhook server")
		hookServer.Register("/mutate-ome-io-v1beta1-servingruntime-preset", &webhook.Admission{
			Handler: &runtimepreset.ServingRuntimeMutator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})
		hookServer.Register("/mutate-ome-io-v1beta1-clusterservingruntime-preset", &webhook.Admission{
			Handler: &runtimepreset.ClusterServingRuntimeMutator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering ControllerRevision immutability validator webhook to the webhook server")
		hookServer.Register("/validate-apps-v1-controllerrevision", &webhook.Admission{
			Handler: &runtimerevisionwebhook.ImmutabilityValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering BaseModel validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-basemodel", &webhook.Admission{
			Handler: &basemodelwebhook.BaseModelValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering ClusterBaseModel validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-clusterbasemodel", &webhook.Admission{
			Handler: &basemodelwebhook.ClusterBaseModelValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering InferenceReplica validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-inferencereplica", &webhook.Admission{
			Handler: &inferencereplicawebhook.Validator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		runtimeSelectorConfig := runtimeselector.NewConfig(mgr.GetClient())
		runtimeSelectorConfig.ModelCacheProvider = modelCacheProvider.Name
		runtimeSelector := runtimeselector.NewWithConfig(runtimeSelectorConfig)

		if err = ctrl.NewWebhookManagedBy(mgr).
			For(&v1beta1.InferenceService{}).
			WithDefaulter(&isvc.InferenceServiceDefaulter{
				Client:    mgr.GetClient(),
				ClientSet: clientSet,
			}).
			WithValidator(&isvc.InferenceServiceValidator{
				Client:          mgr.GetClient(),
				RuntimeSelector: runtimeSelector,
				// Reject ome.io/btp.* (Envoy Gateway) when the active
				// translator is Istio or Noop, and vice versa, at
				// admission. Sourced from the same translator the
				// reconciler uses so the two stay in sync.
				KnownPassthroughPrefixes: trafficTranslator.SupportedPassthroughPrefixes(),
			}).
			Complete(); err != nil {
			setupLog.Error(err, "Failed to create InferenceService webhook", "webhook", "v1beta1")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", func(req *http.Request) error {
		return mgr.GetWebhookServer().StartedChecker()(req)
	}); err != nil {
		setupLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", func(req *http.Request) error {
		return mgr.GetWebhookServer().StartedChecker()(req)
	}); err != nil {
		setupLog.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	// Start the Cmd
	setupLog.Info("Starting manager")
	managerCtx := signals.SetupSignalHandler()
	if modelCacheProvider.Configured() {
		if closeable, ok := modelCacheProvider.Client.(interface{ Close() error }); ok {
			go func() {
				<-managerCtx.Done()
				if err := closeable.Close(); err != nil {
					setupLog.Error(err, "Failed to close model cache provider")
				}
			}()
		}
	}
	if err := mgr.Start(managerCtx); err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}
}
