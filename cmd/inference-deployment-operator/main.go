package main

import (
	"context"
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	omev1alpha1 "github.com/ome-projects/ome/pkg/apis/ome/v1alpha1"
	omev1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	inferencedeployment "github.com/ome-projects/ome/pkg/controller/v1alpha1/inferencedeployment"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(omev1alpha1.AddToScheme(scheme))
	utilruntime.Must(omev1beta1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElect bool
	var clusterCredentialNamespace string

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&clusterCredentialNamespace, "cluster-credential-namespace", defaultClusterCredentialNamespace(), "Namespace containing workload cluster credential Secrets.")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	restConfig := ctrl.GetConfigOrDie()
	clusterRegistry, err := loadClusterRegistry(context.Background(), restConfig, clusterCredentialNamespace)
	if err != nil {
		setupLog.Error(err, "Failed to load cluster registry")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "inference-deployment-operator.ome.io",
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	remoteInferenceDeploymentEvents := make(chan event.GenericEvent, inferencedeployment.DefaultRemoteInferenceServiceEventBufferSize)
	if err := (&inferencedeployment.InferenceDeploymentReconciler{
		Client:                          mgr.GetClient(),
		Log:                             ctrl.Log.WithName("controllers").WithName("InferenceDeployment"),
		Scheme:                          mgr.GetScheme(),
		NewRemoteClient:                 clusterRegistry.ClientFor,
		RemoteInferenceDeploymentEvents: remoteInferenceDeploymentEvents,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create InferenceDeployment controller")
		os.Exit(1)
	}
	if err := mgr.Add(inferencedeployment.NewRemoteInferenceServiceWatcher(
		clusterRegistry.RestConfigs(),
		remoteInferenceDeploymentEvents,
		ctrl.Log.WithName("remote-watch").WithName("InferenceService"),
	)); err != nil {
		setupLog.Error(err, "Failed to create remote InferenceService watcher")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Problem running manager")
		os.Exit(1)
	}
}

func loadClusterRegistry(ctx context.Context, restConfig *rest.Config, namespace string) (*inferencedeployment.ClusterRegistry, error) {
	registryClient, err := ctrlclient.New(restConfig, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	return inferencedeployment.LoadClusterRegistryFromSecrets(ctx, registryClient, namespace, scheme)
}

func defaultClusterCredentialNamespace() string {
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		return namespace
	}
	return "inference-deployment-system"
}
