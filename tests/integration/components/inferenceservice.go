package components

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	v1beta1isvccontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	trafficfactory "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/factory"
)

// InferenceServiceOption mutates the ISVC reconciler before it is
// registered. Functional-options pattern keeps SetupInferenceService's
// signature stable as the reconciler accumulates dependencies.
type InferenceServiceOption func(*inferenceServiceOpts)

type inferenceServiceOpts struct {
	deployConfig       *controllerconfig.DeployConfig
	ingressConfig      *controllerconfig.IngressConfig
	modelCacheProvider sharded.Provider
	trafficTranslator  traffic.Translator
}

// WithDeployConfig overrides the default DeployConfig (which is
// created with DefaultDeploymentMode = RawDeployment, the safest
// choice for tests that don't care about the cluster default).
func WithDeployConfig(cfg *controllerconfig.DeployConfig) InferenceServiceOption {
	return func(o *inferenceServiceOpts) { o.deployConfig = cfg }
}

// WithIngressConfig overrides the default IngressConfig (which is
// created with DisableIngressCreation=true so test pods don't try to
// reach a non-existent ingress controller).
func WithIngressConfig(cfg *controllerconfig.IngressConfig) InferenceServiceOption {
	return func(o *inferenceServiceOpts) { o.ingressConfig = cfg }
}

// WithISVCModelCacheProvider sets the ISVC reconciler's ModelCache.
// Distinct from basemodel's WithModelCacheProvider because both
// reconcilers carry their own ModelCache field. Suites that exercise
// Sharded models need this — the provider's Name flows into the
// runtimeselector config, and its Config flows into the env-injection
// path that components/base.go reads.
func WithISVCModelCacheProvider(p sharded.Provider) InferenceServiceOption {
	return func(o *inferenceServiceOpts) { o.modelCacheProvider = p }
}

// WithTrafficTranslator pins the active OEP-0011 traffic translator
// for the test. When unset, SetupInferenceService runs the factory
// against the manager's rest.Config so the translator matches what
// the cluster's installed CRDs would yield in production (Envoy
// Gateway when the BTP CRD is loaded via framework.EnvoyGateway,
// Noop otherwise).
func WithTrafficTranslator(tr traffic.Translator) InferenceServiceOption {
	return func(o *inferenceServiceOpts) { o.trafficTranslator = tr }
}

// SetupInferenceService registers the InferenceServiceReconciler with
// the supplied manager. Constructs a kubernetes.Clientset from the
// manager's rest.Config (the reconciler reads ConfigMaps via Clientset
// rather than the cached client). Event recorder uses
// mgr.GetEventRecorderFor — sufficient for tests, no broadcaster sink.
//
// Default DeployConfig.DefaultDeploymentMode is RawDeployment.
// Default IngressConfig disables ingress creation so the controller
// doesn't try to materialize Knative routes / Istio VirtualServices in
// suites that haven't installed those CRDs.
//
// Test suites that exercise different defaults supply WithDeployConfig
// or WithIngressConfig.
func SetupInferenceService(ctx context.Context, mgr ctrl.Manager, opts ...InferenceServiceOption) error {
	cfg := &inferenceServiceOpts{}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.deployConfig == nil {
		cfg.deployConfig = &controllerconfig.DeployConfig{
			DefaultDeploymentMode: "RawDeployment",
		}
	}
	if cfg.ingressConfig == nil {
		cfg.ingressConfig = &controllerconfig.IngressConfig{
			IngressGateway:         "knative-serving/knative-ingress-gateway",
			IngressServiceName:     "istio-ingressgateway.istio-system.svc.cluster.local",
			IngressDomain:          "svc.cluster.local",
			DomainTemplate:         controllerconfig.DefaultDomainTemplate,
			UrlScheme:              controllerconfig.DefaultUrlScheme,
			DisableIngressCreation: true,
		}
	}

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("SetupInferenceService: build clientset: %w", err)
	}

	translator := cfg.trafficTranslator
	if translator == nil {
		tr, err := trafficfactory.New(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("SetupInferenceService: traffic factory: %w", err)
		}
		translator = tr
	}
	trafficReconciler := traffic.NewReconciler(mgr.GetClient(), mgr.GetScheme(), translator)

	r := &v1beta1isvccontroller.InferenceServiceReconciler{
		Client:            mgr.GetClient(),
		Clientset:         clientset,
		Log:               ctrl.Log.WithName("InferenceService"),
		Scheme:            mgr.GetScheme(),
		Recorder:          mgr.GetEventRecorderFor("InferenceService"),
		ModelCache:        cfg.modelCacheProvider,
		TrafficReconciler: trafficReconciler,
	}
	if err := r.SetupWithManager(mgr, cfg.deployConfig, cfg.ingressConfig); err != nil {
		return fmt.Errorf("SetupInferenceService: SetupWithManager: %w", err)
	}
	return nil
}
