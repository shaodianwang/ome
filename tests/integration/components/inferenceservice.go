package components

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	v1beta1isvccontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice"
)

// InferenceServiceOption mutates the ISVC reconciler before it is
// registered. Functional-options pattern keeps SetupInferenceService's
// signature stable as the reconciler accumulates dependencies.
type InferenceServiceOption func(*inferenceServiceOpts)

type inferenceServiceOpts struct {
	deployConfig  *controllerconfig.DeployConfig
	ingressConfig *controllerconfig.IngressConfig
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

	r := &v1beta1isvccontroller.InferenceServiceReconciler{
		Client:    mgr.GetClient(),
		Clientset: clientset,
		Log:       ctrl.Log.WithName("InferenceService"),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("InferenceService"),
	}
	if err := r.SetupWithManager(mgr, cfg.deployConfig, cfg.ingressConfig); err != nil {
		return fmt.Errorf("SetupInferenceService: SetupWithManager: %w", err)
	}
	return nil
}
