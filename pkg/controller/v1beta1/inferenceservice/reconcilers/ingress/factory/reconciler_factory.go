package factory

import (
	"fmt"

	"k8s.io/client-go/kubernetes"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/strategies"
)

// DefaultStrategyFactory implements StrategyFactory interface
type DefaultStrategyFactory struct {
	clientset     kubernetes.Interface
	domainService interfaces.DomainService
	pathService   interfaces.PathService
}

// NewStrategyFactory creates a new strategy factory
func NewStrategyFactory(clientset kubernetes.Interface) interfaces.StrategyFactory {
	return &DefaultStrategyFactory{
		clientset:     clientset,
		domainService: services.NewDomainService(),
		pathService:   services.NewPathService(),
	}
}

// CreateStrategyWithOptions creates the appropriate ingress strategy with options
func (f *DefaultStrategyFactory) CreateStrategyWithOptions(deploymentMode string, opts interfaces.ReconcilerOptions) (interfaces.IngressStrategy, error) {
	switch deploymentMode {
	// OMENative joins the RawDeployment case: both modes emit the
	// same `<isvc>-<comp>` stable Service via the top-level
	// service.ServiceReconciler, so the GatewayAPI / KubernetesIngress
	// strategies — which target `<isvc>-<comp>` — work for every mode
	// without per-mode routing logic.
	case string(constants.RawDeployment), string(constants.OMENative):
		if opts.IngressConfig == nil {
			return nil, fmt.Errorf("ingress config is required for deployment mode: %s", deploymentMode)
		}
		if opts.IngressConfig.EnableGatewayAPI {
			return strategies.NewGatewayAPIStrategy(opts, f.domainService, f.pathService), nil
		}
		return strategies.NewKubernetesIngressStrategy(opts, f.domainService, f.pathService), nil
	default:
		return nil, fmt.Errorf("unsupported deployment mode: %s", deploymentMode)
	}
}
