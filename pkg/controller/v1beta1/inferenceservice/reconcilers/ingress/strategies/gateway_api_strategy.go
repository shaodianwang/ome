package strategies

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
)

const (
	HTTPRouteNotReady                 = "HttpRouteNotReady"
	HTTPRouteParentStatusNotAvailable = "ParentStatusNotAvailable"
)

// GatewayAPIStrategy handles ingress for Gateway API (raw deployment mode)
type GatewayAPIStrategy struct {
	client               client.Client
	scheme               *runtime.Scheme
	ingressConfig        *controllerconfig.IngressConfig
	isvcConfig           *controllerconfig.InferenceServicesConfig
	domainService        interfaces.DomainService
	pathService          interfaces.PathService
	httpRouteBuilder     interfaces.HTTPRouteBuilder
	trafficPolicyBuilder *builders.BackendTrafficPolicyBuilder
}

// NewGatewayAPIStrategy creates a new Gateway API strategy
func NewGatewayAPIStrategy(opts interfaces.ReconcilerOptions, domainService interfaces.DomainService, pathService interfaces.PathService) interfaces.IngressStrategy {
	httpRouteBuilder := builders.NewHTTPRouteBuilder(opts.IngressConfig, opts.IsvcConfig, domainService, pathService)
	trafficPolicyBuilder := builders.NewBackendTrafficPolicyBuilder(opts.IngressConfig)

	return &GatewayAPIStrategy{
		client:               opts.Client,
		scheme:               opts.Scheme,
		ingressConfig:        opts.IngressConfig,
		isvcConfig:           opts.IsvcConfig,
		domainService:        domainService,
		pathService:          pathService,
		httpRouteBuilder:     httpRouteBuilder,
		trafficPolicyBuilder: trafficPolicyBuilder,
	}
}

func (g *GatewayAPIStrategy) GetName() string {
	return "GatewayAPI"
}

func (g *GatewayAPIStrategy) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error {
	var err error

	if !g.ingressConfig.DisableIngressCreation {
		// Reconcile component HTTPRoutes
		if err := g.reconcileComponentHTTPRoute(ctx, isvc, builders.EngineComponent); err != nil {
			return err
		}
		if isvc.Spec.Router != nil {
			if err := g.reconcileComponentHTTPRoute(ctx, isvc, builders.RouterComponent); err != nil {
				return err
			}
		}
		if isvc.Spec.Decoder != nil {
			if err := g.reconcileComponentHTTPRoute(ctx, isvc, builders.DecoderComponent); err != nil {
				return err
			}
		}
		if err := g.reconcileComponentHTTPRoute(ctx, isvc, builders.TopLevelComponent); err != nil {
			return err
		}

		// The default BTP (ConsistentHash on configured headers) is
		// emitted here only when the operator has NOT declared any
		// OEP-0011 traffic intent. When intent is declared, the
		// per-ISVC traffic.Reconciler owns the BTP — both
		// reconcilers writing the same resource name would fight on
		// every loop.
		ownsBackendTrafficPolicy := !traffic.Resolve(isvc).HasIntent()
		if ownsBackendTrafficPolicy {
			if err := g.reconcileBackendTrafficPolicy(ctx, isvc); err != nil {
				return err
			}
		}

		// Check HTTPRoute and (optionally) BackendTrafficPolicy statuses
		if err := g.checkHTTPRouteStatuses(ctx, isvc); err != nil {
			return err
		}

		if ownsBackendTrafficPolicy {
			if err := g.checkBackendTrafficPolicyStatus(ctx, isvc); err != nil {
				return err
			}
		}

		// If we are here, then all the HTTPRoutes are ready, Mark ingress as ready
		isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionTrue,
		})
	} else {
		// Ingress creation is disabled. We set it to true as the isvc condition depends on it.
		isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionTrue,
		})
	}

	// Set status URL and Address
	isvc.Status.URL, err = g.createRawURL(isvc)
	if err != nil {
		return err
	}
	isvc.Status.Address = &duckv1.Addressable{
		URL: &knapis.URL{
			Host:   g.getRawServiceHost(isvc),
			Scheme: g.ingressConfig.UrlScheme,
			Path:   "",
		},
	}
	return nil
}

func (g *GatewayAPIStrategy) reconcileComponentHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string) error {
	// Use builder to create the HTTPRoute
	desired, err := g.httpRouteBuilder.BuildHTTPRoute(ctx, isvc, componentType)
	if err != nil {
		return err
	}
	if desired == nil {
		// Set ingress condition to indicate component not ready
		isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
			Type:    v1beta1.IngressReady,
			Status:  corev1.ConditionFalse,
			Reason:  "ComponentNotReady",
			Message: fmt.Sprintf("%s component not ready for HTTPRoute creation", componentType),
		})
		klog.Info("Builder returned nil HTTPRoute - component not ready", "isvc", isvc.Name, "component", componentType)
		return nil
	}

	httpRoute, ok := desired.(*gatewayapiv1.HTTPRoute)
	if !ok {
		return fmt.Errorf("builder returned unexpected type %T, expected *gatewayapiv1.HTTPRoute", desired)
	}
	if httpRoute == nil {
		// Defensive: BuildHTTPRoute now returns interface-nil for not-ready
		// components, but guard here too so any future caller that hands us
		// a typed-nil pointer (e.g. via a stub builder in tests, or a future
		// dispatcher regression) does not crash the reconciler in
		// controllerutil.SetControllerReference below.
		isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
			Type:    v1beta1.IngressReady,
			Status:  corev1.ConditionFalse,
			Reason:  "ComponentNotReady",
			Message: fmt.Sprintf("%s component not ready for HTTPRoute creation", componentType),
		})
		klog.Info("Builder returned typed-nil HTTPRoute - component not ready", "isvc", isvc.Name, "component", componentType)
		return nil
	}

	if err := controllerutil.SetControllerReference(isvc, httpRoute, g.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference for %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
	}

	existing := &gatewayapiv1.HTTPRoute{}
	err = g.client.Get(ctx, types.NamespacedName{Name: httpRoute.Name, Namespace: isvc.Namespace}, existing)
	if err != nil {
		if apierr.IsNotFound(err) {
			if err := g.client.Create(ctx, httpRoute); err != nil {
				return fmt.Errorf("failed to create %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
			}
		} else {
			return err
		}
	} else {
		// Set ResourceVersion which is required for update operation.
		httpRoute.ResourceVersion = existing.ResourceVersion
		// Do a dry-run update to avoid diffs generated by default values.
		if err := g.client.Update(ctx, httpRoute, client.DryRunAll); err != nil {
			return fmt.Errorf("failed to perform dry-run update for %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
		}
		if !g.semanticHttpRouteEquals(httpRoute, existing) {
			if err := g.client.Update(ctx, httpRoute); err != nil {
				return fmt.Errorf("failed to update %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
			}
		}
	}
	return nil
}

func (g *GatewayAPIStrategy) reconcileBackendTrafficPolicy(ctx context.Context, isvc *v1beta1.InferenceService) error {
	policy := g.trafficPolicyBuilder.Build(isvc)

	if err := controllerutil.SetControllerReference(isvc, policy, g.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference for BackendTrafficPolicy %s: %w", isvc.Name, err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
	err := g.client.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, existing)
	if err != nil {
		if apierr.IsNotFound(err) {
			if err := g.client.Create(ctx, policy); err != nil {
				return fmt.Errorf("failed to create BackendTrafficPolicy %s: %w", isvc.Name, err)
			}
		} else {
			return err
		}
	} else {
		policy.SetResourceVersion(existing.GetResourceVersion())
		if err := g.client.Update(ctx, policy); err != nil {
			return fmt.Errorf("failed to update BackendTrafficPolicy %s: %w", isvc.Name, err)
		}
	}
	return nil
}

func (g *GatewayAPIStrategy) checkHTTPRouteStatuses(ctx context.Context, isvc *v1beta1.InferenceService) error {
	// OEP-0011 C1: the engine HTTPRoute is always emitted as
	// "<isvc>-engine" (distinct from the top-level "<isvc>"); these
	// lookups must match the builder-produced names.
	components := []struct {
		name      string
		condition func() bool
	}{
		{constants.EngineServiceName(isvc.Name), func() bool { return true }},     // Engine: "<isvc>-engine"
		{isvc.Name + "-router", func() bool { return isvc.Spec.Router != nil }},   // Router
		{isvc.Name + "-decoder", func() bool { return isvc.Spec.Decoder != nil }}, // Decoder
		{isvc.Name, func() bool { return true }},                                  // Top level: "<isvc>"
	}

	for _, comp := range components {
		if !comp.condition() {
			continue
		}

		httpRoute := &gatewayapiv1.HTTPRoute{}
		if err := g.client.Get(ctx, types.NamespacedName{
			Name:      comp.name,
			Namespace: isvc.Namespace,
		}, httpRoute); err != nil {
			if apierr.IsNotFound(err) {
				// Per-component HTTPRoute was correctly skipped by
				// reconcileComponentHTTPRoute when the component wasn't
				// Ready (Builder returned nil). The create-path already
				// set IngressReady=False/ComponentNotReady; nothing to
				// do here. Keep iterating so any other component with an
				// existing HTTPRoute still has its status checked, and
				// let the next reconcile pick up after the component
				// becomes Ready and the HTTPRoute materializes.
				continue
			}
			return err
		}

		if ready, reason, message := g.isHTTPRouteReady(httpRoute.Status); !ready {
			componentType := g.getComponentType(comp.name, isvc)
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
				Type:    v1beta1.IngressReady,
				Status:  corev1.ConditionFalse,
				Reason:  *reason,
				Message: fmt.Sprintf("%s %s", componentType, *message),
			})
			return nil
		}
	}
	return nil
}

func (g *GatewayAPIStrategy) checkBackendTrafficPolicyStatus(ctx context.Context, isvc *v1beta1.InferenceService) error {
	policy := &unstructured.Unstructured{}
	policy.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
	if err := g.client.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, policy); err != nil {
		if apierr.IsNotFound(err) {
			return nil
		}
		return err
	}

	status, ok := policy.Object["status"].(map[string]interface{})
	if !ok {
		return nil
	}
	conditions, ok := status["conditions"].([]interface{})
	if !ok {
		return nil
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cond["status"] == "False" {
			reason, _ := cond["reason"].(string)
			message, _ := cond["message"].(string)
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
				Type:    v1beta1.IngressReady,
				Status:  corev1.ConditionFalse,
				Reason:  reason,
				Message: fmt.Sprintf("BackendTrafficPolicy: %s", message),
			})
			return nil
		}
	}
	return nil
}

func (g *GatewayAPIStrategy) createRawURL(isvc *v1beta1.InferenceService) (*knapis.URL, error) {
	var err error
	url := &knapis.URL{}
	url.Scheme = g.ingressConfig.UrlScheme
	url.Host, err = g.domainService.GenerateDomainName(isvc.Name, isvc.ObjectMeta, g.ingressConfig)
	if err != nil {
		return nil, err
	}
	return url, nil
}

func (g *GatewayAPIStrategy) getRawServiceHost(isvc *v1beta1.InferenceService) string {
	if isvc.Spec.Router != nil {
		routerName := isvc.Name + "-router" // Actual router service name
		return routerName + "." + isvc.Namespace + ".svc.cluster.local"
	}
	// Engine Service is named "<isvc>-engine" (constants.EngineServiceName).
	engineName := constants.EngineServiceName(isvc.Name)
	return engineName + "." + isvc.Namespace + ".svc.cluster.local"
}

func (g *GatewayAPIStrategy) semanticHttpRouteEquals(desired, existing *gatewayapiv1.HTTPRoute) bool {
	return equality.Semantic.DeepEqual(desired.Spec, existing.Spec)
}

// isHTTPRouteReady checks if the HTTPRoute is ready. If not, returns the reason and message.
func (g *GatewayAPIStrategy) isHTTPRouteReady(httpRouteStatus gatewayapiv1.HTTPRouteStatus) (bool, *string, *string) {
	if len(httpRouteStatus.Parents) == 0 {
		return false, ptr.To(HTTPRouteParentStatusNotAvailable), ptr.To(HTTPRouteNotReady)
	}
	for _, parent := range httpRouteStatus.Parents {
		for _, condition := range parent.Conditions {
			if condition.Status == metav1.ConditionFalse {
				return false, &condition.Reason, &condition.Message
			}
		}
	}
	return true, nil, nil
}

// getComponentType returns the component type name for display purposes.
// OEP-0011 C1: engine HTTPRoute is "<isvc>-engine", top-level is "<isvc>".
func (g *GatewayAPIStrategy) getComponentType(name string, isvc *v1beta1.InferenceService) string {
	switch name {
	case constants.EngineServiceName(isvc.Name): // "<isvc>-engine"
		return "Engine"
	case isvc.Name + "-router":
		return "Router"
	case isvc.Name + "-decoder":
		return "Decoder"
	case isvc.Name:
		return "TopLevel"
	default:
		return "Component"
	}
}
