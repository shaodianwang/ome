package builders

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"github.com/ome-projects/ome/pkg/utils"
)

const (
	EngineComponent   = "engine"
	RouterComponent   = "router"
	DecoderComponent  = "decoder"
	TopLevelComponent = "toplevel"

	EngineDefaultPort  int32 = 30000
	DecoderDefaultPort int32 = 30000
	RouterDefaultPort  int32 = 30080
)

var DefaultTimeout = toGatewayAPIDuration(600)

// HTTPRouteBuilder builds Gateway API HTTPRoute resources
type HTTPRouteBuilder struct {
	ingressConfig *controllerconfig.IngressConfig
	isvcConfig    *controllerconfig.InferenceServicesConfig
	domainService interfaces.DomainService
	pathService   interfaces.PathService
}

// NewHTTPRouteBuilder creates a new HTTPRoute builder
func NewHTTPRouteBuilder(ingressConfig *controllerconfig.IngressConfig, isvcConfig *controllerconfig.InferenceServicesConfig,
	domainService interfaces.DomainService, pathService interfaces.PathService) interfaces.HTTPRouteBuilder {
	return &HTTPRouteBuilder{
		ingressConfig: ingressConfig,
		isvcConfig:    isvcConfig,
		domainService: domainService,
		pathService:   pathService,
	}
}

func (b *HTTPRouteBuilder) GetResourceType() string {
	return "HTTPRoute"
}

func (b *HTTPRouteBuilder) Build(ctx context.Context, isvc *v1beta1.InferenceService) (client.Object, error) {
	return b.BuildHTTPRoute(ctx, isvc, EngineComponent)
}

func (b *HTTPRouteBuilder) BuildHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string) (client.Object, error) {
	switch componentType {
	case EngineComponent:
		return b.buildEngineHTTPRoute(isvc)
	case RouterComponent:
		return b.buildRouterHTTPRoute(isvc)
	case DecoderComponent:
		return b.buildDecoderHTTPRoute(isvc)
	case TopLevelComponent:
		return b.buildTopLevelHTTPRoute(isvc)
	default:
		return nil, fmt.Errorf("unsupported component type: %s", componentType)
	}
}

// toGatewayAPIDuration converts seconds to gatewayapiv1.Duration
func toGatewayAPIDuration(seconds int64) *gatewayapiv1.Duration {
	duration := gatewayapiv1.Duration(fmt.Sprintf("%ds", seconds))
	return &duration
}

func (b *HTTPRouteBuilder) gatewayHostname() gatewayapiv1.Hostname {
	return gatewayapiv1.Hostname("llm." + b.ingressConfig.IngressDomain)
}

func (b *HTTPRouteBuilder) pathPrefix(isvc *v1beta1.InferenceService, serviceName string) string {
	return "/" + isvc.Namespace + "/" + serviceName + "/"
}

func (b *HTTPRouteBuilder) createPathPrefixMatch(prefix string) gatewayapiv1.HTTPRouteMatch {
	return gatewayapiv1.HTTPRouteMatch{
		Path: &gatewayapiv1.HTTPPathMatch{
			Type:  ptr.To(gatewayapiv1.PathMatchPathPrefix),
			Value: ptr.To(prefix),
		},
	}
}

func (b *HTTPRouteBuilder) urlRewriteFilter() gatewayapiv1.HTTPRouteFilter {
	return gatewayapiv1.HTTPRouteFilter{
		Type: gatewayapiv1.HTTPRouteFilterURLRewrite,
		URLRewrite: &gatewayapiv1.HTTPURLRewriteFilter{
			Path: &gatewayapiv1.HTTPPathModifier{
				Type:               gatewayapiv1.PrefixMatchHTTPPathModifier,
				ReplacePrefixMatch: ptr.To("/"),
			},
		},
	}
}

func (b *HTTPRouteBuilder) addIsvcHeaders(name string, namespace string) gatewayapiv1.HTTPRouteFilter {
	return gatewayapiv1.HTTPRouteFilter{
		Type: gatewayapiv1.HTTPRouteFilterRequestHeaderModifier,
		RequestHeaderModifier: &gatewayapiv1.HTTPHeaderFilter{
			Set: []gatewayapiv1.HTTPHeader{
				{
					Name:  constants.IsvcNameHeader,
					Value: name,
				},
				{
					Name:  constants.IsvcNamespaceHeader,
					Value: namespace,
				},
			},
		},
	}
}

func (b *HTTPRouteBuilder) createHTTPRouteRule(routeMatches []gatewayapiv1.HTTPRouteMatch, filters []gatewayapiv1.HTTPRouteFilter,
	serviceName, namespace string, port int32, timeout *gatewayapiv1.Duration,
) gatewayapiv1.HTTPRouteRule {
	var backendRefs []gatewayapiv1.HTTPBackendRef
	if serviceName != "" {
		backendRefs = []gatewayapiv1.HTTPBackendRef{
			{
				BackendRef: gatewayapiv1.BackendRef{
					BackendObjectReference: gatewayapiv1.BackendObjectReference{
						Kind:      ptr.To(gatewayapiv1.Kind(constants.ServiceKind)),
						Name:      gatewayapiv1.ObjectName(serviceName),
						Namespace: (*gatewayapiv1.Namespace)(&namespace),
						Port:      (*gatewayapiv1.PortNumber)(&port),
					},
				},
			},
		}
	}
	return gatewayapiv1.HTTPRouteRule{
		Matches:     routeMatches,
		Filters:     filters,
		BackendRefs: backendRefs,
		Timeouts: &gatewayapiv1.HTTPRouteTimeouts{
			Request: timeout,
		},
	}
}

// createWeightedHTTPRouteRule emits a rule whose backendRefs are
// derived from ComponentStatusSpec.Traffic[]. OEP-0011 contract:
//
//   - When traffic is nil or has a single entry, the route collapses
//     to a single backendRef pointing at defaultServiceName (today's
//     behavior — backward compatible).
//   - When traffic has multiple entries, each entry maps to a
//     weighted backendRef. The backend Service name is taken from
//     TrafficTarget.RevisionName, which the per-mode reconciler must
//     populate with the full per-revision Service name (typically
//     "<isvc>-<component>-rev-<hash>"). Entries with empty
//     RevisionName fall back to defaultServiceName so the stable
//     entry on a freshly-started rollout still has a backend.
//
// All weighted entries land in one rule (one set of matches +
// filters); the gateway distributes traffic across the listed
// backends per Gateway API standard weight semantics.
func (b *HTTPRouteBuilder) createWeightedHTTPRouteRule(
	routeMatches []gatewayapiv1.HTTPRouteMatch,
	filters []gatewayapiv1.HTTPRouteFilter,
	defaultServiceName, namespace string,
	port int32,
	timeout *gatewayapiv1.Duration,
	traffic []knservingv1.TrafficTarget,
) gatewayapiv1.HTTPRouteRule {
	// No traffic split declared — collapse to a single backend
	// (preserves today's pre-OEP-0011 behavior).
	if len(traffic) == 0 {
		return b.createHTTPRouteRule(routeMatches, filters, defaultServiceName, namespace, port, timeout)
	}

	backendRefs := make([]gatewayapiv1.HTTPBackendRef, 0, len(traffic))
	for _, target := range traffic {
		backendName := target.RevisionName
		if backendName == "" {
			backendName = defaultServiceName
		}
		var weight *int32
		if target.Percent != nil {
			w := clampBackendWeight(*target.Percent)
			weight = &w
		}
		backendRefs = append(backendRefs, gatewayapiv1.HTTPBackendRef{
			BackendRef: gatewayapiv1.BackendRef{
				BackendObjectReference: gatewayapiv1.BackendObjectReference{
					Kind:      ptr.To(gatewayapiv1.Kind(constants.ServiceKind)),
					Name:      gatewayapiv1.ObjectName(backendName),
					Namespace: (*gatewayapiv1.Namespace)(&namespace),
					Port:      (*gatewayapiv1.PortNumber)(&port),
				},
				Weight: weight,
			},
		})
	}
	return gatewayapiv1.HTTPRouteRule{
		Matches:     routeMatches,
		Filters:     filters,
		BackendRefs: backendRefs,
		Timeouts: &gatewayapiv1.HTTPRouteTimeouts{
			Request: timeout,
		},
	}
}

// gatewayAPIMaxWeight is the upper bound Gateway API permits on
// BackendRef.Weight (per the v1 spec). Per-mode reconcilers populate
// status.Traffic[].Percent from Knative-style 0-100 values, but the
// type is *int64 so the cast to int32 needs a guard against
// out-of-spec / overflow values.
const gatewayAPIMaxWeight = 1_000_000

// clampBackendWeight maps a Knative TrafficTarget.Percent (*int64,
// nominally 0-100) into the int32 BackendRef.Weight that Gateway API
// expects, clamping out-of-spec inputs into the [0, 1_000_000] range
// instead of silently overflowing or sending negative weights to the
// gateway controller.
func clampBackendWeight(percent int64) int32 {
	switch {
	case percent < 0:
		return 0
	case percent > gatewayAPIMaxWeight:
		return gatewayAPIMaxWeight
	default:
		return int32(percent)
	}
}

// componentTraffic returns the Traffic[] entries the per-mode
// reconciler has populated for the given Component. Nil when no
// status exists yet (first reconcile) or when the Component's
// reconciler has not declared any traffic split.
func componentTraffic(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) []knservingv1.TrafficTarget {
	if isvc.Status.Components == nil {
		return nil
	}
	status, ok := isvc.Status.Components[c]
	if !ok {
		return nil
	}
	return status.Traffic
}

func (b *HTTPRouteBuilder) buildEngineHTTPRoute(isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	if !isvc.Status.IsConditionReady(v1beta1.EngineReady) {
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionFalse,
			Reason: "Engine ingress not created",
		})
		return nil, nil
	}

	// OEP-0011: the engine HTTPRoute is always named "<isvc>-engine",
	// distinct from the top-level "<isvc>" HTTPRoute. Pre-OEP-0011 the
	// engine route was named via PredictorServiceName which returns
	// "<isvc>" — colliding with the top-level route's name and
	// pointing at a backend Service that no longer exists (the engine
	// Service is named "<isvc>-engine"). Using EngineServiceName for
	// both the route name and the backend fixes both bugs and makes
	// the engine externally addressable on its own subdomain even
	// when a router is present.
	engineName := constants.EngineServiceName(isvc.Name)
	prefix := b.pathPrefix(isvc, engineName)
	filters := []gatewayapiv1.HTTPRouteFilter{b.addIsvcHeaders(isvc.Name, isvc.Namespace), b.urlRewriteFilter()}

	timeout := DefaultTimeout
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.TimeoutSeconds != nil {
		timeout = toGatewayAPIDuration(*isvc.Spec.Engine.TimeoutSeconds)
	}

	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	// OEP-0011 C2: weighted backendRefs from
	// status.components[engine].traffic[]. Collapses to a single
	// backendRef when no traffic split is declared (today's behavior).
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createWeightedHTTPRouteRule(routeMatch, filters, engineName, isvc.Namespace, EngineDefaultPort, timeout,
			componentTraffic(isvc, v1beta1.EngineComponent)),
	}

	return b.buildHTTPRouteResource(isvc, engineName,
		[]gatewayapiv1.Hostname{b.gatewayHostname()}, httpRouteRules), nil
}

func (b *HTTPRouteBuilder) buildRouterHTTPRoute(isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	if !isvc.Status.IsConditionReady(v1beta1.RouterReady) {
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionFalse,
			Reason: "Router ingress not created",
		})
		return nil, nil
	}

	routerName := constants.RouterServiceName(isvc.Name)
	prefix := b.pathPrefix(isvc, routerName)
	filters := []gatewayapiv1.HTTPRouteFilter{b.addIsvcHeaders(isvc.Name, isvc.Namespace), b.urlRewriteFilter()}

	timeout := DefaultTimeout
	if isvc.Spec.Router.TimeoutSeconds != nil {
		timeout = toGatewayAPIDuration(*isvc.Spec.Router.TimeoutSeconds)
	}

	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	// OEP-0011 C2: weighted backendRefs from
	// status.components[router].traffic[].
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createWeightedHTTPRouteRule(routeMatch, filters, routerName, isvc.Namespace, RouterDefaultPort, timeout,
			componentTraffic(isvc, v1beta1.RouterComponent)),
	}

	return b.buildHTTPRouteResource(isvc, constants.RouterServiceName(isvc.Name),
		[]gatewayapiv1.Hostname{b.gatewayHostname()}, httpRouteRules), nil
}

func (b *HTTPRouteBuilder) buildDecoderHTTPRoute(isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	if !isvc.Status.IsConditionReady(v1beta1.DecoderReady) {
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionFalse,
			Reason: "Decoder ingress not created",
		})
		return nil, nil
	}

	decoderName := constants.DecoderServiceName(isvc.Name)
	prefix := b.pathPrefix(isvc, decoderName)
	filters := []gatewayapiv1.HTTPRouteFilter{b.addIsvcHeaders(isvc.Name, isvc.Namespace), b.urlRewriteFilter()}

	timeout := DefaultTimeout
	if isvc.Spec.Decoder.TimeoutSeconds != nil {
		timeout = toGatewayAPIDuration(*isvc.Spec.Decoder.TimeoutSeconds)
	}

	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	// OEP-0011 C2: weighted backendRefs from
	// status.components[decoder].traffic[].
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createWeightedHTTPRouteRule(routeMatch, filters, decoderName, isvc.Namespace, DecoderDefaultPort, timeout,
			componentTraffic(isvc, v1beta1.DecoderComponent)),
	}

	return b.buildHTTPRouteResource(isvc, constants.DecoderServiceName(isvc.Name),
		[]gatewayapiv1.Hostname{b.gatewayHostname()}, httpRouteRules), nil
}

func (b *HTTPRouteBuilder) buildTopLevelHTTPRoute(isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	filters := []gatewayapiv1.HTTPRouteFilter{b.addIsvcHeaders(isvc.Name, isvc.Namespace), b.urlRewriteFilter()}

	var serviceName string
	var port int32
	var timeout *gatewayapiv1.Duration
	// fallbackComponent identifies which Component's
	// status.components[*].traffic[] drives the top-level route's
	// weighted backends. When router is present, top-level falls
	// back to router (and inherits router's traffic split during a
	// router rollout). When no router, top-level falls back to
	// engine.
	var fallbackComponent v1beta1.ComponentType

	if isvc.Spec.Router != nil {
		if !isvc.Status.IsConditionReady(v1beta1.RouterReady) {
			isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
				Type:   v1beta1.IngressReady,
				Status: corev1.ConditionFalse,
				Reason: "Router not ready",
			})
			return nil, nil
		}
		serviceName = constants.RouterServiceName(isvc.Name)
		port = RouterDefaultPort
		timeout = DefaultTimeout
		if isvc.Spec.Router.TimeoutSeconds != nil {
			timeout = toGatewayAPIDuration(*isvc.Spec.Router.TimeoutSeconds)
		}
		fallbackComponent = v1beta1.RouterComponent
	} else {
		if !isvc.Status.IsConditionReady(v1beta1.EngineReady) {
			isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
				Type:   v1beta1.IngressReady,
				Status: corev1.ConditionFalse,
				Reason: "Engine not ready",
			})
			return nil, nil
		}
		// OEP-0011: align with the engine Service name produced by
		// engine.go (defaultEngineName = isvc.Name + "-engine") rather
		// than the legacy PredictorServiceName (returns isvc.Name).
		// The PredictorServiceName path pointed at a backend Service
		// that no longer exists in the Engine-architecture
		// post-Predictor world.
		serviceName = constants.EngineServiceName(isvc.Name)
		port = EngineDefaultPort
		timeout = DefaultTimeout
		if isvc.Spec.Engine != nil && isvc.Spec.Engine.TimeoutSeconds != nil {
			timeout = toGatewayAPIDuration(*isvc.Spec.Engine.TimeoutSeconds)
		}
		fallbackComponent = v1beta1.EngineComponent
	}

	prefix := b.pathPrefix(isvc, isvc.Name)
	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	// OEP-0011 C2: top-level route inherits the weighted backendRefs
	// of whichever Component it falls back to (router or engine).
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createWeightedHTTPRouteRule(routeMatch, filters, serviceName, isvc.Namespace, port, timeout,
			componentTraffic(isvc, fallbackComponent)),
	}

	return b.buildHTTPRouteResource(isvc, isvc.Name,
		[]gatewayapiv1.Hostname{b.gatewayHostname()}, httpRouteRules), nil
}

func (b *HTTPRouteBuilder) buildHTTPRouteResource(isvc *v1beta1.InferenceService, name string, allowedHosts []gatewayapiv1.Hostname, httpRouteRules []gatewayapiv1.HTTPRouteRule) *gatewayapiv1.HTTPRoute {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})
	labels := utils.Filter(isvc.Labels, func(key string) bool {
		return !utils.Includes(constants.RevisionTemplateLabelDisallowedList, key)
	})

	gatewaySlice := strings.Split(b.ingressConfig.OmeIngressGateway, "/")

	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Spec: gatewayapiv1.HTTPRouteSpec{
			Hostnames: allowedHosts,
			Rules:     httpRouteRules,
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{
				ParentRefs: []gatewayapiv1.ParentReference{
					{
						Group:     (*gatewayapiv1.Group)(&gatewayapiv1.GroupVersion.Group),
						Kind:      (*gatewayapiv1.Kind)(ptr.To(constants.GatewayKind)),
						Namespace: (*gatewayapiv1.Namespace)(&gatewaySlice[0]),
						Name:      gatewayapiv1.ObjectName(gatewaySlice[1]),
					},
				},
			},
		},
	}
}
