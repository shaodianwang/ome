package builders

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
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
	var route *gatewayapiv1.HTTPRoute
	var err error
	switch componentType {
	case EngineComponent:
		route, err = b.buildEngineHTTPRoute(isvc)
	case RouterComponent:
		route, err = b.buildRouterHTTPRoute(isvc)
	case DecoderComponent:
		route, err = b.buildDecoderHTTPRoute(isvc)
	case TopLevelComponent:
		route, err = b.buildTopLevelHTTPRoute(isvc)
	default:
		return nil, fmt.Errorf("unsupported component type: %s", componentType)
	}
	if route == nil {
		// Return an explicit interface-nil rather than letting Go wrap a
		// typed-nil (*HTTPRoute)(nil) into a non-nil client.Object. The
		// caller checks `desired == nil`, which is false for a typed-nil
		// inside an interface — that gap caused a panic when a per-
		// component builder returned (nil, nil) for the "component not
		// ready" branch and the strategy then tried to call
		// controllerutil.SetControllerReference on the typed-nil pointer.
		return nil, err
	}
	return route, err
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
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, engineName, isvc.Namespace, EngineDefaultPort, timeout),
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
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, routerName, isvc.Namespace, RouterDefaultPort, timeout),
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
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, decoderName, isvc.Namespace, DecoderDefaultPort, timeout),
	}

	return b.buildHTTPRouteResource(isvc, constants.DecoderServiceName(isvc.Name),
		[]gatewayapiv1.Hostname{b.gatewayHostname()}, httpRouteRules), nil
}

func (b *HTTPRouteBuilder) buildTopLevelHTTPRoute(isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	filters := []gatewayapiv1.HTTPRouteFilter{b.addIsvcHeaders(isvc.Name, isvc.Namespace), b.urlRewriteFilter()}

	var serviceName string
	var port int32
	var timeout *gatewayapiv1.Duration

	if isvc.Spec.Router != nil {
		serviceName = constants.RouterServiceName(isvc.Name)
		port = RouterDefaultPort
		timeout = DefaultTimeout
		if isvc.Spec.Router.TimeoutSeconds != nil {
			timeout = toGatewayAPIDuration(*isvc.Spec.Router.TimeoutSeconds)
		}
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
	}

	prefix := b.pathPrefix(isvc, isvc.Name)
	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, serviceName, isvc.Namespace, port, timeout),
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
