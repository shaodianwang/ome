package builders

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
)

func TestHTTPRouteBuilder_GetResourceType(t *testing.T) {
	builder := createHTTPRouteBuilder()
	assert.Equal(t, "HTTPRoute", builder.GetResourceType())
}

func TestHTTPRouteBuilder_Build(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	httpRoute, err := builder.Build(context.Background(), isvc)
	assert.NoError(t, err)
	assert.NotNil(t, httpRoute)

	route, ok := httpRoute.(*gatewayapiv1.HTTPRoute)
	assert.True(t, ok)
	// engine HTTPRoute is named "<isvc>-engine", distinct from the
	// top-level "<isvc>" route, and backs the "<isvc>-engine" Service
	// (matching engine.go's defaultEngineName).
	assert.Equal(t, "test-isvc-engine", route.Name)
	assert.Equal(t, "default", route.Namespace)
}

func TestHTTPRouteBuilder_BuildHTTPRoute_EngineComponent(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		expectedName    string
		expectedError   bool
		expectedRules   int
		expectedHosts   int
		expectNil       bool
		expectedTimeout *gatewayapiv1.Duration
	}{
		{
			name: "engine component ready",
			isvc: createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			// engine HTTPRoute is "<isvc>-engine", not "<isvc>".
			expectedName:    "test-isvc-engine",
			expectedError:   false,
			expectedRules:   1,
			expectedHosts:   1,
			expectedTimeout: DefaultTimeout, // default
		},
		{
			name:          "engine component not ready",
			isvc:          createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			expectNil:     true,
			expectedError: false,
		},
		{
			name:            "engine component with custom timeout",
			isvc:            createTestInferenceServiceHTTPRouteWithTimeout("test-isvc", "default", 120),
			expectedName:    "test-isvc-engine",
			expectedError:   false,
			expectedRules:   1,
			expectedHosts:   1,
			expectedTimeout: toGatewayAPIDuration(120),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			if !tt.expectNil {
				setEngineReady(tt.isvc)
			}

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, EngineComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			assert.Len(t, httpRoute.Spec.Rules, tt.expectedRules)
			assert.Len(t, httpRoute.Spec.Hostnames, tt.expectedHosts)

			if tt.expectedTimeout != nil {
				assert.Equal(t, tt.expectedTimeout, httpRoute.Spec.Rules[0].Timeouts.Request)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_RouterComponent(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		expectedName    string
		expectedError   bool
		expectNil       bool
		expectedTimeout *gatewayapiv1.Duration
	}{
		{
			name:            "router component ready",
			isvc:            createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			expectedName:    "test-isvc-router",
			expectedError:   false,
			expectedTimeout: DefaultTimeout,
		},
		{
			name:          "router component not ready",
			isvc:          createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			expectNil:     true,
			expectedError: false,
		},
		{
			name:            "router component with custom timeout",
			isvc:            createTestInferenceServiceWithRouterHTTPRouteTimeout("test-isvc", "default", 90),
			expectedName:    "test-isvc-router",
			expectedError:   false,
			expectedTimeout: toGatewayAPIDuration(90),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			if !tt.expectNil {
				setEngineReady(tt.isvc)
				setRouterReady(tt.isvc)
			}

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, RouterComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			if tt.expectedTimeout != nil {
				assert.Equal(t, tt.expectedTimeout, httpRoute.Spec.Rules[0].Timeouts.Request)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_DecoderComponent(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		expectedName    string
		expectedError   bool
		expectNil       bool
		expectedTimeout *gatewayapiv1.Duration
	}{
		{
			name:            "decoder component ready",
			isvc:            createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
			expectedName:    "test-isvc-decoder",
			expectedError:   false,
			expectedTimeout: DefaultTimeout,
		},
		{
			name:          "decoder component not ready",
			isvc:          createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
			expectNil:     true,
			expectedError: false,
		},
		{
			name:            "decoder component with custom timeout",
			isvc:            createTestInferenceServiceWithDecoderHTTPRouteTimeout("test-isvc", "default", 45),
			expectedName:    "test-isvc-decoder",
			expectedError:   false,
			expectedTimeout: toGatewayAPIDuration(45),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			if !tt.expectNil {
				setEngineReady(tt.isvc)
				setDecoderReady(tt.isvc)
			}

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, DecoderComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			if tt.expectedTimeout != nil {
				assert.Equal(t, tt.expectedTimeout, httpRoute.Spec.Rules[0].Timeouts.Request)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_TopLevelComponent(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		setupReadiness func(*v1beta1.InferenceService)
		expectedName   string
		expectedError  bool
		expectedRules  int
		expectNil      bool
	}{
		{
			name:           "top level with engine only",
			isvc:           createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) { setEngineReady(isvc) },
			expectedName:   "test-isvc",
			expectedRules:  1,
		},
		{
			name: "top level with router",
			isvc: createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
				setEngineReady(isvc)
				setRouterReady(isvc)
			},
			expectedName:  "test-isvc",
			expectedRules: 1,
		},
		{
			name: "top level with router and decoder",
			isvc: createTestInferenceServiceWithRouterAndDecoderHTTPRoute("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
				setEngineReady(isvc)
				setRouterReady(isvc)
				setDecoderReady(isvc)
			},
			expectedName:  "test-isvc",
			expectedRules: 1,
		},
		{
			name:           "top level engine not ready returns nil",
			isvc:           createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setupReadiness: func(_ *v1beta1.InferenceService) {},
			expectNil:      true,
		},
		{
			name:           "top level router not ready still creates route",
			isvc:           createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			setupReadiness: func(_ *v1beta1.InferenceService) {},
			expectedName:   "test-isvc",
			expectedRules:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()
			tt.setupReadiness(tt.isvc)

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, TopLevelComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			assert.Len(t, httpRoute.Spec.Rules, tt.expectedRules)
		})
	}
}

// ----- Weighted backendRefs from status.traffic[] -----

// TestHTTPRouteBuilder_WeightedBackends_FromStatusTraffic verifies the
// HTTPRoute builder reads status.components[<c>].traffic[] and emits
// weighted backendRefs. Single-entry status collapses to one
// backendRef (today's behavior). Multi-entry status produces weighted
// backendRefs in the same order. Per-mode reconcilers populate the
// status; this commit only wires the builder to consume it.
func TestHTTPRouteBuilder_TopLevelRoutesToRouterEvenWhenNotReady(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default")

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, "test-isvc-router", string(route.Spec.Rules[0].BackendRefs[0].Name),
		"top-level HTTPRoute must point to router service even when RouterReady is false")
	assert.Equal(t, int32(30080), int32(*route.Spec.Rules[0].BackendRefs[0].Port),
		"top-level HTTPRoute must use router port even when RouterReady is false")
}

// TestHTTPRouteBuilder_EngineAndTopLevel_DistinctNames is the
// naming regression: when both engine and top-level routes are
// reconciled for the same ISVC, they must have distinct names so the
// top-level reconcile doesn't overwrite the engine route. Previously
// both routes were named "<isvc>" (PredictorServiceName == isvc.Name)
// and the top-level always won, leaving the engine externally
// unaddressable.
func TestHTTPRouteBuilder_EngineAndTopLevel_DistinctNames(t *testing.T) {
	builder := createHTTPRouteBuilder()

	// Engine-only ISVC: engine route is "<isvc>-engine", top-level is "<isvc>".
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	engineResult, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	require.NotNil(t, engineResult)
	engineRoute := engineResult.(*gatewayapiv1.HTTPRoute)

	topLevelResult, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	require.NotNil(t, topLevelResult)
	topLevelRoute := topLevelResult.(*gatewayapiv1.HTTPRoute)

	assert.NotEqual(t, engineRoute.Name, topLevelRoute.Name,
		"engine and top-level HTTPRoute names must differ so the top-level reconcile does not overwrite engine")
	assert.Equal(t, "test-isvc-engine", engineRoute.Name)
	assert.Equal(t, "test-isvc", topLevelRoute.Name)
}

// TestHTTPRouteBuilder_EngineBackendUsesEngineService fixes the silent
// pre-existing bug: engine HTTPRoute used to reference "<isvc>" as
// backend Service (PredictorServiceName) but the actual engine Service
// is named "<isvc>-engine" (defaultEngineName in engine.go). The route
// was pointing at a non-existent backend.
func TestHTTPRouteBuilder_EngineBackendUsesEngineService(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	backendName := string(route.Spec.Rules[0].BackendRefs[0].Name)
	assert.Equal(t, "test-isvc-engine", backendName,
		"engine HTTPRoute must back the <isvc>-engine Service (matches defaultEngineName in engine.go)")
}

// TestHTTPRouteBuilder_TopLevelEngineFallback_UsesEngineService
// verifies that when there is no router, the top-level HTTPRoute's
// engine fallback backend is the "<isvc>-engine" Service, not the
// legacy "<isvc>" backend that doesn't exist.
func TestHTTPRouteBuilder_TopLevelEngineFallback_UsesEngineService(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	backendName := string(route.Spec.Rules[0].BackendRefs[0].Name)
	assert.Equal(t, "test-isvc-engine", backendName,
		"top-level HTTPRoute (engine fallback) must back the <isvc>-engine Service when there is no router")
}

// TestHTTPRouteBuilder_BuildHTTPRoute_ReturnsInterfaceNilWhenComponentNotReady
// pins the layer-1 source fix for the typed-nil-through-interface panic.
//
// Pre-fix, the BuildHTTPRoute dispatcher returned the per-component builder's
// (*HTTPRoute, error) directly. When a per-component builder returned
// (nil, nil) for the "component not ready" branch, Go wrapped the typed-nil
// pointer into the returned client.Object interface — the interface itself
// is non-nil (carries type info), only the inner value is nil. Callers
// checking `if obj == nil` saw FALSE and proceeded to dereference the
// typed-nil pointer, crashing the reconciler in
// controllerutil.SetControllerReference (gateway_api_strategy.go:165).
//
// The fix is to detect the typed-nil in the dispatcher and explicitly return
// untyped nil so `desired == nil` in the caller is true.
//
// NOTE: testify's `assert.Nil` uses reflect.IsNil under the hood and would
// pass even for a typed-nil wrapped in an interface — so this test uses the
// raw `== nil` comparison, which is the actual check the strategy performs.
func TestHTTPRouteBuilder_BuildHTTPRoute_ReturnsInterfaceNilWhenComponentNotReady(t *testing.T) {
	cases := []struct {
		name          string
		componentType string
		isvc          *v1beta1.InferenceService
	}{
		{
			name:          "engine not ready",
			componentType: EngineComponent,
			isvc:          createTestInferenceServiceHTTPRoute("test-isvc", "default"),
		},
		{
			name:          "router not ready",
			componentType: RouterComponent,
			isvc:          createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
		},
		{
			name:          "decoder not ready",
			componentType: DecoderComponent,
			isvc:          createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
		},
		{
			name:          "top-level engine fallback not ready",
			componentType: TopLevelComponent,
			isvc:          createTestInferenceServiceHTTPRoute("test-isvc", "default"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			// Deliberately do NOT mark any condition Ready, so the inner
			// per-component builders take the "component not ready" branch
			// and return (nil, nil).
			result, err := builder.BuildHTTPRoute(context.Background(), tc.isvc, tc.componentType)
			require.NoError(t, err)

			// The bug: result is a (*HTTPRoute)(nil) wrapped in a non-nil
			// client.Object interface, so `result == nil` returns false and
			// the strategy panics on the subsequent SetControllerReference.
			// The fix returns interface-nil instead, so this assertion holds.
			//
			// We deliberately use bare `== nil` here, NOT assert.Nil, since
			// the former is the exact check the strategy performs.
			if result != nil {
				t.Fatalf("BuildHTTPRoute(%s) must return interface-nil (== nil) when the component is not ready, "+
					"but returned a typed-nil-wrapped-in-interface (%T). This is the typed-nil-through-interface bug "+
					"that crashed the controller's reconcile loop on PD-disaggregated ISVCs.",
					tc.componentType, result)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_UnsupportedComponent(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, "unsupported")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported component type")
	assert.Nil(t, result)
}

func TestHTTPRouteBuilder_CreatePathPrefixMatch(t *testing.T) {
	builder := createHTTPRouteBuilder()

	match := builder.createPathPrefixMatch("/playground/test-isvc/")

	assert.NotNil(t, match.Path)
	assert.Equal(t, gatewayapiv1.PathMatchPathPrefix, *match.Path.Type)
	assert.Equal(t, "/playground/test-isvc/", *match.Path.Value)
}

func TestHTTPRouteBuilder_AddIsvcHeaders(t *testing.T) {
	builder := createHTTPRouteBuilder()

	filter := builder.addIsvcHeaders("test-service", "test-namespace")

	assert.Equal(t, gatewayapiv1.HTTPRouteFilterRequestHeaderModifier, filter.Type)
	assert.NotNil(t, filter.RequestHeaderModifier)
	assert.Len(t, filter.RequestHeaderModifier.Set, 2)

	headers := make(map[string]string)
	for _, header := range filter.RequestHeaderModifier.Set {
		headers[string(header.Name)] = header.Value
	}

	assert.Equal(t, "test-service", headers["OMe-Isvc-Name"])
	assert.Equal(t, "test-namespace", headers["OME-Isvc-Namespace"])
}

func TestHTTPRouteBuilder_CreateHTTPRouteRule(t *testing.T) {
	builder := createHTTPRouteBuilder()

	matches := []gatewayapiv1.HTTPRouteMatch{
		builder.createPathPrefixMatch("/playground/test-service/"),
	}
	filters := []gatewayapiv1.HTTPRouteFilter{
		builder.addIsvcHeaders("test-service", "test-namespace"),
	}
	timeout := toGatewayAPIDuration(30)

	rule := builder.createHTTPRouteRule(matches, filters, "test-service", "test-namespace", 8080, timeout)

	assert.Len(t, rule.Matches, 1)
	assert.Len(t, rule.Filters, 1)
	assert.Len(t, rule.BackendRefs, 1)
	assert.Equal(t, timeout, rule.Timeouts.Request)

	backend := rule.BackendRefs[0]
	assert.Equal(t, gatewayapiv1.ObjectName("test-service"), backend.BackendRef.BackendObjectReference.Name)
	assert.Equal(t, "test-namespace", string(*backend.BackendRef.BackendObjectReference.Namespace))
	assert.Equal(t, int32(8080), int32(*backend.BackendRef.BackendObjectReference.Port))
}

func TestHTTPRouteBuilder_ToGatewayAPIDuration(t *testing.T) {
	tests := []struct {
		name     string
		seconds  int64
		expected string
	}{
		{
			name:     "30 seconds",
			seconds:  30,
			expected: "30s",
		},
		{
			name:     "60 seconds",
			seconds:  60,
			expected: "60s",
		},
		{
			name:     "120 seconds",
			seconds:  120,
			expected: "120s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration := toGatewayAPIDuration(tt.seconds)
			assert.NotNil(t, duration)
			assert.Equal(t, tt.expected, string(*duration))
		})
	}
}

// Helper functions
func createHTTPRouteBuilder() *HTTPRouteBuilder {
	return &HTTPRouteBuilder{
		ingressConfig: &controllerconfig.IngressConfig{
			IngressDomain:     "example.com",
			DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			OmeIngressGateway: "istio-system/gateway",
		},
		isvcConfig:    &controllerconfig.InferenceServicesConfig{},
		domainService: services.NewDomainService(),
		pathService:   services.NewPathService(),
	}
}

func createTestInferenceServiceHTTPRoute(name, namespace string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
		},
		Status: v1beta1.InferenceServiceStatus{
			Status: duckv1.Status{
				Conditions: []apis.Condition{},
			},
		},
	}
}

func createTestInferenceServiceHTTPRouteWithTimeout(name, namespace string, timeoutSeconds int64) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Engine.TimeoutSeconds = &timeoutSeconds
	return isvc
}

func createTestInferenceServiceWithRouterHTTPRoute(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	return isvc
}

func createTestInferenceServiceWithRouterHTTPRouteTimeout(name, namespace string, timeoutSeconds int64) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceWithRouterHTTPRoute(name, namespace)
	isvc.Spec.Router.TimeoutSeconds = &timeoutSeconds
	return isvc
}

func createTestInferenceServiceWithDecoderHTTPRoute(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func createTestInferenceServiceWithDecoderHTTPRouteTimeout(name, namespace string, timeoutSeconds int64) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceWithDecoderHTTPRoute(name, namespace)
	isvc.Spec.Decoder.TimeoutSeconds = &timeoutSeconds
	return isvc
}

func createTestInferenceServiceWithRouterAndDecoderHTTPRoute(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func setEngineReady(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.EngineReady, &apis.Condition{
		Type:   v1beta1.EngineReady,
		Status: corev1.ConditionTrue,
	})
}

func setRouterReady(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.RouterReady, &apis.Condition{
		Type:   v1beta1.RouterReady,
		Status: corev1.ConditionTrue,
	})
}

func setDecoderReady(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.DecoderReady, &apis.Condition{
		Type:   v1beta1.DecoderReady,
		Status: corev1.ConditionTrue,
	})
}
