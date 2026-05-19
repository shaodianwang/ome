package builders

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
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
	// OEP-0011 C1: engine HTTPRoute is named "<isvc>-engine", distinct
	// from the top-level "<isvc>" route, and backs the
	// "<isvc>-engine" Service (matching engine.go's defaultEngineName).
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
			// OEP-0011 C1: engine HTTPRoute is "<isvc>-engine", not "<isvc>".
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

// ----- OEP-0011 C2: weighted backendRefs from status.traffic[] -----

// TestHTTPRouteBuilder_WeightedBackends_FromStatusTraffic verifies the
// HTTPRoute builder reads status.components[<c>].traffic[] and emits
// weighted backendRefs. Single-entry status collapses to one
// backendRef (today's behavior). Multi-entry status produces weighted
// backendRefs in the same order. Per-mode reconcilers populate the
// status; this commit only wires the builder to consume it.
func TestHTTPRouteBuilder_WeightedBackends_FromStatusTraffic(t *testing.T) {
	builder := createHTTPRouteBuilder()

	t.Run("no status traffic — single backendRef to default service", func(t *testing.T) {
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
		setEngineReady(isvc)
		// no isvc.Status.Components populated

		result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
		require.NoError(t, err)
		route := result.(*gatewayapiv1.HTTPRoute)
		require.Len(t, route.Spec.Rules, 1)
		require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
		assert.Equal(t, "test-isvc-engine", string(route.Spec.Rules[0].BackendRefs[0].Name))
		assert.Nil(t, route.Spec.Rules[0].BackendRefs[0].Weight,
			"single backend (no status) should have no weight set — gateway treats unset as 100%")
	})

	t.Run("status traffic with two revisions — weighted backendRefs", func(t *testing.T) {
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
		setEngineReady(isvc)
		stable := int64(90)
		canary := int64(10)
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {
				Traffic: []knservingv1.TrafficTarget{
					{RevisionName: "test-isvc-engine-rev-abc123", Percent: &stable, Tag: "prev"},
					{RevisionName: "test-isvc-engine-rev-def456", Percent: &canary, Tag: "latest"},
				},
			},
		}

		result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
		require.NoError(t, err)
		route := result.(*gatewayapiv1.HTTPRoute)
		require.Len(t, route.Spec.Rules, 1)
		require.Len(t, route.Spec.Rules[0].BackendRefs, 2,
			"two TrafficTarget entries must produce two backendRefs")

		// Order preserved.
		assert.Equal(t, "test-isvc-engine-rev-abc123", string(route.Spec.Rules[0].BackendRefs[0].Name))
		require.NotNil(t, route.Spec.Rules[0].BackendRefs[0].Weight)
		assert.Equal(t, int32(90), *route.Spec.Rules[0].BackendRefs[0].Weight)

		assert.Equal(t, "test-isvc-engine-rev-def456", string(route.Spec.Rules[0].BackendRefs[1].Name))
		require.NotNil(t, route.Spec.Rules[0].BackendRefs[1].Weight)
		assert.Equal(t, int32(10), *route.Spec.Rules[0].BackendRefs[1].Weight)
	})

	t.Run("status traffic with empty RevisionName falls back to default service", func(t *testing.T) {
		// Defensive: a TrafficTarget without RevisionName must not
		// produce an empty backend reference. The builder substitutes
		// the default Service name so the gateway can still route.
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
		setEngineReady(isvc)
		fullWeight := int64(100)
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {
				Traffic: []knservingv1.TrafficTarget{
					{Percent: &fullWeight}, // RevisionName empty
				},
			},
		}

		result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
		require.NoError(t, err)
		route := result.(*gatewayapiv1.HTTPRoute)
		require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
		assert.Equal(t, "test-isvc-engine", string(route.Spec.Rules[0].BackendRefs[0].Name),
			"empty RevisionName must fall back to the per-Component default Service")
	})

	t.Run("blue-green standby (0% canary, 100% stable) — both backends emitted", func(t *testing.T) {
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
		setEngineReady(isvc)
		stable := int64(100)
		canary := int64(0)
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {
				Traffic: []knservingv1.TrafficTarget{
					{RevisionName: "test-isvc-engine-rev-stable", Percent: &stable},
					{RevisionName: "test-isvc-engine-rev-canary", Percent: &canary},
				},
			},
		}

		result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
		require.NoError(t, err)
		route := result.(*gatewayapiv1.HTTPRoute)
		require.Len(t, route.Spec.Rules[0].BackendRefs, 2,
			"blue-green standby must produce two backendRefs even with 0% canary so the route still references the standby Service")
		assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[1].Weight,
			"canary entry must be emitted with weight 0 (Gateway API valid; gateway routes no traffic but still resolves endpoints)")
	})
}

// TestClampBackendWeight covers the *int64 → int32 weight cast guard
// for status.traffic[].Percent values. The Knative TrafficTarget type
// is *int64 but Gateway API's BackendRef.Weight is *int32 in the
// [0, 1_000_000] range. The guard prevents out-of-spec inputs from
// silently overflowing the cast or sending negative weights to the
// gateway controller.
func TestClampBackendWeight(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want int32
	}{
		{"zero", 0, 0},
		{"normal canary 10", 10, 10},
		{"stable 100", 100, 100},
		{"gateway max", gatewayAPIMaxWeight, gatewayAPIMaxWeight},
		{"just over gateway max", gatewayAPIMaxWeight + 1, gatewayAPIMaxWeight},
		{"int32 overflow", int64(math.MaxInt32) + 1, gatewayAPIMaxWeight},
		{"int64 max", math.MaxInt64, gatewayAPIMaxWeight},
		{"negative one", -1, 0},
		{"int64 min", math.MinInt64, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, clampBackendWeight(tc.in))
		})
	}
}

// TestHTTPRouteBuilder_WeightedBackends_ClampsOutOfRangePercent
// verifies the clamp is wired into the builder path. Status produced
// by a buggy per-mode reconciler with negative or overflow Percent
// values must not propagate the bad weight to the emitted HTTPRoute.
func TestHTTPRouteBuilder_WeightedBackends_ClampsOutOfRangePercent(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)
	negative := int64(-50)
	overflow := int64(math.MaxInt32) + 1
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Traffic: []knservingv1.TrafficTarget{
				{RevisionName: "test-isvc-engine-rev-bad", Percent: &negative},
				{RevisionName: "test-isvc-engine-rev-huge", Percent: &overflow},
			},
		},
	}

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	route := result.(*gatewayapiv1.HTTPRoute)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 2)
	require.NotNil(t, route.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[0].Weight,
		"negative Percent must clamp to 0, not silently propagate")
	require.NotNil(t, route.Spec.Rules[0].BackendRefs[1].Weight)
	assert.Equal(t, int32(gatewayAPIMaxWeight), *route.Spec.Rules[0].BackendRefs[1].Weight,
		"overflow Percent must clamp to Gateway API max, not wrap to negative")
}

// TestHTTPRouteBuilder_TopLevelInheritsTrafficFromFallbackComponent
// verifies that the top-level route inherits the weighted backends of
// whichever Component it falls back to (router if present, otherwise
// engine). This makes a router rollout visible at the top-level URL.
func TestHTTPRouteBuilder_TopLevelInheritsTrafficFromFallbackComponent(t *testing.T) {
	builder := createHTTPRouteBuilder()

	t.Run("top-level inherits engine traffic when no router", func(t *testing.T) {
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
		setEngineReady(isvc)
		stable := int64(90)
		canary := int64(10)
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {
				Traffic: []knservingv1.TrafficTarget{
					{RevisionName: "test-isvc-engine-rev-stable", Percent: &stable},
					{RevisionName: "test-isvc-engine-rev-canary", Percent: &canary},
				},
			},
		}

		result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
		require.NoError(t, err)
		route := result.(*gatewayapiv1.HTTPRoute)
		require.Len(t, route.Spec.Rules[0].BackendRefs, 2)
		assert.Equal(t, "test-isvc-engine-rev-stable", string(route.Spec.Rules[0].BackendRefs[0].Name))
		assert.Equal(t, "test-isvc-engine-rev-canary", string(route.Spec.Rules[0].BackendRefs[1].Name))
	})

	t.Run("top-level inherits router traffic when router is present", func(t *testing.T) {
		isvc := createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default")
		setEngineReady(isvc)
		setRouterReady(isvc)
		stable := int64(80)
		canary := int64(20)
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.RouterComponent: {
				Traffic: []knservingv1.TrafficTarget{
					{RevisionName: "test-isvc-router-rev-stable", Percent: &stable},
					{RevisionName: "test-isvc-router-rev-canary", Percent: &canary},
				},
			},
		}

		result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
		require.NoError(t, err)
		route := result.(*gatewayapiv1.HTTPRoute)
		require.Len(t, route.Spec.Rules[0].BackendRefs, 2,
			"top-level must inherit ROUTER's traffic split when router is present (not engine's)")
		assert.Equal(t, "test-isvc-router-rev-stable", string(route.Spec.Rules[0].BackendRefs[0].Name))
		assert.Equal(t, "test-isvc-router-rev-canary", string(route.Spec.Rules[0].BackendRefs[1].Name))
	})
}

// TestHTTPRouteBuilder_TopLevelRoutesToRouterEvenWhenNotReady verifies
// that the top-level HTTPRoute always points to the router service when
// isvc.Spec.Router is present, regardless of RouterReady condition.
// This prevents stale routes pointing to non-existent services during
// scale-up.
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
// OEP-0011 C1 regression: when both engine and top-level routes are
// reconciled for the same ISVC, they must have distinct names so the
// top-level reconcile doesn't overwrite the engine route. Pre-OEP-0011
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

// TestHTTPRouteBuilder_EngineBackendUsesEngineService is the OEP-0011
// C1 fix for the silent pre-existing bug: engine HTTPRoute used to
// reference "<isvc>" as backend Service (PredictorServiceName) but
// the actual engine Service is named "<isvc>-engine" (defaultEngineName
// in engine.go). The route was pointing at a non-existent backend.
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
