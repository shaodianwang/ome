package strategies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
)

func TestGatewayAPIStrategy_GetName(t *testing.T) {
	strategy := createGatewayAPIStrategy(t)
	assert.Equal(t, "GatewayAPI", strategy.GetName())
}

func TestGatewayAPIStrategy_Reconcile(t *testing.T) {
	tests := []struct {
		name                    string
		isvc                    *v1beta1.InferenceService
		ingressConfig           *controllerconfig.IngressConfig
		existingHTTPRoutes      []client.Object
		expectedError           bool
		expectedIngressReady    corev1.ConditionStatus
		expectedHTTPRoutesCount int
	}{
		{
			name: "successful reconcile with predictor only",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionTrue,
			expectedHTTPRoutesCount: 2, // engine + toplevel
		},
		{
			name: "successful reconcile with router",
			isvc: createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionTrue,
			expectedHTTPRoutesCount: 3, // engine + router + toplevel
		},
		{
			name: "successful reconcile with decoder",
			isvc: createTestInferenceServiceWithDecoderGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionTrue,
			expectedHTTPRoutesCount: 3, // engine + decoder + toplevel
		},
		{
			name: "ingress creation disabled",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: true,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionTrue,
			expectedHTTPRoutesCount: 0,
		},
		{
			name: "cluster local visibility",
			isvc: createTestInferenceServiceWithClusterLocal("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionTrue,
			expectedHTTPRoutesCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			// Set component statuses to ready
			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			objs = append(objs, tt.existingHTTPRoutes...)

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			// Use the proper constructor to create the strategy with builder
			opts := interfaces.ReconcilerOptions{
				Client:        fakeClient,
				Scheme:        scheme,
				IngressConfig: tt.ingressConfig,
				IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
			}
			strategy := NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())

			err := strategy.Reconcile(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedIngressReady, tt.isvc.Status.GetCondition(v1beta1.IngressReady).Status)
			}
		})
	}
}

func TestGatewayAPIStrategy_Reconcile_GateOnOEPIntent(t *testing.T) {
	// The default BackendTrafficPolicy created by this strategy and the
	// per-ISVC BTP created by the traffic.Reconciler write the same
	// resource name. To prevent the two loops from fighting, this
	// strategy must skip BTP create/check when the operator has declared
	// traffic intent (spec.traffic or any ome.io/* annotation).
	//
	// Cases:
	//   - no intent  -> default BTP created (preserves prior behavior)
	//   - intent set -> default BTP NOT created (traffic.Reconciler owns it)
	alg := v1beta1.LoadBalancingTypeRoundRobin

	tests := []struct {
		name             string
		mutateIsvc       func(isvc *v1beta1.InferenceService)
		expectBTPCreated bool
	}{
		{
			name:             "no traffic intent: default BTP is created",
			mutateIsvc:       func(_ *v1beta1.InferenceService) {},
			expectBTPCreated: true,
		},
		{
			name: "traffic intent via spec.traffic: default BTP skipped",
			mutateIsvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
			},
			expectBTPCreated: false,
		},
		{
			name: "traffic intent via ome.io annotation: default BTP skipped",
			mutateIsvc: func(isvc *v1beta1.InferenceService) {
				if isvc.Annotations == nil {
					isvc.Annotations = map[string]string{}
				}
				isvc.Annotations["ome.io/circuit-breaker-max-connections"] = "1024"
			},
			expectBTPCreated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			isvc := createTestInferenceServiceGateway("test-isvc", "default")
			setComponentStatusReadyGateway(isvc)
			tt.mutateIsvc(isvc)

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(isvc).
				Build()

			opts := interfaces.ReconcilerOptions{
				Client: fakeClient,
				Scheme: scheme,
				IngressConfig: &controllerconfig.IngressConfig{
					EnableGatewayAPI:      true,
					IngressDomain:         "example.com",
					OmeIngressGateway:     "istio-system/gateway",
					DomainTemplate:        "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
					UrlScheme:             "https",
					ConsistentHashHeaders: []string{"x-routing-key"},
				},
				IsvcConfig: &controllerconfig.InferenceServicesConfig{},
			}
			strategy := NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())

			require.NoError(t, strategy.Reconcile(context.Background(), isvc))

			policy := &unstructured.Unstructured{}
			policy.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
			err := fakeClient.Get(context.Background(), types.NamespacedName{
				Name: isvc.Name, Namespace: isvc.Namespace,
			}, policy)

			if tt.expectBTPCreated {
				assert.NoError(t, err, "expected default BTP to be created")
			} else {
				assert.True(t, apierr.IsNotFound(err),
					"expected default BTP NOT to be created (traffic.Reconciler owns it); got err=%v", err)
			}
		})
	}
}

func TestGatewayAPIStrategy_ReconcileComponentHTTPRoute(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		componentType  string
		existingRoute  *gatewayapiv1.HTTPRoute
		expectedError  bool
		expectedCreate bool
		expectedUpdate bool
	}{
		{
			name:           "create new engine HTTPRoute",
			isvc:           createTestInferenceServiceGateway("test-isvc", "default"),
			componentType:  "engine",
			expectedError:  false,
			expectedCreate: true,
		},
		{
			name:           "create new router HTTPRoute",
			isvc:           createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			componentType:  "router",
			expectedError:  false,
			expectedCreate: true,
		},
		{
			name:           "create new decoder HTTPRoute",
			isvc:           createTestInferenceServiceWithDecoderGateway("test-isvc", "default"),
			componentType:  "decoder",
			expectedError:  false,
			expectedCreate: true,
		},
		{
			name:          "update existing HTTPRoute",
			isvc:          createTestInferenceServiceGateway("test-isvc", "default"),
			componentType: "engine",
			existingRoute: &gatewayapiv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			},
			expectedError:  false,
			expectedUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			if tt.existingRoute != nil {
				objs = append(objs, tt.existingRoute)
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			err := strategy.(*GatewayAPIStrategy).reconcileComponentHTTPRoute(context.Background(), tt.isvc, tt.componentType)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// typedNilHTTPRouteBuilder is a stub HTTPRouteBuilder that always returns
// a (*gatewayapiv1.HTTPRoute)(nil) wrapped in a non-nil client.Object — the
// exact shape of the value that triggered the production panic before the
// builder-side dispatcher was hardened. The strategy must defend against
// this shape even if a future dispatcher regression reintroduces it.
type typedNilHTTPRouteBuilder struct{}

func (typedNilHTTPRouteBuilder) GetResourceType() string { return "HTTPRoute" }

func (typedNilHTTPRouteBuilder) Build(_ context.Context, _ *v1beta1.InferenceService) (client.Object, error) {
	return (*gatewayapiv1.HTTPRoute)(nil), nil
}

func (typedNilHTTPRouteBuilder) BuildHTTPRoute(_ context.Context, _ *v1beta1.InferenceService, _ string) (client.Object, error) {
	// Wrap a typed-nil *HTTPRoute in the interface return — this is the
	// shape that crashed the reconciler at gateway_api_strategy.go:165
	// pre-fix. `result == nil` returns FALSE for this value because the
	// interface header carries non-nil type info; only the inner value
	// is nil. The strategy's typed-nil guard must catch this.
	return (*gatewayapiv1.HTTPRoute)(nil), nil
}

func (typedNilHTTPRouteBuilder) Endpoints(_ *v1beta1.InferenceService, _ string) ([]interfaces.Endpoint, error) {
	return nil, nil
}

// TestGatewayAPIStrategy_ReconcileComponentHTTPRoute_TypedNilBuilderDoesNotPanic
// pins the layer-2 defense for the typed-nil-through-interface panic.
//
// Production trace (PD-disaggregated ISVC sandbox/sglang-llama4x-nvfp4-pd-cc-fallback):
//
//	panic: runtime error: invalid memory address or nil pointer dereference
//	  gateway_api_strategy.go:165
//	  → reconciler.go:99
//	  → controller.go:516
//
// Root cause: BuildHTTPRoute returned a typed-nil *HTTPRoute wrapped in a
// non-nil client.Object interface. The `desired == nil` check at the top
// of reconcileComponentHTTPRoute returned FALSE (interface non-nil), the
// type assertion succeeded with ok=true, and the subsequent
// controllerutil.SetControllerReference call dereferenced the typed-nil
// pointer.
//
// The layer-1 fix (BuildHTTPRoute dispatcher returns interface-nil for the
// not-ready branch) closes the source. This test exercises layer-2 — a
// defensive nil check inside the strategy that catches any future caller
// (test stub, dispatcher regression, third-party builder) that still
// hands the strategy a typed-nil-wrapped-in-interface. We deliberately
// inject the stub builder directly into the strategy struct, bypassing
// the dispatcher fix, so this test pins the defense regardless of any
// future change to BuildHTTPRoute.
func TestGatewayAPIStrategy_ReconcileComponentHTTPRoute_TypedNilBuilderDoesNotPanic(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	isvc := createTestInferenceServiceWithDecoderGateway("test-isvc", "default")
	// Deliberately do NOT call setComponentStatusReadyGateway — even though
	// our stub builder ignores readiness anyway, leaving it not-ready mirrors
	// the production trigger (decoder component not yet ready).

	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(isvc).Build()

	opts := interfaces.ReconcilerOptions{
		Client:        fakeClient,
		Scheme:        scheme,
		IngressConfig: &controllerconfig.IngressConfig{EnableGatewayAPI: true},
		IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
	}
	strategy := NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService()).(*GatewayAPIStrategy)

	// Swap in the stub builder. This bypasses the layer-1 dispatcher fix so
	// the strategy sees the bad shape directly and we exercise the layer-2
	// defense in isolation.
	strategy.httpRouteBuilder = typedNilHTTPRouteBuilder{}

	// The pre-fix behavior was a panic at SetControllerReference. Wrap in a
	// recover() check to give a precise diagnostic if the defense regresses.
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reconcileComponentHTTPRoute panicked on a typed-nil HTTPRoute (this is the bug we are guarding against): %v", r)
			}
		}()
		err = strategy.reconcileComponentHTTPRoute(context.Background(), isvc, builders.DecoderComponent)
	}()

	require.NoError(t, err, "strategy must return nil error (not a panic and not a real failure) when builder hands it a typed-nil HTTPRoute")

	cond := isvc.Status.GetCondition(v1beta1.IngressReady)
	require.NotNil(t, cond, "IngressReady condition must be set by the typed-nil guard")
	assert.Equal(t, corev1.ConditionFalse, cond.Status, "IngressReady must be False when component is not ready")
	assert.Equal(t, "ComponentNotReady", cond.Reason, "IngressReady reason must identify the not-ready component cause")
}

func TestGatewayAPIStrategy_CheckHTTPRouteStatuses(t *testing.T) {
	tests := []struct {
		name              string
		isvc              *v1beta1.InferenceService
		httpRoutes        []client.Object
		expectedError     bool
		expectedCondition corev1.ConditionStatus
	}{
		{
			// engine HTTPRoute is "<isvc>-engine"; top-level is "<isvc>".
			// Both lookups must succeed.
			name: "all HTTPRoutes ready",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			httpRoutes: []client.Object{
				createReadyHTTPRoute("test-isvc-engine", "default"), // engine
				createReadyHTTPRoute("test-isvc", "default"),        // toplevel
			},
			expectedError:     false,
			expectedCondition: corev1.ConditionTrue,
		},
		{
			name: "HTTPRoute not ready",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			httpRoutes: []client.Object{
				createNotReadyHTTPRoute("test-isvc-engine", "default"), // engine
				createReadyHTTPRoute("test-isvc", "default"),           // toplevel
			},
			expectedError:     false,
			expectedCondition: corev1.ConditionFalse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))

			objs := []client.Object{tt.isvc}
			objs = append(objs, tt.httpRoutes...)

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			err := strategy.(*GatewayAPIStrategy).checkHTTPRouteStatuses(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestGatewayAPIStrategy_CheckHTTPRouteStatuses_NotFoundIsTolerated guards
// against a regression of the live-cluster bug where checkHTTPRouteStatuses
// returned IsNotFound as a hard error, crashing the reconcile every loop
// during PD-disaggregated bring-up. reconcileComponentHTTPRoute correctly
// skips create when a component isn't Ready (Builder returns nil); this
// test asserts the downstream status-check tolerates the resulting absence
// of HTTPRoutes by continuing past them rather than failing.
//
// Production trace (sandbox/sglang-llama4x-nvfp4-pd-cc-fallback):
//
//	ERROR Reconciler error ...
//	  error: "fails to reconcile ingress: HTTPRoute.gateway.networking.k8s.io
//	         \"sglang-llama4x-nvfp4-pd-cc-fallback-engine\" not found"
func TestGatewayAPIStrategy_CheckHTTPRouteStatuses_NotFoundIsTolerated(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	// ISVC declares Engine + Router + Decoder so checkHTTPRouteStatuses
	// will iterate all four name slots ("<isvc>-engine", "<isvc>-router",
	// "<isvc>-decoder", "<isvc>"). None of them exist in the client.
	isvc := createTestInferenceServiceWithRouterGateway("test-isvc", "default")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}

	// Pre-seed an IngressReady condition the way reconcileComponentHTTPRoute
	// would when it skipped a component (False / ComponentNotReady). The
	// status-check must not corrupt this.
	isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
		Type:    v1beta1.IngressReady,
		Status:  corev1.ConditionFalse,
		Reason:  "ComponentNotReady",
		Message: "engine component not ready for HTTPRoute creation",
	})

	// Fake client with NO HTTPRoutes pre-created.
	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		Build()

	strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

	err := strategy.(*GatewayAPIStrategy).checkHTTPRouteStatuses(context.Background(), isvc)
	assert.NoError(t, err, "checkHTTPRouteStatuses must tolerate NotFound and not return an error")

	// The pre-seeded condition from the create-path is preserved
	// (status-check did not touch it).
	condition := isvc.Status.GetCondition(v1beta1.IngressReady)
	require.NotNil(t, condition)
	assert.Equal(t, corev1.ConditionFalse, condition.Status,
		"pre-existing IngressReady=False set by reconcileComponentHTTPRoute must be preserved")
	assert.Equal(t, "ComponentNotReady", condition.Reason)
}

func TestGatewayAPIStrategy_CreateRawURL(t *testing.T) {
	tests := []struct {
		name        string
		isvc        *v1beta1.InferenceService
		config      *controllerconfig.IngressConfig
		expectedURL string
	}{
		{
			name: "simple domain generation",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			config: &controllerconfig.IngressConfig{
				UrlScheme:      "https",
				IngressDomain:  "example.com",
				DomainTemplate: "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			},
			expectedURL: "https://test-isvc.default.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &GatewayAPIStrategy{
				ingressConfig: tt.config,
				domainService: services.NewDomainService(),
			}

			url, err := strategy.createRawURL(tt.isvc)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedURL, url.String())
		})
	}
}

func TestGatewayAPIStrategy_GetRawServiceHost(t *testing.T) {
	tests := []struct {
		name         string
		isvc         *v1beta1.InferenceService
		expectedHost string
	}{
		{
			name:         "with router",
			isvc:         createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			expectedHost: "test-isvc-router.default.svc.cluster.local",
		},
		{
			// Engine Service is named "<isvc>-engine" (constants.EngineServiceName).
			// Pre-fix this returned "<isvc>" which doesn't resolve to a real Service.
			name:         "without router",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedHost: "test-isvc-engine.default.svc.cluster.local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t)
			host := strategy.(*GatewayAPIStrategy).getRawServiceHost(tt.isvc)
			assert.Equal(t, tt.expectedHost, host)
		})
	}
}

func TestGatewayAPIStrategy_IsHTTPRouteReady(t *testing.T) {
	tests := []struct {
		name            string
		status          gatewayapiv1.HTTPRouteStatus
		expectedReady   bool
		expectedReason  *string
		expectedMessage *string
	}{
		{
			name:          "empty status",
			status:        gatewayapiv1.HTTPRouteStatus{},
			expectedReady: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t)
			ready, _, _ := strategy.(*GatewayAPIStrategy).isHTTPRouteReady(tt.status)
			assert.Equal(t, tt.expectedReady, ready)
		})
	}
}

func TestGatewayAPIStrategy_GetComponentType(t *testing.T) {
	tests := []struct {
		name         string
		serviceName  string
		isvc         *v1beta1.InferenceService
		expectedType string
	}{
		{
			// engine HTTPRoute is "<isvc>-engine" (distinct from the
			// top-level "<isvc>"). Previously they collided.
			name:         "engine component",
			serviceName:  "test-isvc-engine",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "Engine",
		},
		{
			name:         "router component",
			serviceName:  "test-isvc-router",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "Router",
		},
		{
			name:         "decoder component",
			serviceName:  "test-isvc-decoder",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "Decoder",
		},
		{
			// Top-level HTTPRoute is named "<isvc>"; distinct from the
			// engine route.
			name:         "toplevel component",
			serviceName:  "test-isvc",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "TopLevel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t)
			componentType := strategy.(*GatewayAPIStrategy).getComponentType(tt.serviceName, tt.isvc)
			assert.Equal(t, tt.expectedType, componentType)
		})
	}
}

// Helper functions
func createGatewayAPIStrategy(t *testing.T) interfaces.IngressStrategy {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).Build()

	opts := interfaces.ReconcilerOptions{
		Client: fakeClient,
		Scheme: scheme,
		IngressConfig: &controllerconfig.IngressConfig{
			EnableGatewayAPI:  true,
			IngressDomain:     "example.com",
			OmeIngressGateway: "istio-system/gateway",
			DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			UrlScheme:         "https",
		},
		IsvcConfig: &controllerconfig.InferenceServicesConfig{},
	}

	return NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())
}

func createGatewayAPIStrategyWithClient(t *testing.T, client client.Client) interfaces.IngressStrategy {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	opts := interfaces.ReconcilerOptions{
		Client: client,
		Scheme: scheme,
		IngressConfig: &controllerconfig.IngressConfig{
			EnableGatewayAPI:  true,
			IngressDomain:     "example.com",
			OmeIngressGateway: "istio-system/gateway",
			DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			UrlScheme:         "https",
		},
		IsvcConfig: &controllerconfig.InferenceServicesConfig{},
	}

	return NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())
}

func createTestInferenceServiceGateway(name, namespace string) *v1beta1.InferenceService {
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

func createTestInferenceServiceWithRouterGateway(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceGateway(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	return isvc
}

func createTestInferenceServiceWithDecoderGateway(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceGateway(name, namespace)
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func createTestInferenceServiceWithClusterLocal(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceGateway(name, namespace)
	isvc.Labels = map[string]string{
		constants.VisibilityLabel: constants.ClusterLocalVisibility,
	}
	return isvc
}

func setComponentStatusReadyGateway(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.EngineReady, &apis.Condition{
		Type:   v1beta1.EngineReady,
		Status: corev1.ConditionTrue,
	})

	if isvc.Spec.Router != nil {
		isvc.Status.SetCondition(v1beta1.RouterReady, &apis.Condition{
			Type:   v1beta1.RouterReady,
			Status: corev1.ConditionTrue,
		})
	}

	if isvc.Spec.Decoder != nil {
		isvc.Status.SetCondition(v1beta1.DecoderReady, &apis.Condition{
			Type:   v1beta1.DecoderReady,
			Status: corev1.ConditionTrue,
		})
	}
}

func createReadyHTTPRoute(name, namespace string) *gatewayapiv1.HTTPRoute {
	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		// Simplified without detailed status
	}
}

func createNotReadyHTTPRoute(name, namespace string) *gatewayapiv1.HTTPRoute {
	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		// Simplified without detailed status
	}
}

func TestGatewayAPIStrategy_ReconcileBackendTrafficPolicy(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		existingPolicy bool
		expectedError  bool
	}{
		{
			name:          "create new policy",
			isvc:          createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			expectedError: false,
		},
		{
			name:           "update existing policy",
			isvc:           createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			existingPolicy: true,
			expectedError:  false,
		},
		{
			name:          "create policy for non-PD",
			isvc:          createTestInferenceServiceGateway("test-isvc", "default"),
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			if tt.existingPolicy {
				existing := &unstructured.Unstructured{}
				existing.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				existing.SetName(tt.isvc.Name)
				existing.SetNamespace(tt.isvc.Namespace)
				existing.Object["spec"] = map[string]interface{}{}
				objs = append(objs, existing)
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			err := strategy.(*GatewayAPIStrategy).reconcileBackendTrafficPolicy(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify the policy was created/updated
				policy := &unstructured.Unstructured{}
				policy.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				err := fakeClient.Get(context.Background(), types.NamespacedName{
					Name:      tt.isvc.Name,
					Namespace: tt.isvc.Namespace,
				}, policy)
				assert.NoError(t, err)
				assert.Equal(t, tt.isvc.Name, policy.GetName())
			}
		})
	}
}

func TestGatewayAPIStrategy_CheckBackendTrafficPolicyStatus(t *testing.T) {
	tests := []struct {
		name              string
		isvc              *v1beta1.InferenceService
		policy            *unstructured.Unstructured
		expectedCondition corev1.ConditionStatus
	}{
		{
			name: "policy not found - no error",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
		},
		{
			name: "policy accepted",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			policy: func() *unstructured.Unstructured {
				p := &unstructured.Unstructured{}
				p.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				p.SetName("test-isvc")
				p.SetNamespace("default")
				p.Object["status"] = map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Accepted",
							"status": "True",
							"reason": "Accepted",
						},
					},
				}
				return p
			}(),
			expectedCondition: corev1.ConditionTrue,
		},
		{
			name: "policy rejected",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			policy: func() *unstructured.Unstructured {
				p := &unstructured.Unstructured{}
				p.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				p.SetName("test-isvc")
				p.SetNamespace("default")
				p.Object["status"] = map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "Accepted",
							"status":  "False",
							"reason":  "Invalid",
							"message": "target not found",
						},
					},
				}
				return p
			}(),
			expectedCondition: corev1.ConditionFalse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			if tt.policy != nil {
				objs = append(objs, tt.policy)
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			err := strategy.(*GatewayAPIStrategy).checkBackendTrafficPolicyStatus(context.Background(), tt.isvc)
			assert.NoError(t, err)

			if tt.expectedCondition == corev1.ConditionFalse {
				condition := tt.isvc.Status.GetCondition(v1beta1.IngressReady)
				require.NotNil(t, condition)
				assert.Equal(t, corev1.ConditionFalse, condition.Status)
				assert.Contains(t, condition.Message, "BackendTrafficPolicy")
			}
		})
	}
}
