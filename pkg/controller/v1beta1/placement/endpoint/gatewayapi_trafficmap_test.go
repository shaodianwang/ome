package endpoint

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func gatewayTrafficMap(owner *v1beta1.InferenceService, entries ...v1beta1.TrafficMapEntry) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: owner.Name, Namespace: owner.Namespace},
		Spec: v1beta1.TrafficMapSpec{
			Service: owner.Name,
			Entries: entries,
		},
	}
}

func gatewayTrafficMapEntry(cluster, host string, weight int32) v1beta1.TrafficMapEntry {
	return v1beta1.TrafficMapEntry{
		Cluster:  cluster,
		Endpoint: &apis.URL{Scheme: "https", Host: host},
		Weight:   weight,
	}
}

type staleGatewayAPIReadClient struct {
	client.Client
}

func (c staleGatewayAPIReadClient) Get(
	_ context.Context,
	key client.ObjectKey,
	_ client.Object,
	_ ...client.GetOption,
) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: gatewayapiv1.GroupVersion.Group, Resource: "objects"}, key.Name)
}

func (c staleGatewayAPIReadClient) List(
	_ context.Context,
	_ client.ObjectList,
	_ ...client.ListOption,
) error {
	return nil
}

func TestGatewayAPITrafficMapPublisherIdentityAndOptions(t *testing.T) {
	p := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())

	assert.Equal(t, GatewayAPITrafficMapPublisherName, p.Name())
	assert.Equal(t, "gatewayapi", p.Name())
	assert.True(t, p.Stateful())
	resolved, err := p.ResolveOptions(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, resolved)

	_, err = p.ResolveOptions(nil, map[string]string{"z": "1", "a": "2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "per-InferenceService options: a, z")
	_, err = p.ResolveOptions(map[string]string{"unused": "value"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "global options: unused")
}

func TestGatewayAPITrafficMapPlanIsImmutableAndAppliesWeights(t *testing.T) {
	scheme := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	cfg := baseConfig()
	p := NewGatewayAPITrafficMapPublisher(c, c, cfg)
	owner := testISVC()
	owner.Annotations = map[string]string{GlobalHostAnnotation: "original.global.example"}
	trafficMap := gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("cluster-b", "b.example:443", 0),
		gatewayTrafficMapEntry("cluster-a", "a.example:443", 7),
	)

	plan, err := p.Plan(owner, trafficMap, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, []string{"v1:httproute:prod/svc-global"}, plan.Claims())

	owner.Annotations[GlobalHostAnnotation] = "mutated.global.example"
	trafficMap.Spec.Entries[0].Weight = 99
	cfg.Labels["team"] = "mutated"
	claims := plan.Claims()
	claims[0] = "mutated"
	assert.Equal(t, []string{"v1:httproute:prod/svc-global"}, plan.Claims())

	result, err := p.Apply(context.Background(), plan)
	require.NoError(t, err)
	require.NotNil(t, result.GatewayRef)
	assert.Equal(t, gatewayapiv1.GroupVersion.Group, result.GatewayRef.Group)
	assert.Equal(t, gatewayAPIHTTPRouteKind, result.GatewayRef.Kind)
	assert.Equal(t, "prod", result.GatewayRef.Namespace)
	assert.Equal(t, "svc-global", result.GatewayRef.Name)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	assert.Equal(t, []gatewayapiv1.Hostname{"original.global.example"}, route.Spec.Hostnames)
	assert.Equal(t, "platform", route.Labels["team"])
	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 2)
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-a"), route.Spec.Rules[0].BackendRefs[0].Name)
	assert.Equal(t, int32(7), *route.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-b"), route.Spec.Rules[0].BackendRefs[1].Name)
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[1].Weight)
}

func TestGatewayAPITrafficMapApplyPreservesEmptyAndAllZeroMaps(t *testing.T) {
	scheme := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	owner := testISVC()

	nonEmpty, err := p.Plan(owner, gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("cluster-a", "a.example", 1),
	), nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), nonEmpty)
	require.NoError(t, err)

	empty, err := p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err)
	result, err := p.Apply(context.Background(), empty)
	require.NoError(t, err)
	require.NotNil(t, result.GatewayRef)
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	require.Len(t, route.Spec.Rules, 1)
	assert.Empty(t, route.Spec.Rules[0].BackendRefs)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global-cluster-a"}, &corev1.Service{})))

	allZero, err := p.Plan(owner, gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("cluster-a", "a.example", 0),
		gatewayTrafficMapEntry("cluster-b", "b.example", 0),
	), nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), allZero)
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	require.Len(t, route.Spec.Rules[0].BackendRefs, 2)
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[1].Weight)
}

func TestGatewayAPITrafficMapApplyWithdrawsWhenHostIsRemoved(t *testing.T) {
	scheme := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	cfg := baseConfig()
	cfg.GlobalHostTemplate = ""
	p := NewGatewayAPITrafficMapPublisher(c, c, cfg)
	owner := testISVC()
	owner.Annotations = map[string]string{GlobalHostAnnotation: "svc.global.example"}
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 5))

	publishPlan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), publishPlan)
	require.NoError(t, err)

	delete(owner.Annotations, GlobalHostAnnotation)
	trafficMap.Spec.Entries = make([]v1beta1.TrafficMapEntry, maxGatewayAPIBackendRefs+1)
	withdrawPlan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	assert.Equal(t, publishPlan.Claims(), withdrawPlan.Claims())
	result, err := p.Apply(context.Background(), withdrawPlan)
	require.NoError(t, err)
	assert.Nil(t, result.GatewayRef)
	assert.True(t, result.Withdrawn)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global-cluster-a"}, &corev1.Service{})))
}

func TestGatewayAPITrafficMapApplyWithdrawsWhenConfigIsDisabled(t *testing.T) {
	scheme := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 5))

	enabled := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	plan, err := enabled.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = enabled.Apply(context.Background(), plan)
	require.NoError(t, err)

	disabledConfig := baseConfig()
	disabledConfig.BackendPort = 0
	disabled := NewGatewayAPITrafficMapPublisher(c, c, disabledConfig)
	plan, err = disabled.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	result, err := disabled.Apply(context.Background(), plan)
	require.NoError(t, err)
	assert.Nil(t, result.GatewayRef)
	assert.True(t, result.Withdrawn)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
}

func TestGatewayAPITrafficMapApplyRejectsUnownedResourcesBeforeMutation(t *testing.T) {
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 5))

	t.Run("route", func(t *testing.T) {
		scheme := pubScheme(t)
		foreign := &gatewayapiv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc-global"},
			Spec: gatewayapiv1.HTTPRouteSpec{
				Hostnames: []gatewayapiv1.Hostname{"foreign.example"},
			},
		}
		c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
		p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
		plan, err := p.Plan(owner, trafficMap, nil)
		require.NoError(t, err)

		_, err = p.Apply(context.Background(), plan)
		require.ErrorContains(t, err, "belongs to another source")
		got := &gatewayapiv1.HTTPRoute{}
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
		assert.Equal(t, []gatewayapiv1.Hostname{"foreign.example"}, got.Spec.Hostnames)
		assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
			types.NamespacedName{Namespace: "prod", Name: "svc-global-cluster-a"}, &corev1.Service{})),
			"strict preflight must reject the route before creating a backend Service")
	})

	t.Run("service", func(t *testing.T) {
		scheme := pubScheme(t)
		foreign := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc-global-cluster-a"},
			Spec: corev1.ServiceSpec{
				Type:         corev1.ServiceTypeExternalName,
				ExternalName: "foreign.example",
			},
		}
		c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
		p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
		plan, err := p.Plan(owner, trafficMap, nil)
		require.NoError(t, err)

		_, err = p.Apply(context.Background(), plan)
		require.ErrorContains(t, err, "belongs to another source")
		got := &corev1.Service{}
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global-cluster-a"}, got))
		assert.Equal(t, "foreign.example", got.Spec.ExternalName)
		assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
			types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})),
			"strict preflight must reject the Service before creating an HTTPRoute")
	})
}

func TestGatewayAPITrafficMapPlanRejectsInvalidInputs(t *testing.T) {
	p := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())
	owner := testISVC()

	entries := make([]v1beta1.TrafficMapEntry, maxGatewayAPIBackendRefs+1)
	for i := range entries {
		entries[i] = gatewayTrafficMapEntry(fmt.Sprintf("cluster-%02d", i), fmt.Sprintf("%d.example", i), 1)
	}
	_, err := p.Plan(owner, gatewayTrafficMap(owner, entries...), nil)
	require.ErrorContains(t, err, "support at most 16 backendRefs")

	tests := []struct {
		name    string
		entries []v1beta1.TrafficMapEntry
		want    string
	}{
		{name: "empty cluster", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("", "a.example", 1)}, want: "cluster must not be empty"},
		{name: "duplicate cluster", entries: []v1beta1.TrafficMapEntry{
			gatewayTrafficMapEntry("cluster-a", "a.example", 1),
			gatewayTrafficMapEntry("cluster-a", "b.example", 1),
		}, want: "more than one entry"},
		{name: "missing endpoint", entries: []v1beta1.TrafficMapEntry{{Cluster: "cluster-a", Weight: 1}}, want: "has no valid endpoint"},
		{name: "negative weight", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "a.example", -1)}, want: "must be between"},
		{name: "weight above Gateway limit", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "a.example", maxGatewayAPIBackendWeight+1)}, want: "must be between"},
		{name: "invalid backend hostname", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "UPPER.example", 1)}, want: "backend hostname"},
		{name: "IP backend hostname", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "192.0.2.1", 1)}, want: "must not be an IP address"},
		{name: "invalid generated service name", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("INVALID", "a.example", 1)}, want: "invalid backend Service name"},
		{name: "cluster cannot fit label", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry(strings.Repeat("a", 64), "a.example", 1)}, want: "label"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Plan(owner, gatewayTrafficMap(owner, tt.entries...), nil)
			require.ErrorContains(t, err, tt.want)
		})
	}
	collisionSuffix := strings.Repeat("a", 54)
	_, err = p.Plan(owner, gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("c13303-"+collisionSuffix, "a.example", 1),
		gatewayTrafficMapEntry("c82303-"+collisionSuffix, "b.example", 1),
	), nil)
	require.ErrorContains(t, err, "generate the same backend Service name")

	_, err = p.Plan(owner, gatewayTrafficMap(owner), map[string]string{"unexpected": "value"})
	require.ErrorContains(t, err, "effective options")

	owner.Finalizers = []string{EndpointFinalizer}
	_, err = p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.ErrorContains(t, err, "legacy endpoint finalizer")
	owner.Finalizers = nil

	owner.Annotations = map[string]string{GlobalHostAnnotation: "INVALID.example"}
	_, err = p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.ErrorContains(t, err, "global hostname")
	owner.Annotations[GlobalHostAnnotation] = "*.example.com"
	_, err = p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err, "Gateway API permits a wildcard in the first hostname label")
}

func TestGatewayAPITrafficMapPlanValidatesPublishedResourceConfig(t *testing.T) {
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner)
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "port above protocol limit",
			mutate: func(cfg *Config) { cfg.BackendPort = 65_536 },
			want:   "backend port",
		},
		{
			name:   "gateway has extra separator",
			mutate: func(cfg *Config) { cfg.GlobalGateway = "system/edge/extra" },
			want:   "name or namespace/name",
		},
		{
			name:   "gateway has invalid namespace",
			mutate: func(cfg *Config) { cfg.GlobalGateway = "INVALID/edge" },
			want:   "gateway namespace",
		},
		{
			name:   "gateway has invalid name",
			mutate: func(cfg *Config) { cfg.GlobalGateway = "system/INVALID" },
			want:   "gateway name",
		},
		{
			name:   "label has invalid key",
			mutate: func(cfg *Config) { cfg.Labels = map[string]string{"bad key": "value"} },
			want:   "label key",
		},
		{
			name: "label has invalid value",
			mutate: func(cfg *Config) {
				cfg.Labels = map[string]string{"example.com/team": strings.Repeat("x", 64)}
			},
			want: "label",
		},
		{
			name: "TLS well-known CA is empty",
			mutate: func(cfg *Config) {
				cfg.GatewayBackend.TLS.Enabled = true
			},
			want: "well-known CA certificates",
		},
		{
			name: "TLS well-known CA is unsupported",
			mutate: func(cfg *Config) {
				cfg.GatewayBackend.TLS.Enabled = true
				cfg.GatewayBackend.TLS.WellKnownCACertificates = "Custom"
			},
			want: "well-known CA certificates",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			p := NewGatewayAPITrafficMapPublisher(nil, nil, cfg)
			_, err := p.Plan(owner, trafficMap, nil)
			require.ErrorContains(t, err, tt.want)
		})
	}

	longNameOwner := testISVC()
	longNameOwner.Name = strings.Repeat("a", 64)
	p := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())
	_, err := p.Plan(longNameOwner, gatewayTrafficMap(longNameOwner), nil)
	require.ErrorContains(t, err, "cannot be used as a label value")
}

func TestValidateGatewayAPIHTTPRouteClaims(t *testing.T) {
	valid := "v1:httproute:gateway-system/service-global"
	routeKeys, err := validateGatewayAPIHTTPRouteClaims([]string{
		"v1:httproute:z-system/z-route",
		valid,
	})
	require.NoError(t, err)
	assert.Equal(t, []types.NamespacedName{
		{Namespace: "gateway-system", Name: "service-global"},
		{Namespace: "z-system", Name: "z-route"},
	}, routeKeys)

	tooMany := make([]string, v1beta1.MaxTrafficMapPublisherTargets+1)
	for i := range tooMany {
		tooMany[i] = valid
	}
	tests := []struct {
		name   string
		claims []string
		want   string
	}{
		{name: "unknown version", claims: []string{"v2:httproute:gateway-system/service-global"}, want: "unsupported claim version or kind"},
		{name: "unknown kind", claims: []string{"v1:service:gateway-system/service-global"}, want: "unsupported claim version or kind"},
		{name: "missing separator", claims: []string{"v1:httproute:gateway-system"}, want: "exactly one namespace/name separator"},
		{name: "extra separator", claims: []string{"v1:httproute:gateway-system/a/b"}, want: "exactly one namespace/name separator"},
		{name: "invalid namespace", claims: []string{"v1:httproute:Gateway/service-global"}, want: "namespace"},
		{name: "invalid name", claims: []string{"v1:httproute:gateway-system/Service"}, want: "name"},
		{name: "noncanonical", claims: []string{"v1:httproute:gateway-system/service-global "}, want: "invalid"},
		{name: "duplicate", claims: []string{valid, valid}, want: "appears more than once"},
		{name: "too many", claims: tooMany, want: "maximum"},
		{name: "too long", claims: []string{gatewayAPIHTTPRouteClaimPrefix + "ns/" + strings.Repeat("a", v1beta1.MaxTrafficMapPublisherTargetLength)}, want: "bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateGatewayAPIHTTPRouteClaims(tt.claims)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestGatewayAPITrafficMapDrainValidatesAllClaimsBeforeMutation(t *testing.T) {
	scheme := pubScheme(t)
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod",
			Name:      "svc-global",
			Labels: map[string]string{
				ManagedByLabel:                      ManagedByValue,
				PlacementEndpointISVCLabel:          "svc",
				PlacementEndpointISVCNamespaceLabel: "prod",
			},
		},
		Spec: gatewayapiv1.HTTPRouteSpec{Rules: []gatewayapiv1.HTTPRouteRule{{
			BackendRefs: []gatewayapiv1.HTTPBackendRef{{
				BackendRef: gatewayapiv1.BackendRef{Weight: ptr.To(int32(9))},
			}},
		}}},
	}
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	validClaim := "v1:httproute:prod/svc-global"

	invalidSourceKey := types.NamespacedName{Namespace: "Prod", Name: "svc"}
	err := p.Drain(context.Background(), invalidSourceKey, []string{validClaim})
	require.ErrorContains(t, err, "drain source")
	got := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
	assert.Equal(t, int32(9), *got.Spec.Rules[0].BackendRefs[0].Weight)

	sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}
	err = p.Drain(context.Background(), sourceKey, []string{validClaim, "v2:httproute:prod/other"})
	require.Error(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
	assert.Equal(t, int32(9), *got.Spec.Rules[0].BackendRefs[0].Weight)

	require.NoError(t, p.Drain(context.Background(), sourceKey, []string{validClaim}))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
	assert.Equal(t, int32(0), *got.Spec.Rules[0].BackendRefs[0].Weight)
}

func TestGatewayAPITrafficMapUnpublishUsesOnlyJournalAndSourceKey(t *testing.T) {
	scheme := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 3))
	plan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), plan)
	require.NoError(t, err)

	journal := v1beta1.TrafficMapPublisherStatus{
		PublisherName:  p.Name(),
		ClaimedTargets: plan.Claims(),
	}
	require.NoError(t, p.Unpublish(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, journal))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global-cluster-a"}, &corev1.Service{})))
}

func TestGatewayAPITrafficMapCleanupUsesUncachedReader(t *testing.T) {
	scheme := pubScheme(t)
	live := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	cfg := baseConfig()
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 3))
	publisher := NewGatewayAPITrafficMapPublisher(live, live, cfg)
	plan, err := publisher.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = publisher.Apply(context.Background(), plan)
	require.NoError(t, err)

	publisher = NewGatewayAPITrafficMapPublisher(staleGatewayAPIReadClient{Client: live}, live, cfg)
	sourceKey := types.NamespacedName{Namespace: owner.Namespace, Name: owner.Name}
	require.NoError(t, publisher.Drain(context.Background(), sourceKey, plan.Claims()))
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, live.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[0].Weight)

	require.NoError(t, publisher.Unpublish(context.Background(), sourceKey, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  publisher.Name(),
		ClaimedTargets: plan.Claims(),
	}))
	assert.True(t, apierrors.IsNotFound(live.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(live.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global-cluster-a"}, &corev1.Service{})))
}

func TestGatewayAPITrafficMapUnpublishValidatesWholeJournalBeforeMutation(t *testing.T) {
	scheme := pubScheme(t)
	route := &gatewayapiv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod",
		Name:      "svc-global",
		Labels: map[string]string{
			ManagedByLabel:                      ManagedByValue,
			PlacementEndpointISVCLabel:          "svc",
			PlacementEndpointISVCNamespaceLabel: "prod",
		},
	}}
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}

	err := p.Unpublish(context.Background(), types.NamespacedName{Namespace: "Prod", Name: "svc"}, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  p.Name(),
		ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
	})
	require.ErrorContains(t, err, "cleanup source")
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{}))

	err = p.Unpublish(context.Background(), sourceKey, v1beta1.TrafficMapPublisherStatus{
		PublisherName: p.Name(),
		ClaimedTargets: []string{
			"v1:httproute:prod/svc-global",
			"v2:httproute:prod/invalid",
		},
	})
	require.Error(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{}))

	err = p.Unpublish(context.Background(), sourceKey, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "other",
		ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
	})
	require.ErrorContains(t, err, "owned by")
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{}))
}
