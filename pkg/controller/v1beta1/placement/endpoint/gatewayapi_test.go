package endpoint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func pubScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, gatewayapiv1.Install(s))
	return s
}

func testISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
	}
}

func baseConfig() Config {
	return Config{
		GlobalHostTemplate: "{{.Name}}.{{.Namespace}}.global.example",
		GlobalGateway:      "ome-system/global-gw",
		BackendPort:        8080,
		Labels:             map[string]string{"team": "platform"},
	}
}

func TestBuildExternalNameService(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	target := Target{GlobalHost: "svc.prod.global.example", Cluster: "cluster-a", BackendHost: "svc.prod.cw.example"}

	svc := p.buildExternalNameService(testISVC(), target)

	assert.Equal(t, "svc-global", svc.Name)
	assert.Equal(t, "prod", svc.Namespace, "namespace falls back to the ISVC namespace")
	assert.Equal(t, corev1.ServiceTypeExternalName, svc.Spec.Type)
	assert.Equal(t, "svc.prod.cw.example", svc.Spec.ExternalName, "ExternalName aliases the winner ingress host")
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
	assert.Equal(t, ManagedByValue, svc.Labels[ManagedByLabel])
	assert.Equal(t, "cluster-a", svc.Labels[PlacementClusterLabel])
	assert.Equal(t, "platform", svc.Labels["team"], "operator labels are merged in")
}

func TestBuildHTTPRoute(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	target := Target{GlobalHost: "svc.prod.global.example", Cluster: "cluster-a", BackendHost: "svc.prod.cw.example"}

	route := p.buildHTTPRoute(testISVC(), target)

	assert.Equal(t, "svc-global", route.Name)
	assert.Equal(t, "prod", route.Namespace)
	require.Len(t, route.Spec.Hostnames, 1)
	assert.Equal(t, gatewayapiv1.Hostname("svc.prod.global.example"), route.Spec.Hostnames[0])

	require.Len(t, route.Spec.ParentRefs, 1)
	pr := route.Spec.ParentRefs[0]
	require.NotNil(t, pr.Namespace)
	assert.Equal(t, "ome-system", string(*pr.Namespace), "gateway namespace parsed from namespace/name")
	assert.Equal(t, "global-gw", string(pr.Name))
	require.NotNil(t, pr.Kind)
	assert.Equal(t, constants.GatewayKind, string(*pr.Kind))

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	br := route.Spec.Rules[0].BackendRefs[0]
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global"), br.Name, "route forwards to the ExternalName Service")
	require.NotNil(t, br.Kind)
	assert.Equal(t, constants.ServiceKind, string(*br.Kind))
	require.NotNil(t, br.Port)
	assert.Equal(t, gatewayapiv1.PortNumber(8080), *br.Port)

	require.Len(t, route.Spec.Rules[0].Matches, 1)
	require.NotNil(t, route.Spec.Rules[0].Matches[0].Path)
	assert.Equal(t, "/", *route.Spec.Rules[0].Matches[0].Path.Value)
}

func TestBuildResources_RouteNamespaceOverride(t *testing.T) {
	cfg := baseConfig()
	cfg.RouteNamespace = "ome-gateways"
	p := NewGatewayAPIPublisher(nil, cfg)
	target := Target{GlobalHost: "h", Cluster: "c", BackendHost: "b"}

	svc := p.buildExternalNameService(testISVC(), target)
	route := p.buildHTTPRoute(testISVC(), target)

	assert.Equal(t, "ome-gateways", svc.Namespace)
	assert.Equal(t, "ome-gateways", route.Namespace)
	require.NotNil(t, route.Spec.Rules[0].BackendRefs[0].Namespace)
	assert.Equal(t, "ome-gateways", string(*route.Spec.Rules[0].BackendRefs[0].Namespace),
		"backendRef namespace tracks the route namespace")
}

func TestBuildHTTPRoute_BareGatewayName(t *testing.T) {
	cfg := baseConfig()
	cfg.GlobalGateway = "global-gw" // no namespace
	p := NewGatewayAPIPublisher(nil, cfg)

	route := p.buildHTTPRoute(testISVC(), Target{GlobalHost: "h", BackendHost: "b"})

	pr := route.Spec.ParentRefs[0]
	require.NotNil(t, pr.Namespace)
	assert.Equal(t, "", string(*pr.Namespace), "bare gateway name yields empty (route-local) namespace")
	assert.Equal(t, "global-gw", string(pr.Name))
}

func TestPublish_CreatesBothResources(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	target := Target{GlobalHost: "svc.prod.global.example", Cluster: "cluster-a", BackendHost: "svc.prod.cw.example"}

	require.NoError(t, p.Publish(context.Background(), isvc, target))

	svc := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, svc))
	assert.Equal(t, "svc.prod.cw.example", svc.Spec.ExternalName)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, gatewayapiv1.Hostname("svc.prod.global.example"), route.Spec.Hostnames[0])
}

func TestPublish_Idempotent(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	target := Target{GlobalHost: "svc.prod.global.example", Cluster: "cluster-a", BackendHost: "svc.prod.cw.example"}

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	rvBefore := route.ResourceVersion

	// Second identical publish must not churn the object (no spurious update).
	require.NoError(t, p.Publish(context.Background(), isvc, target))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, rvBefore, route.ResourceVersion, "identical re-publish must be a no-op")
}

func TestPublish_RepointOnReplacement(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()

	// Initial placement on cluster A.
	require.NoError(t, p.Publish(context.Background(), isvc,
		Target{GlobalHost: "svc.prod.global.example", Cluster: "cluster-a", BackendHost: "svc.prod.cw.example"}))

	// Re-placement onto cluster B: same global host, new backend host + cluster.
	require.NoError(t, p.Publish(context.Background(), isvc,
		Target{GlobalHost: "svc.prod.global.example", Cluster: "cluster-b", BackendHost: "svc.prod.gcp.example"}))

	svc := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, svc))
	assert.Equal(t, "svc.prod.gcp.example", svc.Spec.ExternalName, "ExternalName repointed to the new winner")
	assert.Equal(t, "cluster-b", svc.Labels[PlacementClusterLabel], "cluster label repointed")

	// The HTTPRoute name/host are stable; the backendRef still names the same
	// (stable) ExternalName Service, so the route did not need a backend swap.
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global"), route.Spec.Rules[0].BackendRefs[0].Name)
}

func TestUnpublish_DeletesBoth(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc,
		Target{GlobalHost: "svc.prod.global.example", Cluster: "cluster-a", BackendHost: "svc.prod.cw.example"}))

	require.NoError(t, p.Unpublish(context.Background(), isvc))

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(err), "ExternalName Service deleted")
	err = c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err), "HTTPRoute deleted")
}

func TestUnpublish_MissingIsNoOp(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	// Nothing was ever published; Unpublish must not error.
	require.NoError(t, p.Unpublish(context.Background(), testISVC()))
}
