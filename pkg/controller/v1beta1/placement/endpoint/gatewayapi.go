package endpoint

import (
	"context"
	"fmt"
	"maps"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

const (
	// resourceSuffix names the backing resources the Gateway API publisher
	// creates for an InferenceService. The HTTPRoute and the ExternalName
	// Service that points at the winner's ingress share the "<isvc>-global"
	// name, distinct from the per-cluster ingress resources OME's normal ingress
	// reconciler emits ("<isvc>", "<isvc>-engine", ...) so the two never collide.
	resourceSuffix = "-global"
)

// GatewayAPIPublisher implements EndpointPublisher by programming a Gateway API
// HTTPRoute on the control-plane cluster whose backend is an ExternalName
// Service aliasing the winning workload cluster's ingress host. Because the
// winner's ingress lives on another cluster, an ExternalName Service is the
// portable, implementation-agnostic way to name an off-cluster backend a
// Gateway API HTTPRoute can reference (no vendor-specific external-backend CRD).
//
// On re-placement the ExternalName target is repointed (and the HTTPRoute's
// hostname re-rendered if it changed); on teardown both resources are deleted.
type GatewayAPIPublisher struct {
	client client.Client
	config Config
}

var _ EndpointPublisher = (*GatewayAPIPublisher)(nil)

// NewGatewayAPIPublisher constructs the Gateway API backend. The client is the
// control-plane cluster client (the global gateway and the published resources
// live there).
func NewGatewayAPIPublisher(c client.Client, cfg Config) *GatewayAPIPublisher {
	return &GatewayAPIPublisher{client: c, config: cfg}
}

func (p *GatewayAPIPublisher) Name() string { return "GatewayAPI" }

// resourceName is the shared name of the HTTPRoute + ExternalName Service for an
// InferenceService.
func resourceName(isvc *v1beta1.InferenceService) string {
	return isvc.Name + resourceSuffix
}

// routeNamespace resolves the namespace the published resources live in: the
// configured RouteNamespace, or the ISVC's own namespace when unset.
func (p *GatewayAPIPublisher) routeNamespace(isvc *v1beta1.InferenceService) string {
	if ns := strings.TrimSpace(p.config.RouteNamespace); ns != "" {
		return ns
	}
	return isvc.Namespace
}

// Publish ensures the ExternalName Service and HTTPRoute exist and point the
// global host at target.BackendHost. Repointing on re-placement is just an
// update of the same two named resources.
func (p *GatewayAPIPublisher) Publish(ctx context.Context, isvc *v1beta1.InferenceService, target Target) error {
	if err := p.applyService(ctx, isvc, target); err != nil {
		return err
	}
	return p.applyRoute(ctx, isvc, target)
}

// Unpublish deletes both resources. Missing resources are tolerated so a
// double-teardown (e.g. finalizer + a re-queued unplaced pass) is a no-op.
func (p *GatewayAPIPublisher) Unpublish(ctx context.Context, isvc *v1beta1.InferenceService) error {
	ns := p.routeNamespace(isvc)
	name := resourceName(isvc)
	route := &gatewayapiv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	if err := p.client.Delete(ctx, route); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete global HTTPRoute %s/%s: %w", ns, name, err)
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	if err := p.client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete global backend Service %s/%s: %w", ns, name, err)
	}
	return nil
}

func (p *GatewayAPIPublisher) applyService(ctx context.Context, isvc *v1beta1.InferenceService, target Target) error {
	desired := p.buildExternalNameService(isvc, target)
	existing := &corev1.Service{}
	err := p.client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := p.client.Create(ctx, desired); err != nil {
			return fmt.Errorf("create global backend Service %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	// Repoint (re-placement) or label drift: update in place. Carry the live
	// ResourceVersion and preserve the cluster-assigned spec fields we do not own.
	desired.ResourceVersion = existing.ResourceVersion
	desired.Spec.ClusterIP = existing.Spec.ClusterIP
	desired.Spec.ClusterIPs = existing.Spec.ClusterIPs
	if equality.Semantic.DeepEqual(desired.Spec, existing.Spec) &&
		maps.Equal(desired.Labels, existing.Labels) {
		return nil
	}
	if err := p.client.Update(ctx, desired); err != nil {
		return fmt.Errorf("update global backend Service %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

func (p *GatewayAPIPublisher) applyRoute(ctx context.Context, isvc *v1beta1.InferenceService, target Target) error {
	desired := p.buildHTTPRoute(isvc, target)
	existing := &gatewayapiv1.HTTPRoute{}
	err := p.client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := p.client.Create(ctx, desired); err != nil {
			return fmt.Errorf("create global HTTPRoute %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	desired.ResourceVersion = existing.ResourceVersion
	if equality.Semantic.DeepEqual(desired.Spec, existing.Spec) &&
		maps.Equal(desired.Labels, existing.Labels) {
		return nil
	}
	if err := p.client.Update(ctx, desired); err != nil {
		return fmt.Errorf("update global HTTPRoute %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// buildExternalNameService renders the ExternalName Service that aliases the
// winning cluster's ingress host. The externalName is repointed on
// re-placement; the Service name is stable so the HTTPRoute backendRef never
// has to change.
func (p *GatewayAPIPublisher) buildExternalNameService(isvc *v1beta1.InferenceService, target Target) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(isvc),
			Namespace: p.routeNamespace(isvc),
			Labels:    p.labelsFor(target),
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: target.BackendHost,
			Ports: []corev1.ServicePort{{
				Port: p.config.BackendPort,
			}},
		},
	}
}

// buildHTTPRoute renders the global HTTPRoute: it attaches to the configured
// global gateway, matches the global host at root, and forwards to the
// ExternalName Service.
func (p *GatewayAPIPublisher) buildHTTPRoute(isvc *v1beta1.InferenceService, target Target) *gatewayapiv1.HTTPRoute {
	gwNS, gwName := splitGatewayRef(p.config.GlobalGateway)
	backendNS := p.routeNamespace(isvc)
	port := gatewayapiv1.PortNumber(p.config.BackendPort)
	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(isvc),
			Namespace: backendNS,
			Labels:    p.labelsFor(target),
		},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{
				ParentRefs: []gatewayapiv1.ParentReference{{
					Group:     (*gatewayapiv1.Group)(&gatewayapiv1.GroupVersion.Group),
					Kind:      (*gatewayapiv1.Kind)(ptr.To(constants.GatewayKind)),
					Namespace: (*gatewayapiv1.Namespace)(&gwNS),
					Name:      gatewayapiv1.ObjectName(gwName),
				}},
			},
			Hostnames: []gatewayapiv1.Hostname{gatewayapiv1.Hostname(target.GlobalHost)},
			Rules: []gatewayapiv1.HTTPRouteRule{{
				Matches: []gatewayapiv1.HTTPRouteMatch{{
					Path: &gatewayapiv1.HTTPPathMatch{
						Type:  ptr.To(gatewayapiv1.PathMatchPathPrefix),
						Value: ptr.To("/"),
					},
				}},
				BackendRefs: []gatewayapiv1.HTTPBackendRef{{
					BackendRef: gatewayapiv1.BackendRef{
						BackendObjectReference: gatewayapiv1.BackendObjectReference{
							Kind:      ptr.To(gatewayapiv1.Kind(constants.ServiceKind)),
							Name:      gatewayapiv1.ObjectName(resourceName(isvc)),
							Namespace: (*gatewayapiv1.Namespace)(&backendNS),
							Port:      &port,
						},
					},
				}},
			}},
		},
	}
}

// labelsFor merges the operator-configured Labels with the publisher's own
// ownership markers so published resources are discoverable and the reconciler
// can recognize what it created. The owned keys win over collisions.
func (p *GatewayAPIPublisher) labelsFor(target Target) map[string]string {
	out := map[string]string{}
	maps.Copy(out, p.config.Labels)
	out[ManagedByLabel] = ManagedByValue
	if target.Cluster != "" {
		out[PlacementClusterLabel] = target.Cluster
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// splitGatewayRef parses a "namespace/name" gateway reference. A bare "name"
// (no slash) resolves to an empty namespace, which Gateway API treats as the
// route's own namespace.
func splitGatewayRef(ref string) (namespace, name string) {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
