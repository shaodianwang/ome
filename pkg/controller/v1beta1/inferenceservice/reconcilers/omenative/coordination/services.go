package coordination

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// perRevisionServicePort is the default port the per-revision routing
// Service exposes when the Component spec doesn't pin one. 8080 is the
// long-standing inferencing convention (kserve / kfserving / OME).
// HTTPRoute backendRef consumers target this port; runtimes that listen
// elsewhere should set spec.Engine.Runner.Ports (G14 — make this
// configurable). For now, hard-coded to keep the producer-side surface
// minimal and unblock the ClusterIP validation which requires ≥1 port.
const perRevisionServicePort int32 = 8080

// isvcGroupVersionKind identifies the InferenceService owner reference
// on each per-revision Service. Duplicated from the top-level
// omenative.services.go's copy to keep this subpackage importable from
// either the top-level reconciler or its caller without closing an
// import cycle.
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// PerRevisionService describes the routing + headless pair OMENative
// creates per (component, revisionHash). The two Services share the
// same selector but differ in ClusterIP semantics.
type PerRevisionService struct {
	// RoutingName is the ClusterIP Service name —
	// `<isvc>-<component>-rev-<hash>`. Used as the backend target by
	// the HTTPRoute weighted-backendRef consumer.
	RoutingName string

	// HeadlessName is the headless Service name —
	// `<isvc>-<component>-rev-<hash>-headless`. Used for pod-level
	// DNS within the revision (multi-node tensor-parallel groups need
	// per-pod addressability among one revision's pods).
	HeadlessName string
}

// PerRevisionServiceName returns the per-revision Service name for one
// (component, revisionHash) pair. Format: `<isvc>-<component>-rev-<hash>`.
func PerRevisionServiceName(isvcName string, component v1beta1.ComponentType, revisionHash string) string {
	return fmt.Sprintf("%s-%s-rev-%s", isvcName, component, revisionHash)
}

// PerRevisionHeadlessServiceName returns the per-revision headless
// Service name. Format: `<isvc>-<component>-rev-<hash>-headless`.
func PerRevisionHeadlessServiceName(isvcName string, component v1beta1.ComponentType, revisionHash string) string {
	return PerRevisionServiceName(isvcName, component, revisionHash) + "-headless"
}

// PerRevisionServiceNames returns the routing + headless names for a
// (component, revisionHash) pair.
func PerRevisionServiceNames(isvcName string, component v1beta1.ComponentType, revisionHash string) PerRevisionService {
	return PerRevisionService{
		RoutingName:  PerRevisionServiceName(isvcName, component, revisionHash),
		HeadlessName: PerRevisionHeadlessServiceName(isvcName, component, revisionHash),
	}
}

// BuildPerRevisionRoutingService produces the ClusterIP per-revision
// Service for one (component, revisionHash). Pure compute; idempotent.
func BuildPerRevisionRoutingService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string) (*corev1.Service, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}
	if revisionHash == "" {
		return nil, fmt.Errorf("empty revisionHash")
	}
	selector := perRevisionServiceSelector(isvc.Name, component, revisionHash)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PerRevisionServiceName(isvc.Name, component, revisionHash),
			Namespace: isvc.Namespace,
			Labels:    selector,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			Selector:                 selector,
			PublishNotReadyAddresses: false,
			Ports:                    perRevisionRoutingPorts(),
		},
	}, nil
}

// perRevisionRoutingPorts is the standard inferencing-port set.
// ClusterIP Services require at least one port (k8s validation); the
// headless variant accepts an empty list because peer DNS uses pod-IP
// resolution. HTTPRoute backendRef consumers route to this port.
func perRevisionRoutingPorts() []corev1.ServicePort {
	return []corev1.ServicePort{{
		Name:       "http",
		Port:       perRevisionServicePort,
		TargetPort: intstr.FromInt32(perRevisionServicePort),
		Protocol:   corev1.ProtocolTCP,
	}}
}

// BuildPerRevisionHeadlessService produces the headless per-revision
// Service. Like the routing variant but with ClusterIP=None and
// PublishNotReadyAddresses=true so peer DNS resolves during gang init.
func BuildPerRevisionHeadlessService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string) (*corev1.Service, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}
	if revisionHash == "" {
		return nil, fmt.Errorf("empty revisionHash")
	}
	selector := perRevisionServiceSelector(isvc.Name, component, revisionHash)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PerRevisionHeadlessServiceName(isvc.Name, component, revisionHash),
			Namespace: isvc.Namespace,
			Labels:    selector,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			ClusterIP:                corev1.ClusterIPNone,
			Selector:                 selector,
			PublishNotReadyAddresses: true,
		},
	}, nil
}

// perRevisionServiceSelector is the label set per-revision Services
// use as their .spec.selector. The revision-hash label narrows the
// selector to one ControllerRevision's pods; the OMENative-managed
// label keeps the selector from picking up legacy pods on the
// same Component.
func perRevisionServiceSelector(isvcName string, component v1beta1.ComponentType, revisionHash string) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		query.LabelRevisionHash:               revisionHash,
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}

// EnsurePerRevisionServices ensures both the routing and headless
// Services exist for one (component, revisionHash). Idempotent and
// drift-correcting on the selector / labels / owner reference. Returns
// the names so the coordination layer can record them on
// Status.Components.<c>.Traffic[].
func EnsurePerRevisionServices(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string) (PerRevisionService, error) {
	out := PerRevisionServiceNames(isvc.Name, component, revisionHash)
	if c == nil {
		return out, fmt.Errorf("EnsurePerRevisionServices: nil client")
	}

	if err := ensureService(ctx, c, isvc, BuildPerRevisionRoutingService, component, revisionHash, out.RoutingName, false); err != nil {
		return out, err
	}
	if err := ensureService(ctx, c, isvc, BuildPerRevisionHeadlessService, component, revisionHash, out.HeadlessName, true); err != nil {
		return out, err
	}
	return out, nil
}

// ensureService is the shared CreateOrUpdate helper for the routing /
// headless pair. headless distinguishes the ClusterIP semantics so the
// patch back to the desired shape doesn't try to flip an immutable
// ClusterIP after create.
func ensureService(
	ctx context.Context,
	c client.Client,
	isvc *v1beta1.InferenceService,
	build func(*v1beta1.InferenceService, v1beta1.ComponentType, string) (*corev1.Service, error),
	component v1beta1.ComponentType,
	revisionHash, name string,
	headless bool,
) error {
	desired, err := build(isvc, component, revisionHash)
	if err != nil {
		return fmt.Errorf("build per-revision service %s: %w", name, err)
	}
	target := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, target, func() error {
		if target.CreationTimestamp.IsZero() {
			if headless {
				target.Spec.ClusterIP = corev1.ClusterIPNone
			}
			target.Spec.Type = desired.Spec.Type
		}
		target.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.Ports = desired.Spec.Ports
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("CreateOrUpdate per-revision service %s/%s: %w", target.Namespace, target.Name, err)
	}
	return nil
}

// GCPerRevisionServices deletes both per-revision Services for one
// (component, revisionHash). Called when the revision is past
// retention AND no live pod carries the hash. NotFound is treated as
// success — the caller may invoke GC on stale entries.
func GCPerRevisionServices(ctx context.Context, c client.Client, namespace, isvcName string, component v1beta1.ComponentType, revisionHash string) error {
	if c == nil {
		return fmt.Errorf("GCPerRevisionServices: nil client")
	}
	names := PerRevisionServiceNames(isvcName, component, revisionHash)
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}
		if err := c.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete per-revision service %s/%s: %w", namespace, name, err)
		}
	}
	return nil
}
