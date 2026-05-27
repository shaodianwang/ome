package omenative

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
)

// stableServicePort is the port the per-Component stable Service
// exposes. 8080 matches per-revision routing Services
// (coordination.perRevisionServicePort) and the long-standing
// inferencing convention (kserve / kfserving / OME). Runtimes that
// listen elsewhere should set spec.{Engine,Decoder,Router}.Runner.Ports
// — once that wiring lands, both Services can derive ports from the
// rendered container spec like Raw does.
const stableServicePort int32 = 8080

// isvcGroupVersionKind identifies the InferenceService owner reference
// stamped on every emitted Service. ops/render.go has its own copy for
// pod-owner refs; keeping a separate one here avoids a top-level → ops/
// import (which would close a cycle since the top-level reconciler
// dispatches into ops/).
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// BuildHeadlessService produces the per-Component headless Service that
// gives every OMENative-managed pod a stable FQDN of the form
//
//	<pod-name>.<isvc>-<component>-headless.<namespace>.svc.<cluster-domain>
//
// Properties:
//   - ClusterIP=None — no kube-proxy load-balancing; DNS lookups return
//     per-pod A records.
//   - PublishNotReadyAddresses=true — peer DNS resolves during gang init
//     before all pods are Ready, so workers can discover each other while
//     the controller-owned ome.io/serving gate is still False.
//   - Selector scoped to ome.io/managed-by=OMENative so the Service does
//     not pick up legacy LWS-managed or RawDeployment pods that share the
//     bare component label.
//   - One OwnerReference back to the parent InferenceService; deletion of
//     the ISVC cascades to this Service.
//
// Pure compute, no I/O.
func BuildHeadlessService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (*corev1.Service, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}

	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.HeadlessServiceName(isvc.Name, component),
			Namespace: isvc.Namespace,
			Labels:    selector,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selector,
			// Ports omitted by design: this Service exists for DNS-based
			// peer discovery, not for kube-proxy routing. Each runtime
			// resolves <pod-name>.<this-service>... and dials whatever
			// port it cares about. Adding a placeholder port would imply
			// load-balanceable endpoints, which is the opposite of what
			// the headless contract is for.
			Type: corev1.ServiceTypeClusterIP,
		},
	}, nil
}

// reconcileHeadlessService ensures the per-Component headless Service
// (`<isvc>-<component>-headless`) exists and matches the spec
// BuildHeadlessService produces. Idempotent and drift-correcting: on
// every reconcile it patches Selector / PublishNotReadyAddresses /
// Type / Labels / OwnerReferences back to the controller-desired
// shape, but never touches ClusterIP after Create (immutable
// post-create).
//
// Called at the top of ReconcileComponent before any per-Instance
// work so the drain path's EndpointSlice reads have a Service to
// look at on the first reconcile. Without this, drain.IsPodDrained would
// fall through its "no slices == drained" branch on every drain
// check and skip the wait entirely — see drain.go's revised
// fail-loud behavior.
func reconcileHeadlessService(ctx context.Context, p ReconcileParams) error {
	if p.Client == nil {
		return fmt.Errorf("reconcileHeadlessService: nil client")
	}
	if p.ISVC == nil {
		return fmt.Errorf("reconcileHeadlessService: nil ISVC")
	}

	desired, err := BuildHeadlessService(p.ISVC, p.Component)
	if err != nil {
		return fmt.Errorf("reconcileHeadlessService: build desired: %w", err)
	}

	target := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, p.Client, target, func() error {
		// ClusterIP is immutable after create. Only stamp it on the
		// fresh-create path; existing Services keep whatever the
		// apiserver assigned (or kept from previous reconciles, since
		// we always declare ClusterIPNone).
		if target.CreationTimestamp.IsZero() {
			target.Spec.ClusterIP = desired.Spec.ClusterIP
		}
		target.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.Type = desired.Spec.Type
		// Ports stay nil — see BuildHeadlessService comment for why
		// (peer-DNS only, no kube-proxy routing).
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconcileHeadlessService: CreateOrUpdate %s/%s: %w",
			target.Namespace, target.Name, err)
	}
	return nil
}

// ensureHeadlessService is the entry point ReconcileComponent calls.
// Wrapped for symmetry with future per-Component service emissions
// (routing Service, ingress Service) that will join this top-of-
// reconcile block as separate small helpers under one umbrella.
func ensureHeadlessService(ctx context.Context, p ReconcileParams) error {
	if err := reconcileHeadlessService(ctx, p); err != nil {
		return fmt.Errorf("OMENative reconciler: ensure headless service (component=%s): %w", p.Component, err)
	}
	if err := reconcileStableService(ctx, p); err != nil {
		return fmt.Errorf("OMENative reconciler: ensure stable service (component=%s): %w", p.Component, err)
	}
	return nil
}

// BuildStableService produces the per-Component stable Service —
// `<isvc>-<comp>` — that the ingress reconciler's existing GatewayAPI /
// KubernetesIngress strategies target as a backendRef. Selects all
// OMENative pods for the Component regardless of revision, so traffic
// continues flowing across rolling updates without HTTPRoute churn.
//
// Shape:
//   - ClusterIP (regular, not headless) — kube-proxy load-balances
//     across pods so external clients reach the Service IP.
//   - Single TCP port 8080 (stableServicePort) — matches the
//     per-revision routing Service and the inferencing convention.
//   - Selector scoped to ome.io/managed-by=OMENative so it never picks
//     up legacy LWS or RawDeployment pods that share the bare
//     component/inferenceservice labels.
//   - One OwnerReference back to the ISVC; deletion cascades.
//
// Pure compute, no I/O.
func BuildStableService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (*corev1.Service, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.StableServiceName(isvc.Name, component),
			Namespace: isvc.Namespace,
			Labels:    selector,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       stableServicePort,
				TargetPort: intstr.FromInt32(stableServicePort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}, nil
}

// reconcileStableService is the CreateOrUpdate companion to
// reconcileHeadlessService for the stable Service. Idempotent +
// drift-correcting on Selector / Ports / Labels / OwnerReferences;
// ClusterIP stays whatever the apiserver assigned on Create (immutable
// post-create).
func reconcileStableService(ctx context.Context, p ReconcileParams) error {
	if p.Client == nil {
		return fmt.Errorf("reconcileStableService: nil client")
	}
	if p.ISVC == nil {
		return fmt.Errorf("reconcileStableService: nil ISVC")
	}
	desired, err := BuildStableService(p.ISVC, p.Component)
	if err != nil {
		return fmt.Errorf("reconcileStableService: build desired: %w", err)
	}
	target := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, p.Client, target, func() error {
		if target.CreationTimestamp.IsZero() {
			target.Spec.Type = desired.Spec.Type
		}
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.Ports = desired.Spec.Ports
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconcileStableService: CreateOrUpdate %s/%s: %w",
			target.Namespace, target.Name, err)
	}
	return nil
}
