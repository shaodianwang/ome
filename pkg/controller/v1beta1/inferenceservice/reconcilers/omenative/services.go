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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
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

// componentSelector builds the pod-label selector that scopes every
// per-Component Service (headless + stable) to the OMENative-managed
// pods of one (ISVC, component) pair. Shared between the headless
// spec builder and the stable Service builder so the two Services
// always select the same pod set — drift between the two would let
// ingress routing publish pods that peer-DNS resolution couldn't see
// (or vice versa).
func componentSelector(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}

// BuildHeadlessServiceSpec adapts an InferenceService + ComponentType
// into the typed workload.PerComponentServiceSpec the workload
// renderer consumes. This IS the adapter between the v1beta1 ISVC
// shape and the owner-agnostic workload boundary — workload/services.go
// never sees the ISVC. The future IR adapter will have its own
// equivalent helper populating PerComponentServiceSpec from
// InferenceReplica fields.
func BuildHeadlessServiceSpec(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) workloadtypes.PerComponentServiceSpec {
	selector := componentSelector(isvc, component)
	return workloadtypes.PerComponentServiceSpec{
		Name:      query.HeadlessServiceName(isvc.Name, component),
		Namespace: isvc.Namespace,
		Selector:  selector,
		Labels:    selector,
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
		},
	}
}

// ensureHeadlessService is the entry point ReconcileComponent calls.
// Reconciles BOTH the per-Component headless Service (delegated to
// workload.ReconcileHeadlessService) AND the per-Component stable
// Service (still owned by this package — see BuildStableService).
func ensureHeadlessService(ctx context.Context, p ReconcileParams) error {
	if p.ISVC == nil {
		return fmt.Errorf("OMENative reconciler: ensure headless service (component=%s): nil ISVC", p.Component)
	}
	spec := BuildHeadlessServiceSpec(p.ISVC, p.Component)
	if err := workload.ReconcileHeadlessService(ctx, p.Client, spec); err != nil {
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
	selector := componentSelector(isvc, component)
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

// reconcileStableService is the CreateOrUpdate companion to the
// workload-owned headless Service reconciler for the stable Service.
// Idempotent + drift-correcting on Selector / Ports / Labels /
// OwnerReferences; ClusterIP stays whatever the apiserver assigned on
// Create (immutable post-create).
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
