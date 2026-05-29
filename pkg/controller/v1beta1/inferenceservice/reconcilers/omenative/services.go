package omenative

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// isvcGroupVersionKind identifies the InferenceService owner reference
// stamped on every emitted Service. ops/render.go has its own copy for
// pod-owner refs; keeping a separate one here avoids a top-level → ops/
// import (which would close a cycle since the top-level reconciler
// dispatches into ops/).
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// componentSelector builds the pod-label selector that scopes every
// per-Component Service (headless + stable) to the OMENative-managed
// pods of one (ISVC, component) pair. The `LabelManagedBy=OMENative`
// discriminator narrows the selector to OMENative-stamped pods so a
// later RawDeployment / MultiNode reconciler for the same (ISVC,
// component) cannot accidentally route to (or be routed by) OMENative
// pods during a mode-switch transition. Pods stamped by Raw / MultiNode
// never carry this label (see pkg/controller/v1beta1/inferenceservice/
// components/base.go ProcessBaseLabels).
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
		Name:      query.HeadlessServiceName(isvc.Name, internalsource.ComponentTypeToWorkload(component)),
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
// Service (delegated to the top-level service.ServiceReconciler — the
// SAME reconciler RawDeployment / MultiNode use, so all three modes
// converge on one Service implementation).
func ensureHeadlessService(ctx context.Context, p ReconcileParams) error {
	if p.ISVC == nil {
		return fmt.Errorf("OMENative reconciler: ensure headless service (component=%s): nil ISVC", p.Component)
	}
	spec := BuildHeadlessServiceSpec(p.ISVC, p.Component)
	if err := workload.ReconcileHeadlessService(ctx, p.Client, spec); err != nil {
		return fmt.Errorf("OMENative reconciler: ensure headless service (component=%s): %w", p.Component, err)
	}
	if err := ensureStableServiceViaTopLevel(ctx, p); err != nil {
		return fmt.Errorf("OMENative reconciler: ensure stable service (component=%s): %w", p.Component, err)
	}
	return nil
}

// ensureStableServiceViaTopLevel wires the per-Component stable
// Service (`<isvc>-<comp>`) through the top-level
// service.ServiceReconciler — the same reconciler RawDeployment and
// MultiNode use. The OMENative-specific bits are passed as parameters:
//
//   - componentMeta: a copy of p.ObjectMeta with the OMENative-scoped
//     OwnerReferences stamped on it. p.ObjectMeta already carries the
//     right Name (`<isvc>-<comp>`, matching query.StableServiceName)
//     and Namespace from the dispatch site in
//     components/{engine,decoder,router}.go.
//
//   - selector: componentSelector — three keys including the
//     LabelManagedBy discriminator so the stable Service never picks
//     up legacy Raw / MultiNode pods during a mode-switch transition.
//
// PodSpec drives the port discovery (the top-level reconciler reads
// the first container's ports, falling back to port 8080 / container
// name). When the rendered template carries no PodSpec — the
// MinReplicas=0 cold-start case — we skip the call to match the
// existing EnsurePodMonitor behaviour rather than emit a portless
// Service.
func ensureStableServiceViaTopLevel(ctx context.Context, p ReconcileParams) error {
	if p.Client == nil {
		return fmt.Errorf("ensureStableService: nil client")
	}
	if p.ISVC == nil {
		return fmt.Errorf("ensureStableService: nil ISVC")
	}
	if p.PodSpec == nil {
		// No rendered pod template (MinReplicas=0). The top-level
		// reconciler's buildServicePorts would emit an empty Ports
		// slice; we skip until the Component scales up and a real
		// PodSpec is rendered.
		return nil
	}

	componentMeta := p.ObjectMeta.DeepCopy()
	componentMeta.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(p.ISVC, isvcGroupVersionKind),
	}

	// Scheme is unused by ServiceReconciler.Reconcile (only stored for
	// callers that delegate owner-ref stamping); we stamp the owner ref
	// directly via componentMeta.OwnerReferences above, so nil here is
	// safe.
	sr := service.NewServiceReconciler(p.Client, nil, *componentMeta, p.ComponentExt, p.PodSpec, componentSelector(p.ISVC, p.Component))
	if _, err := sr.Reconcile(); err != nil {
		return fmt.Errorf("ensureStableService: %w", err)
	}
	return nil
}
