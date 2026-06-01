package omenative

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
)

// isvcGroupVersionKind identifies the InferenceService owner reference
// stamped on every emitted Service. ops/render.go has its own copy for
// pod-owner refs; keeping a separate one here avoids a top-level → ops/
// import (which would close a cycle since the top-level reconciler
// dispatches into ops/).
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// componentSelector builds the pod-label selector that scopes the
// per-Component HEADLESS Service to the OMENative-managed pods of one
// (ISVC, component) pair. The `LabelManagedBy=OMENative` discriminator
// narrows the selector to OMENative-stamped pods so a later
// RawDeployment / MultiNode reconciler for the same (ISVC, component)
// cannot accidentally route to (or be routed by) OMENative pods during
// a mode-switch transition. Pods stamped by Raw / MultiNode never carry
// this label (see pkg/controller/v1beta1/inferenceservice/components/
// base.go ProcessBaseLabels).
//
// The headless Service selector intentionally selects ALL pods of the
// Component including workers, regardless of multi-pod-ness — peer
// discovery for distributed init (NCCL all-gather, vLLM ray-init, etc.)
// requires every pod to be addressable via the per-Component DNS name.
// The per-Component STABLE Service (built in components/{engine,
// decoder}.go's reconcileOMENativeSubresources) adds a
// `pod-ordinal=0` filter for multi-pod Components so customer traffic
// lands only on the leader pod.
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
		Name:      query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(component)),
		Namespace: isvc.Namespace,
		Selector:  selector,
		Labels:    selector,
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
		},
	}
}

// ensureHeadlessService is the entry point ReconcileComponent calls.
// Reconciles the per-Component headless Service (delegated to
// workload.ReconcileHeadlessService) — the per-Component DNS Service
// for peer discovery, scoped to OMENative-managed pods via the
// componentSelector triple.
//
// The per-Component stable Service (`<isvc>-<comp>`) and PodMonitor
// used to be reconciled from here too, but were moved out to the
// per-Component Reconcile() in components/{engine,decoder,router}.go's
// reconcileOMENativeSubresources method. That placement covers BOTH
// OMENative sub-paths (direct and IR-managed); the IR-managed path
// (default since PR #320) bypasses ReconcileComponent entirely.
func ensureHeadlessService(ctx context.Context, p ReconcileParams) error {
	if p.ISVC == nil {
		return fmt.Errorf("OMENative reconciler: ensure headless service (component=%s): nil ISVC", p.Component)
	}
	spec := BuildHeadlessServiceSpec(p.ISVC, p.Component)
	if err := workload.ReconcileHeadlessService(ctx, p.Client, spec); err != nil {
		return fmt.Errorf("OMENative reconciler: ensure headless service (component=%s): %w", p.Component, err)
	}
	return nil
}
