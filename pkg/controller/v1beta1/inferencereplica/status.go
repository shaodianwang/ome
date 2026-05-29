package inferencereplica

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// aggregateAndWriteStatus computes the per-IR aggregate counters
// (Replicas / ReadyReplicas / ServingReplicas / AvailableReplicas /
// UpdatedReplicas / UpdatedReadyReplicas), stamps ObservedGeneration +
// LabelSelector, stamps UpdateRevision / CurrentRevision off the
// target ControllerRevision, and (on full convergence) promotes
// UpdateRevision → CurrentRevision.
//
// Per-Instance counters are NOT recomputed here — the workload package
// owns those via the per-op writers (Create's patchInstanceStatusReady,
// Update's promotion, etc.). aggregateAndWriteStatus reads the
// InstanceStatuses the dispatcher just committed and rolls them up
// into Component-level counters via the workload-side helpers.
//
// Reuses workload.ComputeComponentCounters / workload.RolloutComplete
// directly — same shape, same semantics as the ISVC adapter, so the
// dual-write window can byte-compare the two status surfaces.
//
// target may be nil when DesiredSpec.PodSpec is nil (no rendered
// template — webhook-rejected today; defensive nil-guard kept). When
// target is nil revision pointers are left untouched; counters still
// run.
func (r *Reconciler) aggregateAndWriteStatus(ctx context.Context, ir *v1beta1.InferenceReplica, plan workload.ComponentPlan, target *appsv1.ControllerRevision) error {
	if r.Client == nil {
		return fmt.Errorf("aggregateAndWriteStatus: nil client")
	}
	key := client.ObjectKeyFromObject(ir)
	desiredByIdx := workload.DesiredPodCountByInstance(plan)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.Client.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// IR deleted under us; nothing to aggregate into.
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: re-read IR: %w", err)
		}

		insts := internalsource.InstanceStatusSliceToWorkload(fresh.Status.InstanceStatuses)

		// Component-level counters from per-Instance state.
		var targetName string
		if target != nil {
			targetName = target.Name
		}
		counters := workload.ComputeComponentCounters(insts, desiredByIdx, targetName)
		fresh.Status.Replicas = counters.Replicas
		fresh.Status.ReadyReplicas = counters.ReadyReplicas
		fresh.Status.ServingReplicas = counters.ServingReplicas
		fresh.Status.AvailableReplicas = counters.AvailableReplicas
		if target != nil {
			fresh.Status.UpdatedReplicas = counters.UpdatedReplicas
			fresh.Status.UpdatedReadyReplicas = counters.UpdatedReadyReplicas
			fresh.Status.UpdateRevision = target.Name
			if workload.RolloutComplete(insts, target.Name) {
				fresh.Status.CurrentRevision = target.Name
			}
		}

		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.LabelSelector = irLabelSelectorString(fresh.Spec.ParentRef.Name, fresh.Spec.Component)

		if err := r.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: update IR status: %w", err)
		}

		// Mirror the just-committed Component-level fields onto the
		// caller's in-memory IR snapshot so downstream callers in the
		// same reconcile pass observe the post-write state. Matches the
		// mirror pattern in mirrorInstanceStatus / mirrorComponentRevisionFields
		// on the ISVC side.
		ir.Status.Replicas = fresh.Status.Replicas
		ir.Status.ReadyReplicas = fresh.Status.ReadyReplicas
		ir.Status.ServingReplicas = fresh.Status.ServingReplicas
		ir.Status.AvailableReplicas = fresh.Status.AvailableReplicas
		ir.Status.UpdatedReplicas = fresh.Status.UpdatedReplicas
		ir.Status.UpdatedReadyReplicas = fresh.Status.UpdatedReadyReplicas
		ir.Status.UpdateRevision = fresh.Status.UpdateRevision
		ir.Status.CurrentRevision = fresh.Status.CurrentRevision
		ir.Status.ObservedGeneration = fresh.Status.ObservedGeneration
		ir.Status.LabelSelector = fresh.Status.LabelSelector
		return nil
	})
}

// irLabelSelectorString returns the canonical "k=v,k=v" selector
// string the HPA scale subresource consumes via
// IR.status.labelSelector. Formula matches the legacy ISVC-side
// componentLabelSelectorString byte-for-byte so existing HPAs
// continue to resolve unchanged after a workload migrates from
// ISVC-direct to IR-driven.
func irLabelSelectorString(isvc string, component v1beta1.ComponentType) string {
	return labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}).String()
}
