package inferencereplica

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// InferenceReplicaConditionReady is the top-level condition type written
// onto IR.Status.Conditions. Operators waiting on the IR directly
// (`kubectl wait --for=condition=Ready irep/<name>`), the irprojector
// that mirrors IR conditions onto the parent ISVC, and future autoscaler
// scalers that read the IR all key off this condition.
const InferenceReplicaConditionReady = "Ready"

// Reason strings for the InferenceReplicaConditionReady condition. Kept
// short and machine-readable so
// `kubectl get irep -o jsonpath='...conditions[?(@.type=="Ready")].reason'`
// is grep-friendly. Mirrors the omenative status-conditions vocabulary
// for cross-controller consistency.
const (
	// ReasonAllInstancesReady is stamped when every Instance is Ready AND
	// CurrentRevision == UpdateRevision (rollout fully converged).
	ReasonAllInstancesReady = "AllInstancesReady"
	// ReasonRolloutInProgress is stamped when CurrentRevision !=
	// UpdateRevision — explicit "churning, check back" signal. Maps to
	// Status=Unknown so `kubectl wait --for=condition=Ready` doesn't
	// short-circuit while a rollout is mid-flight.
	ReasonRolloutInProgress = "RolloutInProgress"
	// ReasonInstanceFailed is stamped when at least one Instance has
	// terminally failed (stuck-pod escalation, InstanceReadyTimeout).
	// Maps to Status=False so operators paging on Ready=False catch the
	// failure immediately.
	ReasonInstanceFailed = "InstanceFailed"
	// ReasonReplicaCountMismatch is stamped when ReadyReplicas < Replicas
	// with no in-flight rollout — something is stuck below the desired
	// Ready count without revision churn to blame.
	ReasonReplicaCountMismatch = "ReplicaCountMismatch"
	// ReasonNoReplicas is stamped when Replicas == 0 (no desired
	// Instances). Distinct from ReplicaCountMismatch so operators can
	// tell "scaled-to-zero" from "something stuck".
	ReasonNoReplicas = "NoReplicas"
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
		fresh.Status.ServingReplicas = counters.ServingReplicas
		// Override ReadyReplicas + AvailableReplicas with a Phase-based
		// derivation. The legacy workload counters
		// (CountReadyInstances / CountAvailableInstances) read
		// InstanceStatus.ReadyPodCount / AvailablePodCount — fields the
		// IR controller does NOT populate (the ISVC-direct path's
		// AggregateAndWriteStatus writes them inline from live pods, but
		// the IR-driven path has no equivalent step yet). Counting
		// Instances whose Phase is Ready uses the per-Instance
		// single-source-of-truth field the workload pipeline writes on
		// every operation completion, so the IR-level READY column,
		// KEDA/HPA scaling, and the Ready condition all observe a
		// consistent value.
		//
		// AvailableReplicas matches ReadyReplicas at v1 — no
		// minReadySeconds gating field exists on InferenceReplicaSpec
		// or LifecycleSpec today. When one is added, AvailableReplicas
		// would lag ReadyReplicas by that grace period.
		readyReplicas := countReadyInstancesByPhase(fresh.Status.InstanceStatuses)
		fresh.Status.ReadyReplicas = readyReplicas
		fresh.Status.AvailableReplicas = readyReplicas
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

		// Top-level Ready condition. Two-axis rule:
		//   Status=True    when ReadyReplicas == Replicas AND
		//                  CurrentRevision == UpdateRevision (fully
		//                  converged, no rollout in flight).
		//   Status=False   when any Instance has Phase=Failed OR
		//                  ReadyReplicas < Replicas with no in-flight
		//                  rollout (stuck below desired).
		//   Status=Unknown when CurrentRevision != UpdateRevision —
		//                  explicit "churning, check back" so
		//                  `kubectl wait --for=condition=Ready` doesn't
		//                  short-circuit mid-rollout.
		apimeta.SetStatusCondition(&fresh.Status.Conditions, computeReadyCondition(&fresh.Status))

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
		// Conditions: deep-copy the just-committed slice so callers reading
		// IR.Status.Conditions in the same reconcile pass observe the
		// post-write set. The fresh.Status.Conditions slice is owned by
		// the apiserver after Update; copying preserves the standard
		// "no shared backing array" contract.
		if len(fresh.Status.Conditions) > 0 {
			ir.Status.Conditions = make([]metav1.Condition, len(fresh.Status.Conditions))
			copy(ir.Status.Conditions, fresh.Status.Conditions)
		} else {
			ir.Status.Conditions = nil
		}
		return nil
	})
}

// countReadyInstancesByPhase counts the InstanceStatuses whose Phase is
// OMENativeInstanceReady. Phase is the per-Instance single source of
// truth the workload pipeline writes on every operation completion;
// using it for the IR-level ReadyReplicas rollup avoids depending on
// per-pod counter fields that the IR controller doesn't populate.
func countReadyInstancesByPhase(insts []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range insts {
		if s.Phase == v1beta1.OMENativeInstanceReady {
			n++
		}
	}
	return n
}

// hasFailedInstance reports whether at least one Instance has been
// escalated to Phase=Failed (stuck-pod escalation, InstanceReadyTimeout).
// Used by computeReadyCondition to flip Ready=False when the rollout is
// permanently wedged.
func hasFailedInstance(insts []v1beta1.OMENativeInstanceStatus) bool {
	for _, s := range insts {
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			return true
		}
	}
	return false
}

// computeReadyCondition derives the Ready condition for the IR from
// already-rolled-up status counters + per-Instance phases. Caller passes
// a pre-aggregated IR.Status snapshot (Replicas, ReadyReplicas,
// CurrentRevision, UpdateRevision, InstanceStatuses must all reflect the
// post-counter state). Reason precedence:
//
//  1. InstanceFailed       — any Instance Phase=Failed (overrides all)
//  2. RolloutInProgress    — CurrentRevision != UpdateRevision (Unknown)
//  3. NoReplicas           — Replicas == 0 (False)
//  4. AllInstancesReady    — ReadyReplicas == Replicas (True)
//  5. ReplicaCountMismatch — ReadyReplicas < Replicas, no rollout (False)
//
// Returns a metav1.Condition with ObservedGeneration stamped off
// status.ObservedGeneration so consumers can correlate. The caller is
// responsible for invoking apimeta.SetStatusCondition to merge the
// returned condition into status.Conditions (which handles
// LastTransitionTime correctly).
func computeReadyCondition(status *v1beta1.InferenceReplicaStatus) metav1.Condition {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionReady,
		ObservedGeneration: status.ObservedGeneration,
	}
	switch {
	case hasFailedInstance(status.InstanceStatuses):
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonInstanceFailed
		cond.Message = "At least one Instance has Phase=Failed"
	case status.UpdateRevision != "" && status.CurrentRevision != status.UpdateRevision:
		cond.Status = metav1.ConditionUnknown
		cond.Reason = ReasonRolloutInProgress
		cond.Message = fmt.Sprintf("Rolling out %s (%d/%d Instances on target)",
			status.UpdateRevision, status.UpdatedReadyReplicas, status.Replicas)
	case status.Replicas == 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonNoReplicas
		cond.Message = "InferenceReplica has no desired Instances"
	case status.ReadyReplicas == status.Replicas:
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonAllInstancesReady
		cond.Message = fmt.Sprintf("%d/%d Instances Ready", status.ReadyReplicas, status.Replicas)
	default:
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonReplicaCountMismatch
		cond.Message = fmt.Sprintf("Only %d/%d Instances Ready (no rollout in flight)",
			status.ReadyReplicas, status.Replicas)
	}
	return cond
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
