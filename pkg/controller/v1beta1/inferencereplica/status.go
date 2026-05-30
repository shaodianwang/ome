package inferencereplica

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
// Per-Instance pod counters (PodCount / ReadyPodCount /
// ServingPodCount / AvailablePodCount / ScheduledPodCount /
// NodesOccupied) are recomputed here from the live pod set, mirroring
// the omenative direct path's AggregateAndWriteStatus. The workload
// per-op writers (Create's patchInstanceStatusReady, Update's
// promotion, etc.) own the lifecycle fields (Phase, Operation,
// Incarnation, RunningRevision, TargetRevision); this aggregator owns
// the observability fields (per-pod counters + Component-level
// rollups). Without populating the per-Instance pod counters, the
// downstream workload.CountReadyInstances / CountUpdatedInstances
// would read zero ReadyPodCount everywhere — the round-2 recreate-
// path bug where ReadyReplicas stuck at 0 even after the new pod
// flipped containers-ready.
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
	component := internalsource.ComponentTypeToWorkload(ir.Spec.Component)

	// Live-list pods + EndpointSlices outside the retry closure: both
	// reads are idempotent (no apiserver-side mutation), so on a status
	// conflict we just re-stamp the same counters onto the re-read IR.
	// Mirrors the omenative direct path which reads pods + availability
	// once at the top of AggregateAndWriteStatus.
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	pods, err := query.ListOMENativePodsByName(ctx, reader, ir.Namespace, ir.Spec.ParentRef.Name, component)
	if err != nil {
		return fmt.Errorf("aggregateAndWriteStatus: list pods: %w", err)
	}
	byIndex := query.BucketPodsByInstanceIdx(pods)
	serviceName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, component)
	availableByPod, err := workload.AvailablePodSet(ctx, reader, ir.Namespace, serviceName)
	if err != nil {
		return fmt.Errorf("aggregateAndWriteStatus: compute availability: %w", err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.Client.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// IR deleted under us; nothing to aggregate into.
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: re-read IR: %w", err)
		}

		// Per-Instance pod counters from the live pod buckets. Same
		// shape the omenative direct path uses (status_aggregate.go
		// lines 83-93). Without this step the downstream
		// workload.CountReadyInstances reads zero ReadyPodCount on every
		// Instance — the recreate-path bug where the new pod is alive
		// and ContainersReady but ReadyReplicas stuck at 0 because the
		// IR aggregator never observed the pod's live state.
		populateInstancePodCounters(fresh.Status.InstanceStatuses, byIndex, availableByPod)

		insts := internalsource.InstanceStatusSliceToWorkload(fresh.Status.InstanceStatuses)

		// Component-level counters from per-Instance state (now with
		// fresh per-pod counters populated above).
		var targetName string
		if target != nil {
			targetName = target.Name
		}
		counters := workload.ComputeComponentCounters(insts, desiredByIdx, targetName)
		fresh.Status.Replicas = counters.Replicas
		fresh.Status.ServingReplicas = counters.ServingReplicas
		// ReadyReplicas + AvailableReplicas use the workload counters
		// directly now that per-Instance pod counters are populated
		// from live pods above. The surge-tolerant classifier
		// (InstanceMeetsThreshold) means an Instance with the canonical
		// pod ContainersReady is counted even mid-surge or mid-drain,
		// matching the omenative direct path byte-for-byte. The prior
		// Phase-only fallback (countReadyInstancesByPhase) under-
		// counted during recreate/SurgeThenDrain transitions: Phase
		// stays Updating until the workload pipeline stamps Ready, but
		// the live pod can be serving traffic in the interim — and the
		// IR's Ready rollup must reflect serving truth, not the
		// lifecycle bookkeeping phase.
		//
		// AvailableReplicas mirrors ReadyReplicas at v1 — no
		// minReadySeconds gating field exists on InferenceReplicaSpec
		// or LifecycleSpec today. When one is added, AvailableReplicas
		// would lag ReadyReplicas by that grace period.
		fresh.Status.ReadyReplicas = counters.ReadyReplicas
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
		// Mirror the per-Instance pod counters too so downstream callers
		// reading ir.Status.InstanceStatuses observe the fresh per-pod
		// state, not the stale snapshot from when the reconcile started.
		// Per-pod counter freshness flows through the irprojector rollup
		// onto ISVC.status.components[X].omenative.instanceStatuses, so
		// `kubectl describe isvc` shows the same per-Instance numbers
		// the IR controller just observed.
		mirrorInstanceCounters(&ir.Status, fresh.Status.InstanceStatuses)
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

// mirrorInstanceCounters copies the per-Instance pod counter fields
// (PodCount / ReadyPodCount / ServingPodCount / ScheduledPodCount /
// AvailablePodCount / NodesOccupied) from the just-committed
// InstanceStatuses onto the caller's in-memory IR. Matched on Index
// so a parallel mutator that inserted or removed an Instance between
// the Get and Update doesn't end up writing counters onto the wrong
// slot. The lifecycle fields (Phase, Operation, Incarnation,
// RunningRevision, TargetRevision, ActiveOrdinal) are owned by the
// workload per-op writers, not the aggregator — leaving them
// untouched on the mirror avoids clobbering an in-flight per-op
// write.
func mirrorInstanceCounters(status *v1beta1.InferenceReplicaStatus, fresh []v1beta1.OMENativeInstanceStatus) {
	if status == nil {
		return
	}
	freshByIdx := make(map[int32]v1beta1.OMENativeInstanceStatus, len(fresh))
	for _, s := range fresh {
		freshByIdx[s.Index] = s
	}
	for i := range status.InstanceStatuses {
		s := &status.InstanceStatuses[i]
		f, ok := freshByIdx[s.Index]
		if !ok {
			continue
		}
		s.PodCount = f.PodCount
		s.ReadyPodCount = f.ReadyPodCount
		s.ServingPodCount = f.ServingPodCount
		s.ScheduledPodCount = f.ScheduledPodCount
		s.AvailablePodCount = f.AvailablePodCount
		if f.NodesOccupied != nil {
			s.NodesOccupied = append([]string(nil), f.NodesOccupied...)
		} else {
			s.NodesOccupied = nil
		}
	}
}

// populateInstancePodCounters refreshes the per-Instance observability
// fields (PodCount / ReadyPodCount / ServingPodCount /
// ScheduledPodCount / AvailablePodCount / NodesOccupied) from the
// live pod set bucketed by Instance index. The per-pod counters feed
// the workload.CountReadyInstances / CountUpdatedInstances /
// CountAvailableInstances classifiers — without populating them the
// downstream rollups read zero on every Instance even when the
// underlying pods are alive and ContainersReady.
//
// Mirrors the omenative direct path's per-Instance counter loop
// (omenative/status_aggregate.go:83-93) byte-for-byte. The IR adapter
// uses workload.Count* helpers directly via workload.CountReadyPods
// / CountServingPods / etc., which is the same primitive set the
// direct path's local wrappers delegate to — so the two adapters
// produce byte-identical per-Instance counters off the same pod set.
//
// availableByPod is the AvailablePodSet (pod names appearing as Ready
// non-terminating endpoints in the headless Service's EndpointSlices).
// Empty map is OK — every Instance's AvailablePodCount drops to 0,
// which is the correct semantic when EndpointSlices haven't yet caught
// up with a fresh pod's Ready transition.
//
// Instance entries with no matching pods get all-zero counters —
// that's the correct semantic for an Instance whose pods haven't been
// created yet (Pending phase) or have all been deleted (post-drain,
// pre-recreate Phase B).
func populateInstancePodCounters(insts []v1beta1.OMENativeInstanceStatus, byIndex map[int32][]*corev1.Pod, availableByPod map[string]struct{}) {
	for i := range insts {
		s := &insts[i]
		instPods := byIndex[s.Index]
		s.PodCount = int32(len(instPods))
		s.ReadyPodCount = workload.CountReadyPods(instPods)
		s.ServingPodCount = workload.CountServingPods(instPods)
		s.ScheduledPodCount = workload.CountScheduledPods(instPods)
		s.AvailablePodCount = workload.CountAvailablePods(instPods, availableByPod)
		s.NodesOccupied = workload.UniqueNodes(instPods)
	}
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
