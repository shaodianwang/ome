package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	isvcstatus "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/obsmetrics"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// InferenceReplicaConditionReady is the top-level condition type written
// onto IR.Status.Conditions. Operators waiting on the IR directly
// (`kubectl wait --for=condition=Ready irep/<name>`), the irprojector
// that mirrors IR conditions onto the parent ISVC, and future autoscaler
// scalers that read the IR all key off this condition.
const InferenceReplicaConditionReady = "Ready"

// InferenceReplicaConditionRolloutStalled is an ADVISORY condition that
// surfaces a rollout wedged on failing Instances (most commonly a new-revision
// pod stuck in CrashLoopBackOff). It is deliberately NOT a dependent of the
// Ready condition: old-revision pods keep serving during a failed surge
// update, so a healthy IR/ISVC stays Ready=True while this flags the otherwise-
// invisible wedge (rollout stuck at N/total) for operators + dashboards.
const InferenceReplicaConditionRolloutStalled = "RolloutStalled"

const (
	// ReasonInstancesFailing — RolloutStalled=True: >=1 Instance recorded a
	// terminal failure while not yet on the target revision.
	ReasonInstancesFailing = "InstancesFailing"
	// ReasonRolloutProgressing — RolloutStalled=False: rollout in flight with
	// no failing Instances, or no rollout in flight.
	ReasonRolloutProgressing = "Progressing"
)

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
	// ReasonStaged is stamped when the IR has converged to a static
	// rollingUpdate.partition — (replicas-partition) Instances on the target
	// revision and `partition` held on the prior one, all Ready. Ready=True:
	// a partitioned rollout is intentionally complete-and-holding, not
	// "in progress". Distinct from AllInstancesReady (fully rolled).
	ReasonStaged = "Staged"
	// ReasonInstanceFailed is stamped when at least one Instance has
	// terminally failed (stuck-pod escalation, InstanceReadyTimeout).
	// Maps to Status=False so operators paging on Ready=False catch the
	// failure immediately.
	ReasonInstanceFailed = "InstanceFailed"
	// ReasonReplicaCountMismatch is stamped when the serving Instance count is
	// below the lifecycle rolling-update availability floor with no in-flight
	// rollout. Maps to Status=False.
	ReasonReplicaCountMismatch = "ReplicaCountMismatch"
	// ReasonMinimumAvailable is stamped when ReadyReplicas < Replicas but the
	// serving Instance count is still at or above the availability floor —
	// Ready=True, distinct from AllInstancesReady so full readiness remains
	// distinguishable from partial availability.
	ReasonMinimumAvailable = "MinimumAvailable"
	// ReasonNoReplicas is stamped when Replicas == 0 (no desired
	// Instances). Distinct from ReplicaCountMismatch so operators can
	// tell "scaled-to-zero" from "something stuck".
	ReasonNoReplicas = "NoReplicas"
)

// aggregateAndWriteStatus computes the per-IR aggregate counters
// (Replicas / ReadyReplicas / ServingReplicas / AvailableReplicas /
// UpdatedReplicas / UpdatedReadyReplicas), stamps ObservedGeneration +
// LabelSelector, and stamps UpdateRevision off the target
// ControllerRevision. It is observability-only: it reads CurrentRevision
// (for the Ready condition) but never writes it — the reconciler's
// deferred promotion step (buildPromoteCurrentRevision, invoked before
// this aggregator) owns that write.
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
// would read zero ReadyPodCount everywhere, leaving ReadyReplicas
// stuck at 0 even after a recreated pod flips containers-ready.
//
// Reuses workload.ComputeComponentCounters directly — same shape, same
// semantics as the ISVC adapter, so the dual-write window can
// byte-compare the two status surfaces.
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
	ownerUID := ir.UID
	ownerGeneration := ir.Generation
	desiredByIdx := workload.DesiredPodCountByInstance(plan)
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)

	// List pods + EndpointSlices via the CACHED client (r.Client), outside
	// the retry closure: both reads are idempotent (no apiserver-side
	// mutation), so on a status conflict we just re-stamp the same counters
	// onto the re-read IR. Mirrors the omenative direct path which reads pods
	// + availability once at the top of AggregateAndWriteStatus.
	//
	// This aggregator is observability-only COUNTING — the manager cache is
	// scoped to OME pods, has the OMENative Pod field index registered, and
	// watches EndpointSlices, so cache-served counts are both correct and
	// cheap. Going live (r.APIReader) on every reconcile would be the dominant
	// hot-path cost: an uncached LIST per IR reconcile purely for counters,
	// plus a MatchingFields probe the live reader can't serve. r.APIReader
	// stays reserved for the destructive/correctness-critical paths
	// (drain/delete/recreate confirm) that genuinely need a live read.
	pods, err := query.ListOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.Spec.ParentRef.Name, component, true)
	if err != nil {
		return fmt.Errorf("aggregateAndWriteStatus: list pods: %w", err)
	}
	byIndex := query.BucketPodsByInstanceIdx(pods)
	serviceName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, component)
	availableByPod, err := workload.AvailablePodSet(ctx, r.Client, ir.Namespace, serviceName)
	if err != nil {
		return fmt.Errorf("aggregateAndWriteStatus: compute availability: %w", err)
	}

	ownerUnavailable := false
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.APIReader.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// IR deleted under us; nothing to aggregate into.
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultNotFound)
				ownerUnavailable = true
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: re-read IR: %w", err)
		}
		if ownerUID == "" || fresh.UID != ownerUID {
			ownerUnavailable = true
			return nil
		}
		if fresh.Generation != ownerGeneration {
			return workload.ErrStatusMutationPrecondition
		}

		// Snapshot the live status before recomputation so a no-op
		// reconcile (computed == live) skips the Status().Update below.
		// Every status write re-triggers this controller AND the
		// irprojector rollup onto the parent ISVC, so at steady state the
		// unconditional write amplified idle reconciles into a write
		// storm. apimeta.SetStatusCondition preserves LastTransitionTime
		// when the condition value is unchanged, so an unchanged status
		// DeepEquals its prior snapshot.
		before := fresh.Status.DeepCopy()

		// Per-Instance pod counters from the live pod buckets. Same
		// shape the omenative direct path uses (status_aggregate.go
		// lines 83-93). Without this step the downstream
		// workload.CountReadyInstances reads zero ReadyPodCount on every
		// Instance — leaving ReadyReplicas at 0 even when a recreated
		// pod is alive and ContainersReady, because the aggregator
		// never observed the pod's live state.
		populateInstancePodCounters(fresh.Status.InstanceStatuses, byIndex, availableByPod)

		insts := v1beta1convert.InstanceStatusSliceToWorkload(fresh.Status.InstanceStatuses)

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
		}

		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.LabelSelector = irLabelSelectorString(fresh.Spec.ParentRef.Name, fresh.Spec.Component)

		// Top-level Ready condition. Two-axis rule:
		//   Status=True    when ReadyReplicas == Replicas AND
		//                  CurrentRevision == UpdateRevision (fully
		//                  converged, no rollout in flight).
		//   Status=False   when any Instance has Phase=Failed OR the
		//                  serving Instance count is below the availability
		//                  floor (lifecycle rollingUpdate.maxUnavailable)
		//                  with no in-flight rollout.
		//   Status=Unknown when CurrentRevision != UpdateRevision —
		//                  explicit "churning, check back" so
		//                  `kubectl wait --for=condition=Ready` doesn't
		//                  short-circuit mid-rollout.
		apimeta.SetStatusCondition(&fresh.Status.Conditions, computeReadyCondition(&fresh.Status, fresh.Spec.Replicas, fresh.Spec.Lifecycle, fresh.Spec.Pacing))
		// Advisory RolloutStalled condition — surfaces a rollout wedged on
		// failing Instances (e.g. new-rev CrashLoopBackOff) without affecting
		// the Ready condition above (old-rev pods keep serving).
		apimeta.SetStatusCondition(&fresh.Status.Conditions, computeRolloutStalledCondition(&fresh.Status))

		// Skip the write when the recomputed status matches what's
		// already live: a no-op reconcile must perform ZERO writes so it
		// triggers nothing downstream. The in-memory mirror below still
		// runs so callers in this pass observe the fresh counters.
		if equality.Semantic.DeepEqual(*before, fresh.Status) {
			mirrorBack(ir, fresh)
			return nil
		}

		if err := r.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultNotFound)
				ownerUnavailable = true
				return nil
			}
			return fmt.Errorf("aggregateAndWriteStatus: update IR status: %w", err)
		}

		// Mirror the just-committed Component-level fields, per-Instance
		// counters, and Conditions onto the caller's in-memory IR snapshot
		// so downstream callers in the same reconcile pass observe the
		// post-write state.
		mirrorBack(ir, fresh)
		obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultSuccess)
		return nil
	}); err != nil {
		// A generation change replans without a write outcome. Other terminal
		// retry failures retain their status-write classification.
		if !errors.Is(err, workload.ErrStatusMutationPrecondition) {
			if apierrors.IsConflict(err) {
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultConflict)
			} else {
				obsmetrics.RecordStatusUpdate(obsmetrics.ControllerIR, obsmetrics.ResultError)
			}
		}
		return err
	}
	if ownerUnavailable {
		return nil
	}

	// Park the InstanceReadyTimeout clock for any Instance whose pods are
	// held by a scheduling gate (queued for admission, e.g. by Kueue) or
	// by an operator pause: a gated/paused workload is queued, not stuck,
	// so the wait must not count against the deadline. The escalation
	// pass inside workload.Reconcile honors the parked (zeroed) deadline
	// and additionally skips gated Instances outright; once the pods
	// clear the gate the clock restarts from admission.
	return r.reconcileHeldDeadlines(ctx, ir, byIndex, plan.Paused, plan.InstanceReadyTimeout)
}

// mirrorBack copies the just-computed Component-level fields and
// per-Instance pod counters from `fresh` onto the caller's in-memory IR
// snapshot so downstream callers in the same reconcile pass observe the
// post-compute state — regardless of whether a Status().Update was
// actually issued. Matches the mirror pattern in mirrorInstanceStatus /
// mirrorComponentRevisionFields on the ISVC side. The lifecycle fields
// (Phase, Operation, Incarnation, RunningRevision, TargetRevision) stay
// owned by the workload per-op writers and are left untouched.
func mirrorBack(ir, fresh *v1beta1.InferenceReplica) {
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
	// Conditions: deep-copy the just-computed slice so callers reading
	// IR.Status.Conditions in the same reconcile pass observe the
	// post-compute set without sharing a backing array.
	if len(fresh.Status.Conditions) > 0 {
		ir.Status.Conditions = make([]metav1.Condition, len(fresh.Status.Conditions))
		copy(ir.Status.Conditions, fresh.Status.Conditions)
	} else {
		ir.Status.Conditions = nil
	}
}

// reconcileHeldDeadlines parks/restarts InstanceReadyTimeout around either
// admission gating or an operator pause. Pausing holds every in-flight
// Instance; unpausing rearms each parked deadline from that point unless its
// pods remain admission-gated.
func (r *Reconciler) reconcileHeldDeadlines(ctx context.Context, ir *v1beta1.InferenceReplica, byIndex map[int32][]*corev1.Pod, paused bool, timeout time.Duration) error {
	if ir == nil {
		return nil
	}
	held := make(map[int32]bool, len(byIndex))
	for idx, pods := range byIndex {
		for _, p := range pods {
			if workload.PodAdmissionGated(p) {
				held[idx] = true
				break
			}
		}
	}
	// A gang-surge source's attempt pods live in its Operation.SurgeIndex
	// bucket: while they queue for admission, the source's deadline must
	// park too, or it expires during a legitimate wait.
	for i := range ir.Status.InstanceStatuses {
		s := &ir.Status.InstanceStatuses[i]
		op := s.Operation
		if op == nil || op.Type != v1beta1.InstanceOperationUpdate || op.SurgeIndex == nil {
			continue
		}
		if held[*op.SurgeIndex] {
			held[s.Index] = true
		}
	}
	if paused {
		for i := range ir.Status.InstanceStatuses {
			held[ir.Status.InstanceStatuses[i].Index] = true
		}
	}
	return workload.ReconcileGatedDeadlines(ctx, r.buildDeadlineParkInput(ir),
		v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses), held, timeout)
}

// buildDeadlineParkInput builds the minimal workload.ReconcileInput the
// InstanceReadyTimeout parking step (workload.ReconcileGatedDeadlines)
// consumes: the deadline write goes through MutateInstance (same retry +
// in-memory-mirror semantics as dispatch-time writes) and the clock seam
// supplies now. The remaining callbacks satisfy the ReconcileInput
// must-be-set contract as explicit no-ops — parking never removes
// instances, writes conditions, or emits events.
func (r *Reconciler) buildDeadlineParkInput(ir *v1beta1.InferenceReplica) workload.ReconcileInput {
	return workload.ReconcileInput{
		OwnerObject:             ir,
		OwnerGVK:                irGVK,
		Key:                     buildKey(ir),
		ObservedState:           observedFromIR(ir),
		Clock:                   r.Clock,
		MutateInstance:          buildMutateInstance(r.Client, r.APIReader, ir),
		RemoveInstance:          func(_ context.Context, _ int32) (bool, error) { return false, nil },
		WriteAggregateCondition: func(_ context.Context, _ metav1.Condition) error { return nil },
		WarnInstanceFailed:      func(_ int32, _, _ string) {},
	}
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
		s.Admitted = len(instPods) > 0 && !anyAdmissionGated(instPods)
	}
}

// anyAdmissionGated reports whether any pod still carries an admission
// scheduling gate (Kueue has not admitted the workload yet).
func anyAdmissionGated(pods []*corev1.Pod) bool {
	for _, p := range pods {
		if workload.PodAdmissionGated(p) {
			return true
		}
	}
	return false
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
//  1. InstanceFailed                    — any Instance Phase=Failed (False)
//  2. Staged                            — converged static partition (True)
//  3. RolloutInProgress                 — revision rollout in flight (Unknown)
//  4. NoReplicas                        — Replicas == 0 (False)
//  5. AllInstancesReady                 — ReadyReplicas == Replicas (True)
//  6. MinimumAvailable/ReplicaMismatch  — lifecycle availability floor
//
// Returns a metav1.Condition with ObservedGeneration stamped off
// status.ObservedGeneration so consumers can correlate. The caller is
// responsible for invoking apimeta.SetStatusCondition to merge the
// returned condition into status.Conditions (which handles
// LastTransitionTime correctly).
// effectivePartition returns the IR's static rollingUpdate.partition, or 0
// when unset.
func effectivePartition(pacing *v1beta1.InferenceReplicaPacing) int32 {
	if pacing == nil || pacing.Partition == nil {
		return 0
	}
	return *pacing.Partition
}

// stagedAtPartition reports whether the IR has converged to a static,
// non-zero partition: (replicas-partition) Instances Ready on the target
// revision and `partition` Instances held Ready on the prior one. Zero
// partition (full rollout) is not staged — it converges via the normal
// promotion path.
func stagedAtPartition(status *v1beta1.InferenceReplicaStatus, pacing *v1beta1.InferenceReplicaPacing) bool {
	part := effectivePartition(pacing)
	if part <= 0 {
		return false
	}
	return workload.ReachedDesiredShape(
		v1beta1convert.InstanceStatusSliceToWorkload(status.InstanceStatuses),
		status.UpdateRevision, part, status.Replicas)
}

// computeRolloutStalledCondition derives the advisory RolloutStalled
// condition (see InferenceReplicaConditionRolloutStalled). True when a rollout
// is in flight (UpdateRevision set and != CurrentRevision) AND one or more
// Instances carry a terminal LastFailure while not yet on the target revision
// — i.e. the rollout is wedged on failing Instances. Keyed on LastFailure (not
// live Phase) so it stays stable even when a stuck Instance oscillates
// Updating<->Failed each reconcile. Advisory only: not folded into Ready.
func computeRolloutStalledCondition(status *v1beta1.InferenceReplicaStatus) metav1.Condition {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionRolloutStalled,
		ObservedGeneration: status.ObservedGeneration,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonRolloutProgressing,
	}
	if status.UpdateRevision == "" || status.CurrentRevision == status.UpdateRevision {
		cond.Message = "no rollout in flight"
		return cond
	}
	stalled := 0
	reasons := map[string]int{}
	for i := range status.InstanceStatuses {
		s := status.InstanceStatuses[i]
		// Already on the target revision → a stale prior failure isn't blocking.
		if s.LastFailure == nil || s.RunningRevision == status.UpdateRevision {
			continue
		}
		stalled++
		if r := s.LastFailure.Reason; r != "" {
			reasons[r]++
		}
	}
	if stalled == 0 {
		cond.Message = fmt.Sprintf("Rolling out %s (%d/%d Instances on target)",
			status.UpdateRevision, status.UpdatedReadyReplicas, status.Replicas)
		return cond
	}
	cond.Status = metav1.ConditionTrue
	cond.Reason = ReasonInstancesFailing
	cond.Message = fmt.Sprintf("%d/%d Instance(s) failing rollout to %s (%s)",
		stalled, status.Replicas, status.UpdateRevision, summarizeFailureReasons(reasons))
	return cond
}

// summarizeFailureReasons renders a stable, compact "Reason xN" summary
// (sorted by reason for byte-stability across reconciles).
func summarizeFailureReasons(reasons map[string]int) string {
	if len(reasons) == 0 {
		return "terminal failure"
	}
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s x%d", k, reasons[k]))
	}
	return strings.Join(parts, ", ")
}

func computeReadyCondition(status *v1beta1.InferenceReplicaStatus, desiredReplicas *int32, lifecycle *v1beta1.LifecycleSpec, pacing *v1beta1.InferenceReplicaPacing) metav1.Condition {
	cond := metav1.Condition{
		Type:               InferenceReplicaConditionReady,
		ObservedGeneration: status.ObservedGeneration,
	}
	switch {
	case hasFailedInstance(status.InstanceStatuses):
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonInstanceFailed
		cond.Message = "At least one Instance has Phase=Failed"
	case stagedAtPartition(status, pacing):
		// Converged to a static partition: intentionally holding old-revision
		// Instances, all Ready. Ready=True (not RolloutInProgress/Unknown) —
		// the rollout is complete for the configured partition.
		part := effectivePartition(pacing)
		cond.Status = metav1.ConditionTrue
		cond.Reason = ReasonStaged
		cond.Message = fmt.Sprintf("Staged at partition %d: %d/%d Instances on %s, %d held on the prior revision",
			part, status.Replicas-part, status.Replicas, status.UpdateRevision, part)
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
		// Ordinary readiness uses the Instance lifecycle budget. Pacing controls
		// staged rollout coordination and does not relax this serving floor.
		floor := isvcstatus.InstanceAvailabilityFloor(status.Replicas, desiredReplicas, lifecycle)
		if status.ServingReplicas >= floor {
			cond.Status = metav1.ConditionTrue
			cond.Reason = ReasonMinimumAvailable
			cond.Message = fmt.Sprintf("%d/%d Instances serving (min %d)",
				status.ServingReplicas, status.Replicas, floor)
		} else {
			cond.Status = metav1.ConditionFalse
			cond.Reason = ReasonReplicaCountMismatch
			cond.Message = fmt.Sprintf("%d/%d Instances serving, need %d (no rollout in flight)",
				status.ServingReplicas, status.Replicas, floor)
		}
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
