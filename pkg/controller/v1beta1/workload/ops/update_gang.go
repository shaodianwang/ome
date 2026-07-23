package ops

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// gangSurgeDrainKey is the serving-gate writer key for draining a gang
// surge's SOURCE Instance. Stable per source index so the drain flip is
// idempotent across the multi-pass drain window.
func gangSurgeDrainKey(sourceIdx int32) string {
	return "gang-surge-source-" + strconv.Itoa(int(sourceIdx))
}

// gangSurgeUpdate performs a SurgeThenDrain rollout for a multi-pod
// (gang) Instance by surging a whole replacement gang at a freshly
// allocated instance index, then draining the source gang once the
// replacement is fully Ready. The source keeps serving throughout, so
// capacity never drops below N-1 and the cross-Component coordination
// gate (which counts the source's Op.Step=Surge against MaxSurge) paces
// it like any single-pod surge.
//
// Phases (one advanced per reconcile; the loop requeues between them):
//  1. Stamp the source Op{Update,Surge,SurgeIndex=k} + a GangSurgeTarget
//     marker on k. Both land k in instancePlanIndices' pinned set, so
//     EnsurePodGroups creates k's PodGroup before any of its pods and
//     the scale-down pass won't delete k mid-surge.
//  2. Ensure k's PodGroup, then create k's gang (leader + workers) on the
//     surge's PINNED revision (Op.TargetRevision), not the live `target` —
//     see surgeTargetName. The ensure here guards the window where k's
//     GangSurgeTarget status hasn't yet round-tripped into the plan the
//     top-level EnsurePodGroups keys off (gang scheduler would otherwise
//     reject the pods with "PodGroup not found").
//  3. Wait until every pod of k's gang is Ready, then mark them serving.
//  4. Drain (delete) the source gang's pods.
//  5. Promote k to Ready (RunningRevision = the pinned rev) and remove the
//     source InstanceStatus; the plan settles on k and EnsurePodGroups GCs
//     the source's PodGroup. A spec bump that arrived mid-surge is picked
//     up by the NEXT reconcile (RunningRevision != target → fresh cycle).
//
// Why a new index rather than the single-pod ActiveOrdinal toggle: for a
// gang the pod-ordinal already enumerates gang members (leader=0,
// worker=0..W-1), so there is no free ordinal slot to host a parallel
// generation. A fresh index gives the replacement gang a distinct
// identity and (via per-index PodGroup naming) its own PodGroup for free.
func gangSurgeUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision) (bool, error) {
	sourceIdx := inst.Index
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component

	// 1. Recover the surge index from the in-flight Op, or allocate one
	// and stamp the source + target markers.
	src := findInstanceStatus(input.ObservedState.InstanceStatuses, sourceIdx)
	surging := src != nil && src.Operation != nil &&
		src.Operation.Type == workload.InstanceOperationUpdate &&
		src.Operation.Step == updateStepSurge && src.Operation.SurgeIndex != nil

	// Failed-surge recovery: a prior gang surge escalated the SOURCE to
	// Phase=Failed because its replacement gang wedged on a bad revision
	// (e.g. ImagePullBackOff). Abandon the stale surge and reset the source
	// to Ready so the NEXT reconcile starts a fresh surge cycle toward the
	// current (corrective) target. The source gang keeps serving on its
	// running revision throughout, so capacity never drops. Without this the
	// gang surge stays pinned to the bad TargetRevision below and the
	// rollout is wedged forever — the multi-pod analogue of surgeUpdate's
	// single-pod Failed-recovery path.
	if surging && src.Phase == workload.InstancePhaseFailed {
		// Terminal same-target failure: the abandon's reset pass records the
		// RetryBlock for the surge's committed target (Op.TargetRevision, the
		// rev pinned when the surge was stamped) with the escalators' failure
		// evidence — see the writer-ordering note on abandonFailedGangSurge.
		return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, *src.Operation.SurgeIndex, src.RunningRevision,
			src.Operation.TargetRevision, instanceFailureReason(src, "gang surge abandoned before the target became Ready"))
	}

	// Superseded-surge redirect: the in-flight gang surge is committed to a rev
	// that is no longer desired, and its replacement gang is NOT about to promote
	// (not fully Ready). Abandon it and re-surge toward the current target — the
	// level-triggered contract, the multi-pod analogue of surgeUpdate's superseded
	// redirect. Keyed on target-supersede rather than Phase=Failed so a gang surge
	// that never becomes Ready and never escalates can't pin itself to a dead rev
	// and hold the maxSurge budget until instanceReadyTimeout. A gang about to
	// promote (all pods Ready) keeps its pin so the promote stamps the rev the pods
	// actually run (X-2 anti-corruption).
	if surging && src.Operation.TargetRevision != target.Name {
		surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, *src.Operation.SurgeIndex)
		if err != nil {
			return false, fmt.Errorf("list surge gang pods for supersede check (instance=%d): %w", *src.Operation.SurgeIndex, err)
		}
		if int32(len(surgePods)) < inst.TotalPods() || !query.AllPodsRuntimeReady(surgePods) {
			// Superseded, not failed — the target moved on. No RetryBlock:
			// abandonment by spec change is not a terminal failure of the
			// old target (empty failedTargetRev skips the record).
			return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, *src.Operation.SurgeIndex, src.RunningRevision, "", "")
		}
	}

	// surgeTargetName pins the revision THIS gang surge is committed to
	// driving to — the gang-path equivalent of single-pod surgeUpdate's
	// pin. On the FIRST pass (no in-flight Op) it equals target.Name, the
	// rev the dispatcher just decided to roll to. On SUBSEQUENT passes it
	// is the rev recorded when the surge was stamped, even if `target` has
	// since moved because of a fresh spec bump mid-surge. Pinning prevents
	// the X-2 bump-during-bump corruption: without it the replacement
	// gang's pods would be created on, and the promote would stamp
	// RunningRevision to, the LATEST target while the gang's pods still
	// run the in-flight rev — DetectUpdateTrigger's RunningRevision==target
	// fast-path then short-circuits and the rollout never converges (pods
	// stranded on the intermediate rev). The next surge cycle for the
	// newer target fires naturally after this one promotes, because
	// DetectUpdateTrigger observes RunningRevision != target.Name.
	surgeTargetName := target.Name
	if surging && src.Operation.TargetRevision != "" {
		surgeTargetName = src.Operation.TargetRevision
	}

	var surgeIdx int32
	if surging {
		surgeIdx = *src.Operation.SurgeIndex
	} else {
		surgeIdx = workload.AllocateSurgeIndex(input.ObservedState.InstanceStatuses)
		if err := patchInstanceStatusGangSurging(ctx, input, sourceIdx, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("stamp gang surge source (instance=%d): %w", sourceIdx, err)
		}
		if err := patchInstanceStatusGangSurgeTarget(ctx, input, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("stamp gang surge target (instance=%d): %w", surgeIdx, err)
		}
		// Reserve the chosen index in the IN-MEMORY snapshot so sibling
		// Instances surging in the SAME reconcile pass allocate a DISTINCT
		// index. The patches above only hit the API; input.ObservedState is the
		// snapshot taken at the start of the pass, so without this the next
		// sibling's AllocateSurgeIndex re-picks the same lowest-free index — the
		// shared-surgeIndex collision that drains the whole fleet when the single
		// shared surge becomes Ready. AllocateSurgeIndex now excludes
		// Operation.SurgeIndex, so recording the claim on the source suffices.
		if src != nil {
			si := surgeIdx
			src.Operation = &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           updateStepSurge,
				SurgeIndex:     &si,
				TargetRevision: surgeTargetName,
			}
		}
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
			"OMENative %s gang surge to revision %s (surge-index=%d)",
			instanceKey(comp, sourceIdx), surgeTargetName, surgeIdx)
		// Requeue so the next pass sees k in the plan — EnsurePodGroups
		// creates k's PodGroup before we create its pods.
		return false, nil
	}

	// 2. Create the replacement gang at k on the target revision (leader
	// + workers; createMissingPods picks the per-runner spec).
	surgeInst := workload.InstancePlan{
		Index:         surgeIdx,
		Incarnation:   1,
		Runners:       append([]workload.RunnerPlan(nil), inst.Runners...),
		ExcludedNodes: inst.ExcludedNodes, // Exclusion memory follows the instance through surge replacement.
	}
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list surge gang pods (instance=%d): %w", surgeIdx, err)
	}
	existingByName := query.IndexPodsByName(surgePods)
	missing := make([]podTarget, 0)
	for _, t := range expectedPodNamesForInstance(input, plan, surgeInst) {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(ns, owner, comp, surgeIdx) {
			return false, nil
		}
		// Announce the surge gang's PodGroup before its pods. The top-level
		// EnsurePodGroups pass keys off the plan, which only pins the surge
		// index once its GangSurgeTarget status round-trips into ObservedState
		// — a lag a gang-aware scheduler (mkube/coscheduler) turns into
		// "PodGroup not found" on the freshly-created surge pods. Ensuring it
		// here makes PodGroup-before-pods hold regardless of that timing. The
		// callback is wired by the caller (gang package owns the podgroup dep,
		// keeping ops free of it); nil callback / single-pod surge / absent
		// CRD all no-op inside EnsureGangPodGroup.
		renderPlan := plan
		if deps.EnsureGangPodGroup != nil {
			effectiveTopology, pgErr := deps.EnsureGangPodGroup(ctx, input, plan, surgeInst)
			if pgErr != nil {
				return false, fmt.Errorf("ensure surge gang PodGroup (instance=%d): %w", surgeIdx, pgErr)
			}
			renderPlan.InstanceTopologyKeys = cloneInstanceTopologyKeys(plan.InstanceTopologyKeys)
			renderPlan.InstanceTopologyKeys[surgeIdx] = effectiveTopology
		}
		// Stamp the pinned in-flight rev hash (NOT the latest target) so
		// the gang's pods match the revision this surge committed to and
		// the per-revision drain Service selects them. See surgeTargetName.
		if _, cerr := createMissingPods(ctx, deps, input, renderPlan, surgeInst, surgeIdx, missing, query.RevisionHashFromControllerRevisionName(surgeTargetName)); cerr != nil {
			return false, fmt.Errorf("create surge gang (instance=%d): %w", surgeIdx, cerr)
		}
		return false, nil
	}

	// 3. Wait for the whole replacement gang to pass its readiness probes
	// (ContainersReady), then put it into serving rotation. MarkPodServing is
	// gated on ContainersReady, NOT PodReady, on purpose: ome.io/serving is
	// itself a readiness gate, so gating the serving flip on PodReady would
	// deadlock (PodReady needs serving=True needs PodReady).
	if int32(len(surgePods)) < inst.TotalPods() || !query.AllPodsRuntimeReady(surgePods) {
		return false, nil
	}
	for _, pod := range surgePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("serving=True on surge pod %s: %w", pod.Name, err)
		}
	}

	// 4. Drain the source gang once the replacement is in rotation.
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("list source gang pods (instance=%d): %w", sourceIdx, err)
	}
	if len(sourcePods) > 0 {
		// Hold the drain until the replacement gang is actually PodReady —
		// containers ready AND the ome.io/serving gate AND'd in by kubelet —
		// not merely ContainersReady. Serving is itself a readiness gate, so a
		// just-served pod is not yet PodReady (not in Service rotation) until
		// kubelet re-evaluates the gate a beat after MarkPodServing. Draining
		// the source before then drops it out of rotation while the replacement
		// is still not in it — an N-1 capacity TROUGH. Across Components rolled
		// on independent timelines those offset troughs skew the live
		// RatioBalanced ratio (the engine 2 : decoder 1 regression). Waiting
		// keeps true capacity overlap: N -> N+1 -> N, never below N. (Gated
		// only when there is a source to drain; a source already gone — the
		// promote step below — has no trough to guard against.)
		for _, pod := range surgePods {
			if !podreadiness.IsPodReady(pod) {
				return false, nil
			}
		}
		// Flip the source gang OUT of serving rotation before deleting it. A
		// pod merely Deleted keeps serving=True (ContainersReady) through its
		// whole termination grace, so an undrained source would linger in
		// ServingReplicas beside the replacement (a durable N+1). Flipping it
		// now collapses serving back to N — and because the replacement is
		// already PodReady, serving never dips below N in the gap.
		for _, pod := range sourcePods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, pod,
				podreadiness.WriterUpdateSurgeDrain, gangSurgeDrainKey(sourceIdx)); err != nil {
				return false, fmt.Errorf("drain source gang pod %s: %w", pod.Name, err)
			}
		}
		if !deps.ExpectationsCache().Satisfied(ns, owner, comp, sourceIdx) {
			return false, nil
		}
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, sourceIdx, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(ns, owner, comp, sourceIdx)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete source pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// 5. Promote the replacement to Ready and drop the source. The plan
	// settles on k; EnsurePodGroups GCs the source's PodGroup next pass.
	// Promote on the PINNED rev — the rev the surged gang actually runs —
	// NOT the latest target if a mid-surge bump moved it. Using target.Name
	// here would write RunningRevision=<latest-target> while the pods are
	// on the prior rev (the X-2 corruption mode); DetectUpdateTrigger picks
	// up RunningRevision != target on the next reconcile and fires a fresh
	// surge cycle toward the newer rev.
	if err := patchInstanceStatusReadyOnRevision(ctx, input, surgeIdx, surgeTargetName); err != nil {
		return false, fmt.Errorf("promote surge gang (instance=%d): %w", surgeIdx, err)
	}
	if _, rerr := input.RemoveInstance(ctx, sourceIdx); rerr != nil {
		return false, fmt.Errorf("remove source InstanceStatus (instance=%d): %w", sourceIdx, rerr)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
		"OMENative %s gang surge complete: instance %d promoted to revision %s",
		instanceKey(comp, sourceIdx), surgeIdx, surgeTargetName)
	return true, nil
}

func cloneInstanceTopologyKeys(in map[int32]string) map[int32]string {
	out := make(map[int32]string, len(in)+1)
	for index, key := range in {
		out[index] = key
	}
	return out
}

// abandonFailedGangSurge tears down a gang surge that escalated to
// Phase=Failed and resets the source Instance to Ready on its running
// revision, so the next reconcile's DetectUpdateTrigger starts a fresh
// surge cycle toward the current (corrective) target.
//
// Steps (one advanced per reconcile; the loop requeues between them):
//  1. Delete the wedged replacement gang's pods at surgeIdx.
//  2. Drop the surge index's marker InstanceStatus (if any).
//  3. Record the terminal failure on failedTargetRev's RetryBlock, then
//     reset the source: Phase=Ready, RunningRevision unchanged, Operation
//     cleared. The source's own gang never left its running revision, so
//     this is an accurate state — and RunningRevision != target then
//     re-fires a clean surge on the next pass, through the RetryBlock gate.
//
// Writer-ordering invariant: the RetryBlock upsert lands in
// the same pass as — immediately before — the reset that clears the
// failed attempt's Operation. Upsert-first is crash-safe: a re-entered
// abandon (upsert landed, reset didn't) re-routes here via the Failed
// branch, where the writer's wave dedup refreshes without recounting; no
// attempt-stamp helper runs while the Failed branch owns dispatch, so the
// fresh Backoff cannot be prematurely flipped to RetryInProgress. An
// empty failedTargetRev means the abandon is a supersede, not a failure —
// nothing is recorded.
func abandonFailedGangSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx, surgeIdx int32, sourceRunningRev, failedTargetRev, failureReason string) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component

	// 1. Delete the wedged surge gang's pods.
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list failed surge gang pods (instance=%d): %w", surgeIdx, err)
	}
	if len(surgePods) > 0 {
		if !deps.ExpectationsCache().Satisfied(ns, owner, comp, surgeIdx) {
			return false, nil
		}
		for _, pod := range surgePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, surgeIdx, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(ns, owner, comp, surgeIdx)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete failed surge pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// 2. Drop the surge index's marker InstanceStatus (no-op if absent).
	if _, err := input.RemoveInstance(ctx, surgeIdx); err != nil {
		return false, fmt.Errorf("remove failed surge marker (instance=%d): %w", surgeIdx, err)
	}

	// 3. Record the failure, then reset the source to Ready on its running
	// revision, clearing the failed surge Operation. The reset's own prune
	// targets sourceRunningRev — a different revision than failedTargetRev
	// (an update never re-targets the running rev), so the block survives.
	if err := recordUpdateFailureInRetryBlock(ctx, input, failedTargetRev, failureReason); err != nil {
		return false, fmt.Errorf("record retry block for failed gang surge (rev=%s): %w", failedTargetRev, err)
	}
	if err := patchInstanceStatusReadyOnRevision(ctx, input, sourceIdx, sourceRunningRev); err != nil {
		return false, fmt.Errorf("reset failed gang surge source (instance=%d): %w", sourceIdx, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
		"OMENative %s abandoned failed gang surge (surge-index=%d); resetting to revision %s for a fresh rollout",
		instanceKey(comp, sourceIdx), surgeIdx, sourceRunningRev)
	return false, nil
}
