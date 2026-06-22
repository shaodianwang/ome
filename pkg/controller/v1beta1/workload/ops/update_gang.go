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
		Index:       surgeIdx,
		Incarnation: 1,
		Runners:     append([]workload.RunnerPlan(nil), inst.Runners...),
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
		if deps.EnsureGangPodGroup != nil {
			if pgErr := deps.EnsureGangPodGroup(ctx, input, plan, surgeInst); pgErr != nil {
				return false, fmt.Errorf("ensure surge gang PodGroup (instance=%d): %w", surgeIdx, pgErr)
			}
		}
		// Stamp the pinned in-flight rev hash (NOT the latest target) so
		// the gang's pods match the revision this surge committed to and
		// the per-revision drain Service selects them. See surgeTargetName.
		if _, cerr := createMissingPods(ctx, deps, input, plan, surgeInst, surgeIdx, missing, query.RevisionHashFromControllerRevisionName(surgeTargetName)); cerr != nil {
			return false, fmt.Errorf("create surge gang (instance=%d): %w", surgeIdx, cerr)
		}
		return false, nil
	}

	// 3. Wait for the WHOLE replacement gang to be Ready, then serving.
	// Draining the source before every member is Ready would reintroduce
	// the capacity dip this whole change exists to remove.
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

	// 4. Drain the source gang — replacement is Ready, so capacity holds.
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("list source gang pods (instance=%d): %w", sourceIdx, err)
	}
	if len(sourcePods) > 0 {
		// Flip the source gang OUT of serving rotation in the SAME pass the
		// replacement gang joined it — mirroring single-pod surgeUpdate. A
		// pod merely Deleted keeps serving=True (ContainersReady) through its
		// whole termination grace, so the source would linger in
		// ServingReplicas beside the replacement (the N+1 PEAK) for the
		// drain window. For a gang that window spans reconciles, holding the
		// live cross-Component serving ratio out of band under RatioBalanced.
		// Draining first collapses serving back to N atomically.
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
