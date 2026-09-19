package ops

import (
	"context"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Wreckage is per-instance rollout debris keyed to a SUPERSEDED
// revision: state that no revision-diff update trigger can ever reach,
// because the trigger's predicate is "this instance must move to a
// different revision" while the wreckage sits at zero revision distance
// (a corrective roll-back) or on a third-party revision. Two shapes:
//
//   - GANG: a Failed source still carrying its gang-surge continuation
//     (Op{Update, SurgeIndex}) toward a revision that is no longer the
//     roll target. When the corrective target equals the source's
//     RunningRevision the trigger never fires, so the abandon
//     continuation must be dispatched explicitly.
//   - ALIEN PODS: live pods whose revision-hash label matches neither
//     the instance's RunningRevision nor the current roll target — the
//     dead pod an exhausted attempt toward a superseded revision left
//     behind (e.g. parked at the surge ordinal).
//
// Cleanup restores the invariant that nothing keyed to a superseded
// revision gates, steers, or occupies reconciliation.

// EvaluateWreckage is the pure wreckage predicate for one Instance.
// Plan consults it only for instances the update trigger declined
// (in-flight and re-triggered instances clean their own debris through
// the update machinery). instancePods must already be filtered to the
// instance's index; the clock is not read.
//
// Excluded by design: migrate-owned statuses (the migration record owns
// them), gang surge-target markers (a live marker is owned by its
// source; an orphaned one is collected by the plan's marker-liveness
// scale-down), and transient phases (Creating / Deleting / Restarting
// are not interruptible).
func EvaluateWreckage(s *workload.InstanceStatus, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) bool {
	if s == nil || target == nil {
		return false
	}
	if isMigrateOwnedStatus(s) || isGangSurgeTargetMarker(s) {
		return false
	}
	if s.Phase != workload.InstancePhaseReady && s.Phase != workload.InstancePhaseFailed {
		return false
	}
	if failedGangSurgeContinuation(s, target.Name) {
		return true
	}
	return len(alienRevisionPods(s, target.Name, instancePods)) > 0
}

// failedGangSurgeContinuation reports the gang wreckage shape: a Failed
// source whose preserved gang-surge operation targets a revision other
// than the current roll target.
func failedGangSurgeContinuation(s *workload.InstanceStatus, targetName string) bool {
	return s.Phase == workload.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
		s.Operation.SurgeIndex != nil &&
		s.Operation.TargetRevision != targetName
}

// alienRevisionPods returns the instance's live pods labeled with a
// revision that matches neither the instance's RunningRevision nor the
// current roll target. Unlabeled pods are never selected (legacy pods
// are the ordinal partition's business). An empty RunningRevision
// yields no aliens: with no recorded baseline, alienness is
// undecidable — that state is owned by the trigger's per-pod diff /
// adoption path, and deleting an unproven-but-innocent pod there would
// destroy the very pod adoption is waiting on.
func alienRevisionPods(s *workload.InstanceStatus, targetName string, pods []*corev1.Pod) []*corev1.Pod {
	if s.RunningRevision == "" {
		return nil
	}
	running := query.RevisionFromName(s.RunningRevision)
	target := query.RevisionFromName(targetName)
	var out []*corev1.Pod
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		hash, ok := pod.Labels[query.LabelRevisionHash]
		if !ok || hash == "" {
			continue
		}
		rev := query.RevisionFromPod(pod)
		if rev.Same(running) || rev.Same(target) {
			continue
		}
		out = append(out, pod)
	}
	return out
}

// CleanupWreckage abandons one instance's superseded-revision wreckage
// toward the CURRENT desired state — legal when target equals the
// instance's RunningRevision (the corrective roll-back), where the
// update machinery is unreachable. Effects route through the existing
// machinery:
//
//   - gang continuation → abandonFailedGangSurge (deletes the dead
//     replacement gang, drops its marker, records the failure on the
//     superseded revision's RetryBlock, resets the source Ready on its
//     running revision);
//   - alien-revision pods → taken out of rotation, then deleted on a later
//     pass, the same eviction the superseded-surge redirect applies to a
//     not-yet-promoted surge pod.
//
// The serving source is never touched: no serving-gate flip, no status
// stamp — after the debris is gone the Create pass re-proves readiness
// and promotes. done=true means no wreckage remains for this instance.
func CleanupWreckage(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("CleanupWreckage: nil client")
	}
	if target == nil {
		return true, nil
	}
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	if s == nil {
		return true, nil
	}

	if failedGangSurgeContinuation(s, target.Name) {
		return abandonFailedGangSurge(ctx, deps, input, plan, inst.Index, *s.Operation.SurgeIndex,
			s.RunningRevision, s.Operation.TargetRevision,
			instanceFailureReason(s, "gang surge abandoned after a corrective edit"), instanceFailureWorkloadCaused(s))
	}

	aliens := alienRevisionPods(s, target.Name, instancePods)
	if len(aliens) == 0 {
		return true, nil
	}
	return deleteSupersededRevisionPods(ctx, deps, input, inst.Index, aliens, target.Name)
}

// deleteSupersededRevisionPods evicts an Instance's pods keyed to a revision
// it is not converging to — the wreckage sweep's aliens, and the pods a
// retired Create attempt still holds the names of. A pod that is still
// routed is taken out of rotation and left for the next pass: deleting it in
// the same pass gives the endpoint controllers no window to act on the
// withdrawal, and a bare delete drops the connections it is carrying.
// Deletes are counted on the expectations cache before they are issued, and
// compensated when the apiserver refuses, so the next pass reads a pod set
// it can trust.
//
// done=false whenever there is still work here: outstanding expectations,
// a pod just unrouted, or pods deleted that the next pass has to re-read.
func deleteSupersededRevisionPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, pods []*corev1.Pod, targetName string) (bool, error) {
	cache := deps.ExpectationsCache()
	ns, owner, component := input.Key.Namespace, input.Key.OwnerName, input.Key.Component
	if !cache.Satisfied(ns, owner, component, idx) {
		return false, nil
	}
	unrouted := false
	for _, pod := range pods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod,
			podreadiness.WriterDeleteDrain, deleteDrainKey(idx)); err != nil {
			// A pod that has vanished, or come back under a new UID, is out
			// of rotation by other means already; the delete resolves it.
			if !apierrors.IsNotFound(err) && !errors.Is(err, podreadiness.ErrPodIdentityChanged) {
				return false, fmt.Errorf("unroute superseded-revision pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			continue
		}
		unrouted = true
	}
	// Deleting in the pass that unrouted gives the endpoint controllers no
	// window to act on the withdrawal, so a pod that was still carrying
	// traffic goes on the next pass. One that was already out of rotation
	// has nothing to wait for and goes now.
	if unrouted {
		return false, nil
	}
	deleted := 0
	for _, pod := range pods {
		cache.ExpectDeletes(ns, owner, component, idx, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			cache.ObservedDelete(ns, owner, component, idx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete superseded-revision pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		deleted++
	}
	if deleted > 0 {
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonSupersededWreckageCleaned,
			"OMENative %s deleted %d superseded-revision pod(s); current target %s",
			instanceKey(component, idx), deleted, targetName)
	}
	return false, nil
}
