package omenative

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// restartDrainKey identifies a Restart drain writer against a
// specific Instance materialization. Same key shape as the Update
// drain key but a distinct WriterRestartDrain userAgent so the two
// can't accidentally cancel each other out.
func restartDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// restartRequeueInterval is the wait between passes while a Restart is
// in flight.
const restartRequeueInterval = 5 * time.Second

// Restart drives one Instance through the Restart On Pod Loss sequence.
// Multi-pass; idempotent at every step.
//
//  1. Patch status to Phase=Restarting, Operation={Restart, Drain, Reason},
//     bumping Incarnation by one.
//  2. Flip ome.io/serving=False on every pod still labeled at the old
//     incarnation.
//  3. Wait for EndpointSlice drain on those pods.
//  4. Delete every old-incarnation pod.
//  5. Wait until no old-incarnation pods remain.
//  6. Create the new pod set at the bumped Incarnation (pods are
//     distinguished from the just-deleted set by ome.io/instance-incarnation).
//  7. Wait for ContainersReady on every new pod.
//  8. Flip ome.io/serving=True on every new pod.
//  9. Patch status to Phase=Ready, Operation=nil.
//
// reason is recorded on Operation.Reason at step 1 only; subsequent passes
// preserve the existing reason via the skip-write guard.
//
// done=true means step 9 completed and the Instance is back to a steady
// Ready state. done=false means caller should requeue.
func Restart(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, reason string) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}

	// Step 1: stamp Phase=Restarting and bump Incarnation. patchStatus
	// returns the post-bump value (or the existing value if a previous
	// reconcile already bumped). Use that value — not inst.Incarnation
	// from the plan — when classifying pods, because BuildPlan ran
	// against an earlier read.
	wasNotRestarting := true
	if s := findInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil && s.Phase == v1beta1.OMENativeInstanceRestarting {
		wasNotRestarting = false
	}
	newInc, err := patchInstanceStatusRestarting(ctx, p, plan.Component, inst.Index, reason, plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("patch status Restarting (instance=%d): %w", inst.Index, err)
	}
	if wasNotRestarting {
		recordWarning(p, EventReasonRestartTriggered,
			"OMENative %s restart triggered: %s (incarnation=%d)",
			instanceKey(plan.Component, inst.Index), reason, newInc)
	}

	pods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: list pods (instance=%d): %w", inst.Index, err)
	}
	oldPods, newPods, unknownPods := partitionPodsByIncarnation(pods, newInc)

	// OPS-3: surface orphan pods (label-missing) as warning events and
	// refuse to delete them. A label strip or a third-party operator
	// stamping the OMENative managed-by label would otherwise be
	// silently torn down by Phase A. The Restart op blocks until an
	// operator either re-labels the pod (re-classifying it as old or
	// fresh) or deletes the pod themselves.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			recordWarning(p, EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to delete",
				instanceKey(plan.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	// Phase A — old pods still present: drain + delete.
	if len(oldPods) > 0 {
		// Step 2: serving=False on each old pod.
		for _, pod := range oldPods {
			if !IsServing(pod) {
				continue
			}
			// Restart drain on OLD pods. NEW pods at the bumped
			// incarnation get the Lifecycle key instead.
			if err := MarkPodNotServing(ctx, p.Client, pod, WriterRestartDrain, restartDrainKey(inst.Index, newInc)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}

		// Step 3: wait for EndpointSlice drain on every old pod.
		// Drain check uses the live reader so stale cache doesn't
		// declare drain done while kube-proxy still routes to the pod.
		serviceName := headlessServiceName(p.ISVC.Name, plan.Component)
		for _, pod := range oldPods {
			drained, err := IsPodDrained(ctx, p.reader(), p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}

		// Step 4: delete pods. Gate the batch on expectations so a cache
		// lag between issuing deletes and observing them via the watch
		// doesn't cause us to re-delete.
		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}
		deletedCount := 0
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			if err := p.Client.Delete(ctx, pod); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			deletedCount++
		}
		if deletedCount > 0 {
			p.expectations().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, deletedCount)
		}
		// Step 5: requeue until the next pass observes the pods gone.
		return false, nil
	}

	// Phase B — recreate the new pod set at the bumped Incarnation.
	// OPS-2: live-read the OLD pod set first. If the cache says
	// "zero old pods" but the API server still holds them, a
	// stable-name Create here would AlreadyExists-conflict against
	// the still-terminating previous incarnation. Tolerating
	// terminating-but-not-yet-gone matches K8s foreground propagation.
	clear, err := liveOldPodsClearedForRecreate(ctx, p, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("Restart: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	// Build an ad-hoc InstancePlan that carries newInc so Render() stamps
	// the right label.
	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(p.ISVC.Name, plan.Component, newInst)
	existingByName := indexPodsByName(newPods)
	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	if len(missing) > 0 {
		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}
		created, err := createMissingPods(ctx, p, plan, newInst, missing)
		if err != nil {
			return false, err
		}
		if created > 0 {
			p.expectations().ExpectCreates(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, created)
		}
		return false, nil
	}

	// Phase C — all new pods exist. Wait for runtime ready, flip serving,
	// mark the Instance Ready.
	if !allPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if IsServing(pod) {
			continue
		}
		// NEW pods are fresh — go through Lifecycle. The
		// Restart-drain key was on the now-deleted OLD pods.
		if err := MarkPodServing(ctx, p.Client, pod, WriterLifecycle, KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	if err := patchInstanceStatusReady(ctx, p, plan.Component, inst.Index); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	recordNormal(p, EventReasonRestartCompleted,
		"OMENative %s restart complete (incarnation=%d)",
		instanceKey(plan.Component, inst.Index), newInc)
	return true, nil
}

// detectRestartTrigger decides whether the given Instance needs to enter
// (or continue) Restart. Returns (true, reason) when:
//
//   - the Instance is already mid-restart (Phase=Restarting), or
//   - the Instance is in Phase=Ready and an observed pod has Phase=Failed
//     or the live pod count is below the desired count.
//
// Phase=Creating / Phase=Deleting are not interruptible by Restart; those
// operations finish first.
func detectRestartTrigger(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan) (bool, string, error) {
	s := findInstanceStatus(p.ISVC, plan.Component, inst.Index)
	if s == nil {
		return false, "", nil
	}
	if s.Phase == v1beta1.OMENativeInstanceRestarting {
		return true, "", nil
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		return false, "", nil
	}

	pods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, "", err
	}
	expected := totalPodsForInstance(inst)

	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodFailed {
			return true, fmt.Sprintf("pod %s Failed", pod.Name), nil
		}
	}
	if int32(len(pods)) < expected {
		return true, fmt.Sprintf("pod count %d below desired %d", len(pods), expected), nil
	}
	return false, "", nil
}

// patchInstanceStatusRestarting moves the InstanceStatus to Phase=Restarting,
// stamps a Restart operation in Step=Drain with the given reason, and
// increments Incarnation by one. Idempotent: when status is already in the
// Restarting+Restart shape, the existing Incarnation is preserved and no
// write is issued. Returns the post-write Incarnation.
func patchInstanceStatusRestarting(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, reason string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceRestarting &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationRestart {
			observedIncarnation = s.Incarnation
			return false
		}
		// First transition into Restarting — bump Incarnation now so old
		// pods can be distinguished from the about-to-be-created set.
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		observedIncarnation = s.Incarnation
		s.Phase = v1beta1.OMENativeInstanceRestarting
		now := metav1.NewTime(time.Now())
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("restart-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationRestart,
			Step:           "Drain",
			Reason:         reason,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
	return observedIncarnation, err
}

// findInstanceStatus returns the InstanceStatus entry for (component, idx)
// from the cached ISVC, or nil if absent. Caller must not mutate the
// returned slot — it points into the cached ISVC.
func findInstanceStatus(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	for i := range cs.OMENative.InstanceStatuses {
		if cs.OMENative.InstanceStatuses[i].Index == idx {
			return &cs.OMENative.InstanceStatuses[i]
		}
	}
	return nil
}

// totalPodsForInstance sums the desired pod count across every Runner of
// the Instance — 1 for the single-pod case, 1+N for leader+worker.
func totalPodsForInstance(inst InstancePlan) int32 {
	var n int32
	for _, r := range inst.Runners {
		n += r.Size
	}
	return n
}

// partitionPodsByIncarnation splits pods into three buckets by the
// ome.io/instance-incarnation label:
//
//   - old:     label present AND value < current. Safe to delete in
//     Phase A of Restart — these are the prior incarnation
//     that bumping current displaces.
//   - fresh:   label present AND value >= current. The new incarnation
//     (or strictly above; pods at a future incarnation should
//     never exist but are left alone rather than deleted —
//     deleting them could lose work).
//   - unknown: label MISSING or malformed (OPS-3). Could be a
//     third-party operator stamping the same managed-by label,
//     or a manual label strip. The previous behavior of
//     treating these as old produced silent deletes of
//     foreign pods. Callers emit `FoundOrphan` and refuse to
//     delete in Phase A; operators must classify the pod
//     before the restart can proceed.
func partitionPodsByIncarnation(pods []*corev1.Pod, current int64) (old, fresh, unknown []*corev1.Pod) {
	for _, pod := range pods {
		inc, ok := instanceIncarnationFromLabels(pod)
		if !ok {
			unknown = append(unknown, pod)
			continue
		}
		if inc < current {
			old = append(old, pod)
			continue
		}
		fresh = append(fresh, pod)
	}
	return
}

// instanceIncarnationFromLabels reads ome.io/instance-incarnation off pod
// labels. Returns (0, false) when the label is absent or malformed.
func instanceIncarnationFromLabels(pod *corev1.Pod) (int64, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[labelInstanceIncarnation]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
