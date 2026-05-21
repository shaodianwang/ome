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
	newInc, err := patchInstanceStatusRestarting(ctx, p, plan.Component, inst.Index, reason, plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("patch status Restarting (instance=%d): %w", inst.Index, err)
	}

	pods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: list pods (instance=%d): %w", inst.Index, err)
	}
	oldPods, newPods := partitionPodsByIncarnation(pods, newInc)

	// Phase A — old pods still present: drain + delete.
	if len(oldPods) > 0 {
		// Step 2: serving=False on each old pod.
		for _, pod := range oldPods {
			if !IsServing(pod) {
				continue
			}
			if err := MarkPodNotServing(ctx, p.Client, pod, "Restarting", "OMENative restarting Instance"); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}

		// Step 3: wait for EndpointSlice drain on every old pod.
		serviceName := headlessServiceName(p.ISVC.Name, plan.Component)
		for _, pod := range oldPods {
			drained, err := IsPodDrained(ctx, p.Client, p.ISVC.Namespace, serviceName, pod)
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
		if !DefaultExpectations.Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
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
			DefaultExpectations.ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, deletedCount)
		}
		// Step 5: requeue until the next pass observes the pods gone.
		return false, nil
	}

	// Phase B — recreate the new pod set at the bumped Incarnation.
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
		if !DefaultExpectations.Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}
		created, err := createMissingPods(ctx, p, plan, newInst, missing)
		if err != nil {
			return false, err
		}
		if created > 0 {
			DefaultExpectations.ExpectCreates(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, created)
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
		if err := MarkPodServing(ctx, p.Client, pod); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	if err := patchInstanceStatusReady(ctx, p, plan.Component, inst.Index); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
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

// partitionPodsByIncarnation splits pods into (old, new) buckets by the
// ome.io/instance-incarnation label. Pods at strictly smaller incarnations
// (or missing the label altogether) are old; pods at >= current are new.
// Pods at incarnations strictly above current shouldn't exist under normal
// operation but are treated as new — Restart will never create such a pod
// itself and a stray with a future incarnation is left alone rather than
// deleted, since deleting it could lose work.
func partitionPodsByIncarnation(pods []*corev1.Pod, current int64) (old, fresh []*corev1.Pod) {
	for _, pod := range pods {
		inc, ok := instanceIncarnationFromLabels(pod)
		if !ok || inc < current {
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
