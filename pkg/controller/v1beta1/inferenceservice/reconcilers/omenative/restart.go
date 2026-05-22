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

// restartDrainKey identifies a Restart drain writer against one Instance
// materialization. Same shape as the Update drain key, but the writer
// userAgent differs so the two can't cancel each other out.
func restartDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// restartRequeueInterval is the wait between passes while a Restart is
// in flight.
const restartRequeueInterval = 5 * time.Second

// Restart drives one Instance through Restart-on-pod-loss. Multi-pass,
// idempotent. Three phases:
//
//	A. Drain + delete every pod at the old incarnation.
//	B. Recreate the pod set at the bumped incarnation
//	   (distinguished by the ome.io/instance-incarnation label).
//	C. Wait for runtime ready, flip ome.io/serving=True, promote to Ready.
//
// reason lands on Operation.Reason on the first pass only; later passes
// preserve it via the skip-write guard.
func Restart(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, reason string) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}

	// Use the post-patch Incarnation, not inst.Incarnation — BuildPlan
	// ran against an earlier read.
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

	// Surface orphan pods (label-missing) as warnings and refuse to delete.
	// A stripped label or a third-party operator stamping the OMENative
	// managed-by label would otherwise get torn down by Phase A. Operator
	// must re-classify or delete the pod for Restart to proceed.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			recordWarning(p, EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to delete",
				instanceKey(plan.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	// Phase A: drain + delete old pods.
	if len(oldPods) > 0 {
		for _, pod := range oldPods {
			if !IsServing(pod) {
				continue
			}
			if err := MarkPodNotServing(ctx, p.Client, pod, WriterRestartDrain, restartDrainKey(inst.Index, newInc)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}

		// Live reader on drain check so kube-proxy isn't still routing.
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

		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			p.expectations().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, 1)
			if err := p.Client.Delete(ctx, pod); err != nil {
				p.expectations().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase B: recreate at the bumped Incarnation.
	// Live-read the OLD pod set first — a stable-name Create with the
	// cache stale would AlreadyExists against the still-terminating
	// previous incarnation. Matches K8s foreground-deletion propagation.
	clear, err := liveOldPodsClearedForRecreate(ctx, p, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("Restart: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	// Override Incarnation on the ad-hoc plan so Render stamps the new label.
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
		if _, err := createMissingPods(ctx, p, plan, newInst, inst.Index, missing); err != nil {
			return false, err
		}
		return false, nil
	}

	// Phase C: flip serving, promote Ready.
	if !allPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if IsServing(pod) {
			continue
		}
		// Lifecycle key on fresh pods; the Restart-drain key only lived
		// on the now-deleted OLD pods.
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

// detectRestartTrigger fires when the Instance is mid-restart, or when
// Phase=Ready and a pod is Failed / the live pod count is below desired.
// Creating / Deleting are not interruptible — they finish first.
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

// patchInstanceStatusRestarting idempotently stamps Phase=Restarting +
// Restart/Drain with the given reason and increments Incarnation by one.
// Returns the post-write Incarnation; if a previous pass already moved
// into Restart, the existing Incarnation is preserved.
func patchInstanceStatusRestarting(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, reason string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceRestarting &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationRestart {
			observedIncarnation = s.Incarnation
			return false
		}
		// Bump first so old pods can be distinguished from the new set.
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

// findInstanceStatus returns the cached InstanceStatus entry, nil if
// absent. Callers must NOT mutate — it aliases the cached ISVC.
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

// totalPodsForInstance sums desired pod count across Runners — 1 for
// single-pod, 1+N for leader+worker.
func totalPodsForInstance(inst InstancePlan) int32 {
	var n int32
	for _, r := range inst.Runners {
		n += r.Size
	}
	return n
}

// partitionPodsByIncarnation splits pods by the ome.io/instance-incarnation
// label:
//   - old: value < current — safe to delete (prior materialization).
//   - fresh: value >= current — the new set, kept even if strictly above
//     (deleting future-labeled pods could lose work).
//   - unknown: label missing/malformed — orphan candidates. Callers refuse
//     to delete and emit FoundOrphan; an operator must re-classify the
//     pod before Restart can proceed. Without this guard, a stripped
//     label or third-party reuse of managed-by would silently tear down
//     foreign pods.
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

// instanceIncarnationFromLabels returns (0, false) when the label is
// absent or malformed.
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
