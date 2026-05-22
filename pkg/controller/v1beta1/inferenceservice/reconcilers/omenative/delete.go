package omenative

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// deleteRequeueInterval is the wait between drain / gone-confirmation
// checks while a Delete is in flight.
const deleteRequeueInterval = 5 * time.Second

// Delete drives one Instance through the Delete / Scale Down sequence.
// Multi-pass state machine; idempotent at every step so a controller
// restart resumes wherever the previous pass left off.
//
//  1. Patch InstanceStatus to Phase=Deleting, Operation={Delete, Drain}.
//  2. Flip ome.io/serving=False on every pod (skips pods already flipped).
//  3. Wait for EndpointSlice drain on every pod.
//  4. Delete every pod that's not already terminating.
//  5. Wait until the live pod list is empty.
//  6. Remove the InstanceStatus entry.
//
// Returns done=true once step 6 completes — caller should stop driving
// this index. done=false means the caller should requeue.
func Delete(ctx context.Context, p ReconcileParams, plan ComponentPlan, idx int32) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Delete: nil client")
	}

	pods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, idx)
	if err != nil {
		return false, fmt.Errorf("Delete: list pods (instance=%d): %w", idx, err)
	}

	// If neither pods nor an InstanceStatus entry exist, there is nothing
	// to do — treat as already done. If pods are gone but the status
	// entry remains, clear it.
	if len(pods) == 0 {
		hadStatus, err := removeInstanceStatus(ctx, p, plan.Component, idx)
		if err != nil {
			return false, fmt.Errorf("remove InstanceStatus (instance=%d): %w", idx, err)
		}
		// Either way, this Instance is fully gone now.
		_ = hadStatus
		return true, nil
	}

	// Mark Deleting before the first mutation so a crash mid-delete still
	// shows the right Phase in status.
	if err := patchInstanceStatusDeleting(ctx, p, plan.Component, idx, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Deleting (instance=%d): %w", idx, err)
	}

	// If a previous reconcile's deletes haven't been observed yet, do not
	// re-issue the delete batch. The drain-and-check steps above are
	// reads, so running them again is harmless; only step 4 is gated.
	expectationsSatisfied := p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx)

	// Step 2: flip serving=False on each pod.
	for _, pod := range pods {
		if !IsServing(pod) {
			continue
		}
		if err := MarkPodNotServing(ctx, p.Client, pod, "Draining", "OMENative draining pod for delete"); err != nil {
			return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", idx, pod.Name, err)
		}
	}

	// Step 3: every pod must be drained from the Component Service's
	// EndpointSlices before we can delete it.
	serviceName := headlessServiceName(p.ISVC.Name, plan.Component)
	for _, pod := range pods {
		drained, err := IsPodDrained(ctx, p.Client, p.ISVC.Namespace, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", idx, pod.Name, err)
		}
		if !drained {
			return false, nil
		}
	}

	if !expectationsSatisfied {
		// Drained, but a previous pass already issued the deletes; wait
		// for the watch to confirm before either re-deleting or removing
		// the status entry.
		return false, nil
	}

	// Step 4: delete every pod that isn't already terminating.
	deletedCount := 0
	for _, pod := range pods {
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
		p.expectations().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx, deletedCount)
	}
	// Pods are now gone (or terminating); the next reconcile will read
	// an empty pod list and clear the InstanceStatus.
	return false, nil
}

// extraInstanceIndices returns the InstanceStatus indices that the plan
// is no longer driving — set-difference of status indices minus
// plan.Instances[*].Index. These are the targets for scale-down. The
// set-difference framing handles sparse indices introduced by surge
// migration correctly: an InstanceStatus at index 7 is "extra" only
// when no InstancePlan covers index 7, regardless of replica count.
//
// Instances currently mid-migration are excluded from the extras set
// even when the plan doesn't cover them, so the in-flight surge
// pair (source + surge) is never scale-down-deleted out from under
// Migrate.
func extraInstanceIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, plan ComponentPlan) []int32 {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range cs.OMENative.InstanceStatuses {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		if s.Phase == v1beta1.OMENativeInstanceMigrating {
			continue
		}
		if s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationMigrate {
			continue
		}
		extras = append(extras, s.Index)
	}
	return extras
}

// listPodsForInstance returns the pods labeled for the given Instance.
// It applies the same selector listOMENativePods uses, plus a filter on
// ome.io/instance-index. Callers operating on a single Instance should
// prefer this over bucketing a full list.
func listPodsForInstance(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) ([]*corev1.Pod, error) {
	pods, err := listOMENativePods(ctx, c, isvc, component)
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if i, ok := instanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out, nil
}

// patchInstanceStatusDeleting moves the InstanceStatus to Phase=Deleting
// and stamps a Delete operation in Step=Drain. Idempotent: skips the
// write when status is already in that shape.
func patchInstanceStatusDeleting(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceDeleting &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationDelete {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceDeleting
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("delete-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationDelete,
			Step:           "Drain",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// removeInstanceStatus drops the InstanceStatus entry for (component,
// idx) from the parent ISVC. Returns (hadEntry, err): hadEntry=true if
// an entry was actually removed, false if the index wasn't present
// (already cleared by a previous pass).
func removeInstanceStatus(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32) (bool, error) {
	fresh := &v1beta1.InferenceService{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
		return false, fmt.Errorf("re-read ISVC: %w", err)
	}
	if fresh.Status.Components == nil {
		return false, nil
	}
	cs, ok := fresh.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return false, nil
	}
	insts := cs.OMENative.InstanceStatuses
	pos := -1
	for i, s := range insts {
		if s.Index == idx {
			pos = i
			break
		}
	}
	if pos == -1 {
		return false, nil
	}
	cs.OMENative.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
	fresh.Status.Components[component] = cs

	if err := p.Client.Status().Update(ctx, fresh); err != nil {
		return false, fmt.Errorf("update ISVC status: %w", err)
	}
	// Forget the expectations entry so the (now-vanished) Instance isn't
	// stuck in an "expected deletes" state if its index is later reused.
	p.expectations().Forget(p.ISVC.Namespace, p.ISVC.Name, component, idx)
	return true, nil
}
