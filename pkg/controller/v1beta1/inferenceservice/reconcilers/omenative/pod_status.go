package omenative

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsContainersReady reports whether the kubelet-owned ContainersReady
// condition is True — i.e., all containers in the pod have passed their
// readiness probes.
//
// This is deliberately not corev1.PodReady. PodReady is the AND of
// ContainersReady and every readiness gate, including the controller-
// owned ome.io/serving gate. Gating MarkPodServing on PodReady would
// deadlock: kubelet won't flip PodReady=True until ome.io/serving=True,
// and OMENative won't write ome.io/serving=True until PodReady=True.
// ContainersReady reflects only probe-level readiness and is the correct
// signal that the runtime is healthy enough to receive traffic.
func IsContainersReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.ContainersReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// IsServing reports whether the ome.io/serving condition has been recorded
// as True on the pod's status. Distinct from IsPodReady: the controller
// flips this only after the runtime probe reports Ready, and uses it to
// drain traffic during update / restart / migration.
func IsServing(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == servingConditionType {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// MarkPodServing patches the pod's status subresource to set the
// controller-owned ome.io/serving condition to True. Idempotent: skips
// the write when the condition is already True.
func MarkPodServing(ctx context.Context, c client.Client, pod *corev1.Pod) error {
	return setServingCondition(ctx, c, pod, corev1.ConditionTrue, "Serving",
		"OMENative marked pod as serving traffic")
}

// MarkPodNotServing flips the condition to False, e.g., for drain.
func MarkPodNotServing(ctx context.Context, c client.Client, pod *corev1.Pod, reason, message string) error {
	return setServingCondition(ctx, c, pod, corev1.ConditionFalse, reason, message)
}

func setServingCondition(ctx context.Context, c client.Client, pod *corev1.Pod, status corev1.ConditionStatus, reason, message string) error {
	if pod == nil {
		return fmt.Errorf("nil pod")
	}
	// Re-read the pod to avoid clobbering kubelet-managed conditions
	// (PodScheduled, Initialized, ContainersReady, Ready) and any other
	// controller's custom readiness gates.
	fresh := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
		return fmt.Errorf("re-read pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	// Locate (or append) the ome.io/serving condition.
	now := metav1.NewTime(time.Now())
	found := false
	for i, cond := range fresh.Status.Conditions {
		if cond.Type != servingConditionType {
			continue
		}
		if cond.Status == status {
			// Already in the desired state.
			return nil
		}
		fresh.Status.Conditions[i].Status = status
		fresh.Status.Conditions[i].Reason = reason
		fresh.Status.Conditions[i].Message = message
		fresh.Status.Conditions[i].LastTransitionTime = now
		found = true
		break
	}
	if !found {
		fresh.Status.Conditions = append(fresh.Status.Conditions, corev1.PodCondition{
			Type:               servingConditionType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: now,
		})
	}

	if err := c.Status().Update(ctx, fresh); err != nil {
		return fmt.Errorf("patch %s on pod %s/%s: %w", servingConditionType, fresh.Namespace, fresh.Name, err)
	}
	return nil
}
