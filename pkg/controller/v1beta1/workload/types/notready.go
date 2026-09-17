package types

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReasonContainersNotReady is the failure reason for a pod stuck in
// readiness limbo: phase Running, every container started, and
// ContainersReady never True. It is the kubelet's own reason string on
// that condition, so the record an operator reads matches what
// `kubectl describe pod` shows.
//
// AMBIGUOUS: a probe that never passes can be the image or its
// configuration, but it can equally be the node the pod landed on, so the
// reason is evidence only. It never selects the disposition branch and
// never charges the revision's retry ladder; relocation keeps first try.
const ReasonContainersNotReady = "ContainersNotReady"

// PodRunningNotReady reports whether a live pod is in readiness limbo.
//
// The shape is deliberately narrow: phase Running, at least one
// container status, EVERY regular container actually running, and
// ContainersReady not True. A container parked in a waiting state
// (CrashLoopBackOff, ImagePullBackOff, ...) is excluded because the
// kubelet has already named that failure and the engine classifies it
// on its own terms.
func PodRunningNotReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running == nil {
			return false
		}
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.ContainersReady {
			return cond.Status != corev1.ConditionTrue
		}
	}
	// No ContainersReady condition yet: the kubelet has not vouched for
	// the pod, so it is not ready.
	return true
}

// PodHasWaitingContainer reports whether any container of a pod —
// regular or init — is parked in a waiting state. The readiness-limbo
// classification is suppressed for a pod set containing one: the kubelet
// has named a failure there, and that naming decides the blame.
func PodHasWaitingContainer(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil {
				return true
			}
		}
	}
	return false
}

// ContainersNotReadyTermination is the LastFailure record for a pod in
// readiness limbo: which containers are not ready, and what the
// ContainersReady condition says about them. Without it the only trace a
// deadline expiry leaves is the elapsed timeout, which tells an operator
// nothing about the probe that never passed.
//
// Time is the condition's last transition when the kubelet stamped one,
// so re-observing an unchanged pod yields an identical record and the
// status no-op write guard holds.
func ContainersNotReadyTermination(pod *corev1.Pod, now metav1.Time) *InstanceTermination {
	if pod == nil {
		return nil
	}
	t := &InstanceTermination{
		PodName: pod.Name,
		Reason:  ReasonContainersNotReady,
		Message: "pod is Running but its containers never became ready",
		Time:    now,
	}
	if names := notReadyContainerNames(pod); len(names) > 0 {
		t.ContainerName = names[0]
		t.Message += "; not ready: " + strings.Join(names, ", ")
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.ContainersReady {
			continue
		}
		if cond.Reason != "" {
			t.Message += "; ContainersReady=False (" + cond.Reason + ")"
		}
		if cond.Message != "" {
			t.Message += ": " + cond.Message
		}
		if !cond.LastTransitionTime.IsZero() {
			t.Time = cond.LastTransitionTime
		}
		break
	}
	return t
}

// notReadyContainerNames lists the pod's containers the kubelet has not
// marked Ready, regular containers first so the primary runner leads.
func notReadyContainerNames(pod *corev1.Pod) []string {
	var names []string
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if !cs.Ready {
				names = append(names, cs.Name)
			}
		}
	}
	return names
}
