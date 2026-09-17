package types

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WaitingReasonUnschedulable is the InstanceOperation.Waiting token
// recorded while a pod of the operation carries PodScheduled=False with
// the scheduler's Unschedulable reason. It lives beside the apiserver
// rejection tokens for the same reason they do: the string is fixed
// Kubernetes API semantics, identical on every cluster, not
// operator-tunable behavior.
//
// ENVIRONMENT-CAUSED: the cluster has no placement for a pod the
// revision renders correctly, so the token is deliberately absent from
// workloadCausedWaitingReasons and never charges a retry ladder.
const WaitingReasonUnschedulable = corev1.PodReasonUnschedulable

// OperationUnschedulable reports whether an operation is parked on the
// scheduler-hold waiting token — the scheduler has ruled that one of its
// pods cannot be placed anywhere in the cluster.
func OperationUnschedulable(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonUnschedulable
}

// OperationExternallyHeld reports whether an operation is parked on any
// wait outside the workload's control: admission refusing its create for
// lack of quota, or the scheduler finding no placement for its pods.
// Both are waits an operator or another workload resolves, so the
// InstanceReadyTimeout clock must not run through either.
func OperationExternallyHeld(op *InstanceOperation) bool {
	return OperationCapacityBlocked(op) || OperationUnschedulable(op)
}

// PodUnschedulable reports whether a live pod is one the scheduler
// cannot place, and returns the scheduler's own explanation plus the
// moment PodScheduled last transitioned.
//
// That transition — not the pod's creation — is the start of the hold,
// and it is what a grace window must be measured from: creation time
// would also count the queueing before the scheduler ever ruled, so a
// busy scheduler would shorten the operator's window.
func PodUnschedulable(pod *corev1.Pod) (message string, since metav1.Time, ok bool) {
	if pod == nil || pod.DeletionTimestamp != nil {
		return "", metav1.Time{}, false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.PodScheduled {
			continue
		}
		if cond.Status != corev1.ConditionFalse || cond.Reason != WaitingReasonUnschedulable {
			return "", metav1.Time{}, false
		}
		return cond.Message, cond.LastTransitionTime, true
	}
	return "", metav1.Time{}, false
}
