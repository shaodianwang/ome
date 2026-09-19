// restart_crashloop.go — repair for a wedge no other pass owns.
//
// No pass owns an operation-free Ready row, so this trigger does: the
// stuck-pod fast escalator qualifies such a row only through the
// wedged-pod shape, which is a revision-hash disagreement, so a pod
// crash-looping on the revision the Instance is supposed to be running
// reaches no escalation path. The trigger belongs to every restart
// policy; RestartPolicy=None keeps its meaning for mere container
// restarts, because a container that died and came back is not a wedge.
//
// The repair is a Restart, not a Failed stamp: the pod set can be
// rebuilt. Routing it through the restart pass means the rebuild is
// admitted the way any attempt that takes capacity offline is — the
// per-Component unavailability budget and the cross-Component
// coordination gate must both admit it before it opens, and the
// RetryBlock recorded against the revision denies it once the attempts
// are spent. A Component wedged on a bad revision therefore repairs at
// the operator's configured pace instead of recycling every Instance in
// one pass, and a revision that keeps crash-looping is held rather than
// recycled forever.
package ops

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// crashLoopRepairReason reports whether a Ready, operation-free row
// holds a pod wedged in a terminal kubelet waiting reason on the
// Component's current revision for longer than the configured stuck-pod
// grace, and names the wedge for the operation's Reason.
//
// Four things keep it narrow:
//
//   - The grace comes from lifecycle.stuckPodGracePeriod and is measured
//     the way every other reader of this evidence measures it. Unset
//     disables the repair, exactly as it disables the fast escalation.
//   - An off-revision wedge is a leftover, not this row's workload:
//     there is no revision for a rebuild to land on, and the escalation
//     pass owns that pod through its wedged-pod edge to Failed.
//   - A fully serving pod set is never recycled on the strength of a
//     container waiting reason: the Instance is answering traffic.
//   - A RetryBlock held against the revision denies the rebuild, the
//     same authority a repair of a never-Ready Instance answers to.
func crashLoopRepairReason(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if s == nil || s.Phase != workload.InstancePhaseReady || s.Operation != nil {
		return "", false
	}
	if input.StuckPodGrace <= 0 {
		return "", false
	}
	if query.PodSetFullyServing(pods, expected) {
		return "", false
	}
	currentHash := query.RevisionFromName(input.ObservedState.CurrentRevision).Hash()
	now := input.Now()
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if !podOnCurrentRevision(pod, currentHash) {
			continue
		}
		reason, stuck := workload.PodStuckInTerminalWaiting(pod, now, input.StuckPodGrace)
		if !stuck {
			continue
		}
		if rebuildRetryBlockDenies(input, s) {
			return "", false
		}
		return fmt.Sprintf("pod %s wedged in %s past the stuck-pod grace", pod.Name, reason), true
	}
	return "", false
}

// RestartOpensUnavailability reports whether a restart selection for
// inst would OPEN a fresh crash-loop repair this pass, as opposed to
// driving one already in flight or recovering capacity that is already
// gone. Only a fresh open takes serving capacity offline, so only a
// fresh open is put to the unavailability budget and the coordination
// gate; a Restart already in flight must be driven to completion, and
// the pod-loss triggers repair an outage rather than causing one.
func RestartOpensUnavailability(input workload.ReconcileInput, inst workload.InstancePlan, instancePods []*corev1.Pod) bool {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	_, wedged := crashLoopRepairReason(input, s, inst.TotalPods(), instancePods)
	return wedged
}

// podOnCurrentRevision reports whether a pod belongs to the Component's
// current revision. An unknown hash on either side leaves the pod in
// scope: the label is the only evidence of ownership, and without it
// every pod of the Instance stays a candidate.
func podOnCurrentRevision(pod *corev1.Pod, currentHash string) bool {
	podRev := query.RevisionFromPod(pod)
	if currentHash == "" || podRev.IsZero() {
		return true
	}
	return podRev.Same(query.RevisionFromHash(currentHash))
}
