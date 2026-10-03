package canary

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// CanaryCrash is a canary-revision pod in a crash loop after its Instance
// was serving, the first one a pass observed across the unit's members. It
// is the one capacity change the step machine does not wait out: a pod that
// is gone, or that died once and is coming back, is the workload's repair
// and the step keeps its split; a pod that keeps dying after it served is
// the canary's verdict and the ladder parks.
type CanaryCrash struct {
	Component v1beta1.ComponentType
	PodName   string
	// Detail is what the kubelet reported for the run that died: the
	// termination reason and exit code when it recorded them.
	Detail string
}

// crashAnchor is the moment before which a death is not this ladder's
// business: the current step's anchor, so a resume and an advance answer
// every crash before them, and now for a unit that is not armed yet. A
// crash is read before the pass moves the anchor, so a death inside a step
// is seen by the first pass after it at the latest.
func crashAnchor(cs *v1beta1.CanaryStatus, now time.Time) time.Time {
	if cs == nil {
		return now
	}
	if cs.StepEnteredTime == nil {
		return time.Time{}
	}
	return cs.StepEnteredTime.Time
}

// crashedCanaryPod finds a pod of the member's target revision whose runner
// is in a crash loop after its Instance was serving: the pod died a second
// time since the step began, the kubelet is backing off its restarts, or
// the Instance was already rebuilt for a death inside the step and the
// rebuilt pod died again. A single death is a capacity dip the capacity
// gate waits out: the kubelet restarts the runner in place or the restart
// policy rebuilds the Instance, and the step keeps its state meanwhile.
// The Instance row anchors the reading, as the workload's own restart
// trigger does: a Ready row speaks for its pods, a Restarting row only for
// the pod set it is draining, and a row that is forming, rolling or
// migrating has no serving run to lose.
//
// A death is evidenced by the pod itself, a recorded termination or its
// restart count, never by the Instance's readiness: a fresh pod with no
// restart is never a crash, so a pod replaced after a deletion reads as
// the dip it is while the replacement comes up. A pod under deletion is
// stopped by the kubelet on its way out, with the term signal, the kill
// after its grace or a clean exit, and a pod in a terminal phase is one the
// workload reads as absent and recycles; neither pod's statuses are read.
func crashedCanaryPod(component v1beta1.ComponentType, observed observedCanaryRevisions, pods []*corev1.Pod, since time.Time) *CanaryCrash {
	if observed.targetHash == "" {
		return nil
	}
	rows := make(map[int32]*v1beta1.OMENativeInstanceStatus, len(observed.rows))
	for i := range observed.rows {
		rows[observed.rows[i].Index] = &observed.rows[i]
	}
	for _, pod := range pods {
		if pod == nil || pod.Labels[query.LabelRevisionHash] != observed.targetHash {
			continue
		}
		if pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		index, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		anchor, ok := servingAnchor(rows[index], pod, since)
		if !ok {
			continue
		}
		if detail, looping := runnerCrashLooping(pod, rows[index], anchor, since); looping {
			return &CanaryCrash{Component: component, PodName: pod.Name, Detail: detail}
		}
	}
	return nil
}

// servingAnchor is the moment after which a death of pod's runner is a death
// of a serving Instance: the later of the row's entry into Ready and since.
// It is absent for a row that never served, and for a pod of a repair's
// rebuilt set, whose boot restarts are the ordinary start of a fresh pod.
func servingAnchor(row *v1beta1.OMENativeInstanceStatus, pod *corev1.Pod, since time.Time) (time.Time, bool) {
	if row == nil || row.ReadySince == nil || row.ReadySince.IsZero() {
		return time.Time{}, false
	}
	switch row.Phase {
	case v1beta1.OMENativeInstanceReady:
	case v1beta1.OMENativeInstanceRestarting:
		if incarnation, ok := query.InstanceIncarnationFromLabels(pod); !ok || incarnation >= row.Incarnation {
			return time.Time{}, false
		}
	default:
		return time.Time{}, false
	}
	anchor := row.ReadySince.Time
	if since.After(anchor) {
		anchor = since
	}
	return anchor, true
}

// runnerCrashLooping reports whether pod's runner container is in a crash
// loop dated after anchor, and names the death. The latest recorded
// termination, the current one or the one before the current run, dates
// the death; a run with no termination recorded is a start, not a death.
// Three faces read as the loop: the kubelet backing off the next restart,
// a death of a run that a restart inside the window began, so the pod has
// died twice since the step began, and a death of the rebuilt pod of an
// Instance the workload already rebuilt for a death inside the step, the
// Instance's second death under one restart policy's repair. Sidecars and
// init containers never count; the kubelet restarts them in place without
// touching the serving process.
func runnerCrashLooping(pod *corev1.Pod, row *v1beta1.OMENativeInstanceStatus, anchor, since time.Time) (string, bool) {
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != constants.MainContainerName {
			continue
		}
		// priorRuns is the index of the run the recorded termination
		// belongs to: the restart count while that run is the current or
		// the awaited one, one less once the kubelet has started the next.
		died := status.State.Terminated
		priorRuns := status.RestartCount
		if died == nil {
			died = status.LastTerminationState.Terminated
			if status.State.Running != nil {
				priorRuns--
			}
		}
		if waiting := status.State.Waiting; waiting != nil && waiting.Reason == constants.StateReasonCrashLoopBackOff {
			if died == nil || died.FinishedAt.Time.After(anchor) {
				return crashDetail(status.Name, waiting.Reason, died), true
			}
			return "", false
		}
		if died == nil || !died.FinishedAt.Time.After(anchor) {
			return "", false
		}
		if priorRuns >= 1 && died.StartedAt.Time.After(anchor) {
			return crashDetail(status.Name, died.Reason, died), true
		}
		if rebuiltForDeathAfter(row, since) && died.FinishedAt.Time.After(row.LastFailure.Time.Time) {
			return crashDetail(status.Name, died.Reason, died), true
		}
		return "", false
	}
	return "", false
}

// rebuiltForDeathAfter reports whether the workload rebuilt row's Instance
// for a pod failure dated after since: the row keeps the termination that
// opened its last failure-driven rebuild, and a rebuild for a pod that
// merely vanished records none. Without a step anchor nothing is dated.
func rebuiltForDeathAfter(row *v1beta1.OMENativeInstanceStatus, since time.Time) bool {
	if row == nil || row.LastFailure == nil || since.IsZero() {
		return false
	}
	return row.LastFailure.Time.Time.After(since)
}

// crashDetail names the death for the park's event and record.
func crashDetail(container, reason string, died *corev1.ContainerStateTerminated) string {
	if reason == "" {
		reason = constants.StateReasonError
	}
	if died == nil {
		return fmt.Sprintf("container %s %s", container, reason)
	}
	return fmt.Sprintf("container %s %s (exit %d)", container, reason, died.ExitCode)
}

// memberRetargeted reports whether the pinned run moves comp to a revision
// other than its stable one, so the pods on its target are canary pods. A
// member the run left alone serves its stable revision on its "target", and
// a crash there is the stable side's, not the canary's. A member the run
// recorded no target for is read as retargeted, as the arming guard does.
func memberRetargeted(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) bool {
	target, ok := activeRunTarget(isvc, comp)
	if !ok {
		return true
	}
	return target.Revision != target.StableRevision
}
