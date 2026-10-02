package canary

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// CanaryCrash is a canary-revision pod whose runner died after its Instance
// was serving, the first one a pass observed across the unit's members. It
// is the one capacity change the step machine does not wait out: a pod that
// is gone is the workload's repair and the step keeps its split, a pod that
// served and then died is the canary's verdict and the ladder parks.
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
// died after its Instance was serving: the container restarted or finished
// after the Instance entered Ready and after since. The Instance row anchors
// the reading, as the workload's own restart trigger does: a Ready row speaks
// for its pods, a Restarting row only for the pod set it is draining, and a
// row that is forming, rolling or migrating has no serving run to lose. A pod
// already marked for deletion still counts, so a repair that drains the
// dead pod cannot hide the crash.
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
		index, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		anchor, ok := servingAnchor(rows[index], pod, since)
		if !ok {
			continue
		}
		if detail, died := runnerDiedAfter(pod, anchor); died {
			return &CanaryCrash{Component: component, PodName: pod.Name, Detail: detail}
		}
	}
	return nil
}

// servingAnchor is the moment after which a death of pod's runner is a crash
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

// runnerDiedAfter reports whether pod's runner container carries a death
// dated after anchor. A recorded termination, the current one or the one
// before the current run, dates the death exactly and decides on its own;
// only when the kubelet recorded none does a current run that began after
// the anchor stand in for it. Sidecars and init containers never count; the
// kubelet restarts them in place without touching the serving process.
func runnerDiedAfter(pod *corev1.Pod, anchor time.Time) (string, bool) {
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != constants.MainContainerName {
			continue
		}
		terminated := status.State.Terminated
		if terminated == nil {
			terminated = status.LastTerminationState.Terminated
		}
		if terminated != nil {
			if !terminated.FinishedAt.Time.After(anchor) {
				return "", false
			}
			reason := terminated.Reason
			if reason == "" {
				reason = "Error"
			}
			return fmt.Sprintf("container %s %s (exit %d)", status.Name, reason, terminated.ExitCode), true
		}
		if running := status.State.Running; running != nil && running.StartedAt.Time.After(anchor) {
			return fmt.Sprintf("container %s restarted", status.Name), true
		}
	}
	return "", false
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
