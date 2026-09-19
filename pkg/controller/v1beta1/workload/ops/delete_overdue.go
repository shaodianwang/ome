// delete_overdue.go — the one thing an elapsed drain deadline does.
//
// A Delete operation carries the same deadline every other operation
// does, but a drain has no backstop to hand it to: the phase must not
// move (the row is on its way out), the delete wave keeps the index,
// and force-deleting a wedged pod is gated on the configured policy
// plus node evidence. What is missing without this pass is the operator
// ever being told. So the deadline is consumed for visibility: one
// Warning naming the Instance, its pods and how far past the deadline
// the drain is, plus a record on the row that readers of the status can
// aggregate. Announced once per episode, keyed by the deadline itself.
package ops

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// announceOverdueDrains records and reports every delete-owned row
// whose drain is past its operation deadline. The record lands first
// and the event fires from the commit callback, so an announcement
// survives exactly as long as the write that earned it: a failed write
// emits nothing and the next pass re-announces.
//
// Every owned row, not only the wave this pass drives. A row deferred
// behind the pod budget is the one an operator is least likely to
// notice and most likely to be waiting on, and reporting it costs
// nothing: the announcement reads the observation the wave was selected
// from and takes no external effect.
func announceOverdueDrains(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	podsByInstance map[int32][]*corev1.Pod,
) error {
	now := input.Now()
	statuses := input.ObservedState.InstanceStatuses
	mutations := make([]workload.InstanceMutation, 0, len(statuses))
	for _, status := range statuses {
		pods := sortedInstancePods(podsByInstance[status.Index])
		overdue, ok := overdueDrain(status, pods, now)
		if !ok {
			continue
		}
		deadline := status.Operation.Deadline
		index := status.Index
		detail := overdueDrainPodDetail(pods)
		record := &workload.InstanceTermination{
			PodName: overdueDrainPodName(pods),
			Reason:  workload.DrainOverdueReason,
			Message: detail,
			Time:    deadline,
		}
		mutations = append(mutations, workload.InstanceMutation{
			Index:  index,
			Mutate: announceOverdueDrainMutation(deadline, record),
			OnCommit: func(*workload.InstanceStatus, *workload.InstanceStatus) {
				recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonDrainOverdue,
					"OMENative %s: drain overdue by %s (deadline %s); %s",
					instanceKey(plan.Component, index), overdue.Round(time.Second),
					deadline.UTC().Format(time.RFC3339), detail)
			},
		})
	}
	if len(mutations) == 0 {
		return nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return fmt.Errorf("DeleteBatch: owner-aware atomic status adapter is required")
	}
	if err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, "", nil); err != nil {
		return fmt.Errorf("DeleteBatch: announce overdue drain: %w", err)
	}
	return nil
}

// sortedInstancePods copies a row's pod bucket into namespace/name
// order, so the names an announcement reports are stable across passes
// whatever order the observation arrived in. The authoritative bucket
// is left untouched.
func sortedInstancePods(pods []*corev1.Pod) []*corev1.Pod {
	out := append([]*corev1.Pod(nil), pods...)
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left == nil || right == nil {
			return left == nil && right != nil
		}
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		return left.Name < right.Name
	})
	return out
}

// overdueDrain reports how far past its deadline a Delete-owned row's
// drain is, and whether it is overdue at all. A row with no pods left
// has nothing to wait on — it is completing this pass — and a zero
// deadline means the operation was never given one.
func overdueDrain(s workload.InstanceStatus, pods []*corev1.Pod, now time.Time) (time.Duration, bool) {
	if !deleteOwned(s) || len(pods) == 0 {
		return 0, false
	}
	deadline := s.Operation.Deadline
	if deadline.IsZero() || !now.After(deadline.Time) {
		return 0, false
	}
	if drainOverdueAnnounced(s, deadline) {
		return 0, false
	}
	return now.Sub(deadline.Time), true
}

// announceOverdueDrainMutation writes the announcement record and
// touches nothing else. Eligibility is re-tested against the row the
// write lands on: the pass-start observation can be stale, and a row
// that has since been readmitted under a new delete operation or
// already carries this episode's record must keep what it has.
func announceOverdueDrainMutation(deadline metav1.Time, record *workload.InstanceTermination) func(*workload.InstanceStatus) bool {
	return func(s *workload.InstanceStatus) bool {
		if s == nil || !deleteOwned(*s) || !s.Operation.Deadline.Equal(&deadline) {
			return false
		}
		if drainOverdueAnnounced(*s, deadline) {
			return false
		}
		captured := *record
		s.LastFailure = &captured
		return true
	}
}

// drainOverdueAnnounced reports whether the row already carries this
// episode's announcement. The episode is the deadline: a row readmitted
// for a fresh delete gets a fresh deadline and is announced again.
func drainOverdueAnnounced(s workload.InstanceStatus, deadline metav1.Time) bool {
	return s.LastFailure != nil && s.LastFailure.Reason == workload.DrainOverdueReason &&
		s.LastFailure.Time.Equal(&deadline)
}

// overdueDrainPodDetail names what the drain is still waiting on, as
// the pass observed it. Pods already on their way out are the common
// wedge and are reported as such; pods the wave has not asked to
// terminate are still in rotation, so an operator can tell a stuck
// kubelet from a stuck drain gate.
func overdueDrainPodDetail(pods []*corev1.Pod) string {
	terminating := make([]string, 0, len(pods))
	pending := make([]string, 0, len(pods))
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		if pod.DeletionTimestamp != nil {
			terminating = append(terminating, pod.Name)
			continue
		}
		pending = append(pending, pod.Name)
	}
	parts := make([]string, 0, 2)
	if len(terminating) > 0 {
		parts = append(parts, fmt.Sprintf("%d pod(s) still Terminating (%s)", len(terminating), strings.Join(terminating, ", ")))
	}
	if len(pending) > 0 {
		parts = append(parts, fmt.Sprintf("%d pod(s) still draining (%s)", len(pending), strings.Join(pending, ", ")))
	}
	if len(parts) == 0 {
		return "no pods observed"
	}
	return strings.Join(parts, "; ")
}

// overdueDrainPodName picks the record's representative pod: the first
// pod still Terminating, else the first pod of the row. The slice is
// already sorted by namespace and name, so the choice is stable.
func overdueDrainPodName(pods []*corev1.Pod) string {
	for _, pod := range pods {
		if pod != nil && pod.DeletionTimestamp != nil {
			return pod.Name
		}
	}
	for _, pod := range pods {
		if pod != nil {
			return pod.Name
		}
	}
	return ""
}
