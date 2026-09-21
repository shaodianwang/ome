// escalation_refresh.go — the one effect the escalation pass has on a
// row that is already Failed.
//
// Failed is sticky: the pass takes no second edge there, so a row failed
// on the generic deadline backstop keeps reporting "DeadlineExceeded"
// even after the kubelet parks its pod in a reason that names the actual
// cause. The record is the only trace left once the pod is recreated or
// torn down, so it is refreshed once — reason, pod, container and
// message — and nothing else: the recorded time stays where it is, and
// no event, RetryBlock, phase or operation moves. Edge-triggered by the
// recorded reason itself, which stops being the generic one the moment
// the refresh lands.
package workload

// refreshedFailureEvidence returns the record that should replace a
// Failed row's LastFailure, or nil when there is nothing to refresh:
// the recorded reason is already concrete, or no pod of the row names a
// terminal kubelet reason past the stuck-pod grace.
//
// Only the generic deadline reason is refreshable. A concrete reason
// already on the record is the FIRST named cause, and an operator
// reading it must not have it replaced by whatever state the pod
// happens to be in later.
//
// The recorded time survives the refresh. What changes is the cause,
// not when the row failed, and moving the stamp forward would both
// misdate the failure and make a row that is merely wedged look like it
// keeps failing anew.
func refreshedFailureEvidence(row InstanceStatus, rowEvidence InstanceEvidence) *InstanceTermination {
	if rowEvidence.StuckPod == nil || rowEvidence.StuckReason == "" {
		return nil
	}
	if row.LastFailure == nil || row.LastFailure.Reason != DeadlineExceededReason {
		return nil
	}
	return PodTerminationWithReason(rowEvidence.StuckPod, rowEvidence.StuckReason, row.LastFailure.Time)
}

// refreshFailureMutation writes the refreshed evidence onto LastFailure
// and touches nothing else. The eligibility test is repeated inside the
// mutation because it is evaluated against the row the write actually
// lands on: the pass-start observation can be stale, and a row that has
// since left Failed or recorded a concrete cause of its own must keep
// what it has.
func refreshFailureMutation(termination *InstanceTermination) func(*InstanceStatus) bool {
	return func(row *InstanceStatus) bool {
		if row.Phase != InstancePhaseFailed {
			return false
		}
		if row.LastFailure == nil || row.LastFailure.Reason != DeadlineExceededReason {
			return false
		}
		captured := *termination
		captured.Time = row.LastFailure.Time
		row.LastFailure = &captured
		return true
	}
}
