// unschedulable.go — the scheduler-hold arm of the escalation pass. A
// pod the scheduler reports it cannot place is waiting on cluster
// capacity, not failing: the hold is recorded on the Instance operation
// so the InstanceReadyTimeout clock parks, and only an
// operator-configured grace turns it into a terminal failure.
package workload

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// firstUnschedulablePod returns the live pod with no placement whose
// name sorts lowest, with the scheduler's message and when the hold
// started. One unplaceable pod holds the whole Instance: a gang
// converges as a unit, and a single-pod Instance has nothing else to
// wait on.
//
// Chosen by NAME rather than by position. Pods arrive bucketed from an
// informer cache, whose order is a map iteration and differs between
// passes; taking whichever came first would make a gang with several
// unplaceable members record a different pod every pass and rewrite its
// hold forever, which is exactly what the edge trigger exists to
// prevent.
func firstUnschedulablePod(pods []*corev1.Pod) (*corev1.Pod, string, metav1.Time) {
	var chosen *corev1.Pod
	var message string
	var since metav1.Time
	for _, pod := range pods {
		podMessage, podSince, ok := PodUnschedulable(pod)
		if !ok {
			continue
		}
		if chosen != nil && pod.Name >= chosen.Name {
			continue
		}
		chosen, message, since = pod, podMessage, podSince
	}
	return chosen, message, since
}

// unschedulableGraceElapsed reports whether the hold has outlasted the
// operator's grace. A non-positive grace means the escalation is
// unconfigured — the hold then lasts until an operator resolves it — and
// a zero transition time means the scheduler left no start to measure
// from, which is never treated as "long enough".
func unschedulableGraceElapsed(since metav1.Time, now time.Time, grace time.Duration) bool {
	if grace <= 0 || since.IsZero() {
		return false
	}
	return now.Sub(since.Time) > grace
}

// unschedulableFailureMessage is the operator-facing summary of a
// scheduler hold: the grep-stable token plus the scheduler's own
// explanation of which constraint had no placement.
func unschedulableFailureMessage(message string) string {
	if message == "" {
		return WaitingReasonUnschedulable
	}
	return WaitingReasonUnschedulable + ": " + message
}

// unschedulableTermination is the evidence record for a scheduler hold:
// the pod that could not be placed, the token, and the scheduler's
// message. Time is the condition transition, so re-observing an
// unchanged hold produces an identical record and the status no-op
// guard keeps the pass write-free.
func unschedulableTermination(pod *corev1.Pod, message string, since metav1.Time) *InstanceTermination {
	return &InstanceTermination{
		PodName: pod.Name,
		Reason:  WaitingReasonUnschedulable,
		Message: message,
		Time:    since,
	}
}

// recordUnschedulableHold writes the hold onto the Instance: the
// Waiting token the deadline-parking step reads, and the scheduler's
// message on LastFailure so `kubectl describe` names the constraint
// rather than leaving an operator to guess why nothing is starting.
//
// Both go through the shared external-hold writer, which owns the
// precedence between authorities and the edge trigger.
func recordUnschedulableHold(ctx context.Context, input ReconcileInput, idx int32, termination *InstanceTermination) (bool, error) {
	_, held, err := MarkExternalHold(ctx, input, idx, WaitingReasonUnschedulable, termination, schedulerHoldWritable)
	return held, err
}

// schedulerHoldWritable reports whether this code may own the
// operation's Waiting token.
//
// A Delete is excluded outright. DeleteBatch owns a teardown's progress,
// its stuck-Terminating escalation and its completion, and it may pace a
// removal for longer than any grace; recording a hold there — still less
// failing the row once the grace elapses — would contradict that
// ownership, and a pod on its way out has nowhere it needs to be placed.
//
// An operation already parked on a DIFFERENT external wait also keeps
// it: that token's writer owns both the token and the authority it
// names, and overwriting it would misreport what the workload is queued
// behind.
//
// An operator pause is the exception, and the only one. It is the one
// wait that says nothing about the Instance — it reports a decision to
// stop rolling forward, while this reports that the cluster has nowhere
// to put a pod — so the fact takes the row. The pause is not ended by
// that: it still plans no further step, and the deadline stays parked on
// whichever token the row carries.
func schedulerHoldWritable(op *InstanceOperation) bool {
	if op == nil || op.Type == InstanceOperationDelete {
		return false
	}
	return op.Waiting == "" || OperationUnschedulable(op) || OperationPaused(op)
}

// clearUnschedulableHold releases the token once the pod is placed or
// gone. LastFailure is left in place: it is the record of the hold that
// just ended, and the next real failure overwrites it.
func clearUnschedulableHold(ctx context.Context, input ReconcileInput, idx int32) error {
	return ClearExternalHold(ctx, input, idx, WaitingReasonUnschedulable)
}

// releaseSchedulerHold drops the token when the scheduler's verdict no
// longer applies. A fully-serving pod set counts as released: its
// bookkeeping may be stale, but a stray unplaceable pod cannot stop a
// workload that is already in rotation.
//
// Every authority releases before any of them marks, so a row can hand
// over from one wait to another within a single pass instead of
// spending one unheld — which would restart a whole
// InstanceReadyTimeout window.
func releaseSchedulerHold(ctx context.Context, input ReconcileInput, s InstanceStatus, ev InstanceEvidence, serving bool) error {
	if ev.Unschedulable != nil && !serving {
		return nil
	}
	if !OperationUnschedulable(s.Operation) {
		return nil
	}
	return clearUnschedulableHold(ctx, input, s.Index)
}

// recordSchedulerHold marks the hold and reports whether the row carries
// the scheduler token afterwards. Eligibility is judged on the FRESH row
// inside the writer, never on the pass-start copy: a release earlier in
// this same pass may have freed the token, and refusing on the stale
// copy would leave the row unheld until the next reconcile.
func recordSchedulerHold(ctx context.Context, input ReconcileInput, s InstanceStatus, ev InstanceEvidence, serving bool) (bool, error) {
	if ev.Unschedulable == nil || serving {
		return false, nil
	}
	termination := unschedulableTermination(ev.Unschedulable, ev.UnschedulableMessage, ev.UnschedulableSince)
	return recordUnschedulableHold(ctx, input, s.Index, termination)
}

// escalateSchedulerHold ends an attempt whose pod outlasted the
// operator's placement grace. ENVIRONMENT-CAUSED: the evidence names the
// scheduler's message and the revision's retry ladder is left alone,
// because an unplaceable pod proves nothing about the pod template. The
// route mirrors the deadline backstop — a disposable attempt is
// classified and cleared, a gang keeps its Operation so the abandon path
// can consume the continuation.
//
// This escalation owns its operator event on BOTH routes. The
// disposition suppresses a warning whose reason repeats the one already
// on LastFailure, which is exactly what the hold wrote while it waited,
// so leaving the announcement to it would let the row go Failed in
// silence. Its callback is withheld for that call and fired here
// instead — but only when the disposition actually wrote something. It
// declines outright for a superseded leftover, and announcing a
// transition that did not happen would re-fire every pass.
func escalateSchedulerHold(ctx context.Context, deps Deps, input ReconcileInput, dd DispositionDeps, buf *failureStampBuffer, s InstanceStatus, pods []*corev1.Pod, desired int32, ev InstanceEvidence, now time.Time) error {
	reason := unschedulableFailureMessage(ev.UnschedulableMessage)
	idx, podName := s.Index, ev.Unschedulable.Name
	warn := func() {
		if input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(idx, podName, reason)
		}
	}
	if disposableAttempt(&s, desired) {
		// The disposition's writes are write-ahead-ordered; land the
		// pending plain stamps first.
		if err := buf.flush(ctx); err != nil {
			return err
		}
		silent := input
		silent.WarnInstanceFailed = nil
		outcome, err := DisposeExpiredAttempt(ctx, deps, silent, dd, s, blamedForHold(&s, pods, desired, ev), reason)
		if err != nil {
			return err
		}
		if outcome != DispositionSkippedSuperseded {
			warn()
		}
		return nil
	}
	termination := unschedulableTermination(ev.Unschedulable, reason, metav1.NewTime(now))
	buf.add(idx, deadlineFailedMutation(termination), warn)
	return nil
}

// blamedForHold narrows the pod set the disposition judges. A single-pod
// surge shares its Instance index with the source it is replacing, so
// the blamed set holds both; the hold is about the pod with no placement
// and nothing else. Reading the source's own container failure there
// would route an environment cause into the workload-caused branch and
// charge the target revision a RetryBlock for a shortage of nodes. This
// mirrors how the stuck-pod path narrows to its exact wedged pod.
func blamedForHold(s *InstanceStatus, pods []*corev1.Pod, desired int32, ev InstanceEvidence) []*corev1.Pod {
	if singlePodSurgeAttempt(s, desired) && ev.Unschedulable != nil {
		return []*corev1.Pod{ev.Unschedulable}
	}
	return pods
}
