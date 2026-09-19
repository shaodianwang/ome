// escalation.go — the terminal-failure escalation pass. Reconcile runs
// it once at the end of every eligible non-teardown, non-paused
// reconcile: per Instance it consumes the snapshot's failure evidence
// (stuck pod / elapsed Operation deadline) and decides the Failed
// transition through the shared disposition classification
// (disposition.go). The Failed DECISION lives here, next to the rest of
// the transition decisions; the adapter-side status aggregator owns
// only counters + conditions and never writes a transition field.
package workload

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// escalateFromEvidence walks every observed InstanceStatus and escalates
// terminal failures from the snapshot's evidence. Three paths converge
// on Phase=Failed, and they don't fight — whichever fires first wins and
// the other no-ops on the already-Failed Instance:
//
//   - FAST path: a pod stuck in a terminal kubelet waiting state
//     (CrashLoopBackOff, ImagePullBackOff, ...) past input.StuckPodGrace
//     fails the Instance without waiting for the (default 30 min)
//     per-Instance Operation deadline. Qualification is
//     ShouldCheckForStuckPods: a transient-phase Instance with an
//     in-flight Operation, or the wedged-pod recovery shape (a pod whose
//     revision-hash label disagrees with CurrentRevision). A zero or
//     negative grace disables this path (the snapshot reports no stuck
//     evidence).
//
//   - BROAD path: any operation that never converges — a perpetually-
//     Pending gang, a run-then-crash loop the kubelet never parks in a
//     terminal waiting reason — fails once its Operation.Deadline
//     (StartedAt + InstanceReadyTimeout, stamped by the per-op writers)
//     elapses. Instances whose blamed pods (own bucket, plus the
//     SurgeIndex bucket for a gang-surge source) are held by an
//     admission scheduling gate (e.g. Kueue) are excluded, as are those
//     whose operation is parked on the capacity-blocked Waiting token
//     (a create admission refused for lack of quota leaves no pod to
//     carry a gate): the deadline-parking step (ReconcileGatedDeadlines)
//     owns their clock, and a queued wait must not count against the
//     timeout.
//
//   - SCHEDULER-HOLD path: a pod the scheduler reports it cannot place
//     is queued on cluster capacity, not stuck, so it parks the clock
//     through the Waiting token rather than counting against it. Only
//     input.UnschedulableGrace ends that wait, and it ends it as an
//     ENVIRONMENT cause — the evidence names the scheduler's message and
//     no revision is held. Unconfigured, the hold simply lasts.
//
//   - GANG path: what only the Instance's PodGroup can say, since a gang
//     is admitted as a unit. A deterministic name held by another
//     controller ends the attempt: nothing this controller does frees
//     it. It is an ENVIRONMENT cause and holds no revision. A name still
//     being collected parks the clock instead and never escalates — the
//     gang is announced again once the old object is gone — and a group
//     the gang scheduler has failed ends nothing at all, because the
//     PodGroup pass rebuilds it. Rows whose outcome another state
//     machine owns (Delete, Migrate), rows with no attempt in flight,
//     and a gang-surge SOURCE (whose attempt lives under the surge
//     index) are left alone on every gang reading. Whether a gang can be
//     PLACED is not read here — that arrives on the member pods and
//     takes the SCHEDULER-HOLD path above.
//
// The FAST and BROAD paths skip operations whose state machine owns its
// own terminal handling:
//
//   - Migrate (either pair side): timeout authority is the owner's
//     status.migrations record, consumed by the dispatcher's
//     migration-expiry pass. Stamping here would mark a healthy serving
//     source Failed while the record keeps driving the pair. The
//     scheduler hold still runs for them — it is scoped to the row whose
//     OWN pod is unplaceable, so it can only reach a replacement.
//   - Delete: DeleteBatch owns progress, stuck-Terminating escalation,
//     and completion. Its durable ownership must survive an elapsed
//     generic operation deadline, including when the index is planned
//     again while deletion finishes. The SCHEDULER-HOLD path skips it
//     as well: a paced teardown of a never-placed pod would otherwise
//     be failed out from under the batch still driving it.
//
// FAILED-WHILE-SERVING GUARD: an Instance whose blamed pod set is fully
// healthy — every live (non-deleting) pod ContainersReady AND in the
// serving rotation, at the desired count — is never escalated, whatever
// the evidence says. Stamping Phase=Failed over a serving workload is a
// status lie (the pods are fine; only the bookkeeping is stale), and the
// coordination layer would amplify it into a group-level failure. Such
// an Instance is skipped untouched.
//
// Instances with a disposable in-flight attempt (single-pod-updateable
// Create/Update Operations — see disposableAttempt) route through
// DisposeExpiredAttempt; gang Update attempts keep the
// Failed-preserving-Operation stamp (the gang abandon path consumes the
// continuation), and everything else takes the plain stamp for its path.
//
// WRITE BATCHING: the plain Failed stamps are standalone — escalation is
// the reconcile's final pass and nothing later depends on them being
// persisted first — so they are coalesced into ONE batched status write
// via a failureStampBuffer. Disposition routes flush the buffer first
// and keep their immediate per-call writes: their ordering is
// write-ahead (RetryBlock upsert before the op-clear, ledger persist
// before the status mirror) and must not be deferred.
//
// Adapter-agnostic: writes Phase=Failed via
// input.ApplyInstanceMutations / input.MutateInstance, emits the
// operator-facing event via input.WarnInstanceFailed.
func escalateFromEvidence(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, excluded map[int32]struct{}) error {
	byIdx, err := snapshot.CachedPods(ctx)
	if err != nil {
		return fmt.Errorf("workload.Reconcile: list pods for escalation pass (component=%s): %w", plan.Component, err)
	}
	// The Create-op disposition resolves its RetryBlock target from
	// ObservedState.UpdateRevision; on a first rollout the aggregator may
	// not have stamped it yet, so fall back to this reconcile's target
	// revision. Empty-only: a non-empty stamp stays authoritative (during
	// a canary rollback the roll target diverges from the spec target the
	// stamp names, and the block must hold the spec target).
	if input.ObservedState.UpdateRevision == "" && target != nil {
		input.ObservedState.UpdateRevision = target.Name
	}
	now := input.Now()
	desiredByIdx := DesiredPodCountByInstance(plan)
	currentHash := query.RevisionFromName(input.ObservedState.CurrentRevision).Hash()
	dd := input.Disposition
	dd.MigrationMode = plan.MigrationMode
	buf := &failureStampBuffer{input: input}
	for _, s := range input.ObservedState.InstanceStatuses {
		if _, skip := excluded[s.Index]; skip {
			continue
		}
		// Idempotent: skip an already-Failed Instance so the
		// operator-facing event fires exactly once per escalation. Its
		// recorded evidence still gets one refresh, which takes no edge.
		if s.Phase == InstancePhaseFailed {
			ev := snapshot.EvidenceFor(ctx, s.Index, now, input.StuckPodGrace)
			if t := refreshedFailureEvidence(s, ev); t != nil {
				buf.add(s.Index, refreshFailureMutation(t), nil)
			}
			continue
		}
		ev := snapshot.EvidenceFor(ctx, s.Index, now, input.StuckPodGrace)
		pods := podsForStuckCheck(s, byIdx)
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		serving := podSetFullyServing(pods, desired)
		// SCHEDULER HOLD: a pod with no placement is waiting on cluster
		// capacity. Record the hold so the deadline parks, and let only
		// the operator's grace end it — as an environment cause, since no
		// corrected revision can conjure a node.
		//
		// Runs for EVERY operation type, Migrate included. A migration
		// stamps a deadline on both pair rows, and a replacement the
		// scheduler cannot place must not burn it; the hold is scoped to
		// the row whose OWN pod is unplaceable (ev.Unschedulable), so a
		// serving source is never held or failed for its surge's fate.
		// GANG PATH: a gang is admitted as a unit, so its PodGroup carries
		// facts no single member does — whether the deterministic name is
		// usable by this owner at all, and whether the group has to be
		// rebuilt. A name being collected parks the clock; a name held by
		// another controller ends the attempt, because nothing this
		// controller does frees it. A failed group is neither: the
		// PodGroup pass resets it and the row just waits.
		gang := gangReadingFor(input, s)
		// Releases run before any mark, so a row that stops waiting on one
		// authority and starts waiting on another does it inside one pass.
		// Left unheld for even one pass, the parking step reads the empty
		// token as admission and restarts a whole InstanceReadyTimeout.
		if rerr := releaseSchedulerHold(ctx, input, s, ev, serving); rerr != nil {
			return fmt.Errorf("release scheduler hold (instance=%d): %w", s.Index, rerr)
		}
		if rerr := releaseGangHold(ctx, input, s, gang, serving); rerr != nil {
			return fmt.Errorf("release gang hold (instance=%d): %w", s.Index, rerr)
		}
		held, herr := recordSchedulerHold(ctx, input, s, ev, serving)
		if herr != nil {
			return fmt.Errorf("record scheduler hold (instance=%d): %w", s.Index, herr)
		}
		gangHeld, gerr := recordGangHold(ctx, input, s, gang, serving)
		if gerr != nil {
			return fmt.Errorf("record gang hold (instance=%d): %w", s.Index, gerr)
		}
		// The hold arms above judge on pod health alone, so a row whose
		// pods are healthy still hands a stale token back whatever it is
		// reporting about its rotation. The FAILED-WHILE-SERVING exemption
		// is the stricter question — is this Instance actually capacity —
		// and a surge that has reported its source out of rotation is not.
		if podSetIsCapacity(s, pods, desired) {
			continue
		}
		if reason := gangTerminalReason(gang.State); reason != "" && gangVerdictActionable(s.Operation) {
			escalateGangFailure(input, buf, s, gang, reason, now)
			continue
		}
		if gangHeld {
			// Parked until the object at the gang's name is collected,
			// which resolves without anyone acting. A genuinely wedged
			// sibling pod still escalates through the fast path below.
			if ev.StuckPod == nil {
				continue
			}
		}
		if held {
			if !unschedulableGraceElapsed(ev.UnschedulableSince, now, input.UnschedulableGrace) {
				// Parked. A genuinely wedged sibling pod still escalates
				// through the fast path below; the hold itself does not.
				if ev.StuckPod == nil {
					continue
				}
			} else {
				if ferr := escalateSchedulerHold(ctx, deps, input, dd, buf, s, pods, desired, ev, now); ferr != nil {
					return fmt.Errorf("escalate scheduler hold (instance=%d): %w", s.Index, ferr)
				}
				continue
			}
		}
		if s.Operation != nil {
			switch s.Operation.Type {
			case InstanceOperationMigrate, InstanceOperationDelete:
				continue
			}
		}
		// An attempt an operator is holding is not stalled, it was told
		// to wait. Neither path may end it: the deadline is parked for
		// the length of the hold, and a pod parked in a terminal waiting
		// state is waiting on the unpause rather than on a verdict.
		if OperationPaused(s.Operation) {
			continue
		}
		if ev.StuckPod == nil && !ev.DeadlinePassed {
			continue
		}
		// FAST path.
		if ev.StuckPod != nil && ShouldCheckForStuckPods(&s, pods, currentHash, query.LabelRevisionHash) {
			if disposableAttempt(&s, desired) {
				// The disposition's writes are write-ahead-ordered; land
				// the pending plain stamps first so the overall write +
				// event order matches the unbatched pass.
				if ferr := buf.flush(ctx); ferr != nil {
					return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, ferr)
				}
				dispositionPods := pods
				if singlePodSurgeAttempt(&s, desired) {
					// A single-pod surge shares its Instance index with the
					// serving source. Relocation evidence belongs to the exact
					// stuck target, not the source-plus-target node set.
					dispositionPods = []*corev1.Pod{ev.StuckPod}
				}
				if _, derr := DisposeExpiredAttempt(ctx, deps, input, dd, s, dispositionPods, ev.StuckReason); derr != nil {
					return fmt.Errorf("dispose stuck attempt (instance=%d): %w", s.Index, derr)
				}
				continue
			}
			// Preserve the stuck pod's diagnostics into LastFailure alongside
			// the Phase=Failed flip — the escalation is followed by a recreate
			// (or operator teardown) that deletes the wedged pod, so this is
			// the surviving trace. StuckReason is the live waiting-state reason
			// the classifier matched; PodTerminationWithReason fills in
			// container / exit-code detail when present and falls back to the
			// bare reason.
			termination := PodTerminationWithReason(ev.StuckPod, ev.StuckReason, metav1.NewTime(now))
			idx, podName, reason := s.Index, ev.StuckPod.Name, ev.StuckReason
			buf.add(idx, stuckPodFailedMutation(termination), func() {
				input.WarnInstanceFailed(idx, podName, reason)
			})
			continue
		}
		// BROAD path.
		if !ev.DeadlinePassed {
			continue
		}
		// Admission-gated pods are queued, not stuck: the parking step
		// zeroes their deadline; a gate-enter observed before the park
		// lands must not expire either. A gang-surge source's attempt
		// pods live in its SurgeIndex bucket, so check that bucket too.
		if anyPodAdmissionGated(byIdx[s.Index]) {
			continue
		}
		if s.Operation != nil && s.Operation.SurgeIndex != nil && anyPodAdmissionGated(byIdx[*s.Operation.SurgeIndex]) {
			continue
		}
		// Same rationale for the waits no pod can carry a gate for: a
		// create admission refused for lack of quota leaves no pod at
		// all, and a scheduler hold records itself on the Operation. Both
		// live on the surge row for a gang surge, whose source this check
		// follows the same way the gate check above follows the
		// SurgeIndex bucket. The parking step zeroes that deadline; one
		// stamped before the park landed must not expire either.
		if instanceHeldExternally(&s, input.ObservedState.InstanceStatuses) {
			continue
		}
		op := s.Operation
		if disposableAttempt(&s, desired) {
			if ferr := buf.flush(ctx); ferr != nil {
				return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, ferr)
			}
			if _, derr := DisposeExpiredAttempt(ctx, deps, input, dd, s, pods, deadlineFailureMessage(op)); derr != nil {
				return fmt.Errorf("dispose expired attempt (instance=%d): %w", s.Index, derr)
			}
			continue
		}
		idx := s.Index
		// The teardown that follows deletes the blamed pods, so revision-scoped
		// evidence must survive on LastFailure: a workload-caused waiting reason
		// — or a pod that ran every container yet never reported ready — names
		// the cause more precisely than the elapsed timeout, and it is what the
		// gang abandon charges the revision's retry ladder on.
		termination := deadlineTermination(now, op)
		if pod, reason := firstWorkloadCausedPod(pods); pod != nil {
			termination = PodTerminationWithReason(pod, reason, metav1.NewTime(now))
		} else if pod, _ := firstRunningNotReadyPod(pods, attemptTargetRevision(input, s)); pod != nil {
			// Evidence only: a pod that runs yet never reports ready leaves
			// no container-level diagnostics, so without this the record is
			// the bare elapsed timeout. It does not change who is blamed —
			// the gang abandon reads the reason and this one is not in the
			// workload-caused set.
			termination = types.ContainersNotReadyTermination(pod, metav1.NewTime(now))
		} else if pod := firstGateNotFoldedPod(pods, attemptTargetRevision(input, s)); pod != nil {
			// Evidence only, same standing: a pod whose probes passed but
			// whose readiness gate never folded into Ready is invisible to
			// its Service, and that is what the record must say instead of
			// the bare elapsed timeout.
			termination = types.GateNotFoldedTermination(pod, metav1.NewTime(now))
		}
		buf.add(idx, deadlineFailedMutation(termination), func() {
			input.WarnInstanceFailed(idx, "", deadlineFailureMessage(op))
		})
	}
	if ferr := buf.flush(ctx); ferr != nil {
		return fmt.Errorf("flush escalation stamps (component=%s): %w", plan.Component, ferr)
	}
	return nil
}

func singlePodSurgeAttempt(s *InstanceStatus, desiredPods int32) bool {
	return s != nil && desiredPods == 1 && s.Operation != nil &&
		s.Operation.Type == InstanceOperationUpdate &&
		s.Operation.Step == UpdateStepSurge && s.Operation.SurgeIndex == nil
}

// failureStampBuffer coalesces the escalation pass's plain Failed
// stamps into one batched status write. Buffer only standalone stamps:
// mutations nothing later in the reconcile depends on being persisted
// first. Each buffered warn fires only after its stamp's write
// succeeded, matching the immediate path (a failed write emits nothing;
// the evidence re-escalates next reconcile). A nil warn is a mutation
// that carries no operator-facing event of its own.
type failureStampBuffer struct {
	input ReconcileInput
	muts  []InstanceMutation
	warns []func()
}

func (b *failureStampBuffer) add(idx int32, mutate func(*InstanceStatus) bool, warn func()) {
	b.muts = append(b.muts, InstanceMutation{Index: idx, Mutate: mutate})
	b.warns = append(b.warns, warn)
}

// flush persists the buffered stamps — one batched write when the
// adapter provides ApplyInstanceMutations, one MutateInstance call per
// stamp otherwise — then fires the deferred warnings. Empty buffer =
// zero writes. The buffer resets whichever path ran.
func (b *failureStampBuffer) flush(ctx context.Context) error {
	if len(b.muts) == 0 {
		return nil
	}
	if b.input.ApplyInstanceMutations != nil {
		if err := b.input.ApplyInstanceMutations(ctx, b.muts); err != nil {
			return err
		}
		for _, warn := range b.warns {
			if warn != nil {
				warn()
			}
		}
	} else {
		for i, m := range b.muts {
			if err := b.input.MutateInstance(ctx, m.Index, m.Mutate); err != nil {
				return err
			}
			if b.warns[i] != nil {
				b.warns[i]()
			}
		}
	}
	b.muts, b.warns = nil, nil
	return nil
}

// podSetIsCapacity reports whether the Instance is healthy capacity,
// which is what buys it the FAILED-WHILE-SERVING exemption.
//
// Pod conditions alone do not settle that during a surge. Until the
// drain step the source is what holds the Instance's traffic, and a
// surge whose source has been taken out of rotation by someone else says
// so on its operation: the pods can read fully serving while nothing is
// in rotation, and a replacement wedged in that state must escalate
// rather than be exempted by a source that is not serving.
func podSetIsCapacity(s InstanceStatus, pods []*corev1.Pod, desired int32) bool {
	return podSetFullyServing(pods, desired) && !types.OperationSourceUnrouted(s.Operation)
}

// podSetFullyServing is query.PodSetFullyServing under the name the
// escalation pass's guards read by.
func podSetFullyServing(pods []*corev1.Pod, desired int32) bool {
	return query.PodSetFullyServing(pods, desired)
}

// anyPodAdmissionGated reports whether any pod still carries an
// admission scheduling gate (queued by an external admission authority,
// e.g. Kueue).
func anyPodAdmissionGated(pods []*corev1.Pod) bool {
	for _, p := range pods {
		if PodAdmissionGated(p) {
			return true
		}
	}
	return false
}

// gangReadingFor returns the PodGroup reading that governs one row.
//
// A gang-surge SOURCE gets none. Its own group is the one its serving
// pods already belong to, while the attempt in flight builds members
// under the SURGE index — which carries its own reading and its own
// hold. Judging the source on its own group would hold, or end, a row
// whose replacement is the thing actually being waited on. The empty
// reading still lets the release step retire a token the row took
// before it became a surge source.
func gangReadingFor(input ReconcileInput, s InstanceStatus) GangObservation {
	if s.Operation != nil && s.Operation.SurgeIndex != nil {
		return GangObservation{}
	}
	return input.Gangs.For(s.Index)
}
