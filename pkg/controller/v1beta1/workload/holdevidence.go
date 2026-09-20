// holdevidence.go — the evidence half of the end-of-pass hold
// bookkeeping. Each fact-bearing authority (the scheduler's verdict on a
// pod, the gang's PodGroup) reads its own evidence and records or
// releases its token on InstanceOperation.Waiting. Nothing here acts on
// what it finds: the repair decisions that read these readings live in
// escalation.go.
//
// The split exists because a pause freezes repair, not observation. A
// paused Component runs this half alone, so a row that becomes
// unplaceable — or whose gang name is being collected — still reports
// the fact, its deadline stays parked on a token that names the real
// wait, and the external-hold precedence rule (types/externalhold.go)
// gets the chance to hand the row from the pause to the authority with
// something to report.
package workload

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// instanceHolds is one row's hold reading: the evidence the authorities
// judged on, and whether each of them owns the row's token afterwards.
type instanceHolds struct {
	ev       InstanceEvidence
	pods     []*corev1.Pod
	desired  int32
	serving  bool
	gang     GangObservation
	held     bool
	gangHeld bool
}

// reconcileInstanceHolds runs the evidence half for one row.
//
// SCHEDULER HOLD: a pod with no placement is waiting on cluster
// capacity. Record the hold so the deadline parks, and let only the
// operator's grace end it — as an environment cause, since no corrected
// revision can conjure a node.
//
// Runs for EVERY operation type, Migrate included. A migration stamps a
// deadline on both pair rows, and a replacement the scheduler cannot
// place must not burn it; the hold is scoped to the row whose OWN pod is
// unplaceable (ev.Unschedulable), so a serving source is never held or
// failed for its surge's fate.
//
// GANG PATH: a gang is admitted as a unit, so its PodGroup carries facts
// no single member does — whether the deterministic name is usable by
// this owner at all, and whether the group has to be rebuilt. A name
// being collected parks the clock; a name held by another controller
// ends the attempt, because nothing this controller does frees it. A
// failed group is neither: the PodGroup pass resets it and the row just
// waits.
//
// Releases run before any mark, so a row that stops waiting on one
// authority and starts waiting on another does it inside one pass. Left
// unheld for even one pass, the parking step reads the empty token as
// admission and restarts a whole InstanceReadyTimeout.
func reconcileInstanceHolds(ctx context.Context, input ReconcileInput, s InstanceStatus, snapshot *ObservedSnapshot, byIdx map[int32][]*corev1.Pod, desiredByIdx map[int32]int32, now time.Time) (instanceHolds, error) {
	r := instanceHolds{
		ev:      snapshot.EvidenceFor(ctx, s.Index, now, input.StuckPodGrace),
		pods:    podsForStuckCheck(s, byIdx),
		desired: DesiredFor(desiredByIdx, s.Index, s.PodCount),
	}
	r.serving = podSetFullyServing(r.pods, r.desired)
	r.gang = gangReadingFor(input, s)
	if err := releaseSchedulerHold(ctx, input, s, r.ev, r.serving); err != nil {
		return r, fmt.Errorf("release scheduler hold (instance=%d): %w", s.Index, err)
	}
	if err := releaseGangHold(ctx, input, s, r.gang, r.serving); err != nil {
		return r, fmt.Errorf("release gang hold (instance=%d): %w", s.Index, err)
	}
	held, err := recordSchedulerHold(ctx, input, s, r.ev, r.serving)
	if err != nil {
		return r, fmt.Errorf("record scheduler hold (instance=%d): %w", s.Index, err)
	}
	r.held = held
	gangHeld, err := recordGangHold(ctx, input, s, r.gang, r.serving)
	if err != nil {
		return r, fmt.Errorf("record gang hold (instance=%d): %w", s.Index, err)
	}
	r.gangHeld = gangHeld
	return r, nil
}

// reconcileExternalHolds runs the evidence half across every observed
// row, and nothing else. It is the paused Component's share of the
// end-of-pass bookkeeping: the holds an operator needs to see are still
// recorded and still released, while every repair the action half would
// take — a relocation, a Failed stamp, a recreate — stays withheld for
// the length of the pause.
//
// Same exclusions as the action half: a deferred scale-down victim
// receives no mutation before admission, and a Failed row has no attempt
// left for an authority to hold.
func reconcileExternalHolds(ctx context.Context, input ReconcileInput, plan ComponentPlan, snapshot *ObservedSnapshot, excluded map[int32]struct{}) error {
	byIdx, err := snapshot.CachedPods(ctx)
	if err != nil {
		return fmt.Errorf("workload.Reconcile: list pods for hold evidence (component=%s): %w", plan.Component, err)
	}
	now := input.Now()
	desiredByIdx := DesiredPodCountByInstance(plan)
	for _, s := range input.ObservedState.InstanceStatuses {
		if _, skip := excluded[s.Index]; skip {
			continue
		}
		if s.Phase == InstancePhaseFailed {
			continue
		}
		if _, herr := reconcileInstanceHolds(ctx, input, s, snapshot, byIdx, desiredByIdx, now); herr != nil {
			return herr
		}
	}
	return nil
}
