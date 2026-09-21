package workload

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// Test-only seams for the external test package. The escalation pass is
// engine-internal (invoked only by Reconcile), so tests drive it through
// these exports instead of widening the production surface.

// EscalateFromEvidenceForTest invokes the hold pass and the
// terminal-failure escalation pass in the order a reconcile runs them.
// target is the reconcile's target ControllerRevision (nil is valid —
// the Create-op fallback stays empty).
func EscalateFromEvidenceForTest(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) error {
	held, err := runHoldPass(ctx, deps, input, plan, snapshot, nil)
	if err != nil {
		return err
	}
	return escalateFromEvidence(ctx, deps, input, plan, target, snapshot, nil, held)
}

// HoldPassForTest invokes the hold pass alone — what a paused Component
// runs without the repair that follows it.
func HoldPassForTest(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, snapshot *ObservedSnapshot) error {
	_, err := runHoldPass(ctx, deps, input, plan, snapshot, nil)
	return err
}

// PruneSupersededRetryBlocksForTest invokes the RetryBlock
// supersede-prune directly. target is the reconcile's roll target
// ControllerRevision (nil is valid — no roll-target revision to keep).
func PruneSupersededRetryBlocksForTest(ctx context.Context, input ReconcileInput, target *appsv1.ControllerRevision) error {
	return pruneSupersededRetryBlocks(ctx, input, target)
}

// SnapshotWithPodsForTest returns a snapshot whose pod buckets are
// pre-materialized for both read sources, so tests supply the bucketed
// pods without a client.
func SnapshotWithPodsForTest(input ReconcileInput, byIdx map[int32][]*corev1.Pod) *ObservedSnapshot {
	return SnapshotWithDistinctPodsForTest(input, byIdx, byIdx)
}

// SnapshotWithDistinctPodsForTest keeps the API-reader and cache views
// separate so tests can model informer lag.
func SnapshotWithDistinctPodsForTest(input ReconcileInput, liveByIdx, cachedByIdx map[int32][]*corev1.Pod) *ObservedSnapshot {
	live, err := NewDecisionObservation(input.ObservedState.InstanceStatuses,
		NewAPIReaderSelectorPodObservation(nil, liveByIdx))
	if err != nil {
		panic(err)
	}
	cached, err := NewDecisionObservation(input.ObservedState.InstanceStatuses,
		NewCachedSelectorPodObservation(nil, cachedByIdx))
	if err != nil {
		panic(err)
	}
	return &ObservedSnapshot{
		input:  input,
		insts:  input.ObservedState.InstanceStatuses,
		live:   memoObservation{done: true, observation: live},
		cached: memoObservation{done: true, observation: cached},
	}
}
