package workload

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// Test-only seams for the external test package. The escalation pass is
// engine-internal (invoked only by Reconcile), so tests drive it through
// these exports instead of widening the production surface.

// EscalateFromEvidenceForTest invokes the terminal-failure escalation
// pass directly. target is the reconcile's target ControllerRevision
// (nil is valid — the Create-op fallback stays empty).
func EscalateFromEvidenceForTest(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) error {
	return escalateFromEvidence(ctx, deps, input, plan, target, snapshot)
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
	return &ObservedSnapshot{
		input:  input,
		insts:  input.ObservedState.InstanceStatuses,
		live:   memoBuckets{done: true, buckets: byIdx},
		cached: memoBuckets{done: true, buckets: byIdx},
	}
}
