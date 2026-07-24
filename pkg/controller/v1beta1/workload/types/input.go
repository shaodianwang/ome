package types

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// MigrationRequestSummary is the type DetectMigrationRequest returns
// to the dispatcher. Direct alias of audit.MigrationRequest, the type
// the workload Migrate state machine consumes.
type MigrationRequestSummary = audit.MigrationRequest

// ReconcileInput carries everything a workload reconcile needs as
// plain data. Callers populate this struct from their source-of-truth
// types; workload code reads it and never reaches back. The boundary
// lets one workload package serve both the ISVC and IR control
// planes without sharing a type system.
type ReconcileInput struct {
	// OwnerObject is the K8s object whose UID/Kind/APIVersion becomes
	// the controllerOwner on every emitted pod / revision / service /
	// PodGroup / PDB.
	OwnerObject client.Object

	// OwnerGVK is the GroupVersionKind stamped on emitted pods'
	// OwnerReference. Passed alongside OwnerObject because
	// controller-runtime strips TypeMeta during deserialization, so
	// the workload package cannot reliably derive the GVK.
	OwnerGVK schema.GroupVersionKind

	// EventTarget is the object emitted events are stamped against.
	// Usually OwnerObject; the IR caller may set this to the parent
	// ISVC instead so user-facing event streams stay coherent. Nil
	// falls back to OwnerObject.
	EventTarget client.Object

	// LedgerOwner owns the migration audit-ledger ConfigMap (load +
	// persist + its controller OwnerReference). Usually OwnerObject; the
	// IR caller sets it to the parent ISVC so the migration ledger lives
	// on the same user-facing resource as the operator's migration-request
	// annotation (the IR owns the pods; the ISVC owns the migration audit
	// trail). Nil falls back to OwnerObject. LedgerOwnerGVK is its GVK.
	LedgerOwner    client.Object
	LedgerOwnerGVK schema.GroupVersionKind

	// Key is the workload-owned identity. Adapters compose it from the
	// owner-CRD shape; workload code reads it opaquely.
	Key Key

	// DesiredSpec is the per-reconcile projection of the source spec
	// the workload pipeline drives toward.
	DesiredSpec WorkloadDesiredSpec

	// ObservedState is the per-reconcile snapshot of the source status
	// subtree. Read-only from workload code; status writes go through
	// MutateInstance.
	ObservedState WorkloadObservedState

	// MutateInstance applies the mutate callback to the idx-th
	// InstanceStatus entry. Callers wrap persistence (apiserver round-
	// trip under retry.RetryOnConflict, in-memory mirror) inside the
	// closure. The mutate callback returns true when a real change was
	// made; false short-circuits the status write.
	//
	// MUST be set when ObservedState carries any Instance entries —
	// workload code panics on a nil callback rather than silently
	// dropping status updates.
	MutateInstance func(ctx context.Context, idx int32, mutate func(*InstanceStatus) bool) error

	// RemoveInstance drops the InstanceStatus entry for idx and
	// returns (true, nil) when a real removal happened, (false, nil)
	// when already absent. Called from Delete's final step once every
	// pod for the Instance is gone.
	//
	// MUST be set when ObservedState carries any Instance entries the
	// Delete state machine might converge to gone — nil panics rather
	// than silently leak status entries.
	RemoveInstance func(ctx context.Context, idx int32) (bool, error)

	// WriteAggregateCondition merges cond into the owner's per-Component
	// condition list so workload code can stamp top-level Component-
	// scoped conditions (today only GangSchedulingUnavailable) without
	// reaching into the owner-CRD typed status.
	//
	// MUST be set on every constructed ReconcileInput — nil panics.
	// No-op adapters wire an explicit
	// `func(_ context.Context, _ metav1.Condition) error { return nil }`.
	WriteAggregateCondition func(ctx context.Context, cond metav1.Condition) error

	// ClearMigrationRequest removes the source-of-truth annotation
	// for the migration request identified by uuid. Without removal,
	// the requester re-delivers the same UUID indefinitely.
	//
	// MUST be set on every constructed ReconcileInput — nil panics.
	// No-op adapters wire an explicit
	// `func(_ context.Context, _ string) error { return nil }`.
	ClearMigrationRequest func(ctx context.Context, uuid string) error

	// WarnInstanceFailed emits a Warning event against EventTarget
	// reporting that the (idx) Instance escalated to Phase=Failed.
	//
	// MUST be set on every constructed ReconcileInput — nil panics.
	// No-op adapters wire `func(_ int32, _, _ string) {}`.
	WarnInstanceFailed func(idx int32, podName, reason string)

	// WarnRetryHeld emits the operator-facing Warning when a revision's
	// retry budget exhausts (spec: emitted at the Held transition, once).
	// Nil-safe: unset means no event.
	WarnRetryHeld func(targetRevision string, attempts int32, reason string)

	// DetectMigrationRequest, when non-nil, returns the next migration
	// to drive. Source-of-truth carriers are adapter-specific
	// (ISVC annotations + audit ledger; IR equivalent).
	//
	// found=false means no migration in flight; the dispatcher skips
	// the Migrate pass. Nil callback is treated as found=false.
	DetectMigrationRequest func(ctx context.Context) (uuid string, req *MigrationRequestSummary, sourceIdx int32, found bool, err error)

	// UpdateGate, when non-nil, is called per Instance in the Update
	// pass before starting a fresh Update operation. Lets the adapter
	// inject cross-Component coordination gates (ratio-balanced
	// pacing, surge / unavailability budgets, sequential rollout
	// ordering) that workload code MUST NOT know about.
	//
	// allowed=false skips this Instance for this reconcile pass; the
	// dispatcher emits a short requeue. inFlightSurge / inFlightUnavail
	// are the dispatcher's within-pass counters so the gate can
	// project against the post-this-pass shape.
	//
	// Nil is treated as always-allowed.
	UpdateGate func(strategy UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (allowed bool, denyReason string)

	// MutateRetryBlock reads-modifies-writes the owner's persisted
	// RetryBlock for targetRevision. mutate receives the existing block
	// (or a zero block with TargetRevision set) and returns whether to
	// persist, remove, or leave it. Nil closure disables retry-block
	// WRITES only; the update-trigger gate still honors blocks present
	// in ObservedState.RetryBlocks.
	MutateRetryBlock func(ctx context.Context, targetRevision string, mutate func(*RetryBlock) RetryBlockDisposition) error

	// UpdateRetryPolicy bounds automatic same-target update retries.
	// nil = unconfigured → fail-safe: first failure Holds.
	UpdateRetryPolicy *RetryPolicy

	// ForceDelete gates the stuck-Terminating force-delete escalation.
	// nil = unconfigured → the escalation is disabled entirely (does not
	// exist); when non-nil both durations are > 0 — config validation
	// guarantees it, consumers never re-check.
	ForceDelete *ForceDeletePolicy

	// Teardown marks this reconcile as owner-deletion teardown. When
	// set, the dispatcher treats the planned index set as empty — every
	// observed Instance is a scale-down extra and runs the ordinary
	// Delete pipeline (drain gate, graceful delete, stuck-Terminating
	// force-delete escalation, audit) — and runs NOTHING else: no
	// Paused gate, no Restart / Migrate / Update / Create. The caller
	// owns completion detection (live component pod list) and all
	// finalizer decisions.
	Teardown bool

	// Clock mirrors Deps.Clock for helpers that receive only the input.
	// See Deps.Now for the rule.
	Clock clock.Clock
}

// ForceDeletePolicy configures the stuck-Terminating force-delete
// escalation: a Terminating pod overdue past its own deletion deadline
// by OverdueSlack, on a node whose unreachable evidence is at least
// NodeUnreachableThreshold old, may be force-deleted. Config-driven
// (chart values → inferenceservice-config); nil means unconfigured and
// the escalation is disabled entirely. Both durations are always > 0
// when the policy is non-nil (config validation rejects anything else).
type ForceDeletePolicy struct {
	// OverdueSlack is how long past the pod's own DeletionTimestamp
	// (which already includes the pod's own grace period) a Terminating
	// pod must be before it counts as wedged.
	OverdueSlack time.Duration
	// NodeUnreachableThreshold is the minimum age of the node's
	// unreachable evidence (taint TimeAdded / NotReady
	// LastTransitionTime, or the Node object gone) before the
	// escalation may act.
	NodeUnreachableThreshold time.Duration
}

// Now returns the injected clock's time, or time.Now() when no clock
// is wired. Lifecycle code holding a Deps or ReconcileInput should read
// time through Now, not time.Now(); subpackages without a seam
// (audit, podreadiness, gang) still read real time.
func (r *ReconcileInput) Now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}
