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

	// Clock mirrors Deps.Clock for helpers that receive only the input.
	// See Deps.Now for the rule.
	Clock clock.Clock
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
