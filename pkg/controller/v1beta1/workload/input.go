package workload

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReconcileInput carries everything a workload reconcile needs as plain
// data. The caller (the ISVC OMENative dispatcher today, the future
// InferenceReplica controller tomorrow) populates this struct from its
// own source-of-truth types and passes it to workload.Reconcile.
//
// Workload code reads ReconcileInput; it never reaches back into the
// caller's types. This is the boundary that lets one workload package
// serve both the ISVC and IR control planes without sharing a type
// system.
//
// DESIGN NOTE: ReconcileInput replaces the prior `Source` interface
// (deleted in this slice). The interface required adapters to implement
// 7 methods, 4 of which were trivial data projections. Bundling them as
// struct fields eliminates the adapter scaffolding without losing the
// decoupling — the only behaviour worth abstracting (per-Instance
// status mutation under retry.RetryOnConflict, plus side-effecting
// top-level conditions) is captured as the MutateInstance function
// value. When the InferenceReplica controller lands, factoring a
// dedicated Source interface back out of the populated struct is a
// 30-minute refactor.
type ReconcileInput struct {
	// OwnerObject is the K8s object whose UID/Kind/APIVersion becomes
	// the controllerOwner on every emitted pod / revision / service /
	// PodGroup / PDB. The ISVC caller sets this to
	// *v1beta1.InferenceService; the IR caller will set it to
	// *v1beta1.InferenceReplica.
	OwnerObject client.Object

	// OwnerGVK is the GroupVersionKind the workload renderer stamps on
	// every emitted pod's OwnerReference. Decoded objects often have
	// an empty TypeMeta because controller-runtime strips it during
	// deserialization, so the workload package cannot reliably derive
	// the GVK from OwnerObject — the caller passes the correct value
	// alongside. The ISVC caller sets this to
	// `ome.io/v1beta1, Kind=InferenceService`; the IR caller will set
	// the IR equivalent.
	OwnerGVK schema.GroupVersionKind

	// EventTarget is the object emitted events are stamped against.
	// Usually OwnerObject; the IR caller may set this to the parent
	// ISVC instead so user-facing event streams stay coherent once the
	// ISVC controller delegates Component management to IR. Nil means
	// "fall back to OwnerObject" — workload helpers MUST handle the
	// nil case.
	EventTarget client.Object

	// Key is the workload-owned identity. Adapters compose it from the
	// owner-CRD shape; workload code reads it opaquely for label /
	// selector / revision-name composition.
	Key Key

	// DesiredSpec is the per-reconcile projection of the source spec
	// the workload pipeline drives toward. The caller computes it once
	// per reconcile before calling workload.Reconcile.
	DesiredSpec WorkloadDesiredSpec

	// ObservedState is the per-reconcile snapshot of the source status
	// subtree the reconciler reasons about. The caller fetches and
	// converts the per-Component status from its owner CRD before
	// calling workload.Reconcile. The reconciler treats this as
	// read-only; status writes go through MutateInstance and any
	// future WriteAggregateStatus callback.
	ObservedState WorkloadObservedState

	// MutateInstance applies the mutate callback to the idx-th
	// InstanceStatus entry. The caller wraps any persistence
	// (apiserver round-trip under retry.RetryOnConflict, in-memory
	// status-subtree mirror) inside this closure; workload code calls
	// it once per logical mutation and trusts the caller's
	// implementation to handle retry / conflict resolution and any
	// side-effecting top-level conditions the adapter owns.
	//
	// The mutate callback receives the workload-owned InstanceStatus
	// mirror; the caller converts to / from the owner-CRD shape inside
	// the closure (e.g. via internalsource conversion helpers). The
	// mutate callback returns true when a real change was made;
	// returning false short-circuits the status write.
	//
	// MutateInstance MUST be set when ObservedState carries any
	// Instance entries — workload code panics on a nil callback rather
	// than silently dropping status updates.
	MutateInstance func(ctx context.Context, idx int32, mutate func(*InstanceStatus) bool) error

	// RemoveInstance drops the InstanceStatus entry for idx and
	// returns (true, nil) when a real removal happened, (false, nil)
	// when the entry was already absent. Like MutateInstance the
	// caller wraps apiserver round-trip (under retry.RetryOnConflict)
	// and any expectations-bookkeeping forget step inside this
	// closure. workload code calls it from Delete's final step once
	// every pod for the Instance is gone.
	//
	// MUST be set when ObservedState carries any Instance entries
	// the Delete state machine might converge to gone — workload
	// code panics on a nil callback rather than silently leaking
	// status entries.
	RemoveInstance func(ctx context.Context, idx int32) (bool, error)

	// WriteAggregateCondition merges cond into the owner's
	// per-Component condition list (under retry.RetryOnConflict) so
	// workload code can stamp top-level Component-scoped conditions —
	// today only `GangSchedulingUnavailable` (extracted gang.go) —
	// without reaching back into the owner-CRD typed status. The
	// caller wraps apimeta.SetStatusCondition + the apiserver round
	// trip inside the closure so LastTransitionTime only bumps on a
	// real transition. nil-OK: when unset, workload code falls back
	// to a no-op write (the existing path through the next aggregate
	// status flush picks up the condition on the following reconcile).
	WriteAggregateCondition func(ctx context.Context, cond metav1.Condition) error

	// ClearMigrationRequest removes the source-of-truth annotation /
	// trigger carrying the migration request identified by uuid. The
	// requester contract typically requires accept/reject within a
	// short window via this removal; without it the requester
	// re-delivers the same UUID indefinitely. The ISVC adapter wires
	// this to a retry.RetryOnConflict round-trip that strips the
	// per-UUID annotation; the IR adapter (when it lands) will wire
	// its own removal flow.
	//
	// nil-safe: when unset (e.g. workload-side unit tests that don't
	// exercise the annotation contract) Migrate skips the clear
	// step. Workload code MUST NOT panic on a nil callback for this
	// field — the Delete / Create state machines never call it, and
	// Migrate tolerates a nil callback as "trigger source has no
	// removal contract".
	ClearMigrationRequest func(ctx context.Context, uuid string) error

	// WarnInstanceFailed emits a Warning event against the workload's
	// EventTarget reporting that the (idx) Instance escalated to
	// Phase=Failed. The caller wraps the adapter-specific recorder
	// (e.g., the ISVC adapter wires status.RecordWarning against the
	// parent ISVC) inside this closure; workload-side escalator code
	// calls it once per escalation with the stuck pod name and reason.
	//
	// nil-safe: workload code MUST tolerate a nil callback (e.g.,
	// tests that don't wire a recorder). When the callback is nil the
	// escalation still mutates Phase=Failed but no event is emitted.
	WarnInstanceFailed func(idx int32, podName, reason string)
}
