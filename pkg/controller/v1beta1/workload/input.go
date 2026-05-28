package workload

import (
	"context"

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
}
