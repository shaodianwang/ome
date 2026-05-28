// Package workload is the source-agnostic per-Component reconciler
// pipeline. It drives the lifecycle of one logical workload — Plan →
// Render → Revision → Ops → Aggregate — without knowing whether the
// workload's spec lives on an InferenceService Component or on an
// InferenceReplica object.
//
// Boundary contract: the workload package owns its own status / phase /
// operation / key types (types.go) and does NOT import the OME CRD API
// package (`pkg/apis/ome/v1beta1`) from any file that is exported to
// callers. Adapters at the edge convert between the owner-CRD shape
// and the workload-owned types; once that conversion is done, every
// downstream helper in this package speaks workload types.
//
// Entry points:
//
//   - `workload/ops` — per-Instance Create / Update / Restart / Delete /
//     Migrate state machines. Callers pass workload.ReconcileInput +
//     workload.Deps + workload.ComponentPlan; ops drives one Instance to
//     convergence per call and returns done / requeue.
//
//   - `workload.AggregateStatus` (status_aggregate.go) — builds the
//     per-Component aggregate status (counts, conditions, traffic) from
//     the per-Instance observed state. Callers persist it through the
//     adapter-owned writer.
//
//   - `workload/gang` — per-Instance PodGroup lifecycle for multi-pod
//     Components. Wired before ops.Create so the gang is announced to
//     the scheduler before the first pod lands.
//
// Caller contract: the caller populates a workload.ReconcileInput
// (input.go) — a plain-data bundle of identity (OwnerObject, OwnerGVK,
// Key, EventTarget), the projected DesiredSpec / ObservedState, and
// the function-value callbacks (MutateInstance, RemoveInstance,
// WriteAggregateCondition, ClearMigrationRequest, WarnInstanceFailed)
// — and hands it to the entry point. Workload code reads
// ReconcileInput and never reaches back into the caller's types.
//
// Two callers:
//
//   - `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative`
//     dispatches a per-Component ReconcileInput from the OMENative-mode
//     ISVC reconciler. This is the production wiring today.
//
//   - `pkg/controller/v1beta1/inferencereplica` will dispatch a
//     ReconcileInput from the InferenceReplica controller once that
//     controller lands.
//
// Coordination boundary: the workload package never imports
// `inferenceservice/reconcilers/omenative/coordination/`. Cross-Component
// gate decisions are computed by the ISVC caller and projected into
// WorkloadDesiredSpec.Pacing.Decisions as plain data. The dispatcher in
// `workload/ops/dispatch.go` reads `Pacing.Decisions.UpdateAllowed` and
// nothing else from coordination. The IR caller passes Decisions=nil
// (allowed); harder gates are surfaced by the ISVC controller via
// InferenceReplica.spec.paused.
package workload
