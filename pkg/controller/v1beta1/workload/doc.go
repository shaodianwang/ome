// Package workload is the extracted, source-agnostic OMENative reconciler
// pipeline. It drives the lifecycle of one (logical) workload — Plan →
// Render → Revision → Ops → Aggregate — without knowing whether the
// workload's spec lives on an InferenceService Component or on an
// InferenceReplica object.
//
// The contract is the ReconcileInput struct (input.go) — a plain-data
// bundle of identity, desired state, observed state, and one mutation
// function value. Callers populate it from their owner-CRD shape and
// hand it to workload.Reconcile. Two call sites are planned:
//
//   - pkg/controller/v1beta1/inferenceservice/reconcilers/omenative
//     dispatches a per-Component ReconcileInput from the OMENative-mode
//     ISVC reconciler. This is the production wiring as of slice 4b+ of
//     the workload extraction series.
//
//   - pkg/controller/v1beta1/inferencereplica will dispatch a
//     ReconcileInput from the InferenceReplica controller once that
//     controller lands.
//
// Earlier slices used a `Source` interface implemented by adapter
// structs; slice 4a replaced that with the plain-data ReconcileInput.
// The interface had 7 methods, 4 of which were trivial projections of
// pre-computed adapter fields; only the per-Instance status mutation
// (MutateInstance) genuinely needed polymorphism. Bundling everything
// as struct fields and a function value preserves the decoupling at
// far lower cost, and a Source interface can be factored back out
// trivially when there are 2+ callers to abstract over.
//
// Coordination boundary: the workload package never imports
// `inferenceservice/reconcilers/omenative/coordination/`. Cross-Component
// gate decisions are computed by the ISVC caller and projected into
// WorkloadDesiredSpec.Pacing.Decisions as plain data. The dispatcher in
// workload/ops/dispatch.go reads `Pacing.Decisions.UpdateAllowed` and
// nothing else from coordination. The IR caller passes Decisions=nil
// (allowed); harder gates are surfaced by the ISVC controller via
// InferenceReplica.spec.paused.
//
// See .claude/specs/2026-05-27-workload-extraction/plan.md for the
// commit-by-commit design rationale.
package workload
