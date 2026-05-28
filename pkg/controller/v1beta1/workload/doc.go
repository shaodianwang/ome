// Package workload is the extracted, source-agnostic OMENative reconciler
// pipeline. It drives the lifecycle of one (logical) workload — Plan →
// Render → Revision → Ops → Aggregate — without knowing whether the
// workload's spec lives on an InferenceService Component or on an
// InferenceReplica object.
//
// The contract is the Source interface (source.go). Two adapters today
// implement it:
//
//   - pkg/controller/v1beta1/inferenceservice/reconcilers/omenative
//     (`adapter.go`) projects an OMENative-mode ISVC Component into
//     a Source. This is the production wiring as of this refactor.
//
//   - pkg/controller/v1beta1/inferencereplica (`adapter.go`) projects an
//     InferenceReplica object into a Source. The IR controller is
//     wired but not yet created from anywhere; the path is exercised
//     by adapter + diff tests.
//
// Coordination boundary: the workload package never imports
// `inferenceservice/reconcilers/omenative/coordination/`. Cross-Component
// gate decisions are computed by the ISVC adapter and projected into
// WorkloadDesiredSpec.Pacing.Decisions as plain data. The dispatcher in
// workload/ops/dispatch.go reads `Pacing.Decisions.UpdateAllowed` and
// nothing else from coordination. The IR adapter passes Decisions=nil
// (allowed); harder gates are surfaced by the ISVC controller via
// InferenceReplica.spec.paused.
//
// See .claude/specs/2026-05-27-workload-extraction/plan.md for the
// commit-by-commit design rationale.
package workload
