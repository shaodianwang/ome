// Package canary executes spec.rollout.canary: a progressive rollout that steps
// per-revision pod capacity and the external traffic weight, with Manual/Auto
// promotion and rollback.
//
// It mirrors the omenative/coordination package shape: a pure Reconcile that
// reads spec + observed per-revision pod counts and mutates isvc.Status
// in-memory; the controller persists status afterward. It reuses coordination's
// per-revision Services and traffic-weight writer rather than re-implementing
// them.
//
// Scope (Phase 1): single-component. The capacity/traffic/promotion machinery
// is component-agnostic; multi-component (PD) capacity fan-out and the
// same-revision internal-pairing dependency are a later increment.
package canary
