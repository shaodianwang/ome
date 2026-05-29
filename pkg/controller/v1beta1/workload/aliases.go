// aliases.go is the backward-compatibility re-export layer that lets
// every existing caller — `pkg/controller/v1beta1/inferenceservice/...`,
// `workload/ops/...`, `workload/gang/...`, `workload/query/...`, etc.
// — keep referencing types as `workload.X`, `workload.Y` after the
// underlying definitions moved into the `workload/types` subpackage.
//
// Why the split happened: `workload/ops/*.go` previously imported the
// parent `workload` package for type references (workload.ComponentPlan,
// workload.Deps, workload.ReconcileInput, ...). That blocked the new
// `workload.Reconcile` entry point from calling into `workload/ops`
// directly — closing `workload → workload/ops → workload` would have
// been an import cycle. Splitting the types out into `workload/types/`
// inverts the direction: ops imports types, and reconcile.go can now
// import ops without going through workload.
//
// Every type / const below is a Go alias of the corresponding
// `workload/types` declaration. Aliases are zero-cost at runtime; they
// only re-export the same identifier under a different package path.
// No conversion, no copy, no shim — `workload.Deps{Client: c}` and
// `types.Deps{Client: c}` are the same value.
//
// New code should prefer the `workload/types` import directly when it
// lives under `workload/` (the subpackages already do this). External
// callers (omenative, inferencereplica) keep the `workload.X` spelling.
package workload

import (
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Types — pure value types ops, query, gang, podgroup, render all
// consume directly from the workload boundary.
type (
	// ComponentType identifies router | engine | decoder.
	ComponentType = types.ComponentType
	// InstancePhase is the per-Instance lifecycle phase.
	InstancePhase = types.InstancePhase
	// UpdateStrategyType is the workload-internal rollout-mechanism
	// selector. Mirrors the CRD UpdateStrategyType enum.
	UpdateStrategyType = types.UpdateStrategyType
	// RestartPolicy is the workload-internal Instance-group restart
	// policy. Mirrors the CRD InstanceRestartPolicy enum.
	RestartPolicy = types.RestartPolicy
	// MigrationMode is the workload-internal migration disposition.
	// Mirrors the CRD MigrationPolicyMode enum.
	MigrationMode = types.MigrationMode
	// InstanceOperationType is the workload-internal kind of an
	// in-flight destructive operation against one Instance.
	InstanceOperationType = types.InstanceOperationType
	// InstanceOperation is the durable recovery anchor for one
	// in-flight per-Instance destructive action.
	InstanceOperation = types.InstanceOperation
	// InstanceStatus is the workload-internal per-Instance status —
	// the mirror of v1beta1.OMENativeInstanceStatus that workload
	// code reads and writes directly.
	InstanceStatus = types.InstanceStatus
	// Key uniquely identifies one logical workload (per-Component
	// slice of an owner).
	Key = types.Key

	// ReconcileInput is the plain-data bundle a caller hands to
	// workload.Reconcile each reconcile pass.
	ReconcileInput = types.ReconcileInput
	// MigrationRequestSummary is the dispatcher↔state-machine
	// contract type for migration requests. Alias of
	// audit.MigrationRequest.
	MigrationRequestSummary = types.MigrationRequestSummary

	// WorkloadDesiredSpec is the per-reconcile projection of the
	// source spec the workload pipeline drives toward.
	WorkloadDesiredSpec = types.WorkloadDesiredSpec
	// WorkloadObservedState is the per-reconcile snapshot of the
	// source status subtree.
	WorkloadObservedState = types.WorkloadObservedState
	// WorkloadAggregateStatus is the per-reconcile flush of
	// counters, conditions, and traffic.
	WorkloadAggregateStatus = types.WorkloadAggregateStatus
	// WorkloadPacing is the projected rollout pacing per reconcile.
	WorkloadPacing = types.WorkloadPacing
	// PacingDecisions is the workload-facing projection of
	// cross-Component coordination gates.
	PacingDecisions = types.PacingDecisions
	// Runner is the workload-package projection of one Runner role
	// within an Instance.
	Runner = types.Runner

	// ComponentPlan is the desired Component → Instance → Runner →
	// Pod shape computed each reconcile.
	ComponentPlan = types.ComponentPlan
	// InstancePlan is the desired state for one Instance — the
	// atomic unit of gang scheduling, restart, and migration.
	InstancePlan = types.InstancePlan
	// MigrationOverlay records the per-Instance placement
	// constraints a surge migration carries onto the rendered pod.
	MigrationOverlay = types.MigrationOverlay
	// RunnerPlan is the desired state for one Runner within an
	// Instance.
	RunnerPlan = types.RunnerPlan

	// Deps is the manager-scoped wiring (Client, APIReader,
	// Recorder, Expectations, RenderHook) every workload reconcile
	// receives.
	Deps = types.Deps
	// RenderHook is the optional adapter-specific render-time pod
	// mutator.
	RenderHook = types.RenderHook

	// Expectations is the create/delete bookkeeping cache the
	// reconciler uses to avoid re-issuing batches.
	Expectations = types.Expectations

	// EventReason is the workload-internal event-reason identifier the
	// ops state machines stamp on K8s Events.
	EventReason = types.EventReason
	// ConditionType is the workload-internal Type identifier stamped on
	// metav1.Condition entries.
	ConditionType = types.ConditionType
	// ConditionReason is the workload-internal Reason identifier
	// stamped on metav1.Condition entries.
	ConditionReason = types.ConditionReason
)

// Constants — every package-level const moves with its enum type.
const (
	// ComponentRouter is the router Component.
	ComponentRouter = types.ComponentRouter
	// ComponentEngine is the engine Component.
	ComponentEngine = types.ComponentEngine
	// ComponentDecoder is the decoder Component.
	ComponentDecoder = types.ComponentDecoder

	// InstancePhaseEmpty is the zero-value Phase before any
	// observation lands.
	InstancePhaseEmpty = types.InstancePhaseEmpty
	// InstancePhasePending mirrors the Pending CRD phase.
	InstancePhasePending = types.InstancePhasePending
	// InstancePhaseCreating mirrors the Creating CRD phase.
	InstancePhaseCreating = types.InstancePhaseCreating
	// InstancePhaseReady mirrors the Ready CRD phase.
	InstancePhaseReady = types.InstancePhaseReady
	// InstancePhaseUpdating mirrors the Updating CRD phase.
	InstancePhaseUpdating = types.InstancePhaseUpdating
	// InstancePhaseRestarting mirrors the Restarting CRD phase.
	InstancePhaseRestarting = types.InstancePhaseRestarting
	// InstancePhaseMigrating mirrors the Migrating CRD phase.
	InstancePhaseMigrating = types.InstancePhaseMigrating
	// InstancePhaseFailed mirrors the Failed CRD phase.
	InstancePhaseFailed = types.InstancePhaseFailed
	// InstancePhaseDeleting mirrors the Deleting CRD phase.
	InstancePhaseDeleting = types.InstancePhaseDeleting

	// UpdateStrategySurgeThenDrain mirrors the SurgeThenDrain CRD
	// strategy.
	UpdateStrategySurgeThenDrain = types.UpdateStrategySurgeThenDrain
	// UpdateStrategyRecreatePod mirrors the RecreatePod CRD
	// strategy.
	UpdateStrategyRecreatePod = types.UpdateStrategyRecreatePod
	// UpdateStrategyInPlaceIfPossible mirrors the InPlaceIfPossible
	// CRD strategy.
	UpdateStrategyInPlaceIfPossible = types.UpdateStrategyInPlaceIfPossible
	// UpdateStrategyInPlaceOnly mirrors the InPlaceOnly CRD
	// strategy.
	UpdateStrategyInPlaceOnly = types.UpdateStrategyInPlaceOnly

	// RestartPolicyNone mirrors the None CRD policy.
	RestartPolicyNone = types.RestartPolicyNone
	// RestartPolicyRecreateInstance mirrors the
	// RecreateInstanceOnPodRestart CRD policy.
	RestartPolicyRecreateInstance = types.RestartPolicyRecreateInstance
	// RestartPolicyRecreateISVC mirrors the
	// RecreateISVCOnPodRestart CRD policy.
	RestartPolicyRecreateISVC = types.RestartPolicyRecreateISVC

	// MigrationModeAuto mirrors the auto CRD mode.
	MigrationModeAuto = types.MigrationModeAuto
	// MigrationModeSurge mirrors the surge CRD mode.
	MigrationModeSurge = types.MigrationModeSurge
	// MigrationModeNever mirrors the never CRD mode.
	MigrationModeNever = types.MigrationModeNever

	// InstanceOperationCreate mirrors the Create CRD operation.
	InstanceOperationCreate = types.InstanceOperationCreate
	// InstanceOperationUpdate mirrors the Update CRD operation.
	InstanceOperationUpdate = types.InstanceOperationUpdate
	// InstanceOperationRestart mirrors the Restart CRD operation.
	InstanceOperationRestart = types.InstanceOperationRestart
	// InstanceOperationMigrate mirrors the Migrate CRD operation.
	InstanceOperationMigrate = types.InstanceOperationMigrate
	// InstanceOperationDelete mirrors the Delete CRD operation.
	InstanceOperationDelete = types.InstanceOperationDelete

	// Workload-emitted event reasons (workload/types/events.go).
	EventReasonInstanceCreated               = types.EventReasonInstanceCreated
	EventReasonInstanceReady                 = types.EventReasonInstanceReady
	EventReasonInPlaceUpdateStarted          = types.EventReasonInPlaceUpdateStarted
	EventReasonInPlaceUpdateCompleted        = types.EventReasonInPlaceUpdateCompleted
	EventReasonInPlaceUpdateNotPossible      = types.EventReasonInPlaceUpdateNotPossible
	EventReasonRecreateUpdateStarted         = types.EventReasonRecreateUpdateStarted
	EventReasonRecreateUpdateCompleted       = types.EventReasonRecreateUpdateCompleted
	EventReasonRestartTriggered              = types.EventReasonRestartTriggered
	EventReasonRestartCompleted              = types.EventReasonRestartCompleted
	EventReasonFoundOrphan                   = types.EventReasonFoundOrphan
	EventReasonMigrationRequestAccepted      = types.EventReasonMigrationRequestAccepted
	EventReasonMigrationRequestRejected      = types.EventReasonMigrationRequestRejected
	EventReasonMigrationCompleted            = types.EventReasonMigrationCompleted
	EventReasonRateLimited                   = types.EventReasonRateLimited
	EventReasonMigrationFromNodeMismatch     = types.EventReasonMigrationFromNodeMismatch
	EventReasonMigrationNodeAffinityConflict = types.EventReasonMigrationNodeAffinityConflict
	EventReasonMaybeNoGangScheduler          = types.EventReasonMaybeNoGangScheduler

	// Workload-emitted condition types (workload/types/conditions.go).
	ConditionGangSchedulingUnavailable = types.ConditionGangSchedulingUnavailable

	// Workload-emitted condition reasons (workload/types/conditions.go).
	ReasonPodGroupCRDNotInstalled = types.ReasonPodGroupCRDNotInstalled
	ReasonGangSchedulingAvailable = types.ReasonGangSchedulingAvailable
)

// NewExpectations returns a fresh Expectations cache. Forwarder so
// callers that constructed the cache as `workload.NewExpectations()`
// before the types-split keep the same construction site.
func NewExpectations() *Expectations {
	return types.NewExpectations()
}

// AllocateSurgeIndex returns a fresh non-negative index for a surge
// replica — the lowest int32 not currently present in any of the
// InstanceStatuses. Forwarder so external callers
// (omenative/core/params.AllocateSurgeIndex) keep the
// `workload.AllocateSurgeIndex(...)` spelling after the types-split.
// The migration state machine in workload/ops calls
// types.AllocateSurgeIndex directly (it cannot import the workload
// parent without closing the cycle the types-split avoids).
func AllocateSurgeIndex(instances []InstanceStatus) int32 {
	return types.AllocateSurgeIndex(instances)
}

// DefaultExpectations re-exports the package-level cache the
// reconciler falls back to when Deps.Expectations is nil. The
// underlying var lives in `workload/types` (the
// `Deps.ExpectationsCache()` method reads it directly there); this
// alias-style mirror is kept identity-equal to the types-package var
// at startup, and SetDefaultExpectations below updates BOTH in one
// step so test reseats stay coherent across spellings.
//
// Read this var; do not assign to it directly — direct assignment
// updates only the workload-side mirror and leaves
// `Deps.ExpectationsCache()` returning the prior cache. Tests that
// want a fresh per-test singleton call SetDefaultExpectations.
var DefaultExpectations = types.DefaultExpectations

// SetDefaultExpectations replaces the package-level singleton in both
// the workload alias mirror AND the underlying `workload/types` var
// so `Deps.ExpectationsCache()` returns the new cache on the next
// call. Tests that previously did
//
//	workload.DefaultExpectations = workload.NewExpectations()
//
// migrate to
//
//	workload.SetDefaultExpectations(workload.NewExpectations())
//
// after the types-split (direct assignment to the alias mirror leaves
// the types-side var stale; see DefaultExpectations above).
func SetDefaultExpectations(e *Expectations) {
	types.DefaultExpectations = e
	DefaultExpectations = e
}
