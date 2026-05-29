// aliases.go re-exports `workload/types` declarations under the
// parent `workload` package so external callers (omenative,
// inferencereplica) keep the `workload.X` spelling. The types live in
// `workload/types/` so `workload/ops` can import them without closing
// the workload → workload/ops → workload import cycle.
package workload

import (
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

type (
	ComponentType           = types.ComponentType
	InstancePhase           = types.InstancePhase
	UpdateStrategyType      = types.UpdateStrategyType
	RestartPolicy           = types.RestartPolicy
	MigrationMode           = types.MigrationMode
	InstanceOperationType   = types.InstanceOperationType
	InstanceOperation       = types.InstanceOperation
	InstanceStatus          = types.InstanceStatus
	Key                     = types.Key
	ReconcileInput          = types.ReconcileInput
	MigrationRequestSummary = types.MigrationRequestSummary
	WorkloadDesiredSpec     = types.WorkloadDesiredSpec
	WorkloadObservedState   = types.WorkloadObservedState
	WorkloadAggregateStatus = types.WorkloadAggregateStatus
	WorkloadPacing          = types.WorkloadPacing
	PacingDecisions         = types.PacingDecisions
	Runner                  = types.Runner
	ComponentPlan           = types.ComponentPlan
	InstancePlan            = types.InstancePlan
	MigrationOverlay        = types.MigrationOverlay
	RunnerPlan              = types.RunnerPlan
	Deps                    = types.Deps
	RenderHook              = types.RenderHook
	Expectations            = types.Expectations
	EventReason             = types.EventReason
	ConditionType           = types.ConditionType
	ConditionReason         = types.ConditionReason
	Lifecycle               = types.Lifecycle
	UpdateStrategy          = types.UpdateStrategy
	RollingUpdate           = types.RollingUpdate
	InPlaceUpdateStrategy   = types.InPlaceUpdateStrategy
	InstanceReadyPolicy     = types.InstanceReadyPolicy
	MigrationPolicy         = types.MigrationPolicy
	ComponentTrafficTarget  = types.ComponentTrafficTarget
)

const (
	ComponentRouter  = types.ComponentRouter
	ComponentEngine  = types.ComponentEngine
	ComponentDecoder = types.ComponentDecoder

	InstancePhaseEmpty      = types.InstancePhaseEmpty
	InstancePhasePending    = types.InstancePhasePending
	InstancePhaseCreating   = types.InstancePhaseCreating
	InstancePhaseReady      = types.InstancePhaseReady
	InstancePhaseUpdating   = types.InstancePhaseUpdating
	InstancePhaseRestarting = types.InstancePhaseRestarting
	InstancePhaseMigrating  = types.InstancePhaseMigrating
	InstancePhaseFailed     = types.InstancePhaseFailed
	InstancePhaseDeleting   = types.InstancePhaseDeleting

	UpdateStrategySurgeThenDrain    = types.UpdateStrategySurgeThenDrain
	UpdateStrategyRecreatePod       = types.UpdateStrategyRecreatePod
	UpdateStrategyInPlaceIfPossible = types.UpdateStrategyInPlaceIfPossible
	UpdateStrategyInPlaceOnly       = types.UpdateStrategyInPlaceOnly

	RestartPolicyNone             = types.RestartPolicyNone
	RestartPolicyRecreateInstance = types.RestartPolicyRecreateInstance
	RestartPolicyRecreateISVC     = types.RestartPolicyRecreateISVC

	MigrationModeAuto  = types.MigrationModeAuto
	MigrationModeSurge = types.MigrationModeSurge
	MigrationModeNever = types.MigrationModeNever

	InstanceReadyPolicyAllPodReady = types.InstanceReadyPolicyAllPodReady
	InstanceReadyPolicyNone        = types.InstanceReadyPolicyNone

	InstanceOperationCreate  = types.InstanceOperationCreate
	InstanceOperationUpdate  = types.InstanceOperationUpdate
	InstanceOperationRestart = types.InstanceOperationRestart
	InstanceOperationMigrate = types.InstanceOperationMigrate
	InstanceOperationDelete  = types.InstanceOperationDelete

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

	ConditionGangSchedulingUnavailable = types.ConditionGangSchedulingUnavailable

	ReasonPodGroupCRDNotInstalled = types.ReasonPodGroupCRDNotInstalled
	ReasonGangSchedulingAvailable = types.ReasonGangSchedulingAvailable
)

// NewExpectations returns a fresh Expectations cache.
func NewExpectations() *Expectations {
	return types.NewExpectations()
}

// AllocateSurgeIndex returns the lowest int32 not present in any of
// the InstanceStatuses — the slot the migration op picks for its +1
// surge pod.
func AllocateSurgeIndex(instances []InstanceStatus) int32 {
	return types.AllocateSurgeIndex(instances)
}

// DefaultExpectations re-exports the package-level cache the
// reconciler falls back to when Deps.Expectations is nil. Read-only —
// use SetDefaultExpectations to reseat, otherwise `Deps.ExpectationsCache()`
// keeps returning the prior cache.
var DefaultExpectations = types.DefaultExpectations

// SetDefaultExpectations replaces the package-level singleton in both
// the workload alias mirror and the underlying `workload/types` var
// so `Deps.ExpectationsCache()` returns the new cache on the next
// call.
func SetDefaultExpectations(e *Expectations) {
	types.DefaultExpectations = e
	DefaultExpectations = e
}
