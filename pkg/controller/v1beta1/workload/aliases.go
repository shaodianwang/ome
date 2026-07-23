// aliases.go re-exports `workload/types` declarations under the
// parent `workload` package so external callers (omenative,
// inferencereplica) keep the `workload.X` spelling. The types live in
// `workload/types/` so `workload/ops` can import them without closing
// the workload → workload/ops → workload import cycle.
package workload

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	InstanceTermination     = types.InstanceTermination
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
	RetryBlock              = types.RetryBlock
	RetryBlockState         = types.RetryBlockState
	RetryBlockDisposition   = types.RetryBlockDisposition
	RetryPolicy             = types.RetryPolicy
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

	UpdateStepGangSurgeTarget = types.UpdateStepGangSurgeTarget

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
	EventReasonAutoMigrationTriggered        = types.EventReasonAutoMigrationTriggered
	EventReasonAutoMigrationCapReached       = types.EventReasonAutoMigrationCapReached
	EventReasonInstanceFailed                = types.EventReasonInstanceFailed
	EventReasonRetryHeld                     = types.EventReasonRetryHeld
	EventReasonMigrationRequestAccepted      = types.EventReasonMigrationRequestAccepted
	EventReasonMigrationRequestRejected      = types.EventReasonMigrationRequestRejected
	EventReasonMigrationCompleted            = types.EventReasonMigrationCompleted
	EventReasonRateLimited                   = types.EventReasonRateLimited
	EventReasonMigrationFromNodeMismatch     = types.EventReasonMigrationFromNodeMismatch
	EventReasonMigrationNodeAffinityConflict = types.EventReasonMigrationNodeAffinityConflict
	EventReasonMaybeNoGangScheduler          = types.EventReasonMaybeNoGangScheduler
	EventReasonGangSplitRisk                 = types.EventReasonGangSplitRisk

	RetryBlockBackoff         = types.RetryBlockBackoff
	RetryBlockHeld            = types.RetryBlockHeld
	RetryBlockRetryInProgress = types.RetryBlockRetryInProgress

	RetryBlockUnchanged = types.RetryBlockUnchanged
	RetryBlockPersist   = types.RetryBlockPersist
	RetryBlockRemove    = types.RetryBlockRemove

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

// PodTermination re-exports types.PodTermination: extracts the most
// operator-relevant container-failure diagnostics from pod into an
// *InstanceTermination. Returns nil only when pod is nil.
func PodTermination(pod *corev1.Pod, now metav1.Time) *InstanceTermination {
	return types.PodTermination(pod, now)
}

// PodTerminationWithReason re-exports types.PodTerminationWithReason: like
// PodTermination but with an explicit reason override used by the stuck-pod
// escalator, which has already classified the wedge reason.
func PodTerminationWithReason(pod *corev1.Pod, reason string, now metav1.Time) *InstanceTermination {
	return types.PodTerminationWithReason(pod, reason, now)
}

// RecordUpdateFailureInRetryBlock re-exports the shared RetryBlock
// failure writer (types.RecordUpdateFailureInRetryBlock) — one
// implementation used by the ops gang abandon and the workload-root
// deadline disposition.
func RecordUpdateFailureInRetryBlock(ctx context.Context, input ReconcileInput, targetRev, reason string) error {
	return types.RecordUpdateFailureInRetryBlock(ctx, input, targetRev, reason)
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
