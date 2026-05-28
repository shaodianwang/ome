// Package internalsource contains adapter helpers that bridge between
// workload-owned types and the v1beta1.OMENative* source-of-truth types
// the OMENative ISVC dispatch path reads and writes. These converters are
// the only place in the repo where the workload package's type set
// crosses the boundary into v1beta1.OMENative* — workload code itself
// never imports pkg/apis/ome/v1beta1.
//
// The converters are field-for-field mirrors. They round-trip cleanly
// for every value of every enum; round-trip tests live in convert_test.go.
//
// Slice ordering: this package lands in part-2 slice 2 of the workload
// extraction refactor. The converters exist but no caller wires them in
// yet — slice 3+ flips the Source interface signatures and migrates each
// op (`omenative/ops/`) over to the workload types one at a time, calling
// these helpers at the adapter boundary. See
// `.claude/specs/2026-05-27-workload-extraction/plan.md` §3.X.
package internalsource

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// ComponentTypeToWorkload converts a v1beta1.ComponentType to a
// workload.ComponentType. Unknown values map to the empty string so
// adapters tolerate API-version skew without panicking.
func ComponentTypeToWorkload(v v1beta1.ComponentType) workload.ComponentType {
	switch v {
	case v1beta1.RouterComponent:
		return workload.ComponentRouter
	case v1beta1.EngineComponent:
		return workload.ComponentEngine
	case v1beta1.DecoderComponent:
		return workload.ComponentDecoder
	default:
		return workload.ComponentType("")
	}
}

// ComponentTypeFromWorkload converts a workload.ComponentType to a
// v1beta1.ComponentType. Unknown values map to the empty string so
// adapters tolerate API-version skew without panicking.
func ComponentTypeFromWorkload(w workload.ComponentType) v1beta1.ComponentType {
	switch w {
	case workload.ComponentRouter:
		return v1beta1.RouterComponent
	case workload.ComponentEngine:
		return v1beta1.EngineComponent
	case workload.ComponentDecoder:
		return v1beta1.DecoderComponent
	default:
		return v1beta1.ComponentType("")
	}
}

// InstancePhaseToWorkload converts a v1beta1.OMENativeInstancePhase to
// a workload.InstancePhase. Unknown values map to the empty phase so
// adapters tolerate API-version skew without panicking.
func InstancePhaseToWorkload(v v1beta1.OMENativeInstancePhase) workload.InstancePhase {
	switch v {
	case "":
		return workload.InstancePhaseEmpty
	case v1beta1.OMENativeInstancePending:
		return workload.InstancePhasePending
	case v1beta1.OMENativeInstanceCreating:
		return workload.InstancePhaseCreating
	case v1beta1.OMENativeInstanceReady:
		return workload.InstancePhaseReady
	case v1beta1.OMENativeInstanceUpdating:
		return workload.InstancePhaseUpdating
	case v1beta1.OMENativeInstanceRestarting:
		return workload.InstancePhaseRestarting
	case v1beta1.OMENativeInstanceMigrating:
		return workload.InstancePhaseMigrating
	case v1beta1.OMENativeInstanceFailed:
		return workload.InstancePhaseFailed
	case v1beta1.OMENativeInstanceDeleting:
		return workload.InstancePhaseDeleting
	default:
		return workload.InstancePhaseEmpty
	}
}

// InstancePhaseFromWorkload converts a workload.InstancePhase to a
// v1beta1.OMENativeInstancePhase. Unknown values map to the empty
// phase so adapters tolerate API-version skew without panicking.
func InstancePhaseFromWorkload(w workload.InstancePhase) v1beta1.OMENativeInstancePhase {
	switch w {
	case workload.InstancePhaseEmpty:
		return v1beta1.OMENativeInstancePhase("")
	case workload.InstancePhasePending:
		return v1beta1.OMENativeInstancePending
	case workload.InstancePhaseCreating:
		return v1beta1.OMENativeInstanceCreating
	case workload.InstancePhaseReady:
		return v1beta1.OMENativeInstanceReady
	case workload.InstancePhaseUpdating:
		return v1beta1.OMENativeInstanceUpdating
	case workload.InstancePhaseRestarting:
		return v1beta1.OMENativeInstanceRestarting
	case workload.InstancePhaseMigrating:
		return v1beta1.OMENativeInstanceMigrating
	case workload.InstancePhaseFailed:
		return v1beta1.OMENativeInstanceFailed
	case workload.InstancePhaseDeleting:
		return v1beta1.OMENativeInstanceDeleting
	default:
		return v1beta1.OMENativeInstancePhase("")
	}
}

// InstanceOperationTypeToWorkload converts a v1beta1.InstanceOperationType
// to a workload.InstanceOperationType. Unknown values map to the empty
// string so adapters tolerate API-version skew without panicking.
func InstanceOperationTypeToWorkload(v v1beta1.InstanceOperationType) workload.InstanceOperationType {
	switch v {
	case v1beta1.InstanceOperationCreate:
		return workload.InstanceOperationCreate
	case v1beta1.InstanceOperationUpdate:
		return workload.InstanceOperationUpdate
	case v1beta1.InstanceOperationRestart:
		return workload.InstanceOperationRestart
	case v1beta1.InstanceOperationMigrate:
		return workload.InstanceOperationMigrate
	case v1beta1.InstanceOperationDelete:
		return workload.InstanceOperationDelete
	default:
		return workload.InstanceOperationType("")
	}
}

// InstanceOperationTypeFromWorkload converts a workload.InstanceOperationType
// to a v1beta1.InstanceOperationType. Unknown values map to the empty
// string so adapters tolerate API-version skew without panicking.
func InstanceOperationTypeFromWorkload(w workload.InstanceOperationType) v1beta1.InstanceOperationType {
	switch w {
	case workload.InstanceOperationCreate:
		return v1beta1.InstanceOperationCreate
	case workload.InstanceOperationUpdate:
		return v1beta1.InstanceOperationUpdate
	case workload.InstanceOperationRestart:
		return v1beta1.InstanceOperationRestart
	case workload.InstanceOperationMigrate:
		return v1beta1.InstanceOperationMigrate
	case workload.InstanceOperationDelete:
		return v1beta1.InstanceOperationDelete
	default:
		return v1beta1.InstanceOperationType("")
	}
}

// InstanceOperationToWorkload converts a *v1beta1.InstanceOperation to
// a *workload.InstanceOperation. Returns nil if v is nil. SurgeIndex is
// copied by value (the returned struct allocates a fresh *int32 so
// callers can mutate it independently). HintTargetNodes is copied
// element-by-element into a freshly allocated slice for the same reason.
func InstanceOperationToWorkload(v *v1beta1.InstanceOperation) *workload.InstanceOperation {
	if v == nil {
		return nil
	}
	out := &workload.InstanceOperation{
		ID:             v.ID,
		Type:           InstanceOperationTypeToWorkload(v.Type),
		Step:           v.Step,
		StartedAt:      v.StartedAt,
		LastProgressAt: v.LastProgressAt,
		Deadline:       v.Deadline,
		RetryCount:     v.RetryCount,
		TargetRevision: v.TargetRevision,
		Reason:         v.Reason,
		FromNode:       v.FromNode,
		RequestUUID:    v.RequestUUID,
	}
	if v.SurgeIndex != nil {
		s := *v.SurgeIndex
		out.SurgeIndex = &s
	}
	if v.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), v.HintTargetNodes...)
	}
	return out
}

// InstanceOperationFromWorkload converts a *workload.InstanceOperation
// to a *v1beta1.InstanceOperation. Returns nil if w is nil. Pointer and
// slice fields are deep-copied for the same isolation reasons as
// InstanceOperationToWorkload.
func InstanceOperationFromWorkload(w *workload.InstanceOperation) *v1beta1.InstanceOperation {
	if w == nil {
		return nil
	}
	out := &v1beta1.InstanceOperation{
		ID:             w.ID,
		Type:           InstanceOperationTypeFromWorkload(w.Type),
		Step:           w.Step,
		StartedAt:      w.StartedAt,
		LastProgressAt: w.LastProgressAt,
		Deadline:       w.Deadline,
		RetryCount:     w.RetryCount,
		TargetRevision: w.TargetRevision,
		Reason:         w.Reason,
		FromNode:       w.FromNode,
		RequestUUID:    w.RequestUUID,
	}
	if w.SurgeIndex != nil {
		s := *w.SurgeIndex
		out.SurgeIndex = &s
	}
	if w.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), w.HintTargetNodes...)
	}
	return out
}

// InstanceStatusToWorkload converts a v1beta1.OMENativeInstanceStatus
// to a workload.InstanceStatus. NodesOccupied and Conditions slices
// are deep-copied so the returned struct can be mutated independently
// of the source. Operation is allocated via InstanceOperationToWorkload.
func InstanceStatusToWorkload(v v1beta1.OMENativeInstanceStatus) workload.InstanceStatus {
	out := workload.InstanceStatus{
		Index:             v.Index,
		Incarnation:       v.Incarnation,
		Phase:             InstancePhaseToWorkload(v.Phase),
		RunningRevision:   v.RunningRevision,
		TargetRevision:    v.TargetRevision,
		PodCount:          v.PodCount,
		ReadyPodCount:     v.ReadyPodCount,
		ServingPodCount:   v.ServingPodCount,
		AvailablePodCount: v.AvailablePodCount,
		ScheduledPodCount: v.ScheduledPodCount,
		ActiveOrdinal:     v.ActiveOrdinal,
		Operation:         InstanceOperationToWorkload(v.Operation),
	}
	if v.NodesOccupied != nil {
		out.NodesOccupied = append([]string(nil), v.NodesOccupied...)
	}
	if v.Conditions != nil {
		out.Conditions = append(out.Conditions, v.Conditions...)
	}
	return out
}

// InstanceStatusFromWorkload converts a workload.InstanceStatus to a
// v1beta1.OMENativeInstanceStatus. Slices and Operation pointer are
// deep-copied for the same isolation reasons as
// InstanceStatusToWorkload.
func InstanceStatusFromWorkload(w workload.InstanceStatus) v1beta1.OMENativeInstanceStatus {
	out := v1beta1.OMENativeInstanceStatus{
		Index:             w.Index,
		Incarnation:       w.Incarnation,
		Phase:             InstancePhaseFromWorkload(w.Phase),
		RunningRevision:   w.RunningRevision,
		TargetRevision:    w.TargetRevision,
		PodCount:          w.PodCount,
		ReadyPodCount:     w.ReadyPodCount,
		ServingPodCount:   w.ServingPodCount,
		AvailablePodCount: w.AvailablePodCount,
		ScheduledPodCount: w.ScheduledPodCount,
		ActiveOrdinal:     w.ActiveOrdinal,
		Operation:         InstanceOperationFromWorkload(w.Operation),
	}
	if w.NodesOccupied != nil {
		out.NodesOccupied = append([]string(nil), w.NodesOccupied...)
	}
	if w.Conditions != nil {
		out.Conditions = append(out.Conditions, w.Conditions...)
	}
	return out
}

// InstanceStatusSliceToWorkload converts a slice of
// v1beta1.OMENativeInstanceStatus to a slice of workload.InstanceStatus.
// Returns nil when in is nil so the round-trip preserves nilness.
func InstanceStatusSliceToWorkload(in []v1beta1.OMENativeInstanceStatus) []workload.InstanceStatus {
	if in == nil {
		return nil
	}
	out := make([]workload.InstanceStatus, len(in))
	for i := range in {
		out[i] = InstanceStatusToWorkload(in[i])
	}
	return out
}

// InstanceStatusSliceFromWorkload converts a slice of
// workload.InstanceStatus to a slice of v1beta1.OMENativeInstanceStatus.
// Returns nil when in is nil so the round-trip preserves nilness.
func InstanceStatusSliceFromWorkload(in []workload.InstanceStatus) []v1beta1.OMENativeInstanceStatus {
	if in == nil {
		return nil
	}
	out := make([]v1beta1.OMENativeInstanceStatus, len(in))
	for i := range in {
		out[i] = InstanceStatusFromWorkload(in[i])
	}
	return out
}
