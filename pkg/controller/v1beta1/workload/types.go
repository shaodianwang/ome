// Package workload — workload-owned value types.
//
// The constraint this file enforces: the workload package owns its own
// status / operation / component / phase / key types and does NOT
// depend on the OME CRD API package or any owner-CRD-specific
// pod-label-key constant. Every type here mirrors the corresponding
// OMENative-on-ISVC status type field-for-field; the adapter boundary
// in `omenative/internalsource/` (ISVC adapter) and the future
// `inferencereplica/` adapter performs the two-way conversion.
//
// HISTORY: closed PR #264 attempted to keep the CRD status types as the
// wire format between `workload/` and adapters. That left the CRD-API
// import plus ISVC-only pod-label-key references inside `workload/`,
// which defeated the architectural boundary. This file is the corrected
// design: workload owns its types, adapters convert at the edge. See
// `.claude/specs/2026-05-27-workload-extraction/plan.md` §3.X for the
// cost/benefit analysis.
//
// SCOPE: This file lands in Part-2 slice 1 — type definitions only.
// `workload/source.go` continues to reference the CRD types in the
// same package until slice 2 introduces the conversion layer and flips
// the Source interface signatures. These types coexist alongside the
// CRD types referenced by source.go during the transition window.
package workload

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ComponentType is the workload-internal Component identifier. Mirrors
// the CRD ComponentType at the string level so the adapter conversion
// is a verbatim cast; the distinct Go type prevents accidental
// cross-package coupling at compile time.
type ComponentType string

const (
	// ComponentRouter is the router Component.
	ComponentRouter ComponentType = "router"
	// ComponentEngine is the engine Component.
	ComponentEngine ComponentType = "engine"
	// ComponentDecoder is the decoder Component.
	ComponentDecoder ComponentType = "decoder"
)

// InstancePhase is the workload-internal lifecycle phase of one
// Instance. Mirrors the CRD OMENativeInstancePhase at the string level.
type InstancePhase string

const (
	// InstancePhaseEmpty is the zero value used when no Phase has
	// been set yet (e.g., freshly-allocated InstanceStatus before the
	// first observation lands).
	InstancePhaseEmpty InstancePhase = ""
	// InstancePhasePending mirrors the Pending CRD phase.
	InstancePhasePending InstancePhase = "Pending"
	// InstancePhaseCreating mirrors the Creating CRD phase.
	InstancePhaseCreating InstancePhase = "Creating"
	// InstancePhaseReady mirrors the Ready CRD phase.
	InstancePhaseReady InstancePhase = "Ready"
	// InstancePhaseUpdating mirrors the Updating CRD phase.
	InstancePhaseUpdating InstancePhase = "Updating"
	// InstancePhaseRestarting mirrors the Restarting CRD phase.
	InstancePhaseRestarting InstancePhase = "Restarting"
	// InstancePhaseMigrating mirrors the Migrating CRD phase.
	InstancePhaseMigrating InstancePhase = "Migrating"
	// InstancePhaseFailed mirrors the Failed CRD phase.
	InstancePhaseFailed InstancePhase = "Failed"
	// InstancePhaseDeleting mirrors the Deleting CRD phase.
	InstancePhaseDeleting InstancePhase = "Deleting"
)

// UpdateStrategyType is the workload-internal rollout-mechanism
// selector. Mirrors the CRD OMENativeUpdateStrategyType at the string
// level so the adapter conversion is a verbatim cast; the distinct Go
// type prevents accidental drift between workload-side dispatch logic
// and the v1beta1 enum (slice 4f-followup audit requirement: the prior
// design compared `string(plan.UpdateStrategy.Type)` against
// const-string literals, which would silently break dispatch if either
// side mutated the spelling).
type UpdateStrategyType string

const (
	// UpdateStrategySurgeThenDrain mirrors the SurgeThenDrain CRD
	// strategy: create a new pod for the target revision, wait
	// for it to serve, then drain + delete the old pod.
	UpdateStrategySurgeThenDrain UpdateStrategyType = "SurgeThenDrain"
	// UpdateStrategyRecreatePod mirrors the RecreatePod CRD strategy:
	// always delete pods and recreate them at a bumped Incarnation.
	UpdateStrategyRecreatePod UpdateStrategyType = "RecreatePod"
	// UpdateStrategyInPlaceIfPossible mirrors the InPlaceIfPossible CRD
	// strategy: try image-patch in place; fall through to recreate on
	// non-image-only diffs.
	UpdateStrategyInPlaceIfPossible UpdateStrategyType = "InPlaceIfPossible"
	// UpdateStrategyInPlaceOnly mirrors the InPlaceOnly CRD strategy:
	// image-patch in place when eligible; error otherwise.
	UpdateStrategyInPlaceOnly UpdateStrategyType = "InPlaceOnly"
)

// InstanceOperationType is the workload-internal kind of an in-flight
// destructive operation against one Instance. Mirrors the CRD
// InstanceOperationType at the string level.
type InstanceOperationType string

const (
	// InstanceOperationCreate mirrors the Create CRD operation.
	InstanceOperationCreate InstanceOperationType = "Create"
	// InstanceOperationUpdate mirrors the Update CRD operation.
	InstanceOperationUpdate InstanceOperationType = "Update"
	// InstanceOperationRestart mirrors the Restart CRD operation.
	InstanceOperationRestart InstanceOperationType = "Restart"
	// InstanceOperationMigrate mirrors the Migrate CRD operation.
	InstanceOperationMigrate InstanceOperationType = "Migrate"
	// InstanceOperationDelete mirrors the Delete CRD operation.
	InstanceOperationDelete InstanceOperationType = "Delete"
)

// InstanceOperation is the workload-internal recovery anchor written
// before any destructive action against an Instance and cleared on
// completion. Mirrors the CRD InstanceOperation field-for-field.
type InstanceOperation struct {
	// ID is the idempotency key for this operation. Retries with the
	// same ID are a no-op.
	ID string

	// Type identifies the operation kind.
	Type InstanceOperationType

	// Step is the fine-grained resume point within the operation
	// (e.g., Drain, DeletePods, WaitZero, Recreate, WaitReady).
	Step string

	// StartedAt is when this operation began.
	StartedAt metav1.Time

	// LastProgressAt is when the operation last advanced. Used for
	// stall detection.
	LastProgressAt metav1.Time

	// Deadline is the hard timeout for the current Step.
	Deadline metav1.Time

	// RetryCount is the per-step escalation counter.
	RetryCount int32

	// TargetRevision is the ControllerRevision the operation is
	// converging toward.
	TargetRevision string

	// Reason is a short cause string for the operation.
	Reason string

	// SurgeIndex is the Instance index allocated for a surge
	// replacement. Set only when Type=Migrate.
	SurgeIndex *int32

	// FromNode is the node the migrating Instance is being moved
	// off. Set only when Type=Migrate.
	FromNode string

	// HintTargetNodes is the caller-supplied preferred-node list for
	// a migration. Set only when Type=Migrate.
	HintTargetNodes []string

	// RequestUUID is the migration request UUID. Set only when
	// Type=Migrate.
	RequestUUID string
}

// InstanceStatus is the workload-internal per-Instance status. Mirrors
// the CRD OMENativeInstanceStatus field-for-field. The conversion layer
// at the adapter boundary round-trips between this type and the CRD
// type; workload code reads / mutates this type only.
type InstanceStatus struct {
	// Index is the stable Instance ordinal. Values may be sparse
	// after surge migration.
	Index int32

	// Incarnation increments each time the Instance is recreated
	// (full recreate update, restart-on-loss). In-place updates do
	// not increment.
	Incarnation int64

	// Phase is the lifecycle phase of the Instance.
	Phase InstancePhase

	// RunningRevision is the ControllerRevision the live pods are
	// running.
	RunningRevision string

	// TargetRevision is the ControllerRevision the live pods are
	// converging toward.
	TargetRevision string

	// PodCount is the total number of pods owned by this Instance.
	PodCount int32

	// ReadyPodCount is the number of pods reporting Ready=True.
	ReadyPodCount int32

	// ServingPodCount is the number of pods that are BOTH
	// ContainersReady AND have serving=True on the controller-managed
	// readiness gate.
	ServingPodCount int32

	// AvailablePodCount is the number of pods ready for at least
	// MinReadySeconds.
	AvailablePodCount int32

	// ScheduledPodCount is the number of pods with Spec.NodeName set.
	ScheduledPodCount int32

	// NodesOccupied is the set of node names hosting pods of this
	// Instance.
	NodesOccupied []string

	// Conditions reports Instance-level conditions.
	Conditions []metav1.Condition

	// Operation is the durable record of an in-flight destructive
	// action against this Instance. Set before the action starts,
	// cleared after it completes.
	Operation *InstanceOperation

	// ActiveOrdinal identifies which of two pod-naming slots
	// (0 or 1) currently holds the canonical pod for this single-pod
	// Instance.
	ActiveOrdinal int32
}

// Key uniquely identifies one logical workload (a per-Component slice
// of an owner). Adapters populate every field; workload code reads
// them opaquely. The ISVC adapter sets OwnerName=isvc.Name and
// SelectorLabels with the ISVC pod-label-key seed; the IR adapter
// sets OwnerName=ir.Name and SelectorLabels with the IR pod-label-key
// seed.
//
// Distinct from the legacy `WorkloadKey` type in source.go — `Key`
// carries the decoupled label-seed shape and will replace `WorkloadKey`
// in slice 2 when source.go flips to consume workload-owned types.
type Key struct {
	// Namespace is the namespace every emitted pod / revision /
	// service is created in.
	Namespace string

	// Component identifies which of router / engine / decoder this
	// workload represents.
	Component ComponentType

	// OwnerName is the bare name of the workload's owner object
	// (ISVC.Name for the ISVC adapter, IR.Name for the IR adapter).
	// workload code uses this to compose pod / Service names — never
	// reaches back into the OwnerObject() for the field. Opaque to
	// workload.
	OwnerName string

	// OwnerLabels is the seed label set the adapter wants stamped on
	// every emitted ControllerRevision / PodGroup / pod's metadata
	// labels block. workload code treats this map as read-only.
	OwnerLabels map[string]string

	// SelectorLabels is the seed label set the adapter wants used in
	// every pod LIST selector and per-revision Service selector.
	// workload code treats this map as read-only.
	SelectorLabels map[string]string
}
