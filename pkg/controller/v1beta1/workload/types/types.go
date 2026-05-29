// Package types holds the workload package's value types — the
// per-Instance / per-Component / per-Plan data structures every layer
// of the workload pipeline reads and writes. Lives separately from
// the parent `workload` package so `workload/ops` and
// `workload.Reconcile` can both depend on these types without closing
// an import cycle.
//
// The workload package owns its own status / operation / component /
// phase / key types and does NOT depend on the OME CRD API package.
// Every type here mirrors the corresponding OMENative-on-ISVC status
// type field-for-field; adapter boundaries (omenative/internalsource,
// inferencereplica) perform the conversion.
//
// External callers reference these types as `workload.X` — the parent
// `workload` package re-exports them as Go aliases.
package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ComponentType identifies router | engine | decoder. String-mirrors
// the CRD ComponentType so the adapter conversion is a verbatim cast;
// the distinct Go type catches accidental cross-package coupling.
type ComponentType string

const (
	ComponentRouter  ComponentType = "router"
	ComponentEngine  ComponentType = "engine"
	ComponentDecoder ComponentType = "decoder"
)

// InstancePhase is the per-Instance lifecycle phase. String-mirrors
// the CRD OMENativeInstancePhase.
type InstancePhase string

const (
	// InstancePhaseEmpty is the zero value used before the first
	// observation lands on a freshly-allocated InstanceStatus.
	InstancePhaseEmpty      InstancePhase = ""
	InstancePhasePending    InstancePhase = "Pending"
	InstancePhaseCreating   InstancePhase = "Creating"
	InstancePhaseReady      InstancePhase = "Ready"
	InstancePhaseUpdating   InstancePhase = "Updating"
	InstancePhaseRestarting InstancePhase = "Restarting"
	InstancePhaseMigrating  InstancePhase = "Migrating"
	InstancePhaseFailed     InstancePhase = "Failed"
	InstancePhaseDeleting   InstancePhase = "Deleting"
)

// UpdateStrategyType is the rollout-mechanism selector. String-mirrors
// the CRD UpdateStrategyType; the distinct Go type catches accidental
// drift between workload-side dispatch logic and the v1beta1 enum.
type UpdateStrategyType string

const (
	// UpdateStrategySurgeThenDrain creates a new pod for the target
	// revision, waits for it to serve, then drains + deletes the old.
	UpdateStrategySurgeThenDrain UpdateStrategyType = "SurgeThenDrain"
	// UpdateStrategyRecreatePod always deletes pods and recreates them
	// at a bumped Incarnation.
	UpdateStrategyRecreatePod UpdateStrategyType = "RecreatePod"
	// UpdateStrategyInPlaceIfPossible tries image-patch in place; falls
	// through to recreate on non-image-only diffs.
	UpdateStrategyInPlaceIfPossible UpdateStrategyType = "InPlaceIfPossible"
	// UpdateStrategyInPlaceOnly image-patches in place when eligible;
	// errors otherwise.
	UpdateStrategyInPlaceOnly UpdateStrategyType = "InPlaceOnly"
)

// RestartPolicy is the Instance-group restart policy. String-mirrors
// the CRD InstanceRestartPolicy.
type RestartPolicy string

const (
	// RestartPolicyNone restarts only the failed pod; OMENative leaves
	// the Instance Phase=Ready and lets the kubelet recover in place.
	RestartPolicyNone RestartPolicy = "None"
	// RestartPolicyRecreateInstance drains, deletes, and recreates
	// every pod in the Instance at a bumped Incarnation.
	RestartPolicyRecreateInstance RestartPolicy = "RecreateInstanceOnPodRestart"
	// RestartPolicyRecreateISVC triggers a fresh top-level rollout.
	// Driven by the parent ISVC controller, not the workload dispatcher.
	RestartPolicyRecreateISVC RestartPolicy = "RecreateISVCOnPodRestart"
)

// MigrationMode is the migration disposition. String-mirrors the CRD
// MigrationPolicyMode.
type MigrationMode string

const (
	// MigrationModeAuto / MigrationModeSurge — operator may trigger
	// migration via the per-UUID annotation; controller drives the
	// surge cycle. surge is a spelling alias for auto.
	MigrationModeAuto  MigrationMode = "auto"
	MigrationModeSurge MigrationMode = "surge"
	// MigrationModeNever — controller refuses any migration;
	// annotations are ignored.
	MigrationModeNever MigrationMode = "never"
)

// InstanceOperationType identifies the kind of an in-flight
// destructive operation against one Instance. String-mirrors the CRD
// InstanceOperationType.
type InstanceOperationType string

const (
	InstanceOperationCreate  InstanceOperationType = "Create"
	InstanceOperationUpdate  InstanceOperationType = "Update"
	InstanceOperationRestart InstanceOperationType = "Restart"
	InstanceOperationMigrate InstanceOperationType = "Migrate"
	InstanceOperationDelete  InstanceOperationType = "Delete"
)

// InstanceOperation is the durable recovery anchor written before any
// destructive action against an Instance and cleared on completion.
// Mirrors the CRD InstanceOperation field-for-field.
type InstanceOperation struct {
	// ID is the idempotency key. Retries with the same ID are a no-op.
	ID string

	Type InstanceOperationType

	// Step is the fine-grained resume point within the operation
	// (e.g., Drain, DeletePods, WaitZero, Recreate, WaitReady).
	Step string

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

	Reason string

	// Migrate-only fields:
	SurgeIndex      *int32
	FromNode        string
	HintTargetNodes []string
	RequestUUID     string
}

// InstanceStatus is the per-Instance status. Mirrors the CRD
// OMENativeInstanceStatus field-for-field; adapters round-trip between
// this type and the CRD type.
type InstanceStatus struct {
	// Index is the stable Instance ordinal. Values may be sparse
	// after surge migration.
	Index int32

	// Incarnation increments each time the Instance is recreated
	// (full recreate update, restart-on-loss). In-place updates do
	// not increment.
	Incarnation int64

	Phase InstancePhase

	// RunningRevision / TargetRevision are the ControllerRevision the
	// live pods are running and converging toward, respectively.
	RunningRevision string
	TargetRevision  string

	PodCount int32
	// ReadyPodCount counts pods reporting Ready=True.
	ReadyPodCount int32
	// ServingPodCount counts pods that are BOTH ContainersReady AND
	// have serving=True on the controller-managed readiness gate.
	ServingPodCount int32
	// AvailablePodCount counts pods ready for at least MinReadySeconds.
	AvailablePodCount int32
	// ScheduledPodCount counts pods with Spec.NodeName set.
	ScheduledPodCount int32

	// NodesOccupied is the set of node names hosting pods of this
	// Instance.
	NodesOccupied []string

	Conditions []metav1.Condition

	// Operation is the durable record of an in-flight destructive
	// action against this Instance. Set before the action starts,
	// cleared after it completes.
	Operation *InstanceOperation

	// ActiveOrdinal identifies which of two pod-naming slots (0 or 1)
	// currently holds the canonical pod for this single-pod Instance.
	ActiveOrdinal int32
}

// Key uniquely identifies one logical workload (a per-Component slice
// of an owner). Adapters populate every field; workload code reads
// them opaquely.
type Key struct {
	Namespace string
	Component ComponentType

	// OwnerName is the bare name of the workload's owner object
	// (ISVC.Name or IR.Name). workload composes pod / Service names
	// from this — never reaches back into OwnerObject for the field.
	OwnerName string

	// OwnerLabels is the seed label set stamped on every emitted
	// ControllerRevision / PodGroup / pod's metadata labels block.
	// Read-only from workload code.
	OwnerLabels map[string]string

	// SelectorLabels is the seed label set used in every pod LIST
	// selector and per-revision Service selector. Read-only from
	// workload code.
	SelectorLabels map[string]string
}

// WorkloadName is the composed identity used for ControllerRevision
// naming and as the prefix on emitted pod / PodGroup / PDB names.
// Always "<OwnerName>-<Component>" — the pod-naming convention every
// adapter shares so existing selectors keep matching.
func (k Key) WorkloadName() string {
	return k.OwnerName + "-" + string(k.Component)
}
