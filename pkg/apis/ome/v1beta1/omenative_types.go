package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OMENativeComponentStatus reports OMENative-managed state for one Component.
// Nil when the Component does not use OMENative.
type OMENativeComponentStatus struct {
	// ObservedGeneration is the InferenceService.metadata.generation reflected by this block.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CurrentRevision names the ControllerRevision currently serving traffic.
	// +optional
	CurrentRevision string `json:"currentRevision,omitempty"`

	// UpdateRevision names the ControllerRevision being rolled out.
	// +optional
	UpdateRevision string `json:"updateRevision,omitempty"`

	// CollisionCount is the salt folded into ControllerRevision hash input
	// after a name collision is observed, so retries produce a different
	// revision name. Same pattern as StatefulSet.
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty"`

	// LabelSelector identifies pods of this Component for HPA-style consumers.
	// +optional
	LabelSelector string `json:"labelSelector,omitempty"`

	// Replicas is the total Instance count (any phase).
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of Instances whose every pod is Ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// AvailableReplicas is the number of Instances that have been Ready
	// continuously for at least MinReadySeconds.
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// UpdatedReplicas is the number of Instances on UpdateRevision.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// UpdatedReadyReplicas is the number of Instances on UpdateRevision and
	// fully Ready. UpdatedReadyReplicas / Replicas drives maxUnavailable
	// rollout pacing.
	// +optional
	UpdatedReadyReplicas int32 `json:"updatedReadyReplicas,omitempty"`

	// Conditions reports Component-level conditions. Defined types include
	// Available, Progressing, FailedScale, FailedUpdate,
	// GangSchedulingUnavailable, OperationStuck.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// InstanceStatuses reports per-Instance state.
	// +optional
	// +listType=map
	// +listMapKey=index
	InstanceStatuses []OMENativeInstanceStatus `json:"instanceStatuses,omitempty"`
}

// OMENativeInstancePhase is the lifecycle phase of an OMENative Instance.
// +kubebuilder:validation:Enum=Pending;Creating;Ready;Updating;Restarting;Migrating;Failed;Deleting
type OMENativeInstancePhase string

const (
	OMENativeInstancePending    OMENativeInstancePhase = "Pending"
	OMENativeInstanceCreating   OMENativeInstancePhase = "Creating"
	OMENativeInstanceReady      OMENativeInstancePhase = "Ready"
	OMENativeInstanceUpdating   OMENativeInstancePhase = "Updating"
	OMENativeInstanceRestarting OMENativeInstancePhase = "Restarting"
	OMENativeInstanceMigrating  OMENativeInstancePhase = "Migrating"
	OMENativeInstanceFailed     OMENativeInstancePhase = "Failed"
	OMENativeInstanceDeleting   OMENativeInstancePhase = "Deleting"
)

// OMENativeInstanceStatus reports state for one OMENative Instance.
type OMENativeInstanceStatus struct {
	// Index is the stable Instance ordinal. Values may be sparse after surge migration.
	Index int32 `json:"index"`

	// Incarnation increments each time the Instance is recreated (full
	// recreate update, restart-on-loss). In-place updates do not increment.
	// Stamped on pods via the `ome.io/instance-incarnation` label.
	// +optional
	Incarnation int64 `json:"incarnation,omitempty"`

	// Phase is the lifecycle phase of the Instance.
	Phase OMENativeInstancePhase `json:"phase"`

	// RunningRevision is the ControllerRevision the live pods are running.
	// +optional
	RunningRevision string `json:"runningRevision,omitempty"`

	// TargetRevision is the ControllerRevision the live pods are converging toward.
	// +optional
	TargetRevision string `json:"targetRevision,omitempty"`

	// PodCount is the total number of pods owned by this Instance.
	// +optional
	PodCount int32 `json:"podCount,omitempty"`

	// ReadyPodCount is the number of pods reporting Ready=True.
	// +optional
	ReadyPodCount int32 `json:"readyPodCount,omitempty"`

	// AvailablePodCount is the number of pods ready for at least MinReadySeconds.
	// +optional
	AvailablePodCount int32 `json:"availablePodCount,omitempty"`

	// ScheduledPodCount is the number of pods with Spec.NodeName set.
	// +optional
	ScheduledPodCount int32 `json:"scheduledPodCount,omitempty"`

	// NodesOccupied is the set of node names hosting pods of this Instance.
	// +optional
	// +listType=set
	NodesOccupied []string `json:"nodesOccupied,omitempty"`

	// Conditions reports Instance-level conditions. Defined types include
	// AllPodsScheduled, AllPodsReady, AllPodsAvailable, Drained, Failed.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Operation is the durable record of an in-flight destructive action
	// against this Instance. Set before the action starts, cleared after
	// it completes. Lets a crashed controller resume work without diffing
	// pod state.
	// +optional
	Operation *InstanceOperation `json:"operation,omitempty"`
}

// InstanceOperation is the recovery anchor written before any destructive
// action against an Instance and cleared on completion.
type InstanceOperation struct {
	// ID is the idempotency key for this operation. Retries with the same
	// ID are a no-op.
	ID string `json:"id"`

	// Type identifies the operation kind: Create, Update, Restart, Migrate, Delete.
	// +kubebuilder:validation:Enum=Create;Update;Restart;Migrate;Delete
	Type InstanceOperationType `json:"type"`

	// Step is the fine-grained resume point within the operation
	// (e.g., Drain, DeletePods, WaitZero, Recreate, WaitReady).
	Step string `json:"step"`

	// StartedAt is when this operation began.
	StartedAt metav1.Time `json:"startedAt"`

	// LastProgressAt is when the operation last advanced. Used for stall detection.
	LastProgressAt metav1.Time `json:"lastProgressAt"`

	// Deadline is the hard timeout for the current Step.
	Deadline metav1.Time `json:"deadline"`

	// RetryCount is the per-step escalation counter.
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`

	// TargetRevision is the ControllerRevision the operation is converging toward.
	// +optional
	TargetRevision string `json:"targetRevision,omitempty"`

	// Reason is a short cause string for the operation.
	// +optional
	Reason string `json:"reason,omitempty"`

	// SurgeIndex is the Instance index allocated for a surge replacement.
	// Set only when Type=Migrate.
	// +optional
	SurgeIndex *int32 `json:"surgeIndex,omitempty"`

	// FromNode is the node the migrating Instance is being moved off.
	// Set only when Type=Migrate.
	// +optional
	FromNode string `json:"fromNode,omitempty"`

	// HintTargetNodes is the caller-supplied preferred-node list for a migration.
	// Set only when Type=Migrate.
	// +optional
	// +listType=atomic
	HintTargetNodes []string `json:"hintTargetNodes,omitempty"`

	// RequestUUID is the migration request UUID. Set only when Type=Migrate.
	// +optional
	RequestUUID string `json:"requestUUID,omitempty"`
}

// InstanceOperationType is the kind of destructive action recorded in
// InstanceOperation.
type InstanceOperationType string

const (
	InstanceOperationCreate  InstanceOperationType = "Create"
	InstanceOperationUpdate  InstanceOperationType = "Update"
	InstanceOperationRestart InstanceOperationType = "Restart"
	InstanceOperationMigrate InstanceOperationType = "Migrate"
	InstanceOperationDelete  InstanceOperationType = "Delete"
)

// MigrationMode is the migration mechanism selected.
// +kubebuilder:validation:Enum=surge
type MigrationMode string

const (
	// MigrationModeSurge creates a replacement Instance before draining the original.
	MigrationModeSurge MigrationMode = "surge"
)

// MigrationPhase is the lifecycle phase of a migration request.
// +kubebuilder:validation:Enum=Pending;InProgress;Completed;Failed
type MigrationPhase string

const (
	MigrationPhasePending    MigrationPhase = "Pending"
	MigrationPhaseInProgress MigrationPhase = "InProgress"
	MigrationPhaseCompleted  MigrationPhase = "Completed"
	MigrationPhaseFailed     MigrationPhase = "Failed"
)

// MigrationEvent is one observation logged during a migration.
type MigrationEvent struct {
	// At is the time the event was recorded.
	At metav1.Time `json:"at"`
	// Message describes the observation.
	Message string `json:"message"`
}

// MigrationHistoryEntry is one entry of the rolling MigrationHistory window
// recorded on InferenceServiceStatus.
type MigrationHistoryEntry struct {
	// ID is the request UUID supplied by the caller.
	ID string `json:"id"`

	// Component identifies which Component was migrated.
	Component ComponentType `json:"component"`

	// Instance is the original Instance index that was migrated away from.
	Instance int32 `json:"instance"`

	// ReplacementInstance is the Instance index allocated for the surge replacement.
	// Nil when the migration was rejected before allocation. Distinct from
	// index 0, which is a valid replacement index after sparse migration.
	// +optional
	ReplacementInstance *int32 `json:"replacementInstance,omitempty"`

	// Mode is the migration mechanism. Always `surge` in v1.
	Mode MigrationMode `json:"mode"`

	// Phase is the current lifecycle phase.
	Phase MigrationPhase `json:"phase"`

	// RequestedAt is the time the request annotation was first observed.
	RequestedAt metav1.Time `json:"requestedAt"`

	// StartedAt is the time the controller began executing the migration.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is the time the migration reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Reason is the caller-supplied reason (e.g., "fragmentation").
	// +optional
	Reason string `json:"reason,omitempty"`

	// RequestedBy identifies the caller that wrote the request annotation.
	// +optional
	RequestedBy string `json:"requestedBy,omitempty"`

	// Events records observations made during the migration.
	// +optional
	// +listType=atomic
	Events []MigrationEvent `json:"events,omitempty"`

	// OutcomeReason is a short machine-readable reason describing why the
	// migration ended in its terminal phase (e.g., "InstanceReadyTimeout").
	// +optional
	OutcomeReason string `json:"outcomeReason,omitempty"`
}
