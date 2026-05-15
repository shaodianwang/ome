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

	// LabelSelector identifies pods of this Component for HPA-style consumers.
	// +optional
	LabelSelector string `json:"labelSelector,omitempty"`

	// Replicas is the total Instance count.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of Instances reporting Phase=Ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// AvailableReplicas is the number of Instances ready for at least MinReadySeconds.
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

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

	// Incarnation increments each time the Instance is recreated by the restart machinery.
	// +optional
	Incarnation int32 `json:"incarnation,omitempty"`

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
}

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
	// Zero when the migration was rejected before allocation.
	// +optional
	ReplacementInstance int32 `json:"replacementInstance,omitempty"`

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
