package v1beta1

import "k8s.io/apimachinery/pkg/api/resource"

// PlacementMode is the cardinality of a multi-cluster placement: how many
// workload clusters end up serving the InferenceService.
// +kubebuilder:validation:Enum=Single;All;Split;SplitByCapacity
type PlacementMode string

const (
	// PlacementModeSingle places the InferenceService on exactly one workload
	// cluster (the winner of the fan-out race) and sweeps the rest.
	PlacementModeSingle PlacementMode = "Single"

	// PlacementModeAll places the InferenceService on every candidate cluster
	// that admits it; none are swept and each autoscales locally against its own
	// Kueue quota. The endpoint fans out across all serving homes. For
	// redundancy / serve-everywhere.
	PlacementModeAll PlacementMode = "All"

	// PlacementModeSplit assigns exact shares of the requested floor across
	// matching clusters, using equal or explicit affinity weights.
	PlacementModeSplit PlacementMode = "Split"

	// PlacementModeSplitByCapacity apportions the floor using verified nominal
	// whole-replica hardware capacity, independently of quota and utilization.
	PlacementModeSplitByCapacity PlacementMode = "SplitByCapacity"
)

// PlacementSpec declares explicit multi-cluster intent. Affinity omission is
// unconstrained; invalid or obsolete restrictions cannot widen the matched set.
type PlacementSpec struct {
	// Mode is required; an empty placement block does not select a policy.
	Mode PlacementMode `json:"mode"`

	// ClusterAffinity ORs terms whose requirements are ANDed. Omission matches
	// every registration; an explicit empty or null list is invalid.
	// +optional
	// +nullable
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	ClusterAffinity []ClusterAffinityTerm `json:"clusterAffinity,omitempty"`

	// MaxSurge is the whole-replica allowance shared by placement transitions
	// and local rollout surge. Omission blocks disruptive movement between homes.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxSurge *int32 `json:"maxSurge,omitempty"`

	// LegacyFields retains explicit zero-valued obsolete fields during JSON round trips.
	// It is serialization bookkeeping and is not a wire field.
	LegacyFields PlacementLegacyFields `json:"-"`

	// Requirements is retained only to reject obsolete selector intent.
	// Deprecated: use clusterAffinity.matchExpressions or matchFields.
	// +optional
	// +nullable
	Requirements string `json:"requirements,omitempty"`

	// ClusterSelector is retained only to reject obsolete selector intent.
	// Deprecated: use clusterAffinity.
	// +optional
	// +nullable
	ClusterSelector string `json:"clusterSelector,omitempty"`

	// Split provides the requested floor and optional per-home ceiling for
	// Split and SplitByCapacity. Other modes reject this block.
	// +optional
	Split *SplitSpec `json:"split,omitempty"`

	// CapacityFactors is retained only to reject obsolete routing intent.
	// Deprecated: use spec.routing.capacityFactors.
	// +optional
	// +nullable
	CapacityFactors map[string]resource.Quantity `json:"capacityFactors,omitempty"`
}

// SplitSpec declares the fleet floor and optional local ceiling. The floor
// falls back only to an explicitly declared positive engine.minReplicas.
type SplitSpec struct {
	// LegacyFields retains explicit zero-valued obsolete fields during JSON round trips.
	// It is serialization bookkeeping and is not a wire field.
	LegacyFields SplitLegacyFields `json:"-"`

	// Replicas is the fleet-wide desired replica count to distribute across homes.
	// Unset falls back to the engine component's minReplicas (the guaranteed
	// floor) — the count OME actually guarantees running and thus the one worth
	// spreading. maxReplicas is deliberately NOT used (it is an autoscaling
	// ceiling that stays a per-home local concern).
	// +optional
	// +kubebuilder:validation:Minimum=1
	Replicas *int32 `json:"replicas,omitempty"`

	// Spread is retained only to reject obsolete packing intent.
	// Deprecated: mode and affinity weights determine allocation.
	// +optional
	// +nullable
	Spread bool `json:"spread,omitempty"`

	// MaxReplicasPerCluster is an optional local ceiling. A target exceeding a
	// positive ceiling holds the whole new plan; zero leaves the ceiling uncapped.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxReplicasPerCluster int32 `json:"maxReplicasPerCluster,omitempty"`

	// MinReplicasPerCluster is retained only to reject obsolete anti-sliver intent.
	// Deprecated: exact shares cannot discard a small admission.
	// +optional
	// +nullable
	MinReplicasPerCluster int32 `json:"minReplicasPerCluster,omitempty"`
}
