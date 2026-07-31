package v1beta1

// PlacementMode is the cardinality of a multi-cluster placement: how many
// workload clusters end up serving the InferenceService.
// +kubebuilder:validation:Enum=Single;All;Split
type PlacementMode string

const (
	// PlacementModeSingle places the InferenceService on exactly one workload
	// cluster (the winner of the fan-out race) and sweeps the rest. This is the
	// default and the only mode wired end to end today (OEP-0018).
	PlacementModeSingle PlacementMode = "Single"

	// PlacementModeAll places the InferenceService on every candidate cluster
	// that admits it; none are swept and each autoscales locally against its own
	// Kueue quota. The endpoint fans out across all serving homes. For
	// redundancy / serve-everywhere. (OEP-0018.3.)
	PlacementModeAll PlacementMode = "All"

	// PlacementModeSplit distributes the InferenceService's desired replicas
	// fractionally across candidate clusters: each admits as many replicas as
	// fit, every cluster that admits >=1 is kept, and the endpoint is weighted by
	// each home's ready replicas. For scaling past a single cluster's capacity.
	// (OEP-0018.1.)
	PlacementModeSplit PlacementMode = "Split"
)

// PlacementSpec declares how the control plane selects the workload clusters an
// InferenceService is placed onto, and how many of them serve it. It subsumes the
// legacy ome.io/accelerator-requirements and ome.io/cluster-selector annotations
// in a typed, schema-validated form; when this field is nil the control plane
// still honors those annotations for backward compatibility.
type PlacementSpec struct {
	// Mode is the placement cardinality: Single (one cluster), All (every
	// candidate), or Split (replicas distributed across clusters). Defaults to
	// Single. All and Split are rejected by admission on a control plane whose
	// build does not yet implement them.
	// +kubebuilder:default=Single
	// +optional
	Mode PlacementMode `json:"mode,omitempty"`

	// Requirements is the intrinsic capability selector a candidate workload
	// cluster MUST satisfy, expressed as a Kubernetes label-selector string
	// matched against WorkloadCluster labels (e.g. "accelerator in (gb300,
	// tpu7x)"). It is the structured equivalent of the
	// ome.io/accelerator-requirements annotation. Empty means no intrinsic
	// requirement.
	// +optional
	Requirements string `json:"requirements,omitempty"`

	// ClusterSelector is an optional operator-imposed routing overlay
	// (label-selector string) AND-ed onto Requirements to further narrow
	// candidates (e.g. "provider=cw"). Structured equivalent of the
	// ome.io/cluster-selector annotation.
	// +optional
	ClusterSelector string `json:"clusterSelector,omitempty"`
}
