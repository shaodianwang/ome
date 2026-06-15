package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	// InferenceDeploymentReady is true when all target clusters have applied
	// and reported ready OME InferenceServices.
	InferenceDeploymentReady = "Ready"
)

// InferenceDeploymentSpec describes one logical OME InferenceService deployed
// from the management cluster to one or more workload clusters.
type InferenceDeploymentSpec struct {
	// Namespace is the namespace where OME InferenceServices are created in
	// workload clusters. When empty, the InferenceDeployment namespace is used.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// PriorityClassName is applied to generated OME component pods.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// Checkpoint is the model artifact location exposed to generated serving
	// containers as CC_MODEL_PATH.
	Checkpoint string `json:"checkpoint"`

	// Clusters are the workload clusters that should receive the OME
	// InferenceService.
	// +listType=map
	// +listMapKey=name
	Clusters []WorkloadCluster `json:"clusters"`

	// SGLang configures generated prefill and decode component overrides.
	SGLang SGLangSpec `json:"sglang"`

	// Router configures the generated router component. When omitted, no router
	// override is rendered.
	// +optional
	Router *RouterSpec `json:"router,omitempty"`
}

// WorkloadCluster identifies one workload cluster and desired serving allocation.
type WorkloadCluster struct {
	// Name is a stable target cluster identifier.
	Name string `json:"name"`

	// Prefill configures the per-cluster engine component replica envelope.
	Prefill ComponentScale `json:"prefill"`

	// Decode configures the per-cluster decoder component replica envelope.
	// +optional
	Decode *ComponentScale `json:"decode,omitempty"`

	// Router configures the per-cluster router component replica envelope.
	// +optional
	Router *ComponentScale `json:"router,omitempty"`
}

// ComponentScale configures a component's autoscaling replica envelope.
type ComponentScale struct {
	// +kubebuilder:validation:Minimum=0
	MinReplicas int32 `json:"minReplicas"`

	// +kubebuilder:validation:Minimum=0
	MaxReplicas int32 `json:"maxReplicas"`
}

// SGLangSpec contains serving container overrides shared by prefill and decode.
type SGLangSpec struct {
	// Image overrides the default runtime's SGLang image.
	Image string `json:"image"`

	// GPUs is the GPU resource count requested and limited per serving pod.
	// +kubebuilder:validation:Minimum=0
	// +optional
	GPUs int32 `json:"gpus,omitempty"`

	// CPU is the CPU request and limit applied to serving pods.
	// +optional
	CPU string `json:"cpu,omitempty"`

	// Memory is the memory request and limit applied to serving pods.
	// +optional
	Memory string `json:"memory,omitempty"`

	// Env adds or overrides SGLang environment variables.
	// +optional
	Env map[string]string `json:"env,omitempty"`

	// Args adds SGLang CLI flags. Keys may be written with or without the
	// leading "--". Empty or "true" values render as flag-only arguments.
	// +optional
	Args map[string]string `json:"args,omitempty"`

	// Components adds prefill/decode-specific SGLang overrides. Component
	// values override shared Env and Args.
	// +optional
	Components *SGLangComponentOverrides `json:"components,omitempty"`
}

// SGLangComponentOverrides contains serving container overrides scoped to a
// single SGLang component.
type SGLangComponentOverrides struct {
	// +optional
	Prefill *SGLangComponentOverride `json:"prefill,omitempty"`

	// +optional
	Decode *SGLangComponentOverride `json:"decode,omitempty"`
}

// SGLangComponentOverride adds component-specific SGLang settings.
type SGLangComponentOverride struct {
	// Env adds or overrides SGLang environment variables for one component.
	// +optional
	Env map[string]string `json:"env,omitempty"`

	// Args adds or overrides SGLang CLI flags for one component.
	// +optional
	Args map[string]string `json:"args,omitempty"`
}

// RouterSpec contains router component overrides.
type RouterSpec struct {
	// Image overrides the default runtime's router image.
	Image string `json:"image"`

	// CPU is the CPU request and limit applied to router pods.
	// +optional
	CPU string `json:"cpu,omitempty"`

	// Memory is the memory request and limit applied to router pods.
	// +optional
	Memory string `json:"memory,omitempty"`

	// Env adds or overrides router environment variables.
	// +optional
	Env map[string]string `json:"env,omitempty"`

	// Args adds router CLI flags. Keys may be written with or without the
	// leading "--". Empty or "true" values render as flag-only arguments.
	// +optional
	Args map[string]string `json:"args,omitempty"`

	// Config is passed through to the OME RouterSpec config map.
	// +optional
	Config map[string]string `json:"config,omitempty"`
}

// InferenceDeploymentStatus is the control-plane status surface for a
// multi-cluster OME deployment.
type InferenceDeploymentStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=name
	Clusters []ClusterDeploymentStatus `json:"clusters,omitempty"`
}

// ClusterDeploymentStatus captures one target cluster's latest reconcile
// result.
type ClusterDeploymentStatus struct {
	Name string `json:"name"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	LastAttemptTime *metav1.Time `json:"lastAttemptTime,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +optional
	AppliedResource *AppliedResource `json:"appliedResource,omitempty"`

	// +optional
	URL string `json:"url,omitempty"`

	// +optional
	InferenceServiceGeneration int64 `json:"inferenceServiceGeneration,omitempty"`

	// +optional
	InferenceServiceObservedGeneration int64 `json:"inferenceServiceObservedGeneration,omitempty"`

	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// +optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// +optional
	ServingReplicas int32 `json:"servingReplicas,omitempty"`
}

// AppliedResource identifies one OME resource applied to a workload cluster.
type AppliedResource struct {
	Kind string `json:"kind"`

	// +optional
	Namespace string `json:"namespace,omitempty"`

	Name string `json:"name"`

	// +optional
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

// InferenceDeployment is the control-plane CRD for deploying one OME
// InferenceService to multiple workload clusters.
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Clusters",type="string",JSONPath=".status.clusters[*].name",priority=1
// +kubebuilder:printcolumn:name="EngineReady",type="string",JSONPath=".status.clusters[*].conditions[?(@.type=='EngineReady')].status",priority=1
// +kubebuilder:printcolumn:name="RouterReady",type="string",JSONPath=".status.clusters[*].conditions[?(@.type=='RouterReady')].status",priority=1
// +kubebuilder:printcolumn:name="ReadyReplicas",type="string",JSONPath=".status.clusters[*].readyReplicas",priority=1
// +kubebuilder:printcolumn:name="ServingReplicas",type="string",JSONPath=".status.clusters[*].servingReplicas",priority=1
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:path=inferencedeployments,shortName=id
type InferenceDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InferenceDeploymentSpec   `json:"spec,omitempty"`
	Status InferenceDeploymentStatus `json:"status,omitempty"`
}

// InferenceDeploymentList contains a list of InferenceDeployment objects.
// +kubebuilder:object:root=true
type InferenceDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InferenceDeployment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&InferenceDeployment{}, &InferenceDeploymentList{})
}
