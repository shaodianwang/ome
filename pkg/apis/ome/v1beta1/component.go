package v1beta1

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ComponentExtensionSpec defines the deployment configuration for a given InferenceService component
type ComponentExtensionSpec struct {
	// Minimum number of replicas, defaults to 1 but can be set to 0 to enable scale-to-zero.
	// +optional
	MinReplicas *int `json:"minReplicas,omitempty"`
	// Maximum number of replicas for autoscaling.
	// +optional
	MaxReplicas int `json:"maxReplicas,omitempty"`
	// ScaleTarget specifies the integer target value of the metric type the Autoscaler watches for.
	// +optional
	ScaleTarget *int `json:"scaleTarget,omitempty"`
	// ScaleMetric defines the scaling metric type watched by autoscaler.
	// Possible values are cpu, memory, tps.
	// +optional
	ScaleMetric *ScaleMetric `json:"scaleMetric,omitempty"`
	// TimeoutSeconds specifies the number of seconds to wait before timing out a request to the component.
	// +optional
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`
	// CanaryTrafficPercent defines the traffic split percentage between the candidate revision and the last ready revision
	// +optional
	CanaryTrafficPercent *int64 `json:"canaryTrafficPercent,omitempty"`
	// Labels that will be added to the component pod.
	// More info: http://kubernetes.io/docs/user-guide/labels
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations that will be added to the component pod.
	// More info: http://kubernetes.io/docs/user-guide/annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// MinAvailable specifies how many component pods must still be available after the eviction
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// MaxUnavailable specifies how many component pods can be unavailable
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// The deployment strategy to use to replace existing pods with new ones. Only applicable for raw deployment mode.
	// +optional
	DeploymentStrategy *appsv1.DeploymentStrategy `json:"deploymentStrategy,omitempty"`

	// Lifecycle groups OMENative-specific lifecycle policies for this
	// Component. Applies only when the Component is annotated
	// ome.io/deploymentMode=OMENative; ignored otherwise. The status
	// counterpart is status.components.<component>.omenative.
	// +optional
	Lifecycle *LifecycleSpec `json:"lifecycle,omitempty"`

	// Autoscaler configures the per-Component autoscaler dispatch and the
	// underlying KEDA / HPA configuration. When set, takes priority over the
	// legacy ScaleMetric / ScaleTarget fields and the ome.io/autoscalerClass
	// annotation for this Component. Alpha (OEP-0013). The API may change
	// without notice.
	// +optional
	Autoscaler *ComponentAutoscaler `json:"autoscaler,omitempty"`

	// PodGroupDeployment configures podgroup-operator backed lifecycle
	// for this Component. Applies only when the Component is annotated
	// ome.io/deploymentMode=PodGroupDeploymentRollout.
	// +optional
	PodGroupDeployment *PodGroupDeploymentComponentSpec `json:"podGroupDeployment,omitempty"`
}

// PodGroupDeploymentComponentSpec configures how an OME Component is rendered
// into a podgroup.meta.com PodGroupDeployment child managed by a
// PodGroupDeploymentRollout.
type PodGroupDeploymentComponentSpec struct {
	// Groups is the number of PodGroupDeployment groups. Defaults to
	// minReplicas when unset, then 1.
	// +optional
	Groups *int32 `json:"groups,omitempty"`

	// ReplicasPerGroup is the number of pods in each gang-scheduled group.
	// Defaults to 1.
	// +optional
	ReplicasPerGroup *int32 `json:"replicasPerGroup,omitempty"`

	// TopologyKey controls the pod affinity topology for pods in one group.
	// Defaults to kubernetes.io/hostname.
	// +optional
	TopologyKey string `json:"topologyKey,omitempty"`

	// ServiceTargetOrdinal selects which ordinal backs the stable per-group
	// and top-level Services. Defaults to 0.
	// +optional
	ServiceTargetOrdinal *int32 `json:"serviceTargetOrdinal,omitempty"`

	// ScheduleTimeoutSeconds is propagated to scheduler-plugins PodGroup.
	// +optional
	ScheduleTimeoutSeconds *int32 `json:"scheduleTimeoutSeconds,omitempty"`

	// SchedulerName sets the scheduler used by generated pods. Defaults to
	// the rendered PodSpec schedulerName, then default-scheduler.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`

	// UpdateStrategy controls podgroup-operator per-role pod replacement.
	// Defaults to RecreatePod.
	// +optional
	UpdateStrategy *PodGroupDeploymentUpdateStrategy `json:"updateStrategy,omitempty"`

	// MultiNode enables podgroup-operator multinode argument injection.
	// +optional
	MultiNode *PodGroupDeploymentMultiNodeSpec `json:"multiNode,omitempty"`

	// Rollout controls the paired PodGroupDeploymentRollout strategy. It is
	// app-level state; when both engine and decoder set it, engine wins.
	// +optional
	Rollout *PodGroupDeploymentRolloutStrategy `json:"rollout,omitempty"`
}

// PodGroupDeploymentUpdateStrategy configures per-role replacement behavior.
type PodGroupDeploymentUpdateStrategy struct {
	// Type is the podgroup-operator update strategy.
	// +kubebuilder:validation:Enum=RecreatePod;InPlaceIfPossible;InPlaceOnly
	// +optional
	Type string `json:"type,omitempty"`

	// RollingUpdate controls per-role rollout budget.
	// +optional
	RollingUpdate *PodGroupDeploymentRollingUpdateStrategy `json:"rollingUpdate,omitempty"`
}

// PodGroupDeploymentRollingUpdateStrategy mirrors podgroup-operator's
// rollingUpdate fields.
type PodGroupDeploymentRollingUpdateStrategy struct {
	// +optional
	MaxSurge *int32 `json:"maxSurge,omitempty"`
	// +optional
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`
	// +optional
	MaxUnavailablePercent *int32 `json:"maxUnavailablePercent,omitempty"`
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty"`
}

// PodGroupDeploymentMultiNodeSpec configures multinode groups.
type PodGroupDeploymentMultiNodeSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	ContainerName string `json:"containerName,omitempty"`
	// +optional
	TensorParallelSize *int32 `json:"tensorParallelSize,omitempty"`
	// +optional
	DistInitPort *int32 `json:"distInitPort,omitempty"`
}

// PodGroupDeploymentRolloutStrategy configures paired rollout waves.
type PodGroupDeploymentRolloutStrategy struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	MaxUnavailablePercent *int32 `json:"maxUnavailablePercent,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSkew *int32 `json:"maxSkew,omitempty"`
}

// ScaleMetric enum
// +kubebuilder:validation:Enum=cpu;memory;tps
type ScaleMetric string

const (
	MetricCPU    ScaleMetric = "cpu"
	MetricMemory ScaleMetric = "memory"
	MetricTPS    ScaleMetric = "tps"
)
