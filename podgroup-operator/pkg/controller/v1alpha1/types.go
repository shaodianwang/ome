package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PodGroupDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PodGroupDeploymentSpec   `json:"spec,omitempty"`
	Status            PodGroupDeploymentStatus `json:"status,omitempty"`
}

type PodGroupDeploymentSpec struct {
	Groups                 int32                  `json:"groups,omitempty"`
	ReplicasPerGroup       int32                  `json:"replicasPerGroup,omitempty"`
	UpdateStrategy         UpdateStrategy         `json:"updateStrategy,omitempty"`
	MultiNode              MultiNodeSpec          `json:"multiNode,omitempty"`
	TopologyKey            string                 `json:"topologyKey,omitempty"`
	ServiceTargetOrdinal   int32                  `json:"serviceTargetOrdinal,omitempty"`
	ScheduleTimeoutSeconds int32                  `json:"scheduleTimeoutSeconds,omitempty"`
	SchedulerName          string                 `json:"schedulerName,omitempty"`
	Template               corev1.PodTemplateSpec `json:"template,omitempty"`
}

type UpdateStrategy struct {
	Type          UpdateStrategyType    `json:"type,omitempty"`
	RollingUpdate RollingUpdateStrategy `json:"rollingUpdate,omitempty"`
}

type UpdateStrategyType string

type RollingUpdateStrategy struct {
	MaxSurge                int32 `json:"maxSurge,omitempty"`
	MaxUnavailable          int32 `json:"maxUnavailable,omitempty"`
	MaxUnavailablePercent   int32 `json:"maxUnavailablePercent,omitempty"`
	ProgressDeadlineSeconds int32 `json:"progressDeadlineSeconds,omitempty"`
}

type MultiNodeSpec struct {
	Enabled            bool   `json:"enabled,omitempty"`
	ContainerName      string `json:"containerName,omitempty"`
	TensorParallelSize int32  `json:"tensorParallelSize,omitempty"`
	DistInitPort       int32  `json:"distInitPort,omitempty"`
}

type PodGroupDeploymentStatus struct {
	ObservedGeneration int64         `json:"observedGeneration,omitempty"`
	CurrentRevision    string        `json:"currentRevision,omitempty"`
	Groups             []GroupStatus `json:"groups,omitempty"`
	Total              TotalStatus   `json:"total,omitempty"`
	RolloutGate        RolloutGate   `json:"rolloutGate,omitempty"`
}

type GroupStatus struct {
	Index             int32  `json:"index"`
	SchedulerPodGroup string `json:"schedulerPodGroup,omitempty"`
	Service           string `json:"service,omitempty"`
	GroupInstance     string `json:"groupInstance,omitempty"`
	Revision          string `json:"revision,omitempty"`
	TopologyValue     string `json:"topologyValue,omitempty"`
	Phase             string `json:"phase,omitempty"`
	Running           int32  `json:"running,omitempty"`
	Ready             int32  `json:"ready,omitempty"`
	Pending           int32  `json:"pending,omitempty"`
	Failed            int32  `json:"failed,omitempty"`
	TargetPod         string `json:"targetPod,omitempty"`
}

type TotalStatus struct {
	DesiredGroups int32 `json:"desiredGroups,omitempty"`
	DesiredPods   int32 `json:"desiredPods,omitempty"`
	RunningPods   int32 `json:"runningPods,omitempty"`
	ReadyPods     int32 `json:"readyPods,omitempty"`
	PendingPods   int32 `json:"pendingPods,omitempty"`
	FailedPods    int32 `json:"failedPods,omitempty"`
}

type RolloutGate struct {
	Rollout      string `json:"rollout,omitempty"`
	Wave         int32  `json:"wave,omitempty"`
	AllowedStart int32  `json:"allowedStart,omitempty"`
	AllowedEnd   int32  `json:"allowedEnd,omitempty"`
	Action       string `json:"action,omitempty"`
	Phase        string `json:"phase,omitempty"`
	PausedReason string `json:"pausedReason,omitempty"`
}

type PodGroupDeploymentRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PodGroupDeploymentRolloutSpec   `json:"spec,omitempty"`
	Status            PodGroupDeploymentRolloutStatus `json:"status,omitempty"`
}

type PodGroupDeploymentRolloutSpec struct {
	Mode     string                  `json:"mode,omitempty"`
	Strategy RolloutStrategy         `json:"strategy,omitempty"`
	Base     *PodGroupDeploymentSpec `json:"base,omitempty"`
	Prefill  *PodGroupDeploymentSpec `json:"prefill,omitempty"`
	Decode   *PodGroupDeploymentSpec `json:"decode,omitempty"`
	Unified  *PodGroupDeploymentSpec `json:"unified,omitempty"`
}

type RolloutStrategy struct {
	MaxUnavailablePercent int32 `json:"maxUnavailablePercent,omitempty"`
	MaxSkew               int32 `json:"maxSkew,omitempty"`
}

type PodGroupDeploymentRolloutStatus struct {
	ObservedGeneration int64                 `json:"observedGeneration,omitempty"`
	Mode               string                `json:"mode,omitempty"`
	CurrentWave        int32                 `json:"currentWave"`
	CurrentPhase       string                `json:"currentPhase,omitempty"`
	Actions            string                `json:"actions,omitempty"`
	Phase              string                `json:"phase,omitempty"`
	Reason             string                `json:"reason,omitempty"`
	Members            []RolloutMemberStatus `json:"members,omitempty"`
}

type RolloutMemberStatus struct {
	Role               string `json:"role,omitempty"`
	PodGroupDeployment string `json:"podGroupDeployment,omitempty"`
	Groups             int32  `json:"groups,omitempty"`
	WaveSize           int32  `json:"waveSize,omitempty"`
	AllowedStart       int32  `json:"allowedStart,omitempty"`
	AllowedEnd         int32  `json:"allowedEnd,omitempty"`
	Action             string `json:"action,omitempty"`
	UpdatedReadyGroups int32  `json:"updatedReadyGroups,omitempty"`
	Phase              string `json:"phase,omitempty"`
	BlockedReason      string `json:"blockedReason,omitempty"`
}
