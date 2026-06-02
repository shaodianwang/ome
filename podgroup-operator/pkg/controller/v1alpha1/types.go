package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

type PodGroupDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PodGroupDeploymentSpec   `json:"spec,omitempty"`
	Status            PodGroupDeploymentStatus `json:"status,omitempty"`
}

type PodGroupDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PodGroupDeployment `json:"items"`
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
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	CurrentRevision    string             `json:"currentRevision,omitempty"`
	Groups             []GroupStatus      `json:"groups,omitempty"`
	Total              TotalStatus        `json:"total,omitempty"`
	RolloutGate        RolloutGate        `json:"rolloutGate,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
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

type PodGroupDeploymentRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PodGroupDeploymentRollout `json:"items"`
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
	Conditions         []metav1.Condition    `json:"conditions,omitempty"`
}

type RolloutMemberStatus struct {
	Role               string `json:"role,omitempty"`
	PodGroupDeployment string `json:"podGroupDeployment,omitempty"`
	Generation         int64  `json:"generation,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Groups             int32  `json:"groups,omitempty"`
	WaveSize           int32  `json:"waveSize,omitempty"`
	AllowedStart       int32  `json:"allowedStart,omitempty"`
	AllowedEnd         int32  `json:"allowedEnd,omitempty"`
	Action             string `json:"action,omitempty"`
	UpdatedReadyGroups int32  `json:"updatedReadyGroups,omitempty"`
	Phase              string `json:"phase,omitempty"`
	BlockedReason      string `json:"blockedReason,omitempty"`
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(
		operatorGroupVersion,
		&PodGroupDeployment{},
		&PodGroupDeploymentList{},
		&PodGroupDeploymentRollout{},
		&PodGroupDeploymentRolloutList{},
	)
	metav1.AddToGroupVersion(scheme, operatorGroupVersion)
	return nil
}

func (in *PodGroupDeployment) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	return out
}

func (in *PodGroupDeploymentList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	return out
}

func (in *PodGroupDeploymentRollout) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	return out
}

func (in *PodGroupDeploymentRolloutList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	return out
}

func (in *PodGroupDeployment) DeepCopy() *PodGroupDeployment {
	if in == nil {
		return nil
	}
	out := new(PodGroupDeployment)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = in.Spec.DeepCopy()
	out.Status = in.Status.DeepCopy()
	return out
}

func (in *PodGroupDeploymentList) DeepCopy() *PodGroupDeploymentList {
	if in == nil {
		return nil
	}
	out := new(PodGroupDeploymentList)
	*out = *in
	out.ListMeta = *in.ListMeta.DeepCopy()
	if in.Items != nil {
		out.Items = make([]PodGroupDeployment, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in PodGroupDeploymentSpec) DeepCopy() PodGroupDeploymentSpec {
	out := in
	out.Template = *in.Template.DeepCopy()
	return out
}

func (in PodGroupDeploymentStatus) DeepCopy() PodGroupDeploymentStatus {
	out := in
	if in.Groups != nil {
		out.Groups = append([]GroupStatus(nil), in.Groups...)
	}
	if in.Conditions != nil {
		out.Conditions = append([]metav1.Condition(nil), in.Conditions...)
	}
	return out
}

func (in *PodGroupDeploymentRollout) DeepCopy() *PodGroupDeploymentRollout {
	if in == nil {
		return nil
	}
	out := new(PodGroupDeploymentRollout)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec = in.Spec.DeepCopy()
	out.Status = in.Status.DeepCopy()
	return out
}

func (in *PodGroupDeploymentRolloutList) DeepCopy() *PodGroupDeploymentRolloutList {
	if in == nil {
		return nil
	}
	out := new(PodGroupDeploymentRolloutList)
	*out = *in
	out.ListMeta = *in.ListMeta.DeepCopy()
	if in.Items != nil {
		out.Items = make([]PodGroupDeploymentRollout, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in PodGroupDeploymentRolloutSpec) DeepCopy() PodGroupDeploymentRolloutSpec {
	out := in
	out.Base = deepCopyPodGroupDeploymentSpecPtr(in.Base)
	out.Prefill = deepCopyPodGroupDeploymentSpecPtr(in.Prefill)
	out.Decode = deepCopyPodGroupDeploymentSpecPtr(in.Decode)
	out.Unified = deepCopyPodGroupDeploymentSpecPtr(in.Unified)
	return out
}

func (in PodGroupDeploymentRolloutStatus) DeepCopy() PodGroupDeploymentRolloutStatus {
	out := in
	if in.Members != nil {
		out.Members = append([]RolloutMemberStatus(nil), in.Members...)
	}
	if in.Conditions != nil {
		out.Conditions = append([]metav1.Condition(nil), in.Conditions...)
	}
	return out
}

func deepCopyPodGroupDeploymentSpecPtr(in *PodGroupDeploymentSpec) *PodGroupDeploymentSpec {
	if in == nil {
		return nil
	}
	out := in.DeepCopy()
	return &out
}
