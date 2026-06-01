/*
InferenceReplica is the per-(InferenceService, Component) workload CRD
that owns the OMENative lifecycle subtree.

Access model: this is NOT a user-facing API. The InferenceService
controller is the sole writer of InferenceReplica specs; the
InferenceReplica controller is the sole writer of statuses. A
validating webhook (pkg/webhook/admission/inferencereplica) rejects
direct user writes that lack the ome.io/controller-write annotation.
Operators may kubectl get inferencereplicas for debugging.
*/

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// InferenceReplica is the per-Component workload abstraction for
// OMENative-managed InferenceService Components. One InferenceReplica
// exists per (ISVC, Component) tuple. The ISVC controller writes the
// spec; the InferenceReplica controller writes the status.
//
// The scale subresource lets HPA/KEDA target the InferenceReplica
// directly rather than indirecting through the parent ISVC.
//
// +k8s:openapi-gen=true
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.labelSelector
// +kubebuilder:resource:path=inferencereplicas,shortName=irep,singular=inferencereplica
// +kubebuilder:printcolumn:name="Component",type="string",JSONPath=".spec.component"
// +kubebuilder:printcolumn:name="Desired",type="integer",JSONPath=".spec.replicas"
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.replicas"
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="Available",type="integer",JSONPath=".status.availableReplicas"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type InferenceReplica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InferenceReplicaSpec   `json:"spec,omitempty"`
	Status InferenceReplicaStatus `json:"status,omitempty"`
}

// InferenceReplicaList contains a list of InferenceReplica objects.
//
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type InferenceReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InferenceReplica `json:"items"`
}

// InferenceReplicaSpec is the desired state of one (ISVC, Component)
// workload. The InferenceService controller is the sole writer; the
// admission webhook rejects writes from other actors that lack the
// ome.io/controller-write annotation.
type InferenceReplicaSpec struct {
	// ParentRef names the InferenceService that owns this replica.
	// Set by the ISVC controller at create time; immutable thereafter.
	ParentRef ParentReference `json:"parentRef"`

	// Component is one of engine | decoder | router. Immutable;
	// moving a workload between Component slots requires recreating
	// the InferenceReplica.
	Component ComponentType `json:"component"`

	// Replicas is the desired Instance count. The HPA / KEDA scale
	// subresource writes this field. Defaults to 1 when omitted.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// MinReadySeconds is propagated from the ISVC spec.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinReadySeconds int32 `json:"minReadySeconds,omitempty"`

	// Runners is the fully-rendered set of pod templates per Instance.
	// MUST be non-empty. Single-pod Instances have one Runner with
	// Name="default" and Size=1; multi-node Instances typically have
	// Name="leader" (Size=1) plus Name="worker" (Size=N).
	//
	// The InferenceService controller is the sole writer of this
	// field. The InferenceReplica controller treats each
	// Runner.Template as opaque input (same contract
	// appsv1.Deployment.spec.template uses).
	//
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	Runners []Runner `json:"runners"`

	// Lifecycle holds the OMENative lifecycle policies (RestartPolicy,
	// UpdateStrategy, ReadyPolicy, InstanceReadyTimeout,
	// MigrationPolicy). Reuses the existing LifecycleSpec
	// type so the projection from ISVC.spec.<component>.lifecycle is
	// a verbatim copy.
	// +optional
	Lifecycle *LifecycleSpec `json:"lifecycle,omitempty"`

	// Pacing is the InferenceService controller's projection of the
	// active RolloutCoordinationGroup pacing for this replica.
	// Written by the ISVC controller; read by the InferenceReplica
	// controller. Includes Partition (canary hold) and MaxUnavailable
	// (rollout budget). Nil means independent rollout.
	// +optional
	Pacing *InferenceReplicaPacing `json:"pacing,omitempty"`

	// Autoscaler is the live autoscaler configuration that downstream
	// scalers (HPA / KEDA / external) target. The ISVC controller projects
	// the per-Component autoscaler defaults from ISVC.spec.<component>.autoscaler
	// onto the corresponding IR at create time + on subsequent reconciles.
	// External autoscalers may also write directly
	// to this field via the /scale subresource without going through the
	// ISVC controller.
	// +optional
	Autoscaler *ComponentAutoscaler `json:"autoscaler,omitempty"`

	// Paused, when true, stops the InferenceReplica controller from
	// initiating any new Update / Restart / Create operations.
	// In-flight operations resume on unpause.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// ParentReference holds the InferenceService owner reference info.
// Distinct from the OwnerReference list because the InferenceReplica
// controller composes the parent's name into per-IR label and Service
// names without re-reading the metadata field.
type ParentReference struct {
	// Name is the name of the parent InferenceService.
	Name string `json:"name"`
}

// RunnerName is the enum of recognized Runner roles within an
// Instance. Matches the established Runner vocabulary so existing
// ome.io/runner label semantics carry through.
// +kubebuilder:validation:Enum=default;leader;worker
type RunnerName string

const (
	// RunnerNameDefault is the single-pod Instance Runner role.
	RunnerNameDefault RunnerName = "default"
	// RunnerNameLeader is the multi-node Instance leader Runner role.
	RunnerNameLeader RunnerName = "leader"
	// RunnerNameWorker is the multi-node Instance worker Runner role.
	RunnerNameWorker RunnerName = "worker"
)

// Runner is one logical pod role within an Instance.
//
// Single-pod Instances declare one Runner named "default" with Size=1.
// Multi-node Instances declare "leader" (Size=1) + "worker" (Size>=1).
//
// The revision hash includes every Runner's Template; a worker-only
// image bump still triggers a coordinated rollout across the whole
// Instance because tensor-parallel weights require leader+workers to
// move together.
type Runner struct {
	// Name is the Runner role within the Instance.
	Name RunnerName `json:"name"`

	// Size is the pod count for this Runner per Instance.
	// Constraints:
	//   - "default" Size MUST equal 1
	//   - "leader" Size MUST equal 1
	//   - "worker" Size MUST be >= 1
	// +kubebuilder:validation:Minimum=1
	Size int32 `json:"size"`

	// Template is the fully-rendered pod template for this Runner.
	// Labels include ome.io/runner=<name>. The InferenceReplica
	// controller stamps OME_RUNNER (leader|worker|default) and,
	// for non-leader pods, OME_LEADER_ADDRESS (short-form DNS,
	// matching LWS conventions) onto the rendered pod at create.
	Template corev1.PodTemplateSpec `json:"template"`
}

// InferenceReplicaPacing is the per-replica projection of the active
// RolloutCoordinationGroup pacing. Mirrors the corresponding
// RollingUpdate subset so the projection is a verbatim
// copy.
type InferenceReplicaPacing struct {
	// Partition holds back updates for Instances whose index is less
	// than Partition. Mirrors RollingUpdate.Partition. 0
	// (the default) updates all Instances. Used for canary holds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Partition *int32 `json:"partition,omitempty"`

	// MaxUnavailable caps in-rollout disruption. Accepts either a
	// raw count or a percent string. When nil, the
	// InferenceReplica controller falls back to its own default
	// (currently 25%).
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// InferenceReplicaStatus is the observed state of one
// InferenceReplica. The InferenceReplica controller is the sole
// writer. Fields mirror the legacy LifecycleStatus shape so
// the dual-write window can byte-compare the two.
type InferenceReplicaStatus struct {
	// ObservedGeneration is the InferenceReplica.metadata.generation
	// the most recent status flush reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Replicas is the total Instance count (any phase).
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of Instances whose every pod is Ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ServingReplicas is the number of Instances whose every pod is
	// both ContainersReady AND has serving=True on the
	// controller-managed readiness gate (i.e., actually in the load
	// balancer rotation). MaxUnavailable budgets are computed
	// against this count, not ReadyReplicas, so "missing from
	// traffic" reflects reality during in-place updates.
	// +optional
	ServingReplicas int32 `json:"servingReplicas,omitempty"`

	// AvailableReplicas is the number of Instances that have been
	// Ready for at least MinReadySeconds.
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// UpdatedReplicas is the number of Instances on UpdateRevision.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// UpdatedReadyReplicas is the number of Instances on
	// UpdateRevision that are fully Ready. UpdatedReadyReplicas /
	// Replicas drives MaxUnavailable rollout pacing.
	// +optional
	UpdatedReadyReplicas int32 `json:"updatedReadyReplicas,omitempty"`

	// CurrentRevision names the ControllerRevision currently serving
	// traffic. Set by the InferenceReplica controller.
	// +optional
	CurrentRevision string `json:"currentRevision,omitempty"`

	// UpdateRevision names the ControllerRevision being rolled out.
	// +optional
	UpdateRevision string `json:"updateRevision,omitempty"`

	// CollisionCount is the salt folded into ControllerRevision hash
	// input after a name collision is observed, so retries produce a
	// different revision name. Same pattern as StatefulSet.
	// +optional
	CollisionCount *int32 `json:"collisionCount,omitempty"`

	// LabelSelector identifies pods of this InferenceReplica for the
	// HPA scale subresource. Must match the legacy
	// componentLabelSelectorString formula so existing HPAs continue
	// to resolve unchanged.
	// +optional
	LabelSelector string `json:"labelSelector,omitempty"`

	// InstanceStatuses reports per-Instance state. Reuses the
	// existing OMENativeInstanceStatus type to enable byte-equality
	// diffs against the legacy ISVC.status subtree during the
	// migration window.
	// +optional
	// +listType=map
	// +listMapKey=index
	InstanceStatuses []OMENativeInstanceStatus `json:"instanceStatuses,omitempty"`

	// Conditions reports InferenceReplica-level conditions. Defined
	// types include Available, Progressing, FailedScale,
	// FailedUpdate, GangSchedulingUnavailable, OperationStuck.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Traffic reports per-revision traffic weights for this
	// InferenceReplica. Each entry corresponds to one revision
	// currently receiving traffic. The ISVC controller mirrors this
	// onto ISVC.status.components[c].traffic[] for the
	// HTTPRoute builder.
	// +optional
	// +listType=map
	// +listMapKey=revisionName
	Traffic []ComponentTrafficTarget `json:"traffic,omitempty"`

	// CoordinationGroupRef back-references the
	// RolloutCoordinationGroup this replica is currently bound into
	// (if any). Lets operators find the owning coord group from the
	// child without parsing the ISVC spec.
	// +optional
	CoordinationGroupRef string `json:"coordinationGroupRef,omitempty"`
}

func init() {
	SchemeBuilder.Register(&InferenceReplica{}, &InferenceReplicaList{})
}
