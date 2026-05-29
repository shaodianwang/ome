package types

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// WorkloadDesiredSpec.Lifecycle and WorkloadAggregateStatus.Traffic
// use workload-owned mirror types. Adapters project the CRD-shape
// values into these structs at the boundary.

// WorkloadDesiredSpec is the per-reconcile projection of the source
// spec the workload pipeline drives toward. Read-only from workload
// code.
type WorkloadDesiredSpec struct {
	// Replicas is the desired Instance count.
	Replicas int32

	// MinReadySeconds is the minimum age (in seconds) for an Instance
	// to count as Available after becoming Ready.
	MinReadySeconds int32

	// Runners enumerates the Runner roles (default | leader | worker)
	// and per-role pod counts that constitute one Instance.
	Runners []Runner

	// PodSpec is the rendered leader / single-pod template the
	// renderer drives toward. May be nil when Replicas=0 (nothing to
	// render).
	PodSpec *corev1.PodSpec

	// WorkerPodSpec is the rendered worker template for multi-pod
	// Instances. Nil for single-pod Components.
	WorkerPodSpec *corev1.PodSpec

	// PodTemplateObjectMeta is the rendered per-Component metadata
	// (labels, annotations, owner refs) the renderer stamps onto each
	// emitted pod.
	PodTemplateObjectMeta *metav1.ObjectMeta

	// MultiPod indicates the workload uses Leader + Worker Runners
	// (each Instance materializes more than one pod).
	MultiPod bool

	// Lifecycle holds RestartPolicy / UpdateStrategy / ReadyPolicy /
	// InstanceReadyTimeout / MigrationPolicy as workload-owned mirror
	// values; adapters project the CRD-shape lifecycle into this struct
	// at the boundary.
	Lifecycle Lifecycle

	// Pacing is the projected rollout pacing for this reconcile. Nil
	// means "no pacing constraint" (treated as allowed).
	Pacing *WorkloadPacing

	// Paused, when true, stops the reconciler from starting any fresh
	// Update / Restart / Create operations. In-flight operations
	// resume on unpause.
	Paused bool

	// GangSchedulingAvailable is true when the scheduler-plugins
	// PodGroup CRD was discovered at controller startup. Drives
	// whether podgroup.EnsurePodGroup runs for multi-pod Instances.
	GangSchedulingAvailable bool
}

// WorkloadObservedState is the per-reconcile snapshot of the source
// status subtree the reconciler reasons about.
type WorkloadObservedState struct {
	// ObservedGeneration is the spec generation the last status flush
	// reflects.
	ObservedGeneration int64

	// CollisionCount is the ControllerRevision-hash salt. nil on the
	// first reconcile.
	CollisionCount *int32

	// CurrentRevision names the ControllerRevision currently serving
	// traffic.
	CurrentRevision string

	// UpdateRevision names the ControllerRevision being rolled out.
	UpdateRevision string

	// InstanceStatuses reports per-Instance state. Read-only here;
	// writes go through ReconcileInput.MutateInstance.
	InstanceStatuses []InstanceStatus

	// Conditions reports component-level conditions.
	Conditions []metav1.Condition
}

// WorkloadAggregateStatus is the per-reconcile flush of counters,
// conditions, and traffic that the reconciler hands off to
// Source.WriteAggregateStatus.
type WorkloadAggregateStatus struct {
	ObservedGeneration   int64
	Replicas             int32
	ReadyReplicas        int32
	ServingReplicas      int32
	AvailableReplicas    int32
	UpdatedReplicas      int32
	UpdatedReadyReplicas int32
	CurrentRevision      string
	UpdateRevision       string
	LabelSelector        string
	Conditions           []metav1.Condition
	Traffic              []ComponentTrafficTarget
}

// WorkloadPacing is the projected rollout pacing the reconciler reads.
// Adapters compute it once per reconcile.
type WorkloadPacing struct {
	// Partition holds back updates for Instances whose index is less
	// than Partition. 0 (the default) updates all Instances. Used
	// for canary holds.
	Partition *int32

	// MaxUnavailable caps in-rollout disruption. nil falls back to
	// the reconciler default (25%).
	MaxUnavailable *intstr.IntOrString

	// Decisions is the projected per-gate allow/deny map for this
	// reconcile. The ISVC adapter fills it from coordination gates;
	// the IR adapter leaves it nil. nil means allowed.
	Decisions *PacingDecisions
}

// PacingDecisions is the workload-facing projection of cross-Component
// coordination gates. Plain data — adapters compute it outside the
// workload package.
type PacingDecisions struct {
	// UpdateAllowed is false when the reconciler MUST NOT start any
	// fresh Update operations this reconcile.
	UpdateAllowed bool

	// UpdateDenyReason is the operator-visible reason recorded onto
	// events / conditions when UpdateAllowed is false.
	UpdateDenyReason string

	// SurgeBudgetRemaining is the upper bound on in-flight surge pods
	// the reconciler may keep alive concurrently. The dispatcher
	// tracks its own in-flight count within a pass; this is a
	// per-reconcile ceiling.
	SurgeBudgetRemaining int32
}

// Runner is a named role within an Instance with a per-Instance pod
// count. Single-pod has one "default" Runner of Size=1; multi-pod has
// "leader" (Size=1) + "worker" (Size=N).
type Runner struct {
	// Name is "leader" | "worker" | "default".
	Name string
	// Size is the per-Instance pod count for this Runner.
	Size int32
}
