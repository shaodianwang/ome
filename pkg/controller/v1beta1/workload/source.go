package workload

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// NOTE on residual v1beta1 imports (slice 3 of N):
//
// Slice 3 flips the three named carrier types — ComponentType,
// OMENativeInstanceStatus, the MutateInstance callback — to the
// workload-owned mirrors from types.go. WorkloadDesiredSpec.Lifecycle and
// WorkloadAggregateStatus.Traffic still reference v1beta1 because the
// lifecycle subtree (5 nested structs + 4 string enums) and the traffic
// target struct have no workload mirror yet. Mirroring them inflates
// slice 3 past the +500 LOC ceiling; slice 4+ moves the bodies that
// reason about Lifecycle into workload/ and mirrors these types as part
// of that move. See the report attached to slice 3's PR for the
// rationale.

// Source is the per-workload contract the workload reconciler reads and
// writes. Implemented by adapters: the ISVC OMENative dispatch path
// (omenative/adapter.go) and the InferenceReplica controller path
// (inferencereplica/adapter.go).
//
// The interface is intentionally narrow: identity (3 methods), one
// desired-state read, one observed-state read, three status writers.
// Anything else the reconciler needs (client, recorder, expectations)
// flows through Deps, not Source.
type Source interface {
	// Key returns the namespaced name + a stable opaque identity used
	// for label/selector composition. Returns the workload-owned Key
	// type (types.go) — adapters compose it from the owner-CRD shape.
	// For both adapters today the composed OwnerName is the owner's
	// bare name so existing ControllerRevisions, headless Services,
	// and pod selectors keep the same names.
	Key() Key

	// OwnerObject returns the K8s object whose UID/Kind/APIVersion
	// becomes the controllerOwner on every emitted pod / revision /
	// service / PodGroup / PDB. ISVC adapter returns
	// *v1beta1.InferenceService; IR adapter returns
	// *v1beta1.InferenceReplica.
	OwnerObject() client.Object

	// EventTarget returns the object emitted events are stamped
	// against. Usually OwnerObject(); the IR adapter may return the
	// parent ISVC instead so user-facing event streams stay coherent
	// once the ISVC controller delegates Component management to IR.
	EventTarget() client.Object

	// DesiredSpec is the workload-shape projection the reconciler
	// drives toward. Pure read; no I/O. Adapters precompute the
	// projection once per reconcile.
	DesiredSpec() WorkloadDesiredSpec

	// ObservedState exposes the per-workload status subtree the
	// reconciler reasons about: InstanceStatuses, CollisionCount,
	// revision pointers, Conditions, ObservedGeneration. The return
	// is a snapshot owned by the adapter; the reconciler MUST NOT
	// mutate it in place. Status writes go through MutateInstance and
	// WriteAggregateStatus.
	ObservedState() WorkloadObservedState

	// MutateInstance applies the mutate callback to the (idx)
	// InstanceStatus entry under retry.RetryOnConflict, mirrors the
	// result onto OwnerObject() for in-reconcile readers, and emits
	// any side-effecting top-level conditions the adapter is
	// responsible for. mutate returns true when a real change was
	// made; false short-circuits the status write. The callback
	// receives the workload-owned InstanceStatus mirror — adapters
	// convert the workload type back to their owner-CRD shape inside
	// the implementation (`internalsource.InstanceStatusFromWorkload`
	// for the ISVC adapter).
	MutateInstance(ctx context.Context, idx int32, mutate func(*InstanceStatus) bool) error

	// WriteAggregateStatus commits the per-workload counters,
	// Component-level conditions, traffic, and revision pointers in
	// one status flush. Adapters choose where to write (legacy
	// OMENativeComponentStatus subtree for ISVC, InferenceReplica
	// status root for IR).
	WriteAggregateStatus(ctx context.Context, agg WorkloadAggregateStatus) error

	// BumpCollisionCount increments CollisionCount (initializing to 1
	// when nil) and returns the new pointer for revision-retry
	// callers. Wrapped in retry.RetryOnConflict so concurrent
	// reconciles can't lose the bump.
	BumpCollisionCount(ctx context.Context) (*int32, error)
}

// WorkloadKey identifies one logical workload and seeds label/selector
// composition for the ops/render.go entry points. Both adapters produce
// a Name of "<isvc>-<component>" so existing pod selectors,
// ControllerRevision names, and Service names remain unchanged across
// the refactor.
//
// Distinct from `Key` (types.go) which is the workload-owned identity
// the Source interface returns. Slice 4+ migrates the ops entry points
// onto `Key` and retires this type; until then `WorkloadKey` carries
// the legacy ops/render.go shape (Name + Labels) so leaf-package
// signatures stay stable.
type WorkloadKey struct {
	// Namespace is the namespace every pod / revision / service the
	// reconciler emits is created in.
	Namespace string

	// Name is the composed identity used for ControllerRevision naming
	// and as the prefix on emitted pod / PodGroup / PDB names.
	Name string

	// Component identifies which of router / engine / decoder this
	// workload represents. Used by status conditions, events, and
	// the pod-selector label set.
	Component ComponentType

	// Labels is the seed label set the workload reconciler stamps on
	// every emitted pod. Includes ome.io/inferenceservice,
	// component, and the managed-by=OMENative marker; adapters may
	// add adapter-specific labels (the IR adapter may add
	// ome.io/inferencereplica). The reconciler treats this map as
	// read-only; copy before mutating.
	Labels map[string]string
}

// WorkloadDesiredSpec is the per-reconcile projection of the source
// spec the workload pipeline drives toward. Adapters compute this once
// at the top of Reconcile; the reconciler treats every field as
// read-only.
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

	// Lifecycle holds the OMENative lifecycle policies (RestartPolicy,
	// UpdateStrategy, ReadyPolicy, InstanceReadyTimeout,
	// MigrationPolicy). Reuses the existing OMENativeComponentSpec
	// type — adapters either project ISVC.Spec.<comp>.OMENative or
	// dereference IR.Spec.Lifecycle.
	Lifecycle v1beta1.OMENativeComponentSpec

	// Pacing is the projected rollout pacing for this reconcile. Nil
	// means "no pacing constraint" (treated as allowed).
	Pacing *WorkloadPacing

	// Paused, when true, stops the reconciler from starting any fresh
	// Update / Restart / Create operations. In-flight operations
	// resume on unpause.
	Paused bool

	// GangSchedulingAvailable is the cluster-discovery boolean threaded
	// through from the controller manager — true when the
	// scheduler-plugins PodGroup CRD is installed at controller
	// startup. The reconciler reads this to decide whether to call
	// podgroup.EnsurePodGroup for each multi-pod Instance.
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

	// InstanceStatuses reports per-Instance state. The reconciler
	// reads but does not mutate this slice — writes go through
	// Source.MutateInstance. Workload-owned mirrors (types.go) —
	// adapters convert from the owner-CRD shape via
	// `internalsource.InstanceStatusSliceToWorkload` for the ISVC
	// adapter.
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
	Traffic              []v1beta1.ComponentTrafficTarget
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

	// Decisions is the projected per-gate allow/deny map for THIS
	// reconcile. ISVC adapter fills it from CheckRatioGate /
	// CheckSurgeGate / CheckUnavailabilityGate / CheckSequentialGate;
	// IR adapter leaves it nil (rate-limited via Partition +
	// MaxUnavailable only; the parent ISVC controller pauses via
	// spec.paused for harder gates). nil means "allowed".
	Decisions *PacingDecisions
}

// PacingDecisions is the workload-facing projection of cross-Component
// coordination gates. The workload package treats it as plain data;
// adapters compute it (or leave it nil) outside the workload package.
type PacingDecisions struct {
	// UpdateAllowed is false when the workload reconciler MUST NOT
	// start any fresh Update operations this reconcile.
	UpdateAllowed bool

	// UpdateDenyReason is the operator-visible reason recorded onto
	// events / conditions when UpdateAllowed is false.
	UpdateDenyReason string

	// SurgeBudgetRemaining is the upper bound on in-flight surge
	// pods the workload reconciler may keep alive concurrently.
	// The dispatcher still tracks its own in-flight count within a
	// single reconcile pass; this is a per-reconcile ceiling.
	SurgeBudgetRemaining int32
}

// Runner is the workload-package projection of the Runner concept:
// a named role within an Instance with a per-Instance pod count.
// Single-pod Instances have one Runner named "default" of Size=1;
// multi-pod Instances have "leader" (Size=1) + "worker" (Size=N).
type Runner struct {
	// Name is "leader" | "worker" | "default".
	Name string

	// Size is the per-Instance pod count for this Runner.
	Size int32
}
