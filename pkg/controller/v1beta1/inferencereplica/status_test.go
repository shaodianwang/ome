package inferencereplica

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// TestPopulateInstancePodCounters_StampsLivePodState pins the
// per-Instance counter writer: handed a (status slice, by-index pod
// map, available-pod set) tuple, it stamps each Instance's per-pod
// counters off the live pod state. This is the load-bearing step that
// lets the downstream workload.CountReadyInstances classifier see the
// real pod state instead of the legacy Phase-only proxy.
//
// Regression guard for the round-2 recreate-path bug: before the fix,
// the IR aggregator never populated per-Instance ReadyPodCount —
// downstream rollups read zero on every Instance even when the
// underlying pods were ContainersReady. This test pins the exact
// counter-write contract so a future refactor can't silently drop it.
func TestPopulateInstancePodCounters_StampsLivePodState(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod0.Spec.NodeName = "node-a"
	pod1 := podForIR(ir, 1, "default", 0, false, false)
	insts := []v1beta1.OMENativeInstanceStatus{
		// Pretend the legacy Phase signal lags reality: Instance 0
		// still Phase=Updating from a prior reconcile, but its pod is
		// already ContainersReady + serving. The fix should report
		// ReadyPodCount=1 / ServingPodCount=1 / AvailablePodCount=1
		// off the live pod state regardless of Phase.
		{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, PodCount: 0, ReadyPodCount: 0},
		// Pending Instance with a pod that hasn't flipped Ready —
		// counters should all be zero except PodCount.
		{Index: 1, Phase: v1beta1.OMENativeInstancePending, PodCount: 0, ReadyPodCount: 0},
	}
	byIndex := map[int32][]*corev1.Pod{
		0: {pod0},
		1: {pod1},
	}
	availableByPod := map[string]struct{}{pod0.Name: {}}

	populateInstancePodCounters(insts, byIndex, availableByPod)

	g.Expect(insts[0].PodCount).To(gomega.Equal(int32(1)),
		"Instance 0 has 1 live pod")
	g.Expect(insts[0].ReadyPodCount).To(gomega.Equal(int32(1)),
		"Instance 0's pod is ContainersReady; ReadyPodCount must be 1 even though Phase=Updating")
	g.Expect(insts[0].ServingPodCount).To(gomega.Equal(int32(1)),
		"Instance 0's pod has serving=True; ServingPodCount must be 1")
	g.Expect(insts[0].AvailablePodCount).To(gomega.Equal(int32(1)),
		"Instance 0's pod is in the available EndpointSlice set")
	g.Expect(insts[0].NodesOccupied).To(gomega.Equal([]string{"node-a"}),
		"NodesOccupied must reflect the scheduling node")

	g.Expect(insts[1].PodCount).To(gomega.Equal(int32(1)),
		"Instance 1 has 1 live pod (not yet Ready)")
	g.Expect(insts[1].ReadyPodCount).To(gomega.Equal(int32(0)),
		"Instance 1's pod is not ContainersReady")
	g.Expect(insts[1].AvailablePodCount).To(gomega.Equal(int32(0)),
		"Instance 1's pod is not in the available set")
}

// TestPopulateInstancePodCounters_EmptyPodSet pins the no-pods edge
// case: an Instance with zero matching pods (e.g., post-drain pre-
// recreate, scale-down victim mid-delete) must end up with all-zero
// counters. Catches a future bug where the loop accidentally retains
// stale counter values from a prior reconcile.
func TestPopulateInstancePodCounters_EmptyPodSet(t *testing.T) {
	g := gomega.NewWithT(t)
	insts := []v1beta1.OMENativeInstanceStatus{
		// Stale stamps from a prior reconcile when this Instance had pods.
		{Index: 0, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, AvailablePodCount: 1,
			NodesOccupied: []string{"node-x"}},
	}
	populateInstancePodCounters(insts, map[int32][]*corev1.Pod{}, map[string]struct{}{})

	g.Expect(insts[0].PodCount).To(gomega.BeZero())
	g.Expect(insts[0].ReadyPodCount).To(gomega.BeZero())
	g.Expect(insts[0].ServingPodCount).To(gomega.BeZero())
	g.Expect(insts[0].AvailablePodCount).To(gomega.BeZero())
	g.Expect(insts[0].NodesOccupied).To(gomega.BeNil(),
		"NodesOccupied must round-trip nil ↔ nil rather than nil ↔ empty-slice")
}

// TestPopulateInstancePodCounters_Admitted verifies the Admitted signal
// is computed correctly: true when the Instance has pods and none carry
// an admission scheduling gate (Kueue has granted quota), false while
// gated/queued or before pods exist. Used by the OEP-0018 control plane
// to decide the fan-out race winner.
func TestPopulateInstancePodCounters_Admitted(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 3)

	// Index 0 → one normal (ungated) running pod
	pod0 := podForIR(ir, 0, "default", 0, true, true)

	// Index 1 → one pod with Kueue admission gate still present (queued)
	pod1 := podForIR(ir, 1, "default", 0, false, false)
	pod1.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}

	// Index 2 → no pods (empty instance)

	insts := []v1beta1.OMENativeInstanceStatus{
		{Index: 0},
		{Index: 1},
		{Index: 2},
	}
	byIndex := map[int32][]*corev1.Pod{
		0: {pod0},
		1: {pod1},
		// 2: no entry (zero pods)
	}

	populateInstancePodCounters(insts, byIndex, nil)

	g.Expect(insts[0].Admitted).To(gomega.BeTrue(),
		"Instance 0 has a pod and it is not admission-gated → Admitted=true")
	g.Expect(insts[1].Admitted).To(gomega.BeFalse(),
		"Instance 1's pod still carries the Kueue admission gate → Admitted=false")
	g.Expect(insts[2].Admitted).To(gomega.BeFalse(),
		"Instance 2 has no pods yet → Admitted=false")
}

// TestAggregateStatus_AvailableReplicas_MatchesReadyWithEndpointSlices
// pins the AvailableReplicas counter via the end-to-end Reconcile path.
// With pods ContainersReady AND published as Ready in the per-Component
// headless Service's EndpointSlice, AvailableReplicas should collapse
// onto ReadyReplicas. The aggregator post-fix reads availability off
// the slice exactly like the omenative direct path
// (TestAggregateAndWriteStatus_PerInstanceCountersFromObservedPods),
// so the two adapters produce byte-identical counters.
//
// Replaces the legacy assertion that AvailableReplicas always mirrors
// ReadyReplicas. The two CAN diverge — see
// TestAggregateStatus_AvailableReplicas_ZeroWhenSliceNotReady — so the
// surface tested here is "with EndpointSlices published Ready, the two
// match".
func TestAggregateStatus_AvailableReplicas_MatchesReadyWithEndpointSlices(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 3)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 2, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			PodCount: 1, ReadyPodCount: 0, ServingPodCount: 0, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)
	pod2 := podForIR(ir, 2, "default", 0, false, false)
	slice0 := sliceForIRPod(ir, pod0, true)
	slice1 := sliceForIRPod(ir, pod1, true)
	r, c := newReconciler(t, ir, pod0, pod1, pod2, slice0, slice1)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(2)),
		"ReadyReplicas should be 2 (two pods ContainersReady)")
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(2)),
		"AvailableReplicas should match ReadyReplicas when EndpointSlices publish both pods Ready")
}

// TestAggregateStatus_AvailableReplicas_ZeroWhenSliceNotReady pins the
// EndpointSlice-gated availability semantic: a pod ContainersReady but
// NOT yet in any EndpointSlice's Ready endpoints reads
// AvailablePodCount=0 → AvailableReplicas=0, while ReadyReplicas still
// reports 1. Mirrors the omenative direct path's
// TestAggregateAndWriteStatus_AvailableRequiresEndpointSliceReady.
//
// This is the "newly-Ready pod hasn't been picked up by kube-proxy
// yet" window — operators get a useful signal that the pod is alive
// but not in rotation, instead of the previous always-mirrors-ready
// behavior that hid the kube-proxy lag.
func TestAggregateStatus_AvailableReplicas_ZeroWhenSliceNotReady(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	// EndpointSlice publishes the endpoint as Ready=false (kube-proxy
	// hasn't picked it up yet).
	slice0 := sliceForIRPod(ir, pod0, false)
	r, c := newReconciler(t, ir, pod0, slice0)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)),
		"ReadyReplicas counts the ContainersReady pod regardless of EndpointSlice state")
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(0)),
		"AvailableReplicas waits for EndpointSlice Ready=true (kube-proxy gate)")
}

// TestAggregateStatus_ReadyCondition_TrueWhenAllReady_RolloutDone pins
// the happy-path Ready=True branch: ReadyReplicas == Replicas AND
// CurrentRevision == UpdateRevision → Ready=True/AllInstancesReady.
//
// Tests computeReadyCondition directly because the full-Reconcile path
// is sensitive to the exact ControllerRevision-name shape the workload
// pipeline computes. The helper is the load-bearing logic; the
// integration test TestAggregateStatus_AvailableReplicas_MatchesReadyForSimpleCase
// covers the Reconcile-side wiring.
func TestAggregateStatus_ReadyCondition_TrueWhenAllReady_RolloutDone(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        1,
		ReadyReplicas:   1,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		},
	}
	cond := computeReadyCondition(status, nil)
	g.Expect(cond.Type).To(gomega.Equal(InferenceReplicaConditionReady))
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue),
		"Ready=True when ReadyReplicas == Replicas AND CurrentRevision == UpdateRevision")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonAllInstancesReady))
}

// TestAggregateStatus_ReadyCondition_UnknownDuringRollout pins the
// in-flight-rollout branch: CurrentRevision != UpdateRevision →
// Ready=Unknown/RolloutInProgress.
//
// The explicit Unknown status (not False) is load-bearing: operators
// running `kubectl wait --for=condition=Ready` see "still churning,
// check back" instead of a False that would short-circuit their wait.
func TestAggregateStatus_ReadyCondition_UnknownDuringRollout(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:             2,
		ReadyReplicas:        1,
		UpdatedReadyReplicas: 1,
		CurrentRevision:      "rev-a",
		UpdateRevision:       "rev-b",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-b"},
			{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: "rev-a",
				TargetRevision: "rev-b"},
		},
	}
	cond := computeReadyCondition(status, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionUnknown),
		"Ready=Unknown while CurrentRevision != UpdateRevision (rollout in flight)")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonRolloutInProgress))
}

// TestAggregateStatus_ReadyCondition_FalseOnStuck pins the
// stuck-replica-count branch: ReadyReplicas < Replicas with
// CurrentRevision == UpdateRevision (no rollout in flight) →
// Ready=False/ReplicaCountMismatch.
//
// Operationally this catches: a steady-state Component lost an Instance
// (pod evicted, node drained, etc.) but the workload pipeline hasn't
// either recovered or escalated yet. The False signal pages operators
// instead of letting kubectl-wait hang on a missing condition.
func TestAggregateStatus_ReadyCondition_FalseOnStuck(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        2,
		ReadyReplicas:   1,
		ServingReplicas: 1,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a"},
			// Pending (not Failed) so the InstanceFailed override doesn't
			// fire; we want the ReplicaCountMismatch branch.
			{Index: 1, Phase: v1beta1.OMENativeInstancePending, RunningRevision: "rev-a"},
		},
	}
	// nil pacing → strict floor (all Instances), so serving 1/2 is below floor.
	cond := computeReadyCondition(status, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse),
		"Ready=False when serving is below the availability floor with no rollout in flight")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonReplicaCountMismatch))
}

// TestAggregateStatus_ReadyCondition_TrueWhenServingWithinBudget pins the
// availability-based branch: ReadyReplicas < Replicas with no rollout in flight,
// but the serving Instance count is at or above the floor derived from the
// rollout MaxUnavailable budget → Ready=True/MinimumAvailable. Mirrors the
// ISVC-side EngineReady so `kubectl wait irep --for=condition=Ready` agrees.
func TestAggregateStatus_ReadyCondition_TrueWhenServingWithinBudget(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:        10,
		ReadyReplicas:   9,
		ServingReplicas: 9,
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
	}
	mu := intstr.FromString("25%") // floor = 10 - ceil(2.5) = 7
	cond := computeReadyCondition(status, &v1beta1.InferenceReplicaPacing{MaxUnavailable: &mu})
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue),
		"Ready=True when serving (9) >= floor (7) even though ReadyReplicas < Replicas")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonMinimumAvailable))
}

// TestAggregateStatus_ReadyCondition_FalseOnFailed pins the
// InstanceFailed override: any Instance Phase=Failed → Ready=False/
// InstanceFailed, regardless of revision state.
//
// This is the highest-priority branch in computeReadyCondition's
// precedence chain — a failed Instance overrides RolloutInProgress
// (rollout is permanently wedged), AllInstancesReady (one bad apple),
// and ReplicaCountMismatch (Failed is the more specific reason).
func TestAggregateStatus_ReadyCondition_FalseOnFailed(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		Replicas:      2,
		ReadyReplicas: 1,
		// CurrentRevision != UpdateRevision would normally flip to
		// Unknown; the Failed override must win regardless.
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-b",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			{Index: 1, Phase: v1beta1.OMENativeInstanceFailed},
		},
	}
	cond := computeReadyCondition(status, nil)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse),
		"Ready=False when any Instance has Phase=Failed, overriding rollout-in-flight")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonInstanceFailed))
}

// TestAggregateStatus_ReadyCondition_ObservedGenerationStamped pins
// that the Ready condition carries ObservedGeneration off the
// IR.Status.ObservedGeneration field so consumers can correlate
// condition transitions to a specific spec generation. Matches the
// pattern apimeta.SetStatusCondition + the standard K8s convention.
func TestAggregateStatus_ReadyCondition_ObservedGenerationStamped(t *testing.T) {
	g := gomega.NewWithT(t)
	status := &v1beta1.InferenceReplicaStatus{
		ObservedGeneration: 7,
		Replicas:           1,
		ReadyReplicas:      1,
		CurrentRevision:    "rev-a",
		UpdateRevision:     "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		},
	}
	cond := computeReadyCondition(status, nil)
	g.Expect(cond.ObservedGeneration).To(gomega.Equal(int64(7)),
		"Ready condition must stamp ObservedGeneration off status.ObservedGeneration")
}

// TestAggregateStatus_RecreatePath_ReadyReplicasReflectsLivePods pins
// the round-2 cluster-test bug: during a recreate or SurgeThenDrain
// rollout the per-Instance lifecycle Phase can lag the live pod state
// (Phase=Updating while the new pod has flipped ContainersReady but
// the workload pipeline hasn't yet stamped Phase=Ready). The IR
// rollup MUST count the Instance as Ready off the live pod state
// rather than the lifecycle bookkeeping — otherwise ISVC's
// engine.omenative.readyReplicas reads 0 even though the pod is
// already serving, blocking E-1/X-2/G-2/G-4 + the cascaded E-2/E-3/
// E-4/E-5 tests.
//
// Setup mirrors the post-recreate moment: Instance 0 carries
// Phase=Updating with a deep operation snapshot (drain step
// committed), but its replacement pod is live and ContainersReady.
// After Reconcile, IR.Status.ReadyReplicas must be 1.
func TestAggregateStatus_RecreatePath_ReadyReplicasReflectsLivePods(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	// Pretend a recreate is mid-flight: the controller has stamped
	// Phase=Updating with an Update operation but the new pod just
	// flipped ContainersReady. The aggregator must observe the live
	// pod state and count the Instance Ready regardless of Phase.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     2,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
		},
	}
	// Live pod is ContainersReady + serving (post-recreate, pre-Phase
	// bump). The aggregator must populate ReadyPodCount=1 from this.
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	// EndpointSlice published — kube-proxy has picked the pod up so
	// AvailableReplicas can also reach 1.
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)),
		"ReadyReplicas must reflect live pod state during recreate (Phase=Updating but pod ContainersReady)")
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(1)),
		"AvailableReplicas must match ReadyReplicas when EndpointSlice publishes the pod Ready")
	g.Expect(got.Status.InstanceStatuses[0].ReadyPodCount).To(gomega.Equal(int32(1)),
		"per-Instance ReadyPodCount must be populated from the live pod (not the stale 0)")
}

// TestAggregateStatus_RecreatePath_OldPodCountsReadyDuringDrain pins
// the operationally-critical surge-tolerant counter behavior: during
// the drain phase of a recreate, the OLD pod is still serving (the
// drain hasn't yet completed). The IR rollup MUST count it as Ready
// throughout drain so SurgeThenDrain / RecreatePod observers see a
// continuous ServingReplicas signal, not a drop-to-zero glitch that
// would page operators or trip the RatioBalanced gate.
//
// Setup: Instance Phase=Updating, drain step in progress. The
// canonical pod still carries ContainersReady=True. The aggregator
// must observe the pod state and count it Ready off
// CountReadyInstances' surge-tolerant classifier.
func TestAggregateStatus_RecreatePath_OldPodCountsReadyDuringDrain(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
			// The drain marker is in flight; the pod is still alive
			// and ContainersReady but the workload pipeline has set
			// Phase=Updating.
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate,
				Step: "Drain",
			},
		},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	// Critical: ReadyReplicas stays 1 throughout drain even though
	// Phase=Updating. Without the fix this read as 0, breaking
	// SurgeThenDrain's "serving >= 1" continuity promise.
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(1)),
		"the still-serving canonical pod must count Ready during drain (Phase=Updating)")
}

// TestAggregateStatus_ReadyCondition_PersistedToInformerCache pins the
// end-to-end wiring: a reconciled IR with a real status update lands
// the Ready condition on the apiserver-side IR object, observable via
// apimeta.FindStatusCondition. Catches a future regression where the
// computeReadyCondition helper is correct but the aggregator forgets
// to call SetStatusCondition (or writes to the wrong slice).
//
// Uses the same setup as TestReconcile_StatusAggregator_RollsUpCounters:
// real pods backing the pre-seeded Instances so the workload pipeline
// doesn't reset state. ReadyReplicas=2 and empty CurrentRevision (first
// reconcile) means the True branch can't fire — we just assert the
// condition is present and well-formed.
func TestAggregateStatus_ReadyCondition_PersistedToInformerCache(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)
	r, c := newReconciler(t, ir, pod0, pod1)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	cond := apimeta.FindStatusCondition(got.Status.Conditions, InferenceReplicaConditionReady)
	g.Expect(cond).NotTo(gomega.BeNil(),
		"Ready condition must be persisted to IR.Status.Conditions after every reconcile pass")
	g.Expect(cond.Type).To(gomega.Equal(InferenceReplicaConditionReady))
	// LastTransitionTime is stamped by apimeta.SetStatusCondition when
	// the condition is first inserted.
	g.Expect(cond.LastTransitionTime.IsZero()).To(gomega.BeFalse(),
		"LastTransitionTime must be set by SetStatusCondition")
}

// TestAggregateAndWriteStatus_NoWriteWhenUnchanged pins the no-op
// short-circuit: once an IR's status has converged, a second
// aggregateAndWriteStatus pass that recomputes the identical status must
// perform ZERO Status().Update writes. Each IR status write re-triggers
// this controller AND the irprojector rollup onto the parent ISVC, so at
// ~6000 pods the prior unconditional write amplified idle reconciles into
// a write storm.
//
// Verified via ResourceVersion: the fake client bumps RV on every
// Status().Update. The first pass changes status (RV bumps); the second
// pass over an already-converged IR must leave RV untouched. The knative
// SetCondition / apimeta.SetStatusCondition LastTransitionTime-preserving
// semantics are what make the second-pass status DeepEqual the first.
func TestAggregateAndWriteStatus_NoWriteWhenUnchanged(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	slice0 := sliceForIRPod(ir, pod0, true)
	r, c := newReconciler(t, ir, pod0, slice0)

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	// First pass: status changes (per-Instance counters, conditions,
	// LabelSelector, ObservedGeneration all get stamped) → a write lands.
	g.Expect(r.aggregateAndWriteStatus(context.Background(), ir, nil, plan, nil)).To(gomega.Succeed())
	afterFirst := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, afterFirst)).To(gomega.Succeed())
	rvAfterFirst := afterFirst.ResourceVersion

	// Second pass over the now-converged IR: recomputed status equals
	// live status → the DeepEqual guard must skip Status().Update, so the
	// ResourceVersion is unchanged.
	g.Expect(r.aggregateAndWriteStatus(context.Background(), afterFirst.DeepCopy(), nil, plan, nil)).To(gomega.Succeed())
	afterSecond := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, afterSecond)).To(gomega.Succeed())
	g.Expect(afterSecond.ResourceVersion).To(gomega.Equal(rvAfterFirst),
		"a no-op aggregateAndWriteStatus pass must perform ZERO writes (ResourceVersion unchanged)")
}

// TestAggregateAndWriteStatus_ConflictSurfacesAsConflict pins the
// terminal-conflict contract the deferred Reconcile handler keys on: when
// every status write loses the optimistic-lock race (RetryOnConflict
// exhausted), aggregateAndWriteStatus returns an error that
// apierrors.IsConflict recognizes — so the caller maps it to a benign
// requeue instead of an ERROR-level "Reconciler error".
func TestAggregateAndWriteStatus_ConflictSurfacesAsConflict(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)

	// Force every status write to lose the optimistic-lock race so the
	// aggregator returns the terminal conflict RetryOnConflict surfaces.
	conflict := interceptor.Funcs{
		SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
				obj.GetName(), fmt.Errorf("the object has been modified"))
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir, pod0).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(conflict).
		Build()
	r := &Reconciler{Client: c, Log: logf.Log.WithName("test"), Expectations: workload.NewExpectations()}

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	err := r.aggregateAndWriteStatus(context.Background(), ir, nil, plan, nil)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(apierrors.IsConflict(err)).To(gomega.BeTrue(),
		"a terminal status conflict must remain recognizable so the caller can requeue instead of logging ERROR")
}

// withIRStuckPodGracePeriod swaps in a short grace window for the
// workload-side stuck-pod escalator so unit tests don't have to wait
// the production 60s. Restored on cleanup so parallel tests don't see
// each other's overrides.
func withIRStuckPodGracePeriod(t *testing.T, d time.Duration) {
	t.Helper()
	prev := workload.StuckPodFailureGracePeriod
	workload.StuckPodFailureGracePeriod = d
	t.Cleanup(func() { workload.StuckPodFailureGracePeriod = prev })
}

// TestAggregateStatus_BadImageSurge_EscalatesInstanceToFailed reproduces
// the IR-managed-path failure where a SurgeThenDrain surge pod stuck in
// ImagePullBackOff past StuckPodFailureGracePeriod must escalate the
// Instance Phase to Failed, the same way the omenative direct path does.
// Without this fix, the production cluster observes the Instance stuck at
// Phase=Updating for the full 30-minute InstanceReadyTimeout before the
// (now-wired) deadline backstop ExpireOperationsIfDeadlinePassed catches
// it; the fast-path escalator under test fails it within the stuck-pod
// grace window instead, so the parent ISVC's coordination layer sees
// Component.Failed promptly rather than the group sitting at Surging.
//
// Setup mirrors the operator-reported real-cluster shape:
//   - Instance idx=0 carries Phase=Updating with an in-flight
//     Operation{Type=Update, Step=Surge} (the surge stamp the workload
//     pipeline writes when starting SurgeThenDrain).
//   - Two pods at ordinal 0 / 1 (old serving + surge).
//   - The surge pod (ordinal 1) is past the grace window with
//     ImagePullBackOff in its ContainerStatuses.
//
// Expected post-Reconcile: Instance Phase flips to Failed via the
// workload-side EscalateStuckPodFailures escalator wired into the IR's
// aggregateAndWriteStatus. Without the wire-in the test fails: Phase
// stays at Updating because the IR aggregator never invokes the
// stuck-pod check the omenative direct path already runs.
func TestAggregateStatus_BadImageSurge_EscalatesInstanceToFailed(t *testing.T) {
	g := gomega.NewWithT(t)
	withIRStuckPodGracePeriod(t, 1*time.Millisecond)

	ir := baselineIR("llama-engine", "prod", 1)
	// Mid-surge state: Instance Phase=Updating with an in-flight Surge
	// step. Status pre-seeded so the workload dispatcher's
	// DetectUpdateTrigger sees the in-progress operation rather than
	// firing a fresh surge.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
			ActiveOrdinal:   0,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate,
				Step: "Surge",
				// Far-future deadline so the slow-path expiry
				// (production: 30 min) doesn't race the fast-path
				// escalator we're verifying.
				Deadline: metav1.NewTime(time.Now().Add(30 * time.Minute)),
			},
		},
	}
	// Old (canonical) pod at ordinal 0 — serving v1 traffic.
	oldPod := podForIR(ir, 0, "default", 0, true /*ready*/, true /*serving*/)
	// Surge pod at ordinal 1 — image pulled into ImagePullBackOff.
	// CreationTimestamp set well past the grace window so
	// PodStuckPullFailure's age check fires.
	surgePod := podForIR(ir, 0, "default", 1, false /*ready*/, false /*serving*/)
	surgePod.CreationTimestamp = metav1.NewTime(time.Now().Add(-5 * time.Second))
	surgePod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "ome-container",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
			},
		},
	}

	r, c := newReconciler(t, ir, oldPod, surgePod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"IR aggregator must run the workload stuck-pod escalator: surge pod stuck in ImagePullBackOff past grace must flip Instance Phase to Failed")
	// Operation is preserved as the breadcrumb operators read to find
	// which step wedged.
	g.Expect(got.Status.InstanceStatuses[0].Operation).NotTo(gomega.BeNil(),
		"Operation must be preserved on stuck-pod escalation so operators see what was in flight")
	g.Expect(got.Status.InstanceStatuses[0].Operation.Step).To(gomega.Equal("Surge"))
}

// TestAggregateStatus_StuckPodWithinGrace_NoEscalation pins the
// IR-path debounce: a stuck pod still inside StuckPodFailureGracePeriod
// must NOT escalate. Without this guard, a brief transient pull failure
// on a flaky registry would mis-classify a pod that's about to recover.
// Mirrors the equivalent omenative-direct-path test
// TestSurgeUpdate_ImagePullBackOff_WithinGrace_NoEscalation.
func TestAggregateStatus_StuckPodWithinGrace_NoEscalation(t *testing.T) {
	g := gomega.NewWithT(t)
	// Generous grace so the 5-second-old pod stays inside the window.
	withIRStuckPodGracePeriod(t, 60*time.Second)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceUpdating,
			RunningRevision: "rev-a",
			TargetRevision:  "rev-b",
			ActiveOrdinal:   0,
			Operation: &v1beta1.InstanceOperation{
				Type:     v1beta1.InstanceOperationUpdate,
				Step:     "Surge",
				Deadline: metav1.NewTime(time.Now().Add(30 * time.Minute)),
			},
		},
	}
	oldPod := podForIR(ir, 0, "default", 0, true, true)
	surgePod := podForIR(ir, 0, "default", 1, false, false)
	surgePod.CreationTimestamp = metav1.NewTime(time.Now().Add(-5 * time.Second))
	surgePod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "ome-container", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
	}
	r, c := newReconciler(t, ir, oldPod, surgePod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceUpdating),
			"stuck pod still inside grace window must NOT escalate (debounce holds)")
}

// TestAggregateStatus_DeadlineExpired_EscalatesInstanceToFailed pins the
// broad-backstop wire-in: an Instance stuck in a transient phase whose
// in-flight Operation.Deadline (InstanceReadyTimeout) has elapsed must
// flip to Phase=Failed via the IR aggregator's deadline backstop — even
// when no pod is in a terminal kubelet waiting state, so the fast-path
// stuck-pod escalator CANNOT be what fails it.
//
// This is the regression guard for the original bug: InstanceReadyTimeout
// was stamped onto Operation.Deadline but its consumer
// (ExpireOperationsIfDeadlinePassed) was never wired into the live IR
// path, so the timeout was inert and a never-Ready gang sat at Phase=Updating
// indefinitely.
func TestAggregateStatus_DeadlineExpired_EscalatesInstanceToFailed(t *testing.T) {
	g := gomega.NewWithT(t)
	// Generous stuck-pod grace so the fast path can't fire — the ONLY
	// thing that can fail this Instance is the deadline backstop under
	// test.
	withIRStuckPodGracePeriod(t, 60*time.Second)

	ir := baselineIR("llama-engine", "prod", 1)
	// Create-stuck Instance: Phase=Creating with an in-flight Create
	// operation whose Deadline already elapsed. patchInstanceStatusCreating
	// is idempotent (Phase=Creating + Op.Type=Create + Incarnation>0 → no
	// re-stamp), so the dispatcher's Create pass leaves the expired
	// Deadline intact for the backstop to read. A pod exists at the
	// canonical ordinal but never went Ready — the never-converging gang
	// the InstanceReadyTimeout is meant to bound.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:         0,
			Incarnation:   1,
			Phase:         v1beta1.OMENativeInstanceCreating,
			ActiveOrdinal: 0,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationCreate,
				Step: "CreatePods",
				// Deadline already in the past → the InstanceReadyTimeout
				// backstop must escalate this Instance.
				Deadline: metav1.NewTime(time.Now().Add(-1 * time.Minute)),
			},
		},
	}
	// Pod exists at the canonical ordinal but is not Ready (Pending) and
	// has NO terminal kubelet waiting reason, so the stuck-pod escalator
	// stays out of it.
	pendingPod := podForIR(ir, 0, "default", 0, false /*ready*/, false /*serving*/)

	r, c := newReconciler(t, ir, pendingPod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"IR aggregator must run the deadline backstop: an Operation.Deadline in the past must flip the Instance Phase to Failed")
	// Operation preserved as the breadcrumb operators read.
	g.Expect(got.Status.InstanceStatuses[0].Operation).NotTo(gomega.BeNil())
	// LastFailure records the DeadlineExceeded cause.
	g.Expect(got.Status.InstanceStatuses[0].LastFailure).NotTo(gomega.BeNil(),
		"deadline escalation must record a LastFailure diagnostic")
	g.Expect(got.Status.InstanceStatuses[0].LastFailure.Reason).
		To(gomega.Equal(workload.DeadlineExceededReason))
}

// TestAggregateStatus_GatedInstance_DeadlineParked_NotEscalated pins the
// admission-gating exemption: an Instance whose Operation.Deadline is in
// the past but whose pod is held by a scheduling gate (queued for
// admission, e.g. by Kueue) is NOT stuck — it is queued. The IR aggregator
// must park its deadline (zero it) so the backstop never fails it. Without
// this, every Kueue-gated OME workload would die at InstanceReadyTimeout.
func TestAggregateStatus_GatedInstance_DeadlineParked_NotEscalated(t *testing.T) {
	g := gomega.NewWithT(t)
	withIRStuckPodGracePeriod(t, 60*time.Second)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:         0,
			Incarnation:   1,
			Phase:         v1beta1.OMENativeInstanceCreating,
			ActiveOrdinal: 0,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationCreate,
				Step: "CreatePods",
				// Past deadline: would be escalated WERE the pod not gated.
				Deadline: metav1.NewTime(time.Now().Add(-1 * time.Minute)),
			},
		},
	}
	// Pod exists at the canonical ordinal but is held by a scheduling gate
	// (queued for admission), not Ready and not stuck.
	gatedPod := podForIR(ir, 0, "default", 0, false /*ready*/, false /*serving*/)
	gatedPod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}

	r, c := newReconciler(t, ir, gatedPod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		NotTo(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"a Kueue-gated Instance is queued, not stuck: the deadline backstop must NOT fail it")
	g.Expect(got.Status.InstanceStatuses[0].Operation).NotTo(gomega.BeNil())
	g.Expect(got.Status.InstanceStatuses[0].Operation.Deadline.IsZero()).
		To(gomega.BeTrue(),
			"a gated Instance's deadline must be parked (zeroed) so it never expires while queued")
}

// TestAggregateStatus_PausedInstance_DeadlineParkedAndRearmed verifies that
// pausing an in-flight operation is a real circuit breaker: neither terminal
// pod state nor an elapsed deadline may fail it while paused, and unpausing
// restarts the timeout window instead of expiring the old deadline.
func TestAggregateStatus_PausedInstance_DeadlineParkedAndRearmed(t *testing.T) {
	g := gomega.NewWithT(t)
	withIRStuckPodGracePeriod(t, time.Millisecond)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Paused = true
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:         0,
		Incarnation:   1,
		Phase:         v1beta1.OMENativeInstanceCreating,
		ActiveOrdinal: 0,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationCreate,
			Step:     "CreatePods",
			Deadline: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
	}}
	stuckPod := podForIR(ir, 0, "default", 0, false, false)
	stuckPod.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
	stuckPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "ome-container",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: "CrashLoopBackOff",
		}},
	}}

	r, c := newReconciler(t, ir, stuckPod)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	paused := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, paused)).To(gomega.Succeed())
	g.Expect(paused.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceCreating))
	g.Expect(paused.Status.InstanceStatuses[0].LastFailure).To(gomega.BeNil())
	g.Expect(paused.Status.InstanceStatuses[0].Operation.Deadline.IsZero()).To(gomega.BeTrue())

	// Simulate the container recovering while the operator investigates. The
	// unpause assertion below isolates deadline rearming from the independent
	// stuck-pod fast-failure policy.
	recoveredPod := &corev1.Pod{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(stuckPod), recoveredPod)).To(gomega.Succeed())
	recoveredPod.Status.ContainerStatuses = nil
	g.Expect(c.Status().Update(context.Background(), recoveredPod)).To(gomega.Succeed())

	paused.Spec.Paused = false
	g.Expect(c.Update(context.Background(), paused)).To(gomega.Succeed())
	beforeUnpause := time.Now()
	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	unpaused := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, unpaused)).To(gomega.Succeed())
	g.Expect(unpaused.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceCreating))
	g.Expect(unpaused.Status.InstanceStatuses[0].Operation.Deadline.Time).To(gomega.BeTemporally(">", beforeUnpause))
}

// TestAggregateStatus_DeadlineFuture_NoEscalation pins the negative
// case: a transient-phase Instance whose Operation.Deadline is still in
// the future must NOT be escalated by the deadline backstop.
func TestAggregateStatus_DeadlineFuture_NoEscalation(t *testing.T) {
	g := gomega.NewWithT(t)
	withIRStuckPodGracePeriod(t, 60*time.Second)

	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:         0,
			Incarnation:   1,
			Phase:         v1beta1.OMENativeInstanceCreating,
			ActiveOrdinal: 0,
			Operation: &v1beta1.InstanceOperation{
				Type:     v1beta1.InstanceOperationCreate,
				Step:     "CreatePods",
				Deadline: metav1.NewTime(time.Now().Add(30 * time.Minute)),
			},
		},
	}
	pendingPod := podForIR(ir, 0, "default", 0, false, false)

	r, c := newReconciler(t, ir, pendingPod)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].Phase).
		To(gomega.Equal(v1beta1.OMENativeInstanceCreating),
			"Operation.Deadline in the future must NOT escalate (backstop respects the timeout window)")
}
