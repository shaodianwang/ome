package inferencereplica

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// TestAggregateStatus_ReadyReplicas_CountsReadyInstances pins the
// Phase-based ReadyReplicas rollup. Three Instances — two Phase=Ready,
// one Phase=Updating — should produce ReadyReplicas=2 regardless of
// ReadyPodCount, the field the legacy workload counter reads.
//
// Regression guard for the round-2 cluster bug: PR #245 declared the
// printcolumn / scale-subresource path off .status.readyReplicas, but
// the IR aggregator computed ReadyReplicas off ReadyPodCount — a field
// the IR controller never populates today. `kubectl get irep` showed an
// empty READY column even after Instances flipped Phase=Ready.
func TestAggregateStatus_ReadyReplicas_CountsReadyInstances(t *testing.T) {
	g := gomega.NewWithT(t)
	insts := []v1beta1.OMENativeInstanceStatus{
		// ReadyPodCount=0 on every Instance proves the new rollup ignores
		// the legacy per-pod counter and reads Phase only.
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1, ReadyPodCount: 0},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1, ReadyPodCount: 0},
		{Index: 2, Phase: v1beta1.OMENativeInstanceUpdating, PodCount: 1, ReadyPodCount: 0},
	}
	g.Expect(countReadyInstancesByPhase(insts)).To(gomega.Equal(int32(2)),
		"ReadyReplicas should count Instances with Phase=Ready (2), ignoring ReadyPodCount")
}

// TestAggregateStatus_ReadyReplicas_ZeroWhenAllPending pins the
// zero-Ready edge case: every Instance Phase=Pending → ReadyReplicas=0.
// Catches a future bug where a misplaced default would surface a stale
// "1" or "match Replicas" answer.
func TestAggregateStatus_ReadyReplicas_ZeroWhenAllPending(t *testing.T) {
	g := gomega.NewWithT(t)
	insts := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstancePending, PodCount: 1},
		{Index: 1, Phase: v1beta1.OMENativeInstancePending, PodCount: 1},
	}
	g.Expect(countReadyInstancesByPhase(insts)).To(gomega.Equal(int32(0)),
		"ReadyReplicas must be 0 when no Instance has Phase=Ready")
}

// TestAggregateStatus_AvailableReplicas_MatchesReadyForSimpleCase pins
// the AvailableReplicas == ReadyReplicas contract at v1 via the end-to-
// end reconcile path. No minReadySeconds gating field exists yet on
// InferenceReplicaSpec / LifecycleSpec; the two counters collapse onto
// the same value. When a gating field is added, this test will need an
// explicit divergence case alongside.
//
// Goes through the real Reconcile so the assertion holds for the full
// status-write path, not just the helper. Mirrors the existing
// TestReconcile_StatusAggregator_RollsUpCounters setup (real pods +
// pre-seeded Instance statuses) so the workload pipeline observes the
// expected state instead of restarting Instances.
func TestAggregateStatus_AvailableReplicas_MatchesReadyForSimpleCase(t *testing.T) {
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
	r, c := newReconciler(t, ir, pod0, pod1, pod2)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(got.Status.ReadyReplicas),
		"AvailableReplicas must equal ReadyReplicas while no minReadySeconds gating exists")
	g.Expect(got.Status.AvailableReplicas).To(gomega.Equal(int32(2)),
		"AvailableReplicas should be 2 (matches the 2 Ready Instances)")
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
	cond := computeReadyCondition(status)
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
	cond := computeReadyCondition(status)
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
		CurrentRevision: "rev-a",
		UpdateRevision:  "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a"},
			// Pending (not Failed) so the InstanceFailed override doesn't
			// fire; we want the ReplicaCountMismatch branch.
			{Index: 1, Phase: v1beta1.OMENativeInstancePending, RunningRevision: "rev-a"},
		},
	}
	cond := computeReadyCondition(status)
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse),
		"Ready=False when ReadyReplicas < Replicas with no rollout in flight")
	g.Expect(cond.Reason).To(gomega.Equal(ReasonReplicaCountMismatch))
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
	cond := computeReadyCondition(status)
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
	cond := computeReadyCondition(status)
	g.Expect(cond.ObservedGeneration).To(gomega.Equal(int64(7)),
		"Ready condition must stamp ObservedGeneration off status.ObservedGeneration")
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
