package irprojector

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// engineOMENativeModes is the per-Component mode map for tests whose
// engine flows through the IR-managed path. Mirrors what
// controller.go builds from isvcutils.DetermineDeploymentModes after
// the OMENative cutover.
var engineOMENativeModes = map[v1beta1.ComponentType]constants.DeploymentModeType{
	v1beta1.EngineComponent: constants.OMENative,
}

// engineDirectModes is the per-Component mode map for tests whose
// engine flows through the legacy direct path (Raw / MultiNode /
// missing). Drives the "skip aggregation" branch.
var engineDirectModes = map[v1beta1.ComponentType]constants.DeploymentModeType{
	v1beta1.EngineComponent: constants.RawDeployment,
}

// liveIR returns an IR fixture with realistic Status fields the
// aggregator should mirror onto the ISVC.
func liveIR(parentName, namespace string, replicas int32) *v1beta1.InferenceReplica {
	cc := int32(0)
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      parentName + "-engine",
			Namespace: namespace,
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: parentName},
			Component: v1beta1.EngineComponent,
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   1,
			Replicas:             replicas,
			ReadyReplicas:        replicas,
			ServingReplicas:      replicas,
			AvailableReplicas:    replicas,
			UpdatedReplicas:      replicas,
			UpdatedReadyReplicas: replicas,
			CurrentRevision:      "llama-engine-abc123",
			UpdateRevision:       "llama-engine-abc123",
			CollisionCount:       &cc,
			LabelSelector:        "ome.io/inferenceservice=llama,component=engine,ome.io/managed-by=OMENative",
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
				{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, ActiveOrdinal: 0},
			},
			Conditions: []metav1.Condition{
				{Type: "Available", Status: metav1.ConditionTrue, Reason: "AllReady"},
			},
		},
	}
}

// TestAggregateIRStatus_NonOMENativeMode_NoOp pins the gate: when a
// Component's resolved deployment mode is NOT OMENative, the
// aggregator skips it entirely. The status subtree must NOT be
// populated from any IR (which may not even exist).
func TestAggregateIRStatus_NonOMENativeMode_NoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	// Pre-populate the ISVC status with a sentinel so we can detect
	// any accidental overwrite.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 99},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(isvc).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc, engineDirectModes)).To(gomega.Succeed())

	// The pre-populated sentinel must survive — we didn't touch the status.
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(gomega.BeNil())
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.Replicas).To(gomega.Equal(int32(99)),
		"direct-path status sentinel must NOT be overwritten when the resolved mode is not OMENative")
}

// TestAggregateIRStatus_OMENative_NoIR_NoErr pins the first-reconcile
// race: engine resolves to OMENative but the IR hasn't been created
// yet (the projector's CreateOrUpdate landed but the cache hasn't
// observed it yet). AggregateIRStatus must NOT error — the next
// reconcile will pick up the IR.
func TestAggregateIRStatus_OMENative_NoIR_NoErr(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(isvc).
		Build()

	err := AggregateIRStatus(context.Background(), c, isvc, engineOMENativeModes)
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"missing IR on first reconcile after OMENative dispatch must be a clean no-op")
}

// TestAggregateIRStatus_OMENative_CopiesStatusFromIR pins the happy
// path: engine resolves to OMENative, IR exists with realistic
// Status. The aggregator must write a 1:1 copy of IR.Status into
// ISVC.Status.Components[engine].Lifecycle.
func TestAggregateIRStatus_OMENative_CopiesStatusFromIR(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	ir := liveIR("llama", "prod", 3)
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc, engineOMENativeModes)).To(gomega.Succeed())

	// Read the post-write ISVC from the apiserver — the aggregator
	// writes via Status().Update so we can't rely on the in-memory
	// snapshot alone.
	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	om := got.Status.Components[v1beta1.EngineComponent].Lifecycle
	g.Expect(om).NotTo(gomega.BeNil(),
		"OMENative subtree must be populated for IR-managed Component")
	g.Expect(om.Replicas).To(gomega.Equal(int32(3)),
		"Replicas counter must mirror IR.Status.Replicas")
	g.Expect(om.ReadyReplicas).To(gomega.Equal(int32(3)))
	g.Expect(om.ServingReplicas).To(gomega.Equal(int32(3)))
	g.Expect(om.AvailableReplicas).To(gomega.Equal(int32(3)))
	g.Expect(om.UpdatedReplicas).To(gomega.Equal(int32(3)))
	g.Expect(om.UpdatedReadyReplicas).To(gomega.Equal(int32(3)))
	g.Expect(om.CurrentRevision).To(gomega.Equal("llama-engine-abc123"))
	g.Expect(om.UpdateRevision).To(gomega.Equal("llama-engine-abc123"))
	g.Expect(om.LabelSelector).To(gomega.ContainSubstring("component=engine"))
	g.Expect(om.CollisionCount).NotTo(gomega.BeNil())
	g.Expect(*om.CollisionCount).To(gomega.Equal(int32(0)))
	g.Expect(om.InstanceStatuses).To(gomega.HaveLen(1))
	g.Expect(om.InstanceStatuses[0].Index).To(gomega.Equal(int32(0)))
	g.Expect(om.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceReady))
	g.Expect(om.Conditions).To(gomega.HaveLen(1))
	g.Expect(om.Conditions[0].Type).To(gomega.Equal("Available"))

	// Aggregator must also mirror onto the in-memory ISVC so
	// downstream code (e.g. the outer updateStatus equality
	// short-circuit) observes the post-write state.
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(gomega.BeNil())
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.Replicas).To(gomega.Equal(int32(3)))
}

// TestAggregateIRStatus_OMENative_OnlyTouchesOMENativeComponents pins
// the per-Component isolation: when engine resolves to OMENative but
// decoder resolves to Raw/MultiNode, the aggregator must NOT touch
// the decoder OMENative subtree (the omenative direct path is the
// sole writer for non-OMENative-mode Components — defensive
// preservation in case downstream code re-uses the OMENative subtree
// shape).
func TestAggregateIRStatus_OMENative_OnlyTouchesOMENativeComponents(t *testing.T) {
	g := gomega.NewWithT(t)
	min := 1
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &min},
	}
	// Decoder's resolved mode is intentionally NOT OMENative.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 42}, // sentinel from direct path
		},
	}
	ir := liveIR("llama", "prod", 2)

	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir).
		Build()

	componentModes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent:  constants.OMENative,
		v1beta1.DecoderComponent: constants.RawDeployment,
	}
	g.Expect(AggregateIRStatus(context.Background(), c, isvc, componentModes)).To(gomega.Succeed())

	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	// Engine: mirrored from IR.
	g.Expect(got.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(gomega.BeNil())
	g.Expect(got.Status.Components[v1beta1.EngineComponent].Lifecycle.Replicas).To(gomega.Equal(int32(2)))

	// Decoder: untouched — sentinel survives.
	g.Expect(got.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(gomega.BeNil(),
		"decoder OMENative subtree must NOT be cleared by the IR-managed engine path")
	g.Expect(got.Status.Components[v1beta1.DecoderComponent].Lifecycle.Replicas).To(gomega.Equal(int32(42)),
		"decoder direct-path sentinel must survive the IR-managed engine aggregation")
}

// TestAggregateIRStatus_NilClient_Rejected pins the validation
// guard: nil client at the aggregator means the dispatch site
// dropped r.Client.
func TestAggregateIRStatus_NilClient_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	err := AggregateIRStatus(context.Background(), nil, isvc, engineOMENativeModes)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("nil client"))
}

// TestAggregateIRStatus_NilISVC_Rejected pins the validation
// guard: nil ISVC at the aggregator means the dispatch site
// dropped the reconcile target.
func TestAggregateIRStatus_NilISVC_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	err := AggregateIRStatus(context.Background(), c, nil, engineOMENativeModes)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("nil ISVC"))
}

// TestAggregateIRStatus_OMENative_PreservesNonOMENativeFields pins
// the field-isolation contract: the aggregator only touches
// ISVC.Status.Components[<comp>].Lifecycle — other fields on the
// ComponentStatusSpec (URL, RestURL, Address, RolloutPhase) must
// survive.
func TestAggregateIRStatus_OMENative_PreservesNonOMENativeFields(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			// Pre-existing non-OMENative fields the outer reconciler
			// owns. The aggregator must not clear them.
			LatestReadyRevision: "rev-1",
		},
	}
	ir := liveIR("llama", "prod", 1)
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc, engineOMENativeModes)).To(gomega.Succeed())

	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	cs := got.Status.Components[v1beta1.EngineComponent]
	g.Expect(cs.LatestReadyRevision).To(gomega.Equal("rev-1"),
		"non-OMENative ComponentStatusSpec fields must survive aggregation")
	g.Expect(cs.Lifecycle).NotTo(gomega.BeNil())
	g.Expect(cs.Lifecycle.Replicas).To(gomega.Equal(int32(1)))
}

// TestAggregateIRStatus_EmitsEngineReadyCondition pins the bug fix: the
// IR-managed path must emit the top-level EngineReady condition derived
// from the OMENative counters, mirroring what the legacy direct path
// does (omenative/status_aggregate.go:140-143). Without it, the aggregate
// Ready rollup stays Unknown forever even though the subtree counters
// are correct.
//
// Real-cluster symptom (round-2 testing): a PD-disagg-shaped single-engine
// ISVC in OMENative mode had all subtree fields populated (replicas=1,
// updatedReplicas=1, InstanceStatuses[0].Phase=Ready) and IngressReady=True
// — but ISVC.Status.Conditions carried only IngressReady + Ready=Unknown.
// No EngineReady was emitted, so the top-level Ready aggregator never
// flipped.
func TestAggregateIRStatus_EmitsEngineReadyCondition(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	// liveIR with replicas=1, all-ready counters — the same shape the
	// real-cluster ISVC was in when the bug was observed.
	ir := liveIR("llama", "prod", 1)
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc, engineOMENativeModes)).To(gomega.Succeed())

	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	// Existing subtree assertion: the OMENative subtree must still be
	// populated (regression guard against accidentally moving the
	// subtree write out of the closure).
	om := got.Status.Components[v1beta1.EngineComponent].Lifecycle
	g.Expect(om).NotTo(gomega.BeNil(), "OMENative subtree must be populated")
	g.Expect(om.Replicas).To(gomega.Equal(int32(1)))
	g.Expect(om.ReadyReplicas).To(gomega.Equal(int32(1)))

	// NEW: the EngineReady top-level condition must be emitted with
	// Status=True. This is the bug fix.
	cond := got.Status.GetCondition(v1beta1.EngineReady)
	g.Expect(cond).NotTo(gomega.BeNil(),
		"EngineReady condition must be emitted alongside the OMENative subtree")
	g.Expect(cond.Status).To(gomega.Equal(corev1.ConditionTrue),
		"EngineReady must be True when ReadyReplicas == Replicas > 0")
	g.Expect(cond.Reason).To(gomega.Equal("Ready"))

	// Mirror onto the in-memory ISVC: the outer reconciler's updateStatus
	// flush re-copies p.ISVC.Status onto a fresh re-read of the cluster,
	// so without the mirror the value we just committed gets clobbered
	// on the next pass.
	inMemCond := isvc.Status.GetCondition(v1beta1.EngineReady)
	g.Expect(inMemCond).NotTo(gomega.BeNil(),
		"EngineReady must be mirrored onto the in-memory ISVC for the outer reconciler to observe")
	g.Expect(inMemCond.Status).To(gomega.Equal(corev1.ConditionTrue))
}

// TestAggregateIRStatus_EmitsEngineReadyConditionFalse pins the
// not-yet-ready branch: when the IR reports Replicas=1, ReadyReplicas=0,
// EngineReady must be emitted with Status=False / Reason=NotAllReady.
// Mirrors the legacy direct path's TopLevelComponentReadyFromLifecycle
// derivation for the same input.
func TestAggregateIRStatus_EmitsEngineReadyConditionFalse(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	ir := liveIR("llama", "prod", 1)
	// Pull ReadyReplicas down to 0 — pod not yet Ready.
	ir.Status.ReadyReplicas = 0
	// Also down the per-Instance Phase so callers reading
	// InstanceStatuses see the partial-ready state.
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceCreating
	ir.Status.InstanceStatuses[0].ReadyPodCount = 0
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc, engineOMENativeModes)).To(gomega.Succeed())

	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	cond := got.Status.GetCondition(v1beta1.EngineReady)
	g.Expect(cond).NotTo(gomega.BeNil(),
		"EngineReady must be emitted even when not all replicas are Ready")
	g.Expect(cond.Status).To(gomega.Equal(corev1.ConditionFalse),
		"EngineReady must be False when ReadyReplicas < Replicas")
	g.Expect(cond.Reason).To(gomega.Equal("NotAllReady"))
}

// TestAggregateIRStatus_MultiComponent_EmitsBothConditions pins the
// per-Component independence: with engine + decoder both in OMENative
// mode and both IRs reporting all-ready, both EngineReady and
// DecoderReady conditions must flip True. The aggregator runs once per
// Component; each one independently emits its own condition.
func TestAggregateIRStatus_MultiComponent_EmitsBothConditions(t *testing.T) {
	g := gomega.NewWithT(t)
	min := 1
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &min},
	}

	engineIR := liveIR("llama", "prod", 1)
	// Build the decoder IR by hand — liveIR hard-codes the engine name
	// + EngineComponent label; for decoder we need <isvc>-decoder.
	cc := int32(0)
	decoderIR := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-decoder",
			Namespace: "prod",
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: "llama"},
			Component: v1beta1.DecoderComponent,
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   1,
			Replicas:             2,
			ReadyReplicas:        2,
			ServingReplicas:      2,
			AvailableReplicas:    2,
			UpdatedReplicas:      2,
			UpdatedReadyReplicas: 2,
			CurrentRevision:      "llama-decoder-def456",
			UpdateRevision:       "llama-decoder-def456",
			CollisionCount:       &cc,
			LabelSelector:        "ome.io/inferenceservice=llama,component=decoder,ome.io/managed-by=OMENative",
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
				{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1, ReadyPodCount: 1},
				{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, PodCount: 1, ReadyPodCount: 1},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, engineIR, decoderIR).
		Build()

	componentModes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent:  constants.OMENative,
		v1beta1.DecoderComponent: constants.OMENative,
	}
	g.Expect(AggregateIRStatus(context.Background(), c, isvc, componentModes)).To(gomega.Succeed())

	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	// Engine subtree + condition.
	g.Expect(got.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(gomega.BeNil())
	g.Expect(got.Status.Components[v1beta1.EngineComponent].Lifecycle.Replicas).To(gomega.Equal(int32(1)))
	engineCond := got.Status.GetCondition(v1beta1.EngineReady)
	g.Expect(engineCond).NotTo(gomega.BeNil(), "EngineReady must be emitted")
	g.Expect(engineCond.Status).To(gomega.Equal(corev1.ConditionTrue))

	// Decoder subtree + condition.
	g.Expect(got.Status.Components[v1beta1.DecoderComponent].Lifecycle).NotTo(gomega.BeNil())
	g.Expect(got.Status.Components[v1beta1.DecoderComponent].Lifecycle.Replicas).To(gomega.Equal(int32(2)))
	decoderCond := got.Status.GetCondition(v1beta1.DecoderReady)
	g.Expect(decoderCond).NotTo(gomega.BeNil(), "DecoderReady must be emitted")
	g.Expect(decoderCond.Status).To(gomega.Equal(corev1.ConditionTrue))
}

// TestIrStatusToComponentStatus_DeepCopiesInstanceStatuses pins the
// aliasing guard: the IR.Status.InstanceStatuses slice must not be
// aliased into the LifecycleStatus — downstream mutations
// would clobber the IR controller's source-of-truth.
func TestIrStatusToComponentStatus_DeepCopiesInstanceStatuses(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := &v1beta1.InferenceReplica{
		Status: v1beta1.InferenceReplicaStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
				{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			},
			CollisionCount: ptr.To(int32(5)),
		},
	}
	out := IRStatusToComponentStatus(ir)
	g.Expect(out.InstanceStatuses).To(gomega.HaveLen(1))

	// Mutate the output, verify the IR is unaffected.
	out.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed
	g.Expect(ir.Status.InstanceStatuses[0].Phase).To(gomega.Equal(v1beta1.OMENativeInstanceReady),
		"mutating the output InstanceStatuses must NOT alias back to the IR")

	// CollisionCount pointer must NOT be aliased.
	*out.CollisionCount = 99
	g.Expect(*ir.Status.CollisionCount).To(gomega.Equal(int32(5)),
		"mutating the output CollisionCount must NOT alias back to the IR")
}
