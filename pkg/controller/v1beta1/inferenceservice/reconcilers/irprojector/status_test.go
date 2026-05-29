package irprojector

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// optInISVC returns the baseline ISVC with the per-Component opt-in
// annotation set on engine. Decoder/Router are nil so the aggregator
// only looks up the engine IR.
func optInISVC(name, namespace string) *v1beta1.InferenceService {
	isvc := baselineISVC(name, namespace)
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.UseIRManagedAnnotationKey: constants.UseIRManagedAnnotationVal,
	}
	return isvc
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
			ParentRef: v1beta1.ParentReference{Name: parentName, UID: "uid"},
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

// TestAggregateIRStatus_NoOptIn_NoOp pins the gate: ISVC without the
// IR-managed annotation skips the aggregator entirely. The status
// subtree must NOT be populated from any IR (which may not even
// exist).
func TestAggregateIRStatus_NoOptIn_NoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	// Pre-populate the ISVC status with a sentinel so we can detect
	// any accidental overwrite.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{Replicas: 99},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(isvc).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc)).To(gomega.Succeed())

	// The pre-populated sentinel must survive — we didn't touch the status.
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].OMENative).NotTo(gomega.BeNil())
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].OMENative.Replicas).To(gomega.Equal(int32(99)),
		"omenative direct-path status sentinel must NOT be overwritten when annotation is absent")
}

// TestAggregateIRStatus_OptIn_NoIR_NoErr pins the first-reconcile
// race: opt-in is set but the IR hasn't been created yet (the
// projector's CreateOrUpdate landed but the cache hasn't observed
// it yet). AggregateIRStatus must NOT error — the next reconcile
// will pick up the IR.
func TestAggregateIRStatus_OptIn_NoIR_NoErr(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := optInISVC("llama", "prod")
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(isvc).
		Build()

	err := AggregateIRStatus(context.Background(), c, isvc)
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"missing IR on first reconcile after opt-in must be a clean no-op")
}

// TestAggregateIRStatus_OptIn_CopiesStatusFromIR pins the happy
// path: opt-in is set, IR exists with realistic Status. The
// aggregator must write a 1:1 copy of IR.Status into
// ISVC.Status.Components[engine].OMENative.
func TestAggregateIRStatus_OptIn_CopiesStatusFromIR(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := optInISVC("llama", "prod")
	ir := liveIR("llama", "prod", 3)
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc)).To(gomega.Succeed())

	// Read the post-write ISVC from the apiserver — the aggregator
	// writes via Status().Update so we can't rely on the in-memory
	// snapshot alone.
	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	om := got.Status.Components[v1beta1.EngineComponent].OMENative
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
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].OMENative).NotTo(gomega.BeNil())
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].OMENative.Replicas).To(gomega.Equal(int32(3)))
}

// TestAggregateIRStatus_OptIn_OnlyTouchesOptedComponents pins the
// per-Component isolation: when engine is IR-managed but decoder is
// direct-path, the aggregator must NOT touch the decoder OMENative
// subtree (the omenative direct path is the sole writer for it).
func TestAggregateIRStatus_OptIn_OnlyTouchesOptedComponents(t *testing.T) {
	g := gomega.NewWithT(t)
	min := 1
	isvc := optInISVC("llama", "prod")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &min},
	}
	// Decoder explicitly does NOT have the opt-in annotation.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.DecoderComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{Replicas: 42}, // sentinel from direct path
		},
	}
	ir := liveIR("llama", "prod", 2)

	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir).
		Build()

	g.Expect(AggregateIRStatus(context.Background(), c, isvc)).To(gomega.Succeed())

	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	// Engine: mirrored from IR.
	g.Expect(got.Status.Components[v1beta1.EngineComponent].OMENative).NotTo(gomega.BeNil())
	g.Expect(got.Status.Components[v1beta1.EngineComponent].OMENative.Replicas).To(gomega.Equal(int32(2)))

	// Decoder: untouched — sentinel survives.
	g.Expect(got.Status.Components[v1beta1.DecoderComponent].OMENative).NotTo(gomega.BeNil(),
		"decoder OMENative subtree must NOT be cleared by the IR-managed engine path")
	g.Expect(got.Status.Components[v1beta1.DecoderComponent].OMENative.Replicas).To(gomega.Equal(int32(42)),
		"decoder direct-path sentinel must survive the IR-managed engine aggregation")
}

// TestAggregateIRStatus_NilClient_Rejected pins the validation
// guard: nil client at the aggregator means the dispatch site
// dropped r.Client.
func TestAggregateIRStatus_NilClient_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := optInISVC("llama", "prod")
	err := AggregateIRStatus(context.Background(), nil, isvc)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("nil client"))
}

// TestAggregateIRStatus_NilISVC_Rejected pins the validation
// guard: nil ISVC at the aggregator means the dispatch site
// dropped the reconcile target.
func TestAggregateIRStatus_NilISVC_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	err := AggregateIRStatus(context.Background(), c, nil)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("nil ISVC"))
}

// TestAggregateIRStatus_IROptIn_PreservesNonOMENativeFields pins
// the field-isolation contract: the aggregator only touches
// ISVC.Status.Components[<comp>].OMENative — other fields on the
// ComponentStatusSpec (URL, RestURL, Address, RolloutPhase) must
// survive.
func TestAggregateIRStatus_IROptIn_PreservesNonOMENativeFields(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := optInISVC("llama", "prod")
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

	g.Expect(AggregateIRStatus(context.Background(), c, isvc)).To(gomega.Succeed())

	got := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, got)).To(gomega.Succeed())

	cs := got.Status.Components[v1beta1.EngineComponent]
	g.Expect(cs.LatestReadyRevision).To(gomega.Equal("rev-1"),
		"non-OMENative ComponentStatusSpec fields must survive aggregation")
	g.Expect(cs.OMENative).NotTo(gomega.BeNil())
	g.Expect(cs.OMENative.Replicas).To(gomega.Equal(int32(1)))
}

// TestIrStatusToComponentStatus_DeepCopiesInstanceStatuses pins the
// aliasing guard: the IR.Status.InstanceStatuses slice must not be
// aliased into the OMENativeComponentStatus — downstream mutations
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
	out := irStatusToComponentStatus(ir)
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
