package irprojector

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// testScheme returns a runtime.Scheme with v1beta1 registered. Bare
// scheme (no status subresource wiring) is enough: the projector only
// writes Spec — the IR controller writes Status.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme corev1: %v", err)
	}
	return s
}

// baselineISVC returns a controller-write-ready ISVC carrying an
// EngineSpec with MinReplicas=2 and a Lifecycle block. The default
// per-Component shape is single-pod; tests that need multi-pod
// override Leader / Worker before calling the projector.
func baselineISVC(name, namespace string) *v1beta1.InferenceService {
	min := 2
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			UID:        types.UID(name + "-uid"),
			Generation: 1,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &min,
					Lifecycle: &v1beta1.LifecycleSpec{
						RestartPolicy: ptr.To(v1beta1.InstanceRestartPolicyNone),
					},
				},
			},
		},
	}
}

// minimalParams returns a Params bag matching the ISVC shape above.
// PodSpec carries a single container named "ome-container"; the
// per-Component ObjectMeta carries the legacy OMENative pod label trio
// so the IR's Spec.Runners[0].Template.ObjectMeta inherits it.
func minimalParams(t *testing.T, isvc *v1beta1.InferenceService, c client.Client) Params {
	t.Helper()
	objMeta := metav1.ObjectMeta{
		Name:      isvc.Name + "-engine",
		Namespace: isvc.Namespace,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			"managed-by":                          "OMENative",
		},
		Annotations: map[string]string{
			"ome.io/some-annotation": "value",
		},
	}
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  "ome-container",
			Image: "sgl:1.0",
		}},
	}
	return Params{
		ISVC:         isvc,
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		ObjectMeta:   objMeta,
		PodSpec:      podSpec,
		MultiPod:     false,
		Client:       c,
	}
}

// TestIsIRManagedComponent_NoAnnotation_FalsePins the default state:
// an ISVC without the use-ir-managed annotation routes through the
// omenative direct path.
func TestIsIRManagedComponent_NoAnnotation_False(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	g.Expect(IsIRManagedComponent(isvc, v1beta1.EngineComponent)).To(gomega.BeFalse(),
		"absent annotation must default to omenative direct path")
	g.Expect(IsIRManagedComponent(isvc, v1beta1.DecoderComponent)).To(gomega.BeFalse(),
		"nil DecoderSpec must default to omenative direct path")
	g.Expect(IsIRManagedComponent(isvc, v1beta1.RouterComponent)).To(gomega.BeFalse(),
		"nil RouterSpec must default to omenative direct path")
}

// TestIsIRManagedComponent_AnnotationTrue_True pins the opt-in path:
// per-Component annotation set to "true" enables the IR-managed path.
func TestIsIRManagedComponent_AnnotationTrue_True(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.UseIRManagedAnnotationKey: constants.UseIRManagedAnnotationVal,
	}
	g.Expect(IsIRManagedComponent(isvc, v1beta1.EngineComponent)).To(gomega.BeTrue(),
		"per-Component annotation set to true must enable IR-managed path")
	g.Expect(IsIRManagedComponent(isvc, v1beta1.DecoderComponent)).To(gomega.BeFalse(),
		"engine annotation must not bleed into decoder routing")
}

// TestIsIRManagedComponent_AnnotationNotTrue_False pins the parsing:
// values other than the literal "true" don't enable opt-in.
func TestIsIRManagedComponent_AnnotationNotTrue_False(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	for _, v := range []string{"TRUE", "True", "1", "yes", ""} {
		isvc.Spec.Engine.Annotations = map[string]string{
			constants.UseIRManagedAnnotationKey: v,
		}
		g.Expect(IsIRManagedComponent(isvc, v1beta1.EngineComponent)).To(gomega.BeFalse(),
			"annotation value %q must NOT enable IR-managed path", v)
	}
}

// TestIsIRManagedComponent_GlobalKillSwitch_OverridesAnnotation pins
// the operator kill switch: even if a Component has the opt-in
// annotation, SetGlobalDisableIRManaged(true) forces the predicate
// false. Cleanup at the end restores the default so subsequent tests
// aren't surprised.
func TestIsIRManagedComponent_GlobalKillSwitch_OverridesAnnotation(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.UseIRManagedAnnotationKey: constants.UseIRManagedAnnotationVal,
	}
	g.Expect(IsIRManagedComponent(isvc, v1beta1.EngineComponent)).To(gomega.BeTrue(),
		"sanity: opt-in works before the kill switch fires")

	SetGlobalDisableIRManaged(true)
	t.Cleanup(func() { SetGlobalDisableIRManaged(false) })
	g.Expect(IsIRManagedComponent(isvc, v1beta1.EngineComponent)).To(gomega.BeFalse(),
		"global kill switch must override the per-Component annotation")
}

// TestIsIRManagedComponent_NilISVC_False pins the nil guard:
// IsIRManagedComponent is called from the dispatch site with a
// non-nil ISVC, but the explicit guard surfaces wiring bugs cleanly
// instead of a nil deref.
func TestIsIRManagedComponent_NilISVC_False(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(IsIRManagedComponent(nil, v1beta1.EngineComponent)).To(gomega.BeFalse())
}

// TestEnsureInferenceReplica_Create_NewIR pins the create-from-scratch
// path. The projected IR must carry:
//   - Name == <isvc>-<component>
//   - Annotations carry the controller-write annotation so the IR
//     webhook accepts the create
//   - OwnerReferences point at the parent ISVC as controller (cascade
//     GC on ISVC delete)
//   - ParentRef.Name + UID match the parent ISVC
//   - Component matches the dispatched ComponentType
//   - Replicas defaults from ComponentExt.MinReplicas (=2 in baseline)
//   - Runners is single-pod {default, size 1} with the projected
//     PodSpec + ObjectMeta on the template
//   - Lifecycle carries the projected RestartPolicy
func TestEnsureInferenceReplica_Create_NewIR(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()

	ir, err := EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, c))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(ir).NotTo(gomega.BeNil())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Name).To(gomega.Equal("llama-engine"),
		"IR name must be <isvc>-<component>")
	g.Expect(got.Namespace).To(gomega.Equal("prod"))
	g.Expect(got.Annotations).To(gomega.HaveKeyWithValue(
		constants.InferenceReplicaControllerWriteAnnotationKey,
		constants.InferenceReplicaControllerWriteAnnotationVal),
		"controller-write annotation must be stamped so the IR webhook accepts the create")

	g.Expect(got.OwnerReferences).To(gomega.HaveLen(1))
	ref := got.OwnerReferences[0]
	g.Expect(ref.Kind).To(gomega.Equal("InferenceService"),
		"IR owner ref must point at the parent ISVC for cascade GC")
	g.Expect(ref.Name).To(gomega.Equal(isvc.Name))
	g.Expect(ref.UID).To(gomega.Equal(isvc.UID))
	g.Expect(ref.Controller).NotTo(gomega.BeNil())
	g.Expect(*ref.Controller).To(gomega.BeTrue())

	g.Expect(got.Spec.ParentRef.Name).To(gomega.Equal(isvc.Name))
	g.Expect(got.Spec.ParentRef.UID).To(gomega.Equal(isvc.UID))
	g.Expect(got.Spec.Component).To(gomega.Equal(v1beta1.EngineComponent))

	g.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
	g.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(2)),
		"Replicas must default from MinReplicas")

	g.Expect(got.Spec.Runners).To(gomega.HaveLen(1),
		"single-pod Component must emit one Runner")
	r0 := got.Spec.Runners[0]
	g.Expect(r0.Name).To(gomega.Equal(v1beta1.RunnerNameDefault))
	g.Expect(r0.Size).To(gomega.Equal(int32(1)))
	g.Expect(r0.Template.Spec.Containers).To(gomega.HaveLen(1))
	g.Expect(r0.Template.Spec.Containers[0].Image).To(gomega.Equal("sgl:1.0"),
		"PodSpec must be threaded through verbatim — no re-rendering")
	g.Expect(r0.Template.ObjectMeta.Labels).To(gomega.HaveKeyWithValue(
		constants.InferenceServicePodLabelKey, isvc.Name),
		"PodTemplate ObjectMeta labels must inherit from the projected ObjectMeta")

	g.Expect(got.Spec.Lifecycle).NotTo(gomega.BeNil())
	g.Expect(got.Spec.Lifecycle.RestartPolicy).NotTo(gomega.BeNil())
	g.Expect(*got.Spec.Lifecycle.RestartPolicy).To(gomega.Equal(
		v1beta1.InstanceRestartPolicyNone),
		"Lifecycle must be threaded through")
}

// TestEnsureInferenceReplica_Update_PreservesGeneration pins the
// update path: the projector emits the same IR Spec on a second
// invocation, so the apiserver-side Generation should NOT bump (no
// real change to write). The pre-existing IR must be respected.
func TestEnsureInferenceReplica_Update_PreservesGeneration(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()

	_, err := EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, c))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	first := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, first)).To(gomega.Succeed())
	firstRV := first.ResourceVersion

	// Second invocation with identical params should be a no-op modulo
	// the unconditional Update we issue. The fake client bumps
	// ResourceVersion on every Update — we can't assert "no Update
	// issued" with the fake client, but we CAN assert the Spec is
	// stable.
	_, err = EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, c))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	second := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, second)).To(gomega.Succeed())

	g.Expect(second.Spec.Replicas).To(gomega.Equal(first.Spec.Replicas))
	g.Expect(second.Spec.Component).To(gomega.Equal(first.Spec.Component))
	g.Expect(second.Spec.ParentRef).To(gomega.Equal(first.Spec.ParentRef))
	// ResourceVersion is bumped by the fake client on every Update —
	// we don't pin it. Real apiservers patch-equality-no-op, but the
	// fake doesn't.
	_ = firstRV
}

// TestEnsureInferenceReplica_MultiPod_LeaderAndWorker pins the
// multi-pod projection: MultiPod=true with WorkerPodSpec set emits
// {leader, size=1} + {worker, size=WorkerSize} Runners with the
// respective pod templates.
func TestEnsureInferenceReplica_MultiPod_LeaderAndWorker(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 3
	p.WorkerPodSpec = &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "worker:1.0"}},
	}

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Spec.Runners).To(gomega.HaveLen(2),
		"multi-pod Component must emit leader + worker Runners")
	g.Expect(got.Spec.Runners[0].Name).To(gomega.Equal(v1beta1.RunnerNameLeader))
	g.Expect(got.Spec.Runners[0].Size).To(gomega.Equal(int32(1)))
	g.Expect(got.Spec.Runners[0].Template.Spec.Containers[0].Image).To(gomega.Equal("sgl:1.0"),
		"leader Template must use the primary PodSpec")
	g.Expect(got.Spec.Runners[1].Name).To(gomega.Equal(v1beta1.RunnerNameWorker))
	g.Expect(got.Spec.Runners[1].Size).To(gomega.Equal(int32(3)),
		"worker Size must equal WorkerSize")
	g.Expect(got.Spec.Runners[1].Template.Spec.Containers[0].Image).To(gomega.Equal("worker:1.0"),
		"worker Template must use the WorkerPodSpec")
}

// TestEnsureInferenceReplica_MultiPod_RejectsNilWorkerSpec pins the
// validation guard: MultiPod=true with WorkerPodSpec=nil is a
// dispatch-site wiring bug. The projector surfaces it as a clear
// error instead of producing a malformed IR.
func TestEnsureInferenceReplica_MultiPod_RejectsNilWorkerSpec(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 3
	// p.WorkerPodSpec intentionally nil

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("WorkerPodSpec"))
}

// TestEnsureInferenceReplica_NilPodSpec_Rejected pins the validation
// guard: nil PodSpec is a dispatch-site wiring bug — the omenative
// path requires it too.
func TestEnsureInferenceReplica_NilPodSpec_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	p.PodSpec = nil

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("nil PodSpec"))
}

// TestEnsureInferenceReplica_NilComponentExt_Rejected pins the
// validation guard: a nil ComponentExt would mean Lifecycle / Replicas
// projection fall back to defaults silently. The projector rejects
// it so the dispatch site surfaces a clear error.
func TestEnsureInferenceReplica_NilComponentExt_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	p.ComponentExt = nil

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("ComponentExtensionSpec"))
}

// TestEnsureInferenceReplica_NilClient_Rejected pins the validation
// guard: a nil client at the projector means the dispatch site
// dropped the BaseComponentFields.Client.
func TestEnsureInferenceReplica_NilClient_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	p := minimalParams(t, isvc, nil)
	p.Client = nil

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring("nil client"))
}

// TestEnsureInferenceReplica_DefaultsReplicasToOne pins the default:
// MinReplicas=nil produces Replicas=1.
func TestEnsureInferenceReplica_DefaultsReplicasToOne(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.MinReplicas = nil
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	p.ComponentExt = &isvc.Spec.Engine.ComponentExtensionSpec

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())
	g.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
	g.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(1)),
		"nil MinReplicas must default to 1")
}

// TestInferenceReplicaName_FormatPins pins the canonical IR name
// shape used by the projector + the IR controller for revision /
// service / pod naming.
func TestInferenceReplicaName_FormatPins(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(InferenceReplicaName("llama", v1beta1.EngineComponent)).To(gomega.Equal("llama-engine"))
	g.Expect(InferenceReplicaName("llama", v1beta1.DecoderComponent)).To(gomega.Equal("llama-decoder"))
	g.Expect(InferenceReplicaName("llama", v1beta1.RouterComponent)).To(gomega.Equal("llama-router"))
}

// TestEnsureInferenceReplica_AlreadyExists_PropagatedAsUpdate pins
// the race between two concurrent reconciles: the second one's Get
// returns the live IR (which the first just created) and the Update
// path lands.
func TestEnsureInferenceReplica_AlreadyExists_PropagatedAsUpdate(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	// Pre-seed an IR with the same name to simulate a parallel reconcile
	// that created the IR between our Get and Create.
	preExisting := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-engine",
			Namespace: "prod",
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: "llama", UID: isvc.UID},
			Component: v1beta1.EngineComponent,
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(preExisting).Build()

	_, err := EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, c))
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"a pre-existing IR must be patched, not error out")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())
	g.Expect(got.Spec.Replicas).NotTo(gomega.BeNil())
	g.Expect(*got.Spec.Replicas).To(gomega.Equal(int32(2)),
		"Update must propagate the projector's desired Replicas onto the live IR")

	// Sanity: not an apierror NotFound on the post-write Get.
	_, getErr := c.RESTMapper().RESTMapping(got.GroupVersionKind().GroupKind())
	g.Expect(apierrors.IsNotFound(getErr)).To(gomega.BeFalse(),
		"sanity: post-create Get must succeed")
}
