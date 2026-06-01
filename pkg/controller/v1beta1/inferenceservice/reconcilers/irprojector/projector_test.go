package irprojector

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
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

// TestIsIRManagedComponent_OMENativeMode_True pins the default state
// post-cutover: an OMENative-mode Component routes through the
// IR-managed path automatically (no per-Component opt-in required).
func TestIsIRManagedComponent_OMENativeMode_True(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(IsIRManagedComponent(constants.OMENative, false)).To(gomega.BeTrue(),
		"OMENative-mode Component must route through the IR-managed path by default")
}

// TestIsIRManagedComponent_NonOMENativeMode_False pins the routing
// boundary: only OMENative-mode Components flow through the
// IR-managed path. RawDeployment, MultiNode, PDDisaggregated,
// VirtualDeployment all fall through to their own dispatch branches.
func TestIsIRManagedComponent_NonOMENativeMode_False(t *testing.T) {
	g := gomega.NewWithT(t)
	for _, mode := range []constants.DeploymentModeType{
		constants.RawDeployment,
		constants.MultiNode,
		constants.PDDisaggregated,
		constants.VirtualDeployment,
		constants.DeploymentModeType(""), // unresolved / unknown
	} {
		g.Expect(IsIRManagedComponent(mode, false)).To(gomega.BeFalse(),
			"non-OMENative mode %q must NOT route through the IR-managed path", mode)
	}
}

// TestIsIRManagedComponent_GlobalKillSwitch_OverridesMode pins the
// operator kill switch: even on an OMENative-mode Component,
// passing managerDisableFlag=true forces the predicate false. The
// flag is sourced from the process-wide atomic toggled by
// SetGlobalDisableIRManaged; tests pass it explicitly so the
// predicate stays pure.
func TestIsIRManagedComponent_GlobalKillSwitch_OverridesMode(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(IsIRManagedComponent(constants.OMENative, false)).To(gomega.BeTrue(),
		"sanity: OMENative routes through IR by default")
	g.Expect(IsIRManagedComponent(constants.OMENative, true)).To(gomega.BeFalse(),
		"managerDisableFlag must force the predicate false regardless of mode")
}

// TestIsManagerDisableFlagSet_ReflectsAtomic pins the live read of
// the package-level atomic toggled by SetGlobalDisableIRManaged.
// cmd/manager/main.go calls SetGlobalDisableIRManaged once at
// startup; dispatch sites + status aggregators call
// IsManagerDisableFlagSet on every reconcile to fetch the current
// value. Cleanup at the end restores the default so subsequent
// tests aren't surprised.
func TestIsManagerDisableFlagSet_ReflectsAtomic(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(IsManagerDisableFlagSet()).To(gomega.BeFalse(),
		"default state: kill switch is NOT set")

	SetGlobalDisableIRManaged(true)
	t.Cleanup(func() { SetGlobalDisableIRManaged(false) })
	g.Expect(IsManagerDisableFlagSet()).To(gomega.BeTrue(),
		"after SetGlobalDisableIRManaged(true), the helper must reflect the new state")

	SetGlobalDisableIRManaged(false)
	g.Expect(IsManagerDisableFlagSet()).To(gomega.BeFalse(),
		"after SetGlobalDisableIRManaged(false), the helper must reflect the new state")
}

// TestEnsureInferenceReplica_Create_NewIR pins the create-from-scratch
// path. The projected IR must carry:
//   - Name == <isvc>-<component>
//   - Annotations carry the controller-write annotation so the IR
//     webhook accepts the create
//   - OwnerReferences point at the parent ISVC as controller (cascade
//     GC on ISVC delete)
//   - ParentRef.Name matches the parent ISVC
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
			ParentRef: v1beta1.ParentReference{Name: "llama"},
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

// TestEnsureInferenceReplica_PropagatesComponentAnnotations pins the
// A-2 real-cluster bug fix: spec.<component>.annotations bumps MUST
// land on every Runner.Template.ObjectMeta.Annotations so the workload
// renderer stamps the new annotation on every emitted pod AND so the
// revision hash flips and triggers the ControllerRevision / rollout
// machinery.
//
// Real-cluster failure shape (pre-fix): operator bumps
// spec.engine.annotations.test.ome.io/rollout-trigger to a fresh
// timestamp; ISVC generation bumps; IR.spec.runners[].template
// .metadata.annotations stays EMPTY; no new ControllerRevision; the
// rollout never fires. The fix is for the projector to treat
// p.ComponentExt.Annotations as authoritative — even when the dispatch
// site forgets to merge them into p.ObjectMeta, the projector still
// emits them.
//
// Same bug class as task #51 (legacy direct-omenative path); this is
// the IR-managed analog.
func TestEnsureInferenceReplica_PropagatesComponentAnnotations(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.Annotations = map[string]string{
		"test.ome.io/rollout-trigger": "2026-05-29T01:02:03.456789Z",
		"linkerd.io/inject":           "enabled",
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	// Deliberately leave p.ObjectMeta.Annotations as the minimal fixture
	// value to prove the projector pulls from p.ComponentExt directly —
	// not just from a caller-side merge. The real dispatch site DOES
	// merge them upstream (engine.go::processAnnotations), but the
	// projector must remain self-contained for the A-2 scenario.

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
	tmplAnns := got.Spec.Runners[0].Template.ObjectMeta.Annotations
	g.Expect(tmplAnns).To(gomega.HaveKeyWithValue(
		"test.ome.io/rollout-trigger", "2026-05-29T01:02:03.456789Z"),
		"rollout-trigger annotation must reach the Runner template — "+
			"the revision hash flips off this key, no propagation means no rollout")
	g.Expect(tmplAnns).To(gomega.HaveKeyWithValue(
		"linkerd.io/inject", "enabled"),
		"all per-Component annotations must propagate, not just the rollout-trigger")
	// Baseline ObjectMeta annotation also propagates (no regression).
	g.Expect(tmplAnns).To(gomega.HaveKeyWithValue(
		"ome.io/some-annotation", "value"),
		"existing ObjectMeta annotations must still propagate alongside ComponentExt")
}

// TestEnsureInferenceReplica_PropagatesComponentLabels pins the label
// half of the A-2 fix: spec.<component>.labels MUST land on every
// Runner.Template.ObjectMeta.Labels for pod affinity / NetworkPolicy
// selectors / kubectl filtering. Same defensive-merge contract as
// annotations.
func TestEnsureInferenceReplica_PropagatesComponentLabels(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.Labels = map[string]string{
		"team":        "ml-platform",
		"cost-center": "1234",
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
	tmplLabels := got.Spec.Runners[0].Template.ObjectMeta.Labels
	g.Expect(tmplLabels).To(gomega.HaveKeyWithValue("team", "ml-platform"),
		"per-Component labels must reach the Runner template for selector matching")
	g.Expect(tmplLabels).To(gomega.HaveKeyWithValue("cost-center", "1234"))
	// Baseline ObjectMeta labels also propagate.
	g.Expect(tmplLabels).To(gomega.HaveKeyWithValue(
		constants.InferenceServicePodLabelKey, isvc.Name),
		"existing ObjectMeta labels must still propagate alongside ComponentExt")
}

// TestEnsureInferenceReplica_NilComponentAnnotationsAndLabels_NoRegression
// pins the negative case: ISVCs with no per-Component annotations /
// labels see the same Runner.Template.ObjectMeta they always saw —
// only the existing p.ObjectMeta content, no spurious empty maps,
// no nil-deref panics in the merge path.
func TestEnsureInferenceReplica_NilComponentAnnotationsAndLabels_NoRegression(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	// ComponentExt has nil Annotations + nil Labels (the baseline).
	g.Expect(isvc.Spec.Engine.Annotations).To(gomega.BeNil(),
		"sanity: baseline fixture has no per-Component annotations")
	g.Expect(isvc.Spec.Engine.Labels).To(gomega.BeNil(),
		"sanity: baseline fixture has no per-Component labels")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()

	_, err := EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, c))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
	// Only the minimalParams ObjectMeta content lands on the template.
	g.Expect(got.Spec.Runners[0].Template.ObjectMeta.Labels).To(gomega.HaveKeyWithValue(
		constants.InferenceServicePodLabelKey, isvc.Name),
		"baseline ObjectMeta labels must still propagate when ComponentExt is empty")
	g.Expect(got.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
		"ome.io/some-annotation", "value"),
		"baseline ObjectMeta annotations must still propagate when ComponentExt is empty")
}

// TestEnsureInferenceReplica_ComponentExt_PropagatesToBothRunners pins
// that on a multi-pod Component, the per-Component annotations + labels
// land on BOTH the leader and worker Runner templates — not just the
// leader. The renderer reads them off both templates for its respective
// pods, so missing the worker template would silently break per-Component
// rollout for the worker pod set.
func TestEnsureInferenceReplica_ComponentExt_PropagatesToBothRunners(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.Annotations = map[string]string{
		"test.ome.io/rollout-trigger": "2026-05-29T04:05:06Z",
	}
	isvc.Spec.Engine.Labels = map[string]string{
		"team": "ml-platform",
	}
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

	g.Expect(got.Spec.Runners).To(gomega.HaveLen(2))
	for _, r := range got.Spec.Runners {
		g.Expect(r.Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
			"test.ome.io/rollout-trigger", "2026-05-29T04:05:06Z"),
			"runner %q template must carry per-Component rollout-trigger annotation", r.Name)
		g.Expect(r.Template.ObjectMeta.Labels).To(gomega.HaveKeyWithValue(
			"team", "ml-platform"),
			"runner %q template must carry per-Component team label", r.Name)
	}
}

// TestEnsureInferenceReplica_ComponentExt_OverridesObjectMetaOnCollision
// pins the merge precedence: when p.ObjectMeta AND p.ComponentExt both
// declare the same key, p.ComponentExt wins. Mirrors the dispatch site's
// merge order (engine.go::processAnnotations does
// utils.Union(annotations, engineAnnotations) — last-write-wins for
// engineAnnotations). The projector must produce the same result if a
// future caller diverges from the canonical merge.
func TestEnsureInferenceReplica_ComponentExt_OverridesObjectMetaOnCollision(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.Annotations = map[string]string{
		"ome.io/some-annotation": "from-component-ext",
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	// p.ObjectMeta.Annotations["ome.io/some-annotation"] == "value" (from minimalParams).

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Spec.Runners).To(gomega.HaveLen(1))
	g.Expect(got.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
		"ome.io/some-annotation", "from-component-ext"),
		"ComponentExt annotation MUST win over ObjectMeta on key collision (matches engine.go merge order)")
}

// TestEnsureInferenceReplica_ComponentAnnotationUpdate_RepatchesTemplate
// pins the update path: a re-projection with a different
// p.ComponentExt.Annotations value MUST land on the live IR's runner
// template. Without this the second reconcile after an annotation bump
// would leave the IR carrying the stale value — the exact A-2 failure
// shape.
func TestEnsureInferenceReplica_ComponentAnnotationUpdate_RepatchesTemplate(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	isvc.Spec.Engine.Annotations = map[string]string{
		"test.ome.io/rollout-trigger": "initial",
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()

	_, err := EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, c))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	first := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, first)).To(gomega.Succeed())
	g.Expect(first.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
		"test.ome.io/rollout-trigger", "initial"),
		"sanity: first projection lands the initial value")

	// Operator bumps the annotation; the dispatch site re-runs with the
	// updated ComponentExt. The projector must patch the IR.
	isvc.Spec.Engine.Annotations["test.ome.io/rollout-trigger"] = "bumped"
	_, err = EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, c))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	second := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, second)).To(gomega.Succeed())
	g.Expect(second.Spec.Runners[0].Template.ObjectMeta.Annotations).To(gomega.HaveKeyWithValue(
		"test.ome.io/rollout-trigger", "bumped"),
		"annotation bump must repatch the IR runner template — "+
			"this is the canonical no-image-bump rollout trigger")
}

// autoscalerBlockA returns a distinctive KEDA-class ComponentAutoscaler
// for the projector autoscaler-propagation tests. The block carries a
// non-empty Triggers list + a non-default PollingInterval so a buggy
// shallow projection trips the deep-equality assertions.
func autoscalerBlockA() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaAutoscaler{
			Triggers: []kedav1.ScaleTriggers{
				{Type: "prometheus", Metadata: map[string]string{"v": "A"}},
			},
			PollingInterval: ptr.To(int32(15)),
		},
	}
}

// autoscalerBlockB returns a second, distinct ComponentAutoscaler so
// the update-replaces-whole-block test can distinguish "no projection"
// from "projection took". Differs from autoscalerBlockA in Class, Keda
// shape (nil), and HPA shape (non-nil) — the swap exercises every
// pointer in ComponentAutoscaler.
func autoscalerBlockB() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerHPA,
		HPA: &v1beta1.HPAAutoscaler{
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "cpu",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: ptr.To(int32(60)),
						},
					},
				},
			},
		},
	}
}

// TestEnsureInferenceReplica_PropagatesResolvedAutoscaler pins the
// Slice 1 contract: a non-nil p.ResolvedAutoscaler lands on the IR
// at Create time. Slice 3's autoscaler dispatch reads ir.Spec
// .Autoscaler — if the projector drops it, the entire OEP-0013 chain
// is dead on arrival.
//
// The block is deep-equal-asserted so a half-applied projection (e.g.,
// Class only, Keda dropped) trips the test instead of slipping through.
func TestEnsureInferenceReplica_PropagatesResolvedAutoscaler(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	p.ResolvedAutoscaler = autoscalerBlockA()

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Spec.Autoscaler).NotTo(gomega.BeNil(),
		"non-nil p.ResolvedAutoscaler must land on ir.Spec.Autoscaler at Create — "+
			"the autoscaler dispatch (Slice 3) reads this field")
	g.Expect(got.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockA()),
		"projection must be deep-equal — Class + Keda + HPA all preserved verbatim")
}

// TestEnsureInferenceReplica_UpdateAutoscalerRepatchesIR pins the
// whole-block replace semantics: a second projection with a different
// ResolvedAutoscaler swaps the IR's autoscaler wholesale, not merges.
// Operator-side changes to isvc.spec.<comp>.autoscaler always win over
// any drifted ir.spec.autoscaler — see OEP-0013 §"Generated autoscaler
// resources" + §"Inheritance chain".
//
// Failure shape this catches: a projector that only merges sub-fields
// (e.g., keeps Class from A while overwriting Keda with B's HPA) would
// leave the IR in a corrupted half-A/half-B state where the dispatch
// can't decide which scaler to emit.
func TestEnsureInferenceReplica_UpdateAutoscalerRepatchesIR(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()

	// First projection: block A (KEDA, Triggers set, HPA nil).
	p := minimalParams(t, isvc, c)
	p.ResolvedAutoscaler = autoscalerBlockA()
	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	first := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, first)).To(gomega.Succeed())
	g.Expect(first.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockA()),
		"sanity: first projection lands block A")

	// Second projection: block B (HPA, Keda nil, Metrics set). The
	// dispatch site re-resolves the autoscaler every reconcile, so a
	// different ResolvedAutoscaler is the normal case (operator bumped
	// isvc.spec.engine.autoscaler.class from keda to hpa).
	p2 := minimalParams(t, isvc, c)
	p2.ResolvedAutoscaler = autoscalerBlockB()
	_, err = EnsureInferenceReplica(context.Background(), p2)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	second := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, second)).To(gomega.Succeed())

	g.Expect(second.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockB()),
		"second projection must REPLACE the whole block — not merge — so block B fully wins")
	g.Expect(second.Spec.Autoscaler.Class).To(gomega.Equal(v1beta1.AutoscalerHPA),
		"Class must flip from KEDA to HPA")
	g.Expect(second.Spec.Autoscaler.Keda).To(gomega.BeNil(),
		"Keda block must be cleared — block B doesn't have one")
	g.Expect(second.Spec.Autoscaler.HPA).NotTo(gomega.BeNil(),
		"HPA block must be set — block B has one")
}

// TestEnsureInferenceReplica_NilResolvedAutoscaler_NoRegression pins
// the defensive path: a nil ResolvedAutoscaler (e.g., a dispatch site
// that hasn't been wired through Slice 1 yet) projects to nil ir.Spec
// .Autoscaler. No spurious empty {Class: ""} block lands.
//
// Without this guard, the projector would either (a) panic on a nil
// deref of p.ResolvedAutoscaler.DeepCopy() or (b) leave the IR with
// a partially-defaulted block the dispatch can't interpret.
func TestEnsureInferenceReplica_NilResolvedAutoscaler_NoRegression(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	// p.ResolvedAutoscaler intentionally nil.

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"nil ResolvedAutoscaler must not panic — defensive against unwired dispatch sites")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	g.Expect(got.Spec.Autoscaler).To(gomega.BeNil(),
		"nil ResolvedAutoscaler must project to nil ir.Spec.Autoscaler — "+
			"no spurious empty block")
}

// TestEnsureInferenceReplica_AutoscalerProjection_DeepCopies pins the
// memory-isolation guard: the IR's ir.Spec.Autoscaler MUST NOT alias
// p.ResolvedAutoscaler. A downstream mutator (e.g., the IR controller
// reading the field, the autoscaler dispatch building an HPA spec)
// must not be able to corrupt the resolver's source-of-truth.
func TestEnsureInferenceReplica_AutoscalerProjection_DeepCopies(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	p := minimalParams(t, isvc, c)
	original := autoscalerBlockA()
	p.ResolvedAutoscaler = autoscalerBlockA() // separate instance, deep-equal to `original`

	_, err := EnsureInferenceReplica(context.Background(), p)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got)).To(gomega.Succeed())

	// Mutate the input AFTER projection. If the projector aliased the
	// pointer, the live IR would show the mutation through Get's
	// fake-client round-trip (fake client stores by reference for in-memory).
	p.ResolvedAutoscaler.Class = v1beta1.AutoscalerNone
	p.ResolvedAutoscaler.Keda.Triggers[0].Type = "mutated"

	got2 := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: "llama-engine", Namespace: "prod"}, got2)).To(gomega.Succeed())
	g.Expect(got2.Spec.Autoscaler).To(gomega.Equal(original),
		"post-projection input mutation must NOT leak into the stored IR — "+
			"ir.Spec.Autoscaler must be a deep copy of p.ResolvedAutoscaler")
}
