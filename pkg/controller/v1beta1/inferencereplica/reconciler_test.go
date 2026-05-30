package inferencereplica

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// testScheme returns a runtime.Scheme with the types the IR reconciler
// exercises registered: v1beta1 (with status subresource for IR),
// corev1 (Pods, EndpointSlices), appsv1 (ControllerRevisions),
// discoveryv1 (EndpointSlices).
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("v1beta1.AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("appsv1.AddToScheme: %v", err)
	}
	if err := discoveryv1.AddToScheme(s); err != nil {
		t.Fatalf("discoveryv1.AddToScheme: %v", err)
	}
	return s
}

// baselineIR returns a controller-write-stamped, single-pod
// InferenceReplica matching the shape the ISVC controller will project.
// Single Runner named "default" with size 1, one container, no
// lifecycle defaults (workload.BuildPlan applies them).
func baselineIR(name, namespace string, replicas int32) *v1beta1.InferenceReplica {
	r := replicas
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			Generation: 1,
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{
				Name: "llama",
				UID:  "00000000-0000-0000-0000-000000000001",
			},
			Component: v1beta1.EngineComponent,
			Replicas:  &r,
			Runners: []v1beta1.Runner{
				{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
						},
					},
				},
			},
		},
	}
}

// newReconciler builds a fake-client-backed Reconciler with the IR
// status subresource wired and a fresh Expectations cache so per-test
// create attempts aren't blocked by entries from a previous test.
func newReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	return &Reconciler{
		Client:       c,
		Log:          logf.Log.WithName("test"),
		Scheme:       testScheme(t),
		Expectations: workload.NewExpectations(),
	}, c
}

// TestReconcile_NotFound_NoError pins the early-return contract: an IR
// deleted between enqueue and reconcile must be a clean no-op (owner-ref
// cascade GC handles children).
func TestReconcile_NotFound_NoError(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"deleted IR between enqueue and reconcile should be a no-op")
	g.Expect(result.Requeue).To(gomega.BeFalse())
	g.Expect(result.RequeueAfter).To(gomega.BeZero())
}

// TestReconcile_DeletionTimestamp_NoOp pins that an IR with a
// DeletionTimestamp short-circuits before the workload dispatcher runs.
// Foreground cascade-delete is owned by the apiserver via
// blockOwnerDeletion; the controller has no finalizer.
func TestReconcile_DeletionTimestamp_NoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	now := metav1.Now()
	ir.DeletionTimestamp = &now
	// A DeletionTimestamp without a finalizer makes the fake client
	// drop the object immediately. Stamp a dummy finalizer so the IR
	// remains observable for the test.
	ir.Finalizers = []string{"keep-for-test"}
	r, _ := newReconciler(t, ir)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.Requeue).To(gomega.BeFalse())
	g.Expect(result.RequeueAfter).To(gomega.BeZero())
}

// TestReconcile_Create_MaterializesPods pins the create-from-scratch
// path: new IR with replicas=2 dispatches into workload.Reconcile,
// which reaches the Create pass and creates pods. Status aggregator
// then stamps Replicas / LabelSelector / UpdateRevision.
func TestReconcile_Create_MaterializesPods(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	r, c := newReconciler(t, ir)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"Reconcile should not error on the first Create pass")

	// Pods should now exist for both desired Instances.
	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	g.Expect(pods.Items).To(gomega.HaveLen(2),
		"expected one pod per desired Instance; got %d", len(pods.Items))

	// Pod names must follow the legacy shape <isvc>-<component>-<idx>-default-0
	// so existing selectors keep matching.
	names := podNames(pods.Items)
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", internalsource.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 0)))
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", internalsource.ComponentTypeToWorkload(v1beta1.EngineComponent), 1, "default", 0)))

	// Every pod must be owner-ref'd to the IR (Kind=InferenceReplica),
	// NOT the legacy ISVC owner.
	for _, pod := range pods.Items {
		g.Expect(pod.OwnerReferences).To(gomega.HaveLen(1))
		ref := pod.OwnerReferences[0]
		g.Expect(ref.Kind).To(gomega.Equal("InferenceReplica"),
			"pod %s owner ref should point at the IR, not the ISVC", pod.Name)
		g.Expect(ref.Name).To(gomega.Equal(ir.Name))
		g.Expect(ref.Controller).NotTo(gomega.BeNil())
		g.Expect(*ref.Controller).To(gomega.BeTrue())
	}

	// IR.Status should reflect the desired Replicas + LabelSelector +
	// ObservedGeneration. The status writer runs in defer so it always
	// fires before Reconcile returns.
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.Replicas).To(gomega.Equal(int32(2)),
		"Replicas counter should match desired Instance count")
	g.Expect(got.Status.ObservedGeneration).To(gomega.Equal(int64(1)))
	g.Expect(got.Status.LabelSelector).NotTo(gomega.BeEmpty(),
		"LabelSelector must be set for HPA scale subresource")
	// LabelSelector must encode the legacy OMENative pod-selector trio so
	// existing HPAs continue to resolve.
	g.Expect(got.Status.LabelSelector).To(gomega.ContainSubstring("component=engine"))
	g.Expect(got.Status.LabelSelector).To(gomega.ContainSubstring("ome.io/inferenceservice=llama"))
	g.Expect(got.Status.LabelSelector).To(gomega.ContainSubstring("ome.io/managed-by=OMENative"))
	// UpdateRevision should be stamped off the freshly-ensured CR.
	g.Expect(got.Status.UpdateRevision).NotTo(gomega.BeEmpty(),
		"UpdateRevision must point at the target ControllerRevision")
}

// TestReconcile_Create_AlsoCreatesHeadlessService pins the headless
// Service wire-in (IR PR 2.1): a fresh IR reconcile must call
// workload.ReconcileHeadlessService alongside workload.Reconcile so a
// per-Component headless Service appears in the same pass that creates
// the pods. The Service rendering itself is unit-tested in
// workload/services_test.go — this test only verifies the wire-in.
//
// Asserts on the canonical shape:
//   - Name == query.HeadlessServiceName(parent, component) so any
//     tooling that looks for `<isvc>-<component>-headless` keeps working
//   - ClusterIP == None so DNS returns per-pod A records (no
//     kube-proxy load-balancing) — required for gang-init peer discovery
//   - PublishNotReadyAddresses == true so peer DNS resolves before
//     pods flip Ready (workers must discover each other during init)
//   - Selector carries the legacy OMENative pod-selector trio
//     (ome.io/inferenceservice + component + managed-by=OMENative) so
//     the Service matches the same pods the workload renderer stamps
//   - Owner ref points at the IR (Kind=InferenceReplica), NOT the
//     parent ISVC — the IR is the per-Component lifecycle owner; on
//     IR deletion the Service must cascade with it
func TestReconcile_Create_AlsoCreatesHeadlessService(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	r, c := newReconciler(t, ir)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"Reconcile should not error on the first Create pass with headless Service wire-in")

	// The Service name is derived from the parent ISVC name (ParentRef.Name)
	// — pods, services, and selectors all key off the ISVC name so the
	// shape stays byte-identical to the legacy omenative-controller path.
	wantName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, internalsource.ComponentTypeToWorkload(ir.Spec.Component))
	svc := &corev1.Service{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: wantName, Namespace: ir.Namespace},
		svc)).To(gomega.Succeed(),
		"headless Service %s/%s should be created by the IR reconciler", ir.Namespace, wantName)

	// Headless contract: ClusterIP=None + PublishNotReadyAddresses=true
	// so peer DNS works during gang init before pods flip Ready.
	g.Expect(svc.Spec.ClusterIP).To(gomega.Equal(corev1.ClusterIPNone),
		"headless Service must have ClusterIP=None (DNS returns per-pod A records)")
	g.Expect(svc.Spec.PublishNotReadyAddresses).To(gomega.BeTrue(),
		"headless Service must publish not-ready addresses for gang init")

	// Selector must match the legacy OMENative pod-selector trio so the
	// Service selects the same pods workload renderer stamps. Both paths
	// (ISVC-direct + IR-driven) use the same selector — this is what
	// makes shadow-mode diffs byte-identical.
	g.Expect(svc.Spec.Selector).To(gomega.Equal(map[string]string{
		constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
		constants.OMEComponentLabel:           string(ir.Spec.Component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}))

	// Owner ref points at the IR — not the parent ISVC — so deletion of
	// the IR cascades to the Service. The ISVC controller manages IR
	// lifecycle at the per-Component layer (PR 3); inside its lifetime,
	// the IR controller owns the per-Component supporting resources.
	g.Expect(svc.OwnerReferences).To(gomega.HaveLen(1))
	ref := svc.OwnerReferences[0]
	g.Expect(ref.Kind).To(gomega.Equal("InferenceReplica"),
		"headless Service owner ref should point at the IR, not the ISVC")
	g.Expect(ref.Name).To(gomega.Equal(ir.Name))
	g.Expect(ref.UID).To(gomega.Equal(ir.UID))
	g.Expect(ref.Controller).NotTo(gomega.BeNil())
	g.Expect(*ref.Controller).To(gomega.BeTrue())
}

// TestReconcile_ScaleUp_AddsPods pins the scale-up path: a steady
// IR at replicas=1 with a Ready Instance grows to replicas=2; the next
// reconcile must reach the Create pass and add the second pod.
func TestReconcile_ScaleUp_AddsPods(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	// Pretend the controller already ran once: instance 0 is Ready with
	// its pod alive and serving. Workload Create's idempotency keys on
	// the per-Instance pod-name lookup so the pre-existing pod won't be
	// re-created.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceReady,
			PodCount:        1,
			ReadyPodCount:   1,
			ServingPodCount: 1,
			ActiveOrdinal:   0,
		},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)

	r, c := newReconciler(t, ir, pod0)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	g.Expect(len(pods.Items)).To(gomega.BeNumerically(">=", 2),
		"scale-up should add at least one more pod alongside the existing one")
	names := podNames(pods.Items)
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", internalsource.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 0)))
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", internalsource.ComponentTypeToWorkload(v1beta1.EngineComponent), 1, "default", 0)))
}

// TestReconcile_ScaleDown_DeletesExcessInstances pins the scale-down
// path: an IR at replicas=1 with two Instance entries (one extra)
// triggers workload.Reconcile's Delete pass on the excess Instance.
// The dispatcher returns a non-zero requeue while Delete progresses.
func TestReconcile_ScaleDown_DeletesExcessInstances(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	// Pre-existing status with Instance 1 as the scale-down victim.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0,
		},
		{
			Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0,
		},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)

	r, c := newReconciler(t, ir, pod0, pod1)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	// Delete pass requests a requeue while progressing.
	g.Expect(result.RequeueAfter).NotTo(gomega.BeZero(),
		"scale-down should request a requeue while Delete advances")

	// Instance 1's pod should be gone (or marked for deletion).
	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	for _, p := range pods.Items {
		if p.Name == pod1.Name && p.DeletionTimestamp == nil {
			t.Errorf("expected pod %s to be deleted, but it still exists with no DeletionTimestamp", p.Name)
		}
	}
}

// TestReconcile_StatusAggregator_RollsUpCounters pins the aggregator:
// with three Instance entries (2 Ready, 1 Creating) the Component-level
// Replicas / ReadyReplicas should reflect the per-Instance state after
// one reconcile pass.
func TestReconcile_StatusAggregator_RollsUpCounters(t *testing.T) {
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
	g.Expect(got.Status.Replicas).To(gomega.Equal(int32(3)),
		"Replicas counter should match the InstanceStatuses entry count")
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(2)),
		"ReadyReplicas counter should reflect the 2 Ready Instances")
	g.Expect(got.Status.ServingReplicas).To(gomega.Equal(int32(2)),
		"ServingReplicas counter should reflect the 2 Serving Instances")
}

// TestReconcile_NilPodSpec_NoRevisionEnsured pins the defensive
// nil-PodSpec branch: a Runner with an empty PodSpec (no containers)
// produces a non-nil PodSpec so EnsureControllerRevision can run;
// this test verifies that the IR builder doesn't crash on the
// PodSpec=nil short-circuit (which is dead code in production but
// kept as a safety guard).
func TestReconcile_NilPodSpec_NoRevisionEnsured(t *testing.T) {
	g := gomega.NewWithT(t)
	// Build an IR with no Runners — webhook would reject this in
	// production (Runners has +kubebuilder:validation:MinItems=1) but
	// the reconciler's defensive nil-PodSpec branch must still produce
	// no panic.
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = nil
	r, _ := newReconciler(t, ir)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	// An IR with no Runners produces a desired PodSpec=nil; workload
	// Create returns immediately and the status writer stamps
	// Replicas=0 (no Instances). Either no error or a build-plan
	// error is acceptable; what's critical is NO panic.
	if err == nil {
		// fine — soft path
		return
	}
	// Sanity: the error path should be a build-plan or workload error,
	// not an internal Go panic message.
	g.Expect(err.Error()).NotTo(gomega.ContainSubstring("runtime error"))
}

// podForIR builds a fake pod matching what workload/ops/render would
// produce for a given (IR, instance, runner, ordinal) tuple. The
// label set mirrors render.go's podLabels(); the pod's
// ContainersReady + ome.io/serving conditions are toggled by the
// (ready, serving) booleans.
//
// Owner ref points at the IR (Kind=InferenceReplica) so the
// expectation cache + workload-side query.ListOMENativePods see this
// as a real workload pod, not a foreign one.
func podForIR(ir *v1beta1.InferenceReplica, instanceIdx int32, runnerName string, ordinal int32, ready, serving bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(ir.Spec.ParentRef.Name, internalsource.ComponentTypeToWorkload(ir.Spec.Component), instanceIdx, runnerName, ordinal),
			Namespace: ir.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
				constants.OMEComponentLabel:           string(ir.Spec.Component),
				query.LabelInstanceIdx:                intToLabel(int64(instanceIdx)),
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     runnerName,
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 intToLabel(int64(ordinal)),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       ir.Name,
				UID:        ir.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
		},
	}
	now := metav1.Now()
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.ContainersReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               query.ServingConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	return pod
}

// podNames extracts the names from a slice of pods for assertion
// readability.
func podNames(pods []corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
}

// sliceForIRPod constructs an EndpointSlice carrying one endpoint for
// pod against the IR's per-Component headless Service. Used by status
// tests that need AvailableReplicas to mirror ReadyReplicas — the
// post-fix aggregator reads availability off the EndpointSlice (same
// as the omenative direct path), so without a slice every pod is
// invisible to the availability counter regardless of ContainersReady.
//
// Returns a slice with Endpoints[0].Conditions.Ready set to ready —
// the same toggle the omenative sliceWithEndpoint helper exposes.
// AddressType=IPv4 + a fixed bogus address keep the fake-client
// validation happy; the controller's availability counter only reads
// TargetRef.Name + Ready, not the IP.
func sliceForIRPod(ir *v1beta1.InferenceReplica, pod *corev1.Pod, ready bool) *discoveryv1.EndpointSlice {
	serviceName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, internalsource.ComponentTypeToWorkload(ir.Spec.Component))
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name + "-slice",
			Namespace: ir.Namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(ready),
				},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			},
		},
	}
}

// intToLabel formats a non-negative int64 as the label-safe ASCII
// string the workload-side pod label helpers produce. Mirrors the
// helper in omenative/test_helpers_test.go (test packages can't
// share unexported helpers across directory boundaries).
func intToLabel(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
