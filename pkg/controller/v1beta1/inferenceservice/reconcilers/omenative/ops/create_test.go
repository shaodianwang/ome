package ops

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
)

// newFakeClient builds a controller-runtime fake client with the scheme
// the omenative reconciler needs (corev1 + v1beta1 + appsv1 for
// ControllerRevision) and tells the fake about the status subresource
// for InferenceService so Status().Update works as it would against a
// real API server.
func newFakeClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

// minimalISVC builds an ISVC with the minimum metadata Create() needs.
func minimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
	mr := replicas
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &mr,
				},
			},
		},
	}
}

// testRevisionHash is the synthetic revision hash stamped on test
// pods (matches what production Render emits via ome.io/revision-hash).
// Per-revision Service fixtures in drain tests name themselves after
// this constant via coordination.PerRevisionServiceName.
const testRevisionHash = "testrev1"

// minimalParams builds a core.ReconcileParams in the single-pod / engine shape.
func minimalParams(t *testing.T, isvc *v1beta1.InferenceService, c client.Client) core.ReconcileParams {
	t.Helper()
	return core.ReconcileParams{
		ISVC:         isvc,
		PodSpec:      &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		Client:       c,
	}
}

// resetExpectations clears core.DefaultExpectations so per-test create attempts
// aren't blocked by entries from a previous test. The cache is a process
// singleton in production; tests need isolation.
func resetExpectations(t *testing.T) {
	t.Helper()
	core.DefaultExpectations = core.NewExpectations()
}

func TestCreate_NilClient(t *testing.T) {
	resetExpectations(t)
	plan := core.ComponentPlan{Component: v1beta1.EngineComponent}
	_, err := Create(context.Background(), core.ReconcileParams{}, plan, nil)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestCreate_EmptyClusterCreatesPodsAndRequeues(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	c := newFakeClient(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("core.BuildPlan: %v", err)
	}

	result, err := Create(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected Requeue, got %+v", result)
	}

	// Assert two pods exist with the expected stable names.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("expected 2 pods, got %d", len(pods.Items))
	}
	want := map[string]bool{
		"llama-70b-engine-0-default-0": true,
		"llama-70b-engine-1-default-0": true,
	}
	for _, pod := range pods.Items {
		if !want[pod.Name] {
			t.Errorf("unexpected pod: %s", pod.Name)
		}
		delete(want, pod.Name)
		// Initial create stamps Incarnation=1 on every pod.
		if got := pod.Labels[query.LabelInstanceIncarnation]; got != "1" {
			t.Errorf("pod %s %s label: got %q want 1", pod.Name, query.LabelInstanceIncarnation, got)
		}
	}
	if len(want) > 0 {
		t.Errorf("missing pods: %v", want)
	}

	// InstanceStatus for each Instance should be Creating with an Operation.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative == nil {
		t.Fatal("OMENative status nil")
	}
	if len(cs.OMENative.InstanceStatuses) != 2 {
		t.Fatalf("InstanceStatuses: got %d want 2", len(cs.OMENative.InstanceStatuses))
	}
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Phase != v1beta1.OMENativeInstanceCreating {
			t.Errorf("instance %d Phase: got %q want Creating", s.Index, s.Phase)
		}
		if s.Incarnation != 1 {
			t.Errorf("instance %d Incarnation: got %d want 1", s.Index, s.Incarnation)
		}
		if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationCreate {
			t.Errorf("instance %d Operation: %+v", s.Index, s.Operation)
			continue
		}
		// Deadline must be a strictly future time — proves InstanceReadyTimeout
		// is being threaded through to the status anchor and isn't left zero.
		if !s.Operation.Deadline.After(s.Operation.StartedAt.Time) {
			t.Errorf("instance %d Operation.Deadline: got %v, want > StartedAt %v",
				s.Index, s.Operation.Deadline, s.Operation.StartedAt)
		}
	}
}

func TestCreate_AllPodsExistButNotReady_HoldsCreating(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := podForInstance(isvc, 0, false /* ready */, false /* serving */)
	c := newFakeClient(t, isvc, pod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	result, err := Create(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected Requeue while waiting for Ready")
	}
	// Pod should not have been re-created (still 1).
	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	if len(pods.Items) != 1 {
		t.Errorf("pods: got %d want 1", len(pods.Items))
	}
}

func TestCreate_AllPodsReady_FlipsServingAndMarksReady(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := podForInstance(isvc, 0, true /* ready */, false /* serving */)
	c := newFakeClient(t, isvc, pod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	result, err := Create(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected no Requeue when all pods Ready, got %+v", result)
	}

	// Pod should have ome.io/serving=True now.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if !podreadiness.IsServing(got) {
		t.Errorf("pod %s ome.io/serving not flipped: conditions=%+v", got.Name, got.Status.Conditions)
	}

	// InstanceStatus should be Ready with Operation=nil.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative == nil || len(cs.OMENative.InstanceStatuses) != 1 {
		t.Fatalf("status missing: %+v", cs.OMENative)
	}
	is := cs.OMENative.InstanceStatuses[0]
	if is.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", is.Phase)
	}
	if is.Operation != nil {
		t.Errorf("Operation: want nil, got %+v", is.Operation)
	}
}

func TestCreate_PartialPods_CreatesOnlyMissing(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	// Instance 0 already exists; Instance 1 missing.
	existing := podForInstance(isvc, 0, false, false)
	c := newFakeClient(t, isvc, existing)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	_, err := Create(context.Background(), p, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	if len(pods.Items) != 2 {
		t.Fatalf("expected 2 pods, got %d", len(pods.Items))
	}
}

func TestCreate_IdempotentOnAlreadyExists(t *testing.T) {
	resetExpectations(t)
	// Two reconciles back-to-back without the cache observing the first
	// batch — the second call's client.Create returns AlreadyExists,
	// which we treat as a no-op.
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	// First call creates the pod.
	if _, err := Create(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	// Reset expectations so the second call attempts to re-create.
	core.DefaultExpectations.Forget("prod", "llama-70b", v1beta1.EngineComponent, 0)
	// Second call should not error even though the pod already exists.
	if _, err := Create(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("second Create: %v", err)
	}
}

// podForInstance fabricates a pod matching what Render() would produce
// for the given (ISVC, instance) pair, with knobs for runtime-ready /
// serving state.
//
// The `ready` knob synthesizes ContainersReady=True, not PodReady=True,
// because the controller gates on ContainersReady and PodReady requires
// every readiness gate (including ome.io/serving) to be True — which is
// exactly what the controller is about to write. Synthesizing PodReady
// in tests would mask the real ordering.
func podForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	labels := podLabels(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", 1)
	// Production Render stamps ome.io/revision-hash so per-revision
	// Service selectors match. Test pods must carry it too — the drain
	// path computes the per-revision routed Service from this label.
	labels[query.LabelRevisionHash] = testRevisionHash
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, v1beta1.EngineComponent, instanceIdx, "default", 0),
			Namespace: isvc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
	}
	now := metav1.NewTime(time.Now())
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

// Promote to Ready must record RunningRevision so detectUpdateTrigger's
// fast path short-circuits on the next reconcile. Without it, every
// fresh Instance triggers a spurious recreate on its second pass (the
// per-pod diff false-positives against post-Render mutations).
func TestCreate_PromoteRecordsRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	// Pre-seed a runtime-ready, serving pod so reconcileInstance jumps
	// straight to the promote step.
	pod := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	c := newFakeClient(t, isvc, pod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-deadbeef", Namespace: "prod"},
	}

	if _, err := Create(context.Background(), p, plan, target); err != nil {
		t.Fatalf("Create: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s := query.FindInstanceStatus(fresh, v1beta1.EngineComponent, 0)
	if s == nil {
		t.Fatal("InstanceStatus[0] must exist after promote")
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	if s.RunningRevision != target.Name {
		t.Errorf("RunningRevision: got %q want %q (Create promote must record target hash)",
			s.RunningRevision, target.Name)
	}
}

// Inverse: when target is nil (legacy callers, scale-down-only reconciles),
// Create promotes without writing RunningRevision. Preserves the pre-fix
// behavior for callers that don't yet plumb target through.
func TestCreate_PromoteWithNilTargetSkipsRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := podForInstance(isvc, 0, true, true)
	c := newFakeClient(t, isvc, pod)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	if _, err := Create(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s := query.FindInstanceStatus(fresh, v1beta1.EngineComponent, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("expected Phase=Ready; got %+v", s)
	}
	if s.RunningRevision != "" {
		t.Errorf("RunningRevision: got %q want empty (nil target)", s.RunningRevision)
	}
}
