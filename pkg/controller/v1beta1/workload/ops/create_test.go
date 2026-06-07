package ops_test

import (
	"context"
	"fmt"
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
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Test file is in package ops_test so tests only see the workload.*
// API surface — no privileged access to ops/ internals.

// newFakeClient builds a controller-runtime fake client with the
// scheme the workload reconciler needs.
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
// The fake-client status-write path needs a concrete owner object to
// round-trip; workload code reads the resulting ReconcileInput
// opaquely.
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
const testRevisionHash = "testrev1"

// testPodLabels reproduces the label set workload/ops.Render stamps
// on every emitted pod. Duplicated here because Render's helper is
// private to ops/; tests in ops_test fabricate pods directly.
func testPodLabels(isvc string, component workload.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelInstanceIdx:                fmt.Sprintf("%d", instanceIdx),
		query.LabelInstanceIncarnation:        fmt.Sprintf("%d", incarnation),
		query.LabelRunner:                     runner,
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 fmt.Sprintf("%d", ordinal),
	}
}

// buildTestInput projects the ISVC under test onto a
// workload.ReconcileInput. MutateInstance round-trips through the
// fake client's Status().Update so tests can assert on the persisted
// ISVC.Status.
func buildTestInput(isvc *v1beta1.InferenceService, c client.Client, component workload.ComponentType) workload.ReconcileInput {
	v1component := v1beta1.ComponentType(component)
	var instances []workload.InstanceStatus
	if cs, ok := isvc.Status.Components[v1component]; ok && cs.Lifecycle != nil {
		for _, s := range cs.Lifecycle.InstanceStatuses {
			instances = append(instances, workload.InstanceStatus{
				Index:           s.Index,
				Incarnation:     s.Incarnation,
				Phase:           workload.InstancePhase(s.Phase),
				RunningRevision: s.RunningRevision,
				TargetRevision:  s.TargetRevision,
				ActiveOrdinal:   s.ActiveOrdinal,
				LastFailure:     fromV1beta1Termination(s.LastFailure),
			})
		}
	}
	return workload.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workload.Key{
			Namespace:   isvc.Namespace,
			OwnerName:   isvc.Name,
			Component:   workload.ComponentType(component),
			OwnerLabels: isvc.Labels,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: instances,
		},
		MutateInstance: testMutateInstance(c, isvc, component),
	}
}

// testMutateInstance is the test-side persistence layer for
// ReconcileInput.MutateInstance. Skips retry.RetryOnConflict (fake
// client has no optimistic-concurrency failures to retry).
func testMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
	v1component := v1beta1.ComponentType(component)
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
			return fmt.Errorf("get isvc: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
		}
		cs := fresh.Status.Components[v1component]
		if cs.Lifecycle == nil {
			cs.Lifecycle = &v1beta1.LifecycleStatus{}
		}
		pos := -1
		for i, s := range cs.Lifecycle.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		var w workload.InstanceStatus
		if pos == -1 {
			w = workload.InstanceStatus{Index: idx}
		} else {
			s := cs.Lifecycle.InstanceStatuses[pos]
			w = workload.InstanceStatus{
				Index:           s.Index,
				Incarnation:     s.Incarnation,
				Phase:           workload.InstancePhase(s.Phase),
				RunningRevision: s.RunningRevision,
				TargetRevision:  s.TargetRevision,
				ActiveOrdinal:   s.ActiveOrdinal,
				Operation:       fromV1beta1Op(s.Operation),
				LastFailure:     fromV1beta1Termination(s.LastFailure),
			}
		}
		if !mutate(&w) {
			return nil
		}
		updated := v1beta1.OMENativeInstanceStatus{
			Index:           w.Index,
			Incarnation:     w.Incarnation,
			Phase:           v1beta1.OMENativeInstancePhase(w.Phase),
			RunningRevision: w.RunningRevision,
			TargetRevision:  w.TargetRevision,
			ActiveOrdinal:   w.ActiveOrdinal,
			Operation:       toV1beta1Op(w.Operation),
			LastFailure:     toV1beta1Termination(w.LastFailure),
		}
		if pos == -1 {
			cs.Lifecycle.InstanceStatuses = append(cs.Lifecycle.InstanceStatuses, updated)
		} else {
			cs.Lifecycle.InstanceStatuses[pos] = updated
		}
		fresh.Status.Components[v1component] = cs
		return c.Status().Update(ctx, fresh)
	}
}

func fromV1beta1Op(op *v1beta1.InstanceOperation) *workload.InstanceOperation {
	if op == nil {
		return nil
	}
	out := &workload.InstanceOperation{
		ID:             op.ID,
		Type:           workload.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		RetryCount:     op.RetryCount,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		FromNode:       op.FromNode,
		RequestUUID:    op.RequestUUID,
	}
	if op.SurgeIndex != nil {
		s := *op.SurgeIndex
		out.SurgeIndex = &s
	}
	if op.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), op.HintTargetNodes...)
	}
	return out
}

func toV1beta1Op(op *workload.InstanceOperation) *v1beta1.InstanceOperation {
	if op == nil {
		return nil
	}
	out := &v1beta1.InstanceOperation{
		ID:             op.ID,
		Type:           v1beta1.InstanceOperationType(op.Type),
		Step:           op.Step,
		StartedAt:      op.StartedAt,
		LastProgressAt: op.LastProgressAt,
		Deadline:       op.Deadline,
		RetryCount:     op.RetryCount,
		TargetRevision: op.TargetRevision,
		Reason:         op.Reason,
		FromNode:       op.FromNode,
		RequestUUID:    op.RequestUUID,
	}
	if op.SurgeIndex != nil {
		s := *op.SurgeIndex
		out.SurgeIndex = &s
	}
	if op.HintTargetNodes != nil {
		out.HintTargetNodes = append([]string(nil), op.HintTargetNodes...)
	}
	return out
}

func fromV1beta1Termination(t *v1beta1.InstanceTermination) *workload.InstanceTermination {
	if t == nil {
		return nil
	}
	out := &workload.InstanceTermination{
		PodName:       t.PodName,
		ContainerName: t.ContainerName,
		Reason:        t.Reason,
		Message:       t.Message,
		Time:          t.Time,
	}
	if t.ExitCode != nil {
		e := *t.ExitCode
		out.ExitCode = &e
	}
	return out
}

func toV1beta1Termination(t *workload.InstanceTermination) *v1beta1.InstanceTermination {
	if t == nil {
		return nil
	}
	out := &v1beta1.InstanceTermination{
		PodName:       t.PodName,
		ContainerName: t.ContainerName,
		Reason:        t.Reason,
		Message:       t.Message,
		Time:          t.Time,
	}
	if t.ExitCode != nil {
		e := *t.ExitCode
		out.ExitCode = &e
	}
	return out
}

// findInstanceStatusInISVC is the test-local replacement for the
// removed query.FindInstanceStatus — looks up the InstanceStatus by
// (component, idx) on the v1beta1.InferenceService snapshot.
func findInstanceStatusInISVC(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.Lifecycle == nil {
		return nil
	}
	for i := range cs.Lifecycle.InstanceStatuses {
		if cs.Lifecycle.InstanceStatuses[i].Index == idx {
			return &cs.Lifecycle.InstanceStatuses[i]
		}
	}
	return nil
}

// buildPlanSinglePodEngine builds the workload.ComponentPlan a
// single-pod engine reconcile uses. Replicas drives the count.
func buildPlanSinglePodEngine(replicas int32) workload.ComponentPlan {
	instances := make([]workload.InstancePlan, replicas)
	for i := int32(0); i < replicas; i++ {
		instances[i] = workload.InstancePlan{
			Index:       i,
			Incarnation: 1,
			Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		}
	}
	return workload.ComponentPlan{
		Component:            workload.ComponentEngine,
		Replicas:             replicas,
		Instances:            instances,
		InstanceReadyTimeout: 30 * time.Minute,
	}
}

// resetExpectations re-seats the expectations singleton so back-to-
// back tests don't observe prior ExpectCreates entries.
func resetExpectations(t *testing.T) {
	t.Helper()
	workload.DefaultExpectations = workload.NewExpectations()
}

// podForInstance fabricates a pod matching what Render() would
// produce for the given (ISVC, instance) pair. The `ready` knob
// synthesizes ContainersReady=True, not PodReady=True — PodReady
// requires every readiness gate (including ome.io/serving), which is
// exactly what the controller is about to write.
func podForInstance(isvc *v1beta1.InferenceService, instanceIdx int32, ready, serving bool) *corev1.Pod {
	labels := testPodLabels(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 1, 0)
	labels[query.LabelRevisionHash] = testRevisionHash
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, instanceIdx, "default", 0),
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

func TestCreate_NilClient(t *testing.T) {
	resetExpectations(t)
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	_, err := ops.Create(context.Background(), workload.Deps{}, workload.ReconcileInput{}, plan, nil)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestCreate_EmptyClusterCreatesPodsAndRequeues(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(2)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
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
	if cs.Lifecycle == nil {
		t.Fatal("OMENative status nil")
	}
	if len(cs.Lifecycle.InstanceStatuses) != 2 {
		t.Fatalf("InstanceStatuses: got %d want 2", len(cs.Lifecycle.InstanceStatuses))
	}
	for _, s := range cs.Lifecycle.InstanceStatuses {
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
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
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
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
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
	if cs.Lifecycle == nil || len(cs.Lifecycle.InstanceStatuses) != 1 {
		t.Fatalf("status missing: %+v", cs.Lifecycle)
	}
	is := cs.Lifecycle.InstanceStatuses[0]
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
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(2)

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
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
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	// First call creates the pod.
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	// Reset expectations so the second call attempts to re-create.
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	// Second call should not error even though the pod already exists.
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("second Create: %v", err)
	}
}

// Promote to Ready must record RunningRevision so detectUpdateTrigger's
// fast path short-circuits on the next reconcile. Without it, every
// fresh Instance triggers a spurious recreate on its second pass (the
// per-pod diff false-positives against post-Render mutations).
//
// Bug #58 guard: Create's promote only stamps RunningRevision=target.Name
// when the existing pods actually carry target's revision hash — see
// existingPodsMatchTargetRevision. The test pod is given a rev-hash
// label matching the target so the normal-flow promote fires.
func TestCreate_PromoteRecordsRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-deadbeef", Namespace: "prod"},
	}
	// Pre-seed a runtime-ready, serving pod so reconcileInstance jumps
	// straight to the promote step. The pod's rev-hash matches target's
	// suffix — the production-normal case (createMissingPods stamps the
	// label from revisionHashFromTarget(target)).
	pod := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
		t.Fatalf("Create: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s := findInstanceStatusInISVC(fresh, v1beta1.EngineComponent, 0)
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

// TestCreate_PromoteSkipsRunningRevisionForOffTargetPods pins the bug
// #58 (X-2 bump-during-bump) guard in Create.reconcileInstance:
// when existing pods are runtime-ready but carry a different revision
// hash from target (e.g., they were created by an earlier surge cycle
// pinned to a now-superseded target), the promote MUST NOT stamp
// RunningRevision=target.Name — that would falsely advertise the pods
// as on target. patchInstanceStatusReady (no RunningRevision write)
// preserves whatever revision the prior op recorded, so the next
// reconcile's detectUpdateTrigger fires a fresh surge cycle to roll
// the pods to target.
func TestCreate_PromoteSkipsRunningRevisionForOffTargetPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	// Seed status: Instance is in a transient Phase (e.g., Creating
	// after an earlier surge promote) with the prior revision recorded
	// as RunningRevision. The Create promote should NOT clobber this.
	priorRev := "llama-70b-engine-priorrev"
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
					Index:           0,
					Incarnation:     1,
					Phase:           v1beta1.OMENativeInstanceCreating,
					RunningRevision: priorRev,
				}},
			},
		},
	}
	// Pre-seed a runtime-ready, serving pod labeled with the PRIOR
	// revision's hash (not target's). reconcileInstance will find the
	// pod, see it's runtime-ready, and reach the promote step.
	pod := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRev)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, target); err != nil {
		t.Fatalf("Create: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s := findInstanceStatusInISVC(fresh, v1beta1.EngineComponent, 0)
	if s == nil {
		t.Fatal("InstanceStatus[0] must exist after promote")
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	// The load-bearing assertion: RunningRevision must NOT have been
	// updated to target.Name — the pod is genuinely on priorRev.
	if s.RunningRevision != priorRev {
		t.Errorf("RunningRevision: got %q want %q (X-2 guard — Create promote must NOT clobber RunningRevision when pods are off-target)",
			s.RunningRevision, priorRev)
	}
}

// Inverse: when target is nil (scale-down-only reconciles), Create
// promotes without writing RunningRevision.
func TestCreate_PromoteWithNilTargetSkipsRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	pod := podForInstance(isvc, 0, true, true)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s := findInstanceStatusInISVC(fresh, v1beta1.EngineComponent, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("expected Phase=Ready; got %+v", s)
	}
	if s.RunningRevision != "" {
		t.Errorf("RunningRevision: got %q want empty (nil target)", s.RunningRevision)
	}
}
