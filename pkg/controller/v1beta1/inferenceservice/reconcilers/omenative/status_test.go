package omenative

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
)

// ---------------------------------------------------------------------
// AggregateAndWriteStatus — happy-path counter / revision / retention
// behavior over real (fake-client-served) pod + EndpointSlice state.
// ---------------------------------------------------------------------

func TestAggregateAndWriteStatus_NilClient(t *testing.T) {
	err := AggregateAndWriteStatus(context.Background(), ReconcileParams{}, ComponentPlan{Component: v1beta1.EngineComponent}, nil)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestAggregateAndWriteStatus_PerInstanceCountersFromObservedPods(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// One pod, ContainersReady=True, scheduled, in rotation.
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.NodeName = "node-7"
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].OMENative
	if cs == nil || len(cs.InstanceStatuses) != 1 {
		t.Fatalf("expected one InstanceStatus, got %+v", cs)
	}
	s := cs.InstanceStatuses[0]
	if s.PodCount != 1 || s.ReadyPodCount != 1 || s.ScheduledPodCount != 1 || s.AvailablePodCount != 1 {
		t.Errorf("per-Instance counters: got pods=%d ready=%d scheduled=%d available=%d; want 1/1/1/1",
			s.PodCount, s.ReadyPodCount, s.ScheduledPodCount, s.AvailablePodCount)
	}
	if len(s.NodesOccupied) != 1 || s.NodesOccupied[0] != "node-7" {
		t.Errorf("NodesOccupied: got %v want [node-7]", s.NodesOccupied)
	}
	if cs.Replicas != 1 || cs.ReadyReplicas != 1 || cs.AvailableReplicas != 1 {
		t.Errorf("component counters: got replicas=%d ready=%d available=%d; want 1/1/1",
			cs.Replicas, cs.ReadyReplicas, cs.AvailableReplicas)
	}
	if cs.LabelSelector == "" {
		t.Errorf("LabelSelector should be populated")
	}
}

func TestAggregateAndWriteStatus_AvailableRequiresEndpointSliceReady(t *testing.T) {
	// Pod is ContainersReady but the slice publishes the endpoint as
	// Ready=false. AvailablePodCount should be 0, AvailableReplicas 0,
	// but ReadyPodCount stays 1.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := podAtIncarnation(isvc, 0, 1, true, false)
	pod.Spec.NodeName = "node-7"
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, false)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].OMENative
	s := cs.InstanceStatuses[0]
	if s.ReadyPodCount != 1 {
		t.Errorf("ReadyPodCount: got %d want 1", s.ReadyPodCount)
	}
	if s.AvailablePodCount != 0 {
		t.Errorf("AvailablePodCount: got %d want 0 (slice still publishes Ready=false)", s.AvailablePodCount)
	}
	if cs.AvailableReplicas != 0 {
		t.Errorf("AvailableReplicas: got %d want 0", cs.AvailableReplicas)
	}
}

func TestAggregateAndWriteStatus_TargetWritesUpdateRevisionAndPromotesOnConvergence(t *testing.T) {
	// Single Instance already on the target revision and Phase=Ready ->
	// AggregateAndWriteStatus should set UpdateRevision and promote it
	// to CurrentRevision (rollout complete).
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	targetSpec := targetSpecImage("llama:v1")
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.NodeName = "node-7"
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpec)
	// Mark the Instance Ready and on the target revision.
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].RunningRevision = tcr.Name
	if err := c.Status().Update(context.Background(), isvc); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, tcr); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].OMENative
	if cs.UpdateRevision != tcr.Name {
		t.Errorf("UpdateRevision: got %q want %q", cs.UpdateRevision, tcr.Name)
	}
	if cs.CurrentRevision != tcr.Name {
		t.Errorf("CurrentRevision must promote to target when rollout is complete; got %q", cs.CurrentRevision)
	}
	if cs.UpdatedReplicas != 1 || cs.UpdatedReadyReplicas != 1 {
		t.Errorf("Updated counters: got %d/%d want 1/1", cs.UpdatedReplicas, cs.UpdatedReadyReplicas)
	}
}

func TestAggregateAndWriteStatus_NoPromotionWhileInstanceLagsTarget(t *testing.T) {
	// Instance carries a stale RunningRevision != target.Name; rollout
	// is in progress -> UpdateRevision is stamped but CurrentRevision
	// should NOT advance.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	targetSpec := targetSpecImage("llama:v2")
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpec)
	// Instance is Ready but RunningRevision is some other CR.
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].RunningRevision = "older-rev"
	if err := c.Status().Update(context.Background(), isvc); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, tcr); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].OMENative
	if cs.UpdateRevision != tcr.Name {
		t.Errorf("UpdateRevision: got %q want %q", cs.UpdateRevision, tcr.Name)
	}
	if cs.CurrentRevision == tcr.Name {
		t.Errorf("CurrentRevision must NOT promote while RunningRevision lags target")
	}
	if cs.UpdatedReplicas != 0 {
		t.Errorf("UpdatedReplicas: got %d want 0 (instance still on older-rev)", cs.UpdatedReplicas)
	}
}

func TestAggregateAndWriteStatus_PreservesLifecycleFields(t *testing.T) {
	// Lifecycle fields (Phase, RunningRevision, TargetRevision, Operation,
	// Incarnation) must not be overwritten by status aggregation.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	original := isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	original.Phase = v1beta1.OMENativeInstanceUpdating
	original.TargetRevision = "some-target"
	original.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "InPlace"}
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = original
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase clobbered: got %q want Updating", s.Phase)
	}
	if s.TargetRevision != "some-target" {
		t.Errorf("TargetRevision clobbered: got %q", s.TargetRevision)
	}
	if s.Operation == nil || s.Operation.Step != "InPlace" {
		t.Errorf("Operation clobbered: %+v", s.Operation)
	}
}

// TestAggregateAndWriteStatus_SurgeMidFlightHoldsServingReplicas pins
// the secondary SurgeThenDrain serving-dip fix: while a surge is in
// flight, an Instance briefly hosts TWO pods (the old canonical pod
// still serving and the new surge pod not yet ready). The
// observed-pod ratio (ServingPodCount == PodCount) would mis-classify
// the Instance as non-serving and drop ServingReplicas — even though
// the instance is still serving traffic the entire time.
//
// The fix: classify "instance is serving" as
// `ServingPodCount >= desiredPodCount`, where desired comes from the
// plan (Runner.Size), not from the live pod count.
//
// Reproduction shape (mirrors the cluster repro):
//   - Instance 0: old pod at ord=0 (Ready + Serving) + surge pod at
//     ord=1 (NOT Ready, NOT Serving). The instance must still count
//     as serving.
func TestAggregateAndWriteStatus_SurgeMidFlightHoldsServingReplicas(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Old canonical pod at ordinal 0 — still serving and Ready.
	oldPod := podAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	oldPod.Spec.NodeName = "node-old"
	oldPod.Labels[query.LabelPodOrdinal] = "0"
	// Surge pod at ordinal 1 — created, scheduled, but not yet
	// ContainersReady (kubelet still starting). NOT serving.
	surgePod := podAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	surgePod.Name = query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	surgePod.Spec.NodeName = "node-new"
	surgePod.Labels[query.LabelPodOrdinal] = "1"
	slice := sliceWithEndpoint("prod", "engine-1",
		query.HeadlessServiceName(isvc.Name, v1beta1.EngineComponent), oldPod, true)
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].OMENative
	s := cs.InstanceStatuses[0]

	// Per-pod observed counters reflect the live state — 2 pods total,
	// 1 ready, 1 serving. These are NOT being changed by the fix.
	if s.PodCount != 2 {
		t.Errorf("PodCount: got %d want 2 (old + surge)", s.PodCount)
	}
	if s.ReadyPodCount != 1 {
		t.Errorf("ReadyPodCount: got %d want 1 (only old is ready)", s.ReadyPodCount)
	}
	if s.ServingPodCount != 1 {
		t.Errorf("ServingPodCount: got %d want 1 (only old is serving)", s.ServingPodCount)
	}

	// Component-level counters MUST reflect the canonical truth:
	// the instance is still serving traffic. Without the fix,
	// ServingReplicas would drop to 0 (1 != 2) — the user-visible dip.
	if cs.ServingReplicas != 1 {
		t.Errorf("ServingReplicas dipped during surge: got %d want 1 (instance still serving via old pod)",
			cs.ServingReplicas)
	}
	if cs.ReadyReplicas != 1 {
		t.Errorf("ReadyReplicas dipped during surge: got %d want 1 (instance still ready)",
			cs.ReadyReplicas)
	}
	if cs.AvailableReplicas != 1 {
		t.Errorf("AvailableReplicas dipped during surge: got %d want 1 (old pod still in EP rotation)",
			cs.AvailableReplicas)
	}
}

// TestAggregateAndWriteStatus_SurgeWithFewerCanonicalPodsNotServing
// pins the negative case: when even the canonical (desired-count)
// pods are not all serving, the Instance is NOT counted. Differs from
// the happy-path above by drainin the old pod too — now zero pods
// are serving and ServingReplicas should drop.
func TestAggregateAndWriteStatus_SurgeWithFewerCanonicalPodsNotServing(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Old pod at ord=0 — drained (NOT serving), still Ready.
	oldPod := podAtIncarnation(isvc, 0, 1, true /* ready */, false /* drained */)
	oldPod.Labels[query.LabelPodOrdinal] = "0"
	// Surge pod at ord=1 — not yet Ready.
	surgePod := podAtIncarnation(isvc, 0, 1, false, false)
	surgePod.Name = query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	surgePod.Labels[query.LabelPodOrdinal] = "1"
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].OMENative
	if cs.ServingReplicas != 0 {
		t.Errorf("ServingReplicas: got %d want 0 (no pod is serving anymore)", cs.ServingReplicas)
	}
}

func TestUniqueNodes(t *testing.T) {
	pods := []*corev1.Pod{
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{NodeName: "b"}},
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{}}, // unscheduled — skipped
	}
	got := uniqueNodes(pods)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("uniqueNodes: got %v want [a b]", got)
	}
	if got := uniqueNodes(nil); got != nil {
		t.Errorf("nil input should return nil; got %v", got)
	}
}

func TestRolloutComplete(t *testing.T) {
	allReady := []v1beta1.OMENativeInstanceStatus{
		{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev"},
		{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev"},
	}
	if !rolloutComplete(allReady, "rev") {
		t.Errorf("all Ready on rev should be complete")
	}
	oneCreating := []v1beta1.OMENativeInstanceStatus{
		{Phase: v1beta1.OMENativeInstanceCreating, RunningRevision: "rev"},
		{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev"},
	}
	if rolloutComplete(oneCreating, "rev") {
		t.Errorf("Phase=Creating should keep rollout incomplete")
	}
	stale := []v1beta1.OMENativeInstanceStatus{
		{Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "old"},
	}
	if rolloutComplete(stale, "rev") {
		t.Errorf("stale RunningRevision should keep rollout incomplete")
	}
	if rolloutComplete(nil, "rev") {
		t.Errorf("empty instance list should not be complete")
	}
}

// Silence the linter when test helpers in the omenative package change.
var _ = ptr.To[int32]

// ---------------------------------------------------------------------
// Retry behavior — every status-write entry point wraps Status().Update
// in retry.RetryOnConflict. Inject a 409 on the first attempt and
// assert the second attempt lands.
// ---------------------------------------------------------------------

// conflictOnce returns a SubResourceUpdate interceptor that injects a
// 409 Conflict on the first call, then forwards subsequent calls to
// the underlying client. Used by every C2 retry test to confirm the
// retry.RetryOnConflict wrapper actually lands the write on attempt
// #2.
func conflictOnce(counter *int64) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if atomic.AddInt64(counter, 1) == 1 {
				return apierrors.NewConflict(
					schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
					obj.GetName(),
					errors.New("simulated concurrent write"),
				)
			}
			return c.Status().Update(ctx, obj, opts...)
		},
	}
}

func newStatusRetryTestParams(t *testing.T, isvc *v1beta1.InferenceService, counter *int64) ReconcileParams {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		Build()
	c := interceptor.NewClient(base, conflictOnce(counter))
	return ReconcileParams{
		ISVC:      isvc,
		Component: v1beta1.EngineComponent,
		Client:    c,
	}
}

func freshISVC(ns, name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       "isvc-uid",
		},
	}
}

func TestMutateInstanceStatus_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	p := newStatusRetryTestParams(t, isvc, &calls)

	mutateCalls := 0
	err := status.MutateInstance(context.Background(), p.Client, p.ISVC, v1beta1.EngineComponent, 0,
		func(s *v1beta1.OMENativeInstanceStatus) bool {
			mutateCalls++
			s.Phase = v1beta1.OMENativeInstanceCreating
			return true
		})
	if err != nil {
		t.Fatalf("mutateInstanceStatus returned error after 409+retry: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 Update attempts (1 conflict + 1 success), got %d", calls)
	}
	if mutateCalls != 2 {
		t.Errorf("mutate callback should run once per attempt; got %d", mutateCalls)
	}

	// Verify the write actually landed.
	got := &v1beta1.InferenceService{}
	if err := p.Client.Get(context.Background(), client.ObjectKeyFromObject(isvc), got); err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	cs := got.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative == nil || len(cs.OMENative.InstanceStatuses) != 1 {
		t.Fatalf("InstanceStatus not landed: %+v", cs)
	}
	if cs.OMENative.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("wrong Phase: got %s", cs.OMENative.InstanceStatuses[0].Phase)
	}
}

// TestRemoveInstanceStatus_RetriesOnConflict lived here until removeInstanceStatus
// moved into ops/ — calling it from this package would close a cycle.
// Coverage of the retry.RetryOnConflict wrapper is preserved by
// TestBumpCollisionCount_RetriesOnConflict + TestAggregateAndWriteStatus_RetriesOnConflict
// below, which exercise the same wrapper on adjacent status writers.

func TestBumpCollisionCount_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	p := newStatusRetryTestParams(t, isvc, &calls)

	got, err := bumpCollisionCount(context.Background(), p, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("bumpCollisionCount returned error after 409+retry: %v", err)
	}
	if got == nil || *got != 1 {
		t.Errorf("expected CollisionCount=1, got %v", got)
	}
	if calls != 2 {
		t.Errorf("expected 2 Update attempts, got %d", calls)
	}

	// Verify the apiserver landed the bump.
	served := &v1beta1.InferenceService{}
	if err := p.Client.Get(context.Background(), client.ObjectKeyFromObject(isvc), served); err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	cs := served.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative == nil || cs.OMENative.CollisionCount == nil || *cs.OMENative.CollisionCount != 1 {
		t.Errorf("CollisionCount not persisted: %+v", cs)
	}

	// Verify the cached ISVC was also mirrored (the function does this
	// outside the retry, only on successful commit).
	if p.ISVC.Status.Components[v1beta1.EngineComponent].OMENative == nil ||
		p.ISVC.Status.Components[v1beta1.EngineComponent].OMENative.CollisionCount == nil {
		t.Errorf("cached ISVC not mirrored")
	}
}

func TestAggregateAndWriteStatus_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	p := newStatusRetryTestParams(t, isvc, &calls)
	plan := ComponentPlan{
		Component: v1beta1.EngineComponent,
		Replicas:  1,
		Instances: []InstancePlan{{Index: 0, Incarnation: 1}},
	}

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus returned error after 409+retry: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 Update attempts, got %d", calls)
	}

	got := &v1beta1.InferenceService{}
	if err := p.Client.Get(context.Background(), client.ObjectKeyFromObject(isvc), got); err != nil {
		t.Fatalf("get after retry: %v", err)
	}
	cs := got.Status.Components[v1beta1.EngineComponent]
	if cs.OMENative.Replicas != 1 {
		t.Errorf("Replicas not aggregated: got %d", cs.OMENative.Replicas)
	}
	if cs.OMENative.LabelSelector == "" {
		t.Errorf("LabelSelector not set")
	}
}

func TestMutateInstanceStatus_NoWriteWhenMutateReturnsFalse(t *testing.T) {
	// Sanity: if the mutate callback returns false, no Update is
	// issued, no conflict, no retry, and the test interceptor's
	// counter stays at 0.
	var calls int64
	isvc := freshISVC("ns", "isvc")
	p := newStatusRetryTestParams(t, isvc, &calls)

	err := status.MutateInstance(context.Background(), p.Client, p.ISVC, v1beta1.EngineComponent, 0,
		func(s *v1beta1.OMENativeInstanceStatus) bool { return false })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 0 {
		t.Errorf("expected 0 Update attempts when mutate returns false, got %d", calls)
	}
}
