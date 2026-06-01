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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
)

// ---------------------------------------------------------------------
// AggregateAndWriteStatus — happy-path counter / revision / retention
// behavior over real (fake-client-served) pod + EndpointSlice state.
// ---------------------------------------------------------------------

func TestAggregateAndWriteStatus_NilClient(t *testing.T) {
	err := AggregateAndWriteStatus(context.Background(), ReconcileParams{}, ComponentPlan{Component: v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)}, nil)
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
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
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
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)), pod, false)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
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
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpec)
	// Mark the Instance Ready and on the target revision.
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision = tcr.Name
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
	cs := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
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
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpec)
	// Instance is Ready but RunningRevision is some other CR.
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision = "older-rev"
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
	cs := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
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
	original := isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
	original.Phase = v1beta1.OMENativeInstanceUpdating
	original.TargetRevision = "some-target"
	original.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "InPlace"}
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0] = original
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
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
	surgePod.Name = query.PodName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 1)
	surgePod.Spec.NodeName = "node-new"
	surgePod.Labels[query.LabelPodOrdinal] = "1"
	slice := sliceWithEndpoint("prod", "engine-1",
		query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)), oldPod, true)
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod, slice)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
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
	surgePod.Name = query.PodName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 1)
	surgePod.Labels[query.LabelPodOrdinal] = "1"
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
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
	if cs.Lifecycle == nil || len(cs.Lifecycle.InstanceStatuses) != 1 {
		t.Fatalf("InstanceStatus not landed: %+v", cs)
	}
	if cs.Lifecycle.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("wrong Phase: got %s", cs.Lifecycle.InstanceStatuses[0].Phase)
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
	if cs.Lifecycle == nil || cs.Lifecycle.CollisionCount == nil || *cs.Lifecycle.CollisionCount != 1 {
		t.Errorf("CollisionCount not persisted: %+v", cs)
	}

	// Verify the cached ISVC was also mirrored (the function does this
	// outside the retry, only on successful commit).
	if p.ISVC.Status.Components[v1beta1.EngineComponent].Lifecycle == nil ||
		p.ISVC.Status.Components[v1beta1.EngineComponent].Lifecycle.CollisionCount == nil {
		t.Errorf("cached ISVC not mirrored")
	}
}

func TestAggregateAndWriteStatus_RetriesOnConflict(t *testing.T) {
	var calls int64
	isvc := freshISVC("ns", "isvc")
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	p := newStatusRetryTestParams(t, isvc, &calls)
	plan := ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent),
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
	if cs.Lifecycle.Replicas != 1 {
		t.Errorf("Replicas not aggregated: got %d", cs.Lifecycle.Replicas)
	}
	if cs.Lifecycle.LabelSelector == "" {
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

// TestSurgeUpdate_SingleBump_URMatchesCR_OnlyOneNewCR is the unit
// reproduction of task #56's A-2 cluster symptom: a single bump of an
// ISVC's pod-template annotation must, after the surge cycle fully
// completes, leave:
//   - exactly ONE new ControllerRevision created (count 1 → 2);
//   - status.UpdateRevision == status.CurrentRevision == the new CR's
//     name (no orphan-hash drift between the two status fields).
//
// The production observation (cluster f783c1b0, post-#256+#258):
//   - CRs count 1 → 2 (one new CR created — correct).
//   - status.UpdateRevision = 300e2aa3 (the new target).
//   - status.CurrentRevision = e176faf1 (a hash with NO backing CR).
//
// The orphan in CurrentRevision is the symptom this test pins shut. The
// test drives the full surge cycle (initial create → bump → surge pod
// create → surge ready → drain old → promote) through ReconcileComponent,
// then asserts the status invariant.
//
// Mechanism: AggregateAndWriteStatus stamps cs.Lifecycle.UpdateRevision =
// target.Name on every pass and promotes CurrentRevision = target.Name
// when rolloutComplete=true. target.Name is the .Name of the
// ControllerRevision EnsureControllerRevision returned, which is always
// a backing CR in the cluster — so neither field should ever drift to
// an orphan hash. The test exercises the end-to-end path.
func TestSurgeUpdate_SingleBump_URMatchesCR_OnlyOneNewCR(t *testing.T) {
	resetExpectations(t)
	// Build a SurgeThenDrain ISVC: minReplicas=1, engine annotation set
	// to the "pre-bump" trigger value. The annotation goes into the
	// rendered ObjectMeta and thus into the hash → bumping it triggers a
	// new revision exactly like the cluster repro.
	isvc := minimalISVC("a1", "sandbox", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
		},
	}
	isvc.Spec.Engine.Annotations = map[string]string{
		"test.ome.io/rollout-trigger": "initial",
	}
	c := newFakeClientWithDiscovery(t, isvc)

	// Initial reconcile: drives Create. The exact set of mid-state
	// reconciles isn't load-bearing for this test — what matters is that
	// after the bump, the surge completes and UR == CR == new CR's name.
	//
	// First, set up a steady-state Ready InstanceStatus on the initial
	// revision by hand (so we don't have to spin a full Create loop —
	// that's covered by TestReconcileComponent_DispatchesToCreate).
	initialSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "engine:initial"}},
	}
	initialMeta := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"test.ome.io/rollout-trigger": "initial",
		},
	}
	initialCR, _, err := core.EnsureControllerRevisionForISVC(context.Background(), c, c, isvc, v1beta1.EngineComponent, initialSpec, initialMeta, nil)
	if err != nil {
		t.Fatalf("seed initial CR: %v", err)
	}

	// Seed the steady-state pod for ord=0 on the initial revision.
	oldPod := podAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	oldPod.Labels[query.LabelPodOrdinal] = "0"
	oldPod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(initialCR.Name)
	if err := c.Create(context.Background(), oldPod); err != nil {
		t.Fatalf("seed old pod: %v", err)
	}
	// Update isvc.Status to reflect the steady-state on the initial CR.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				CurrentRevision: initialCR.Name,
				UpdateRevision:  initialCR.Name,
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
					Index:           0,
					Incarnation:     1,
					Phase:           v1beta1.OMENativeInstanceReady,
					RunningRevision: initialCR.Name,
				}},
			},
		},
	}
	if err := c.Status().Update(context.Background(), isvc); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	// Pre-bump sanity: only 1 CR exists.
	preList := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), preList, client.InNamespace("sandbox"))
	if len(preList.Items) != 1 {
		t.Fatalf("pre-bump CR count: got %d want 1", len(preList.Items))
	}

	// --- BUMP ---
	// Operator changes the annotation to a unique nanosecond timestamp —
	// the canonical SurgeThenDrain test trigger. The new value forces a
	// new revision hash and surge cycle.
	isvc.Spec.Engine.Annotations["test.ome.io/rollout-trigger"] = "bumped"

	// Drive ReconcileComponent through the full surge cycle. Each pass
	// advances pod state to simulate the kubelet acknowledging
	// transitions (ContainersReady, serving=True, old pod gone).
	bumpedSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "engine:initial"}},
	}
	bumpedMeta := metav1.ObjectMeta{
		Name:      query.StableServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)),
		Namespace: isvc.Namespace,
		Annotations: map[string]string{
			"test.ome.io/rollout-trigger": "bumped",
		},
	}
	p := ReconcileParams{
		ISVC:         isvc,
		ObjectMeta:   bumpedMeta,
		PodSpec:      bumpedSpec,
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		Client:       c,
	}

	// Pass 1: ensureRevisionWithCollisionRetry creates the new CR.
	// surgeUpdate creates the surge pod at ord=1, stamps Op{Step=Surge}.
	// Defer: AggregateAndWriteStatus writes UR=newCR.Name (rolloutComplete=false → CR stays at initial).
	if _, err := ReconcileComponent(context.Background(), p); err != nil {
		t.Fatalf("pass 1: %v", err)
	}

	// After pass 1, the new CR exists.
	postPass1List := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), postPass1List, client.InNamespace("sandbox"))
	if len(postPass1List.Items) != 2 {
		names := make([]string, 0, len(postPass1List.Items))
		for _, cr := range postPass1List.Items {
			names = append(names, cr.Name)
		}
		t.Fatalf("post-pass-1 CR count: got %d (%v) want 2", len(postPass1List.Items), names)
	}

	// Identify the new CR (the one that's NOT initialCR).
	var newCR *appsv1.ControllerRevision
	for i := range postPass1List.Items {
		cr := &postPass1List.Items[i]
		if cr.Name != initialCR.Name {
			newCR = cr
			break
		}
	}
	if newCR == nil {
		t.Fatalf("could not find the new CR")
	}

	// Simulate kubelet: surge pod becomes ContainersReady.
	surgePodName := query.PodName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 1)
	surgePod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: surgePodName}, surgePod); err != nil {
		t.Fatalf("get surge pod: %v", err)
	}
	surgePod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionFalse},
	}
	if err := c.Status().Update(context.Background(), surgePod); err != nil {
		t.Fatalf("flip surge pod ContainersReady: %v", err)
	}

	// Pass 2: surge advances to Step=SurgeDrain (surge serving=True,
	// old serving=False, wait drain).
	if _, err := ReconcileComponent(context.Background(), p); err != nil {
		t.Fatalf("pass 2: %v", err)
	}

	// Simulate drain completing: delete old pod, flip surge to serving=True.
	if err := c.Delete(context.Background(), oldPod); err != nil {
		t.Fatalf("delete old pod: %v", err)
	}
	surgePodFresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: surgePodName}, surgePodFresh); err != nil {
		t.Fatalf("re-read surge pod: %v", err)
	}
	surgePodFresh.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), surgePodFresh); err != nil {
		t.Fatalf("flip surge to serving=True: %v", err)
	}

	// Pass 3: Phase 3 promote — old pod gone, surge becomes canonical.
	// AggregateAndWriteStatus: rolloutComplete=true → CR=newCR.Name.
	if _, err := ReconcileComponent(context.Background(), p); err != nil {
		t.Fatalf("pass 3: %v", err)
	}

	// Pass 4: idempotent pass to settle any remaining status writes
	// (e.g., the post-promote status mirror). This is the parallel of
	// the cluster's "another reconcile happened after promote" step.
	if _, err := ReconcileComponent(context.Background(), p); err != nil {
		t.Fatalf("pass 4: %v", err)
	}

	// --- ASSERTIONS ---

	// Exactly 2 CRs in the cluster (no extras created by spurious bumps).
	finalCRs := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), finalCRs, client.InNamespace("sandbox"))
	if len(finalCRs.Items) != 2 {
		names := make([]string, 0, len(finalCRs.Items))
		for _, cr := range finalCRs.Items {
			names = append(names, cr.Name)
		}
		t.Errorf("final CR count: got %d (%v) want 2 (initial + new)", len(finalCRs.Items), names)
	}

	// Status: UR == CR == newCR.Name. Both must reference a backing CR.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	cs := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
	if cs == nil {
		t.Fatalf("LifecycleStatus missing")
	}
	if cs.UpdateRevision != newCR.Name {
		t.Errorf("UpdateRevision: got %q want %q (new CR)", cs.UpdateRevision, newCR.Name)
	}
	if cs.CurrentRevision != newCR.Name {
		t.Errorf("CurrentRevision: got %q want %q (new CR, rollout complete)",
			cs.CurrentRevision, newCR.Name)
	}
	if cs.UpdateRevision != cs.CurrentRevision {
		t.Errorf("UR != CR after full surge convergence: UR=%q CR=%q (this is the A-2 orphan-hash bug)",
			cs.UpdateRevision, cs.CurrentRevision)
	}

	// Sanity: every name referenced by status has a backing CR.
	crNames := make(map[string]struct{}, len(finalCRs.Items))
	for _, cr := range finalCRs.Items {
		crNames[cr.Name] = struct{}{}
	}
	if _, ok := crNames[cs.UpdateRevision]; !ok {
		t.Errorf("status.UpdateRevision=%q is an ORPHAN — no backing CR (cluster has %v)",
			cs.UpdateRevision, crNamesSlice(finalCRs.Items))
	}
	if _, ok := crNames[cs.CurrentRevision]; !ok {
		t.Errorf("status.CurrentRevision=%q is an ORPHAN — no backing CR (cluster has %v)",
			cs.CurrentRevision, crNamesSlice(finalCRs.Items))
	}
}

// crNamesSlice returns the .Name of every CR in items, for error formatting.
func crNamesSlice(items []appsv1.ControllerRevision) []string {
	out := make([]string, 0, len(items))
	for _, cr := range items {
		out = append(out, cr.Name)
	}
	return out
}

// TestSurgeUpdate_BumpDuringBump_V3CRCreated_NoV2Straggler pins the
// X-2 bug #58: when a second spec bump (v2 → v3) arrives WHILE the
// first bump's (v1 → v2) surge is still in flight (surge pod created
// but not yet promoted), the controller MUST:
//
//  1. Create the v3 ControllerRevision (it MUST exist after the
//     bump-during-bump reconcile, not just after v2 promotes).
//  2. Eventually drive every pod to v3 (no v2 stragglers).
//  3. Stamp status.CurrentRevision = v3 once convergence completes.
//
// Production observation (cluster sandbox, X-2 spec): the v3 CR was
// never created, a v2-flavored pod lingered (+1 vs expected), and
// status.CurrentRevision stayed at v2 forever. The spec bumps via the
// `test.ome.io/rollout-trigger` annotation (no image change), exactly
// like the X-2 SRT test does.
//
// The test drives the full surge cycle through ReconcileComponent so
// the bug surfaces from the same code path the cluster exercises.
func TestSurgeUpdate_BumpDuringBump_V3CRCreated_NoV2Straggler(t *testing.T) {
	resetExpectations(t)
	// Single-replica SurgeThenDrain ISVC — the simplest reproducer for
	// the bump-during-bump bug. Multi-replica would multiply the in-
	// flight surge count but the failure mode is per-Instance: v3 must
	// be ensured even when v2 surge for the SAME Instance is in flight.
	isvc := minimalISVC("x2", "sandbox", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
		},
	}
	isvc.Spec.Engine.Annotations = map[string]string{
		"test.ome.io/rollout-trigger": "v1",
	}
	c := newFakeClientWithDiscovery(t, isvc)

	// Seed steady-state on v1: a single CR with the initial annotation,
	// a Ready+Serving pod at ord=0, status reflecting both.
	v1Spec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "engine:v1"}},
	}
	v1Meta := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"test.ome.io/rollout-trigger": "v1",
		},
	}
	v1CR, _, err := core.EnsureControllerRevisionForISVC(context.Background(), c, c, isvc, v1beta1.EngineComponent, v1Spec, v1Meta, nil)
	if err != nil {
		t.Fatalf("seed v1 CR: %v", err)
	}
	v1Pod := podAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	v1Pod.Labels[query.LabelPodOrdinal] = "0"
	v1Pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(v1CR.Name)
	if err := c.Create(context.Background(), v1Pod); err != nil {
		t.Fatalf("seed v1 pod: %v", err)
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				CurrentRevision: v1CR.Name,
				UpdateRevision:  v1CR.Name,
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
					Index:           0,
					Incarnation:     1,
					Phase:           v1beta1.OMENativeInstanceReady,
					RunningRevision: v1CR.Name,
				}},
			},
		},
	}
	if err := c.Status().Update(context.Background(), isvc); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	// --- BUMP #1: v1 → v2 ---
	// Mutate the annotation; render the new ObjectMeta (matches what
	// engine.go would produce on the next reconcile).
	isvc.Spec.Engine.Annotations["test.ome.io/rollout-trigger"] = "v2"
	v2Meta := metav1.ObjectMeta{
		Name:      query.StableServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)),
		Namespace: isvc.Namespace,
		Annotations: map[string]string{
			"test.ome.io/rollout-trigger": "v2",
		},
	}
	pV2 := ReconcileParams{
		ISVC:         isvc,
		ObjectMeta:   v2Meta,
		PodSpec:      v1Spec, // same PodSpec — bump comes from annotation only
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		Client:       c,
	}

	// Pass 1 (v2): ensureRevisionWithCollisionRetry creates v2 CR.
	// surgeUpdate stamps Op{Surge, v2}, creates v2 surge pod at ord=1.
	if _, err := ReconcileComponent(context.Background(), pV2); err != nil {
		t.Fatalf("v2 pass 1: %v", err)
	}
	postV2List := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), postV2List, client.InNamespace("sandbox"))
	if len(postV2List.Items) != 2 {
		t.Fatalf("after v2 bump: CR count got %d want 2 (v1 + v2)", len(postV2List.Items))
	}
	var v2CR *appsv1.ControllerRevision
	for i := range postV2List.Items {
		cr := &postV2List.Items[i]
		if cr.Name != v1CR.Name {
			v2CR = cr
			break
		}
	}
	if v2CR == nil {
		t.Fatalf("could not find v2 CR")
	}
	// Confirm v2 surge pod exists. NOT yet ready — this is the
	// "in-flight surge" state where the bump-during-bump arrives.
	v2SurgePodName := query.PodName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 1)
	v2SurgePod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: v2SurgePodName}, v2SurgePod); err != nil {
		t.Fatalf("v2 surge pod missing after v2 pass 1: %v", err)
	}

	// --- BUMP #2: v2 → v3 (DURING the v2 surge — v2 pod NOT yet ready) ---
	// This is the X-2 bump-during-bump trigger.
	isvc.Spec.Engine.Annotations["test.ome.io/rollout-trigger"] = "v3"
	v3Meta := metav1.ObjectMeta{
		Name:      query.StableServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)),
		Namespace: isvc.Namespace,
		Annotations: map[string]string{
			"test.ome.io/rollout-trigger": "v3",
		},
	}
	pV3 := ReconcileParams{
		ISVC:         isvc,
		ObjectMeta:   v3Meta,
		PodSpec:      v1Spec,
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		Client:       c,
	}

	// Pass 1 (v3 bump): ensureRevisionWithCollisionRetry MUST create the
	// v3 CR even while the v2 surge is in flight. This is the
	// load-bearing assertion for bug #58 — the production cluster
	// observed the v3 CR was NEVER created.
	if _, err := ReconcileComponent(context.Background(), pV3); err != nil {
		t.Fatalf("v3 bump pass 1: %v", err)
	}
	postV3List := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), postV3List, client.InNamespace("sandbox"))
	if len(postV3List.Items) != 3 {
		names := make([]string, 0, len(postV3List.Items))
		for _, cr := range postV3List.Items {
			names = append(names, cr.Name)
		}
		t.Fatalf("after v3 bump: CR count got %d (%v) want 3 (v1+v2+v3) — bug #58 fires here when v3 CR is never created",
			len(postV3List.Items), names)
	}
	var v3CR *appsv1.ControllerRevision
	for i := range postV3List.Items {
		cr := &postV3List.Items[i]
		if cr.Name != v1CR.Name && cr.Name != v2CR.Name {
			v3CR = cr
			break
		}
	}
	if v3CR == nil {
		t.Fatalf("could not find v3 CR after bump-during-bump")
	}

	// Drive the surge cycle to completion. Each reconcile pass simulates
	// kubelet acks: surge pod containers ready, drain finishes (old pod
	// deleted), serving flip, then promote.
	//
	// Note: at this point, the v2 surge is in-flight (Op pinned to v2).
	// We drive it to completion FIRST (PR #256 contract: pinned target
	// continues), then the post-v2-promote reconcile fires the v3 surge.

	// Step: flip v2 surge pod to ContainersReady.
	v2SurgePodFresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: v2SurgePodName}, v2SurgePodFresh); err != nil {
		t.Fatalf("re-read v2 surge pod: %v", err)
	}
	v2SurgePodFresh.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionFalse},
	}
	if err := c.Status().Update(context.Background(), v2SurgePodFresh); err != nil {
		t.Fatalf("flip v2 surge ContainersReady: %v", err)
	}

	// Pass 2 (v3): surge advances Step=SurgeDrain; v1 pod flipped to NotServing.
	if _, err := ReconcileComponent(context.Background(), pV3); err != nil {
		t.Fatalf("v3 pass 2: %v", err)
	}

	// Simulate drain completing: delete v1 pod, flip v2 surge serving=True.
	if err := c.Delete(context.Background(), v1Pod); err != nil {
		t.Fatalf("delete v1 pod: %v", err)
	}
	v2SurgePodAfterDrain := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: v2SurgePodName}, v2SurgePodAfterDrain); err != nil {
		t.Fatalf("re-read v2 surge after drain: %v", err)
	}
	v2SurgePodAfterDrain.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), v2SurgePodAfterDrain); err != nil {
		t.Fatalf("flip v2 surge serving=True: %v", err)
	}

	// Pass 3 (v3): v2 surge promotes (RunningRev=v2, ActiveOrdinal=1).
	// Bug #58 fix: workload.Reconcile now requeues without running
	// Create when any Update fired, so the in-place ObservedState
	// staleness window after promote can't corrupt RunningRevision.
	if _, err := ReconcileComponent(context.Background(), pV3); err != nil {
		t.Fatalf("v3 pass 3 (promote v2): %v", err)
	}
	core.DefaultExpectations.Forget("sandbox", "x2", workload.ComponentEngine, 0)

	// Pass 4 (v3): now Phase=Ready + RunningRev=v2 (genuine).
	// detectUpdateTrigger fires (RunningRev != target=v3) and a fresh
	// v3 surge cycle starts — creates v3 surge pod at ord=0. This is
	// the v3 cycle the original X-2 bug failed to reach.
	if _, err := ReconcileComponent(context.Background(), pV3); err != nil {
		t.Fatalf("v3 pass 4 (fire v3 surge): %v", err)
	}

	// Confirm v3 surge pod created at ord=0 (alternate slot since
	// ActiveOrdinal moved to 1 after v2 promote).
	v3SurgePodName := query.PodName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 0)
	v3SurgePod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: v3SurgePodName}, v3SurgePod); err != nil {
		t.Fatalf("v3 surge pod missing after v3 pass 4: %v", err)
	}
	v3HashLabel := query.RevisionHashFromControllerRevisionName(v3CR.Name)
	if got := v3SurgePod.Labels[query.LabelRevisionHash]; got != v3HashLabel {
		t.Errorf("v3 surge pod revision-hash label: got %q want %q", got, v3HashLabel)
	}

	// Step: flip v3 surge pod to ContainersReady.
	v3SurgePodFresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: v3SurgePodName}, v3SurgePodFresh); err != nil {
		t.Fatalf("re-read v3 surge: %v", err)
	}
	v3SurgePodFresh.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionFalse},
	}
	if err := c.Status().Update(context.Background(), v3SurgePodFresh); err != nil {
		t.Fatalf("flip v3 surge ContainersReady: %v", err)
	}

	// Pass 5 (v3): drain v2 surge pod.
	if _, err := ReconcileComponent(context.Background(), pV3); err != nil {
		t.Fatalf("v3 pass 5 (drain v2): %v", err)
	}

	// Simulate drain completing: delete v2 surge pod, flip v3 surge serving=True.
	v2PodNow := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: v2SurgePodName}, v2PodNow); err == nil {
		if err := c.Delete(context.Background(), v2PodNow); err != nil {
			t.Fatalf("delete v2 surge: %v", err)
		}
	}
	v3SurgePodFinal := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "sandbox", Name: v3SurgePodName}, v3SurgePodFinal); err != nil {
		t.Fatalf("re-read v3 surge: %v", err)
	}
	v3SurgePodFinal.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), v3SurgePodFinal); err != nil {
		t.Fatalf("flip v3 surge serving=True: %v", err)
	}
	core.DefaultExpectations.Forget("sandbox", "x2", workload.ComponentEngine, 0)

	// Pass 6 (v3): v3 surge promotes.
	if _, err := ReconcileComponent(context.Background(), pV3); err != nil {
		t.Fatalf("v3 pass 6 (promote v3): %v", err)
	}
	// Pass 7 (v3): idempotent settle (CurrentRevision promote).
	if _, err := ReconcileComponent(context.Background(), pV3); err != nil {
		t.Fatalf("v3 pass 7 (settle): %v", err)
	}

	// --- FINAL ASSERTIONS ---

	// 1. The v3 CR exists (the load-bearing assertion for bug #58).
	finalCRs := &appsv1.ControllerRevisionList{}
	_ = c.List(context.Background(), finalCRs, client.InNamespace("sandbox"))
	crByName := make(map[string]struct{}, len(finalCRs.Items))
	for _, cr := range finalCRs.Items {
		crByName[cr.Name] = struct{}{}
	}
	if _, ok := crByName[v3CR.Name]; !ok {
		t.Errorf("v3 CR %q missing from cluster — bug #58 fires here (have: %v)",
			v3CR.Name, crNamesSlice(finalCRs.Items))
	}

	// 2. status.CurrentRevision == v3CR.Name (final converged state).
	freshFinal := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), freshFinal); err != nil {
		t.Fatalf("re-read ISVC final: %v", err)
	}
	csFinal := freshFinal.Status.Components[v1beta1.EngineComponent].Lifecycle
	if csFinal == nil {
		t.Fatalf("LifecycleStatus missing on final read")
	}
	if csFinal.CurrentRevision != v3CR.Name {
		t.Errorf("final CurrentRevision: got %q want %q (rollout should converge to v3)",
			csFinal.CurrentRevision, v3CR.Name)
	}

	// 3. No v2 stragglers: every live pod is on v1 (the seed, which
	// was untouched in the spec's "ALLOWED=V1+FINAL" set) or v3. The
	// v2 hash MUST NOT survive in any pod label.
	livePods := &corev1.PodList{}
	_ = c.List(context.Background(), livePods, client.InNamespace("sandbox"))
	v1Hash := query.RevisionHashFromControllerRevisionName(v1CR.Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2CR.Name)
	v3Hash := query.RevisionHashFromControllerRevisionName(v3CR.Name)
	for i := range livePods.Items {
		pod := &livePods.Items[i]
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == v2Hash {
			t.Errorf("X-2 FAIL: pod %s/%s labeled with v2 hash %q — v2 straggler survived (bug #58)",
				pod.Namespace, pod.Name, v2Hash)
		}
		if hash != v1Hash && hash != v3Hash {
			t.Errorf("X-2 FAIL: pod %s/%s labeled with unexpected hash %q (allowed: v1=%q v3=%q)",
				pod.Namespace, pod.Name, hash, v1Hash, v3Hash)
		}
	}
}

// TestAggregateAndWriteStatus_MirrorsCommittedRevisionFieldsOntoInMemoryISVC
// pins the (d)-style fix for task #56: after AggregateAndWriteStatus
// commits UR / CR to the cluster, the just-committed values must also
// land on the caller's in-memory ISVC snapshot. Pairs with PR #246's
// MutateInstance mirror (per-Instance fields) and reconciler.go's
// bumpCollisionCount mirror (CollisionCount) — every status-write
// entry point owns the same post-commit observability contract.
//
// Without the mirror, a downstream caller that reads
// p.ISVC.Status.Components[X].Lifecycle within the same reconcile
// (coordination's latestRevisionHash, ops/create.go's
// revisionHashForInstance, etc.) observes the stale pre-write values,
// which is the operative fault mode for the A-2 SurgeThenDrain
// orphan-hash drift symptom on the cluster.
func TestAggregateAndWriteStatus_MirrorsCommittedRevisionFieldsOntoInMemoryISVC(t *testing.T) {
	resetExpectations(t)
	// Single Instance already on the target revision and Phase=Ready —
	// AggregateAndWriteStatus stamps both UR and CR to target.Name. The
	// mirror must reflect both on the in-memory ISVC.
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	targetSpec := targetSpecImage("llama:v1")
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.NodeName = "node-7"
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpec)
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision = tcr.Name
	if err := c.Status().Update(context.Background(), isvc); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	// Sanity: before AggregateAndWriteStatus, the in-memory ISVC has the
	// seed values (UR and CR both empty, since isvcReadyAtIncarnation
	// only seeds Phase=Ready, not the revision fields).
	preCS := isvc.Status.Components[v1beta1.EngineComponent].Lifecycle
	if preCS == nil {
		t.Fatalf("pre-test: LifecycleStatus missing")
	}

	if err := AggregateAndWriteStatus(context.Background(), p, plan, tcr); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}

	// API: UR == CR == tcr.Name (rolloutComplete=true triggers CR promote).
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	apiCS := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
	if apiCS.UpdateRevision != tcr.Name {
		t.Fatalf("API UpdateRevision: got %q want %q", apiCS.UpdateRevision, tcr.Name)
	}
	if apiCS.CurrentRevision != tcr.Name {
		t.Fatalf("API CurrentRevision: got %q want %q", apiCS.CurrentRevision, tcr.Name)
	}

	// MIRROR: the in-memory ISVC must reflect the just-committed values.
	mirrorCS := isvc.Status.Components[v1beta1.EngineComponent].Lifecycle
	if mirrorCS == nil {
		t.Fatalf("mirror missed LifecycleStatus")
	}
	if mirrorCS.UpdateRevision != tcr.Name {
		t.Errorf("in-memory UpdateRevision: got %q want %q (mirror gap — A-2 orphan-hash bug shape)",
			mirrorCS.UpdateRevision, tcr.Name)
	}
	if mirrorCS.CurrentRevision != tcr.Name {
		t.Errorf("in-memory CurrentRevision: got %q want %q (mirror gap — A-2 orphan-hash bug shape)",
			mirrorCS.CurrentRevision, tcr.Name)
	}
}

// TestAggregateAndWriteStatus_MirrorsURWhenRolloutInProgress pins the
// mid-rollout shape: when rolloutComplete is false (typical mid-surge
// state), AggregateAndWriteStatus stamps UR=target.Name but leaves CR
// at its previous value. The mirror must reflect both — UR=new target,
// CR=stale (whatever the cluster previously committed).
func TestAggregateAndWriteStatus_MirrorsURWhenRolloutInProgress(t *testing.T) {
	resetExpectations(t)
	// Instance is Ready but RunningRevision is some other CR → rolloutComplete=false.
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	targetSpec := targetSpecImage("llama:v2")
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	slice := sliceWithEndpoint("prod", "engine-1", query.HeadlessServiceName(isvc.Name, v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent)), pod, true)
	c := newFakeClientWithDiscovery(t, isvc, pod, slice)
	tcr := ensureTargetCR(t, c, isvc, v1beta1.EngineComponent, targetSpec)
	// Pre-seed both API and in-memory CR field with an "older revision" —
	// simulates the prior rolled-out CR's name surviving across reconciles.
	const olderRev = "llama-70b-engine-older"
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision = olderRev
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision = olderRev
	if err := c.Status().Update(context.Background(), isvc); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, tcr); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}

	// API: UR=tcr (new), CR=olderRev (rollout NOT complete).
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	apiCS := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle
	if apiCS.UpdateRevision != tcr.Name {
		t.Errorf("API UpdateRevision: got %q want %q", apiCS.UpdateRevision, tcr.Name)
	}
	if apiCS.CurrentRevision != olderRev {
		t.Errorf("API CurrentRevision: got %q want %q (should not promote mid-rollout)",
			apiCS.CurrentRevision, olderRev)
	}

	// MIRROR: in-memory must match.
	mirrorCS := isvc.Status.Components[v1beta1.EngineComponent].Lifecycle
	if mirrorCS.UpdateRevision != tcr.Name {
		t.Errorf("in-memory UpdateRevision: got %q want %q", mirrorCS.UpdateRevision, tcr.Name)
	}
	if mirrorCS.CurrentRevision != olderRev {
		t.Errorf("in-memory CurrentRevision: got %q want %q (mid-rollout preserves prior CR)",
			mirrorCS.CurrentRevision, olderRev)
	}
}
