package omenative

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

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
	slice := sliceWithEndpoint("prod", "engine-1", headlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, true)
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
	slice := sliceWithEndpoint("prod", "engine-1", headlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, false)
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
	slice := sliceWithEndpoint("prod", "engine-1", headlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, true)
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
	slice := sliceWithEndpoint("prod", "engine-1", headlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, true)
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
var _ = appsv1.ControllerRevision{}
var _ = ptr.To[int32]
