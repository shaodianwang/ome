package ops_test

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

const (
	alienRunningRevision = "llama-70b-engine-aaaaaaaa"
	alienPodRevision     = "llama-70b-engine-bbbbbbbb"
)

// alienFixture is a Ready Instance whose pod carries a third revision —
// neither the Instance's running one nor the current roll target.
func alienFixture(t *testing.T, serving bool) (*v1beta1.InferenceService, client.Client, *corev1.Pod, *appsv1.ControllerRevision, *record.FakeRecorder) {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: alienRunningRevision,
	})
	pod := podForInstance(isvc, 0, true /* ready */, serving)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(alienPodRevision)
	c := newFakeClient(t, isvc, ir, pod)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-cccccccc", Namespace: "prod"},
	}
	return isvc, c, pod, target, record.NewFakeRecorder(8)
}

func TestCleanupWreckage_ServingAlienLeavesRotationBeforeItIsDeleted(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	plan := buildPlanSinglePodEngine(1)
	sweep := func() {
		t.Helper()
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		live := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
			t.Fatalf("re-read alien: %v", err)
		}
		if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
			input, plan, plan.Instances[0], target, []*corev1.Pod{live}); err != nil {
			t.Fatalf("CleanupWreckage: %v", err)
		}
	}

	sweep()
	afterFirst := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), afterFirst); err != nil {
		t.Fatalf("the alien was deleted in the pass that unrouted it: %v", err)
	}
	if podreadiness.IsServing(afterFirst) {
		t.Fatalf("the alien is still in rotation after the first pass")
	}

	sweep()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err == nil {
		t.Errorf("the alien survived the pass after it left rotation")
	}
}

func TestCleanupWreckage_VanishedAlienIsNotAnError(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatalf("delete the alien out from under the sweep: %v", err)
	}
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod}); err != nil {
		t.Fatalf("a pod that vanished mid-sweep must not fail the pass: %v", err)
	}
}

func TestCleanupWreckage_UnroutedAlienIsDeletedImmediately(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, false /* serving */)
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod}); err != nil {
		t.Fatalf("CleanupWreckage: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err == nil {
		t.Errorf("an alien that was never in rotation has nothing to wait for; it must go in this pass")
	}
}

// A superseded-revision pod the cleanup has already deleted is still its
// wreckage while it terminates: the row stays the cleanup's, which
// neither deletes nor expects the pod a second time and reports the work
// unfinished until the object is gone.
func TestCleanupWreckage_TerminatingAlienKeepsTheRowUntilItIsGone(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, false /* serving */)
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	row := input.ObservedState.Instance(0)
	if !ops.EvaluateWreckage(row, target, []*corev1.Pod{pod}) {
		t.Fatalf("an alien pod is wreckage before the cleanup deletes it")
	}
	// Deleted behind a finalizer, so the fake client keeps the object
	// with a deletionTimestamp, as a live List does before the apiserver
	// collects it.
	pod.Finalizers = append(pod.Finalizers, "ome.io/test-hold")
	if err := c.Update(context.Background(), pod); err != nil {
		t.Fatalf("pin the alien with a finalizer: %v", err)
	}
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatalf("delete the alien: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil || pod.DeletionTimestamp == nil {
		t.Fatalf("the alien must be terminating; err=%v deletionTimestamp=%v", err, pod.DeletionTimestamp)
	}
	if !ops.EvaluateWreckage(row, target, []*corev1.Pod{pod}) {
		t.Fatalf("a terminating alien is still the cleanup's wreckage; the row must stay its until the pod is gone")
	}
	done, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("CleanupWreckage: %v", err)
	}
	if done {
		t.Fatalf("a terminating alien is unfinished work; got done=true")
	}
	if !workload.DefaultExpectations.Satisfied(isvc.Namespace, isvc.Name, workload.ComponentEngine, 0) {
		t.Fatalf("a pod already terminating must not be expected or deleted a second time")
	}
	select {
	case event := <-events.Events:
		t.Fatalf("no delete is issued for a pod already terminating; got event %q", event)
	default:
	}
}
