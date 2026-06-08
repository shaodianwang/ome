package ops_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

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

// isvcReadyAtIncarnation builds an ISVC whose InstanceStatus for engine
// index 0 is Phase=Ready at the given Incarnation — the steady-state from
// which a restart trigger fires.
func isvcReadyAtIncarnation(name, ns string, incarnation int64) *v1beta1.InferenceService {
	isvc := minimalISVC(name, ns, 1)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
				},
			},
		},
	}
	return isvc
}

// podAtIncarnation extends podForInstance to stamp a specific incarnation
// label. Restart pods at the bumped incarnation are distinguished from old
// pods by this label.
func podAtIncarnation(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ready, serving bool) *corev1.Pod {
	pod := podForInstance(isvc, instanceIdx, ready, serving)
	pod.Labels[query.LabelInstanceIncarnation] = fmt.Sprintf("%d", incarnation)
	return pod
}

// buildPlanSinglePodEngineForRestart is the per-Restart-test plan builder.
// Same shape as buildPlanSinglePodEngine in create_test.go but reads the
// incarnation from the existing InstanceStatus so subsequent passes drive
// the post-bump value rather than re-stamping 1.
func buildPlanSinglePodEngineForRestart(isvc *v1beta1.InferenceService) workload.ComponentPlan {
	plan := buildPlanSinglePodEngine(1)
	if cs, ok := isvc.Status.Components[v1beta1.EngineComponent]; ok && cs.Lifecycle != nil {
		for _, s := range cs.Lifecycle.InstanceStatuses {
			if s.Index == 0 && s.Incarnation > 0 {
				plan.Instances[0].Incarnation = s.Incarnation
				break
			}
		}
	}
	return plan
}

func TestRestart_NilClient(t *testing.T) {
	resetExpectations(t)
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1}
	if _, err := ops.Restart(context.Background(), workload.Deps{}, workload.ReconcileInput{}, plan, inst, ""); err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestRestart_FirstPassBumpsIncarnationAndPatchesStatus(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := podAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	c := newFakeClient(t, isvc, pod)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "test trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false on first pass (drain not converged)")
	}

	// Status should now be Phase=Restarting, Incarnation=2.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting", s.Phase)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (bumped from 1)", s.Incarnation)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("Operation: %+v", s.Operation)
	}
	if s.Operation.Reason != "test trigger" {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, "test trigger")
	}
	if s.Operation.Step != "Drain" {
		t.Errorf("Operation.Step: got %q want Drain", s.Operation.Step)
	}
}

func TestRestart_DrainOldPod_FlipsServingFalse(t *testing.T) {
	// Pre-bumped status (Restarting at Inc=2); old pod at Inc=1 still has
	// serving=True. Restart should flip serving=False and requeue while
	// EndpointSlice still publishes the pod as Ready.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, true /* still Ready */)
	c := newFakeClient(t, isvc, pod, slice)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while drain hasn't converged")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if podreadiness.IsServing(got) {
		t.Errorf("pod %s ome.io/serving should be False, got %+v", got.Name, got.Status.Conditions)
	}
}

func TestRestart_DeletesOldPodOnceDrained(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, false /* already drained */)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, false /* Ready=false */)
	c := newFakeClient(t, isvc, pod, slice)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after issuing delete (next pass creates new pod)")
	}

	// Old pod should be gone.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err == nil {
		t.Errorf("old pod %s should be deleted", pod.Name)
	}
	if workload.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentEngine, 0) {
		t.Errorf("ExpectDeletes should record the in-flight delete")
	}
}

func TestRestart_RefusesToDeleteOrphanPod(t *testing.T) {
	// A pod under the OMENative selector but missing
	// ome.io/instance-incarnation is an orphan. Restart must emit
	// FoundOrphan and short-circuit rather than deleting it — the operator
	// re-labels or removes it manually.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	orphan := podAtIncarnation(isvc, 0, 1, true, false)
	orphan.Name = "orphan-pod"
	delete(orphan.Labels, query.LabelInstanceIncarnation)
	c := newFakeClient(t, isvc, orphan)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: orphan must block Restart from advancing")
	}

	// Orphan pod must STILL exist.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(orphan), got); err != nil {
		t.Fatalf("orphan pod must still exist after Restart bails: %v", err)
	}
	if got.DeletionTimestamp != nil {
		t.Errorf("orphan pod must NOT have been deleted; got DeletionTimestamp=%v", got.DeletionTimestamp)
	}
}

func TestRestart_CreatesNewPodAtBumpedIncarnation(t *testing.T) {
	// Status is Restarting at Inc=2, no pods (old already deleted, new
	// not yet created). Restart should create the new pod at Inc=2.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     2,
		Phase:           v1beta1.OMENativeInstanceRestarting,
		RunningRevision: "llama-70b-engine-" + testRevisionHash,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	c := newFakeClient(t, isvc)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after creating new pod (not yet Ready)")
	}

	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	if len(pods.Items) != 1 {
		t.Fatalf("pods: got %d want 1", len(pods.Items))
	}
	if got := pods.Items[0].Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("new pod %s incarnation label: got %q want 2", pods.Items[0].Name, got)
	}
}

func TestRestart_ConvergesAcrossPasses(t *testing.T) {
	// End-to-end: start with a Ready instance whose pod is Failed.
	// Trigger Restart and run it until done=true; assert the final state
	// has one new-incarnation pod, serving=True, and Phase=Ready.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Pre-seed RunningRevision so Phase-B recreate has a hash to stamp.
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision = "llama-70b-engine-" + testRevisionHash
	failed := podAtIncarnation(isvc, 0, 1, true, true)
	failed.Status.Phase = corev1.PodFailed
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, failed, false /* drained */)
	c := newFakeClient(t, isvc, failed, slice)

	const maxPasses = 8
	for pass := 0; pass < maxPasses; pass++ {
		// Re-read each pass: buildPlanSinglePodEngineForRestart needs current
		// status to pick up the new incarnation after Restart bumps it.
		fresh := &v1beta1.InferenceService{}
		_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
		input := buildTestInputForDelete(fresh, c, workload.ComponentEngine)
		plan := buildPlanSinglePodEngineForRestart(fresh)

		// Fast-forward: simulate the fake watch observing every delete the
		// previous pass issued.
		workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

		// Synthesize ContainersReady on any new-incarnation pod the
		// previous pass created — the fake client doesn't run kubelet.
		makeNewPodReady(t, c, "prod", "llama-70b-engine-0-default-0", plan.Instances[0].Incarnation)

		done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod Failed")
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if done {
			// Verify end state.
			finalISVC := &v1beta1.InferenceService{}
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), finalISVC)
			s := finalISVC.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
			if s.Phase != v1beta1.OMENativeInstanceReady {
				t.Errorf("final Phase: got %q want Ready", s.Phase)
			}
			if s.Incarnation != 2 {
				t.Errorf("final Incarnation: got %d want 2", s.Incarnation)
			}
			if s.Operation != nil {
				t.Errorf("final Operation: want nil, got %+v", s.Operation)
			}
			return
		}
	}
	t.Fatalf("Restart did not converge after %d passes", maxPasses)
}

// makeNewPodReady is a test helper that synthesizes ContainersReady=True
// on the named pod if it exists and carries the given incarnation label.
// It mimics what kubelet would write once the runtime is up.
func makeNewPodReady(t *testing.T, c client.Client, ns, name string, incarnation int64) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, pod); err != nil {
		return
	}
	if got := pod.Labels[query.LabelInstanceIncarnation]; got != fmt.Sprintf("%d", incarnation) {
		return
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.ContainersReady && cond.Status == corev1.ConditionTrue {
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.ContainersReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	})
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("synthesize ContainersReady: %v", err)
	}
}

// failedPodOOM builds a single-pod-engine pod at the given incarnation
// that is Phase=Failed with an OOMKilled-terminated main container.
func failedPodOOM(isvc *v1beta1.InferenceService, incarnation int64) *corev1.Pod {
	pod := podAtIncarnation(isvc, 0, incarnation, true /* ready */, true /* serving */)
	pod.Status.Phase = corev1.PodFailed
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason:   "OOMKilled",
			ExitCode: 137,
			Message:  "container killed due to memory limit",
		}},
	}}
	return pod
}

// Restart on a failed pod must preserve the pod's container-termination
// diagnostics into InstanceStatus.LastFailure on the FIRST pass — BEFORE
// Phase A drains and deletes the pod, so the trace survives the recreate.
// This is the core debuggability fix: a gang that keeps restarting now
// leaves a durable failure record instead of vanishing.
func TestRestart_CapturesLastFailureBeforeDrain(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Pre-seed RunningRevision so the Restart path has a hash later.
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision =
		"llama-70b-engine-" + testRevisionHash
	failed := failedPodOOM(isvc, 1)
	c := newFakeClient(t, isvc, failed)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod Failed"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	s := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
	if s.LastFailure == nil {
		t.Fatalf("LastFailure: nil, want OOMKilled diagnostics captured before drain")
	}
	if s.LastFailure.PodName != failed.Name {
		t.Errorf("LastFailure.PodName: got %q want %q", s.LastFailure.PodName, failed.Name)
	}
	if s.LastFailure.ContainerName != "main" {
		t.Errorf("LastFailure.ContainerName: got %q want main", s.LastFailure.ContainerName)
	}
	if s.LastFailure.Reason != "OOMKilled" {
		t.Errorf("LastFailure.Reason: got %q want OOMKilled", s.LastFailure.Reason)
	}
	if s.LastFailure.ExitCode == nil || *s.LastFailure.ExitCode != 137 {
		t.Errorf("LastFailure.ExitCode: got %v want 137", s.LastFailure.ExitCode)
	}
}

// A "pod count below desired" restart (the pod already vanished) has no
// pod to read diagnostics from and must NOT clobber a previously-recorded
// LastFailure — the most recent genuine failure trace is preserved.
func TestRestart_PodVanished_PreservesPriorLastFailure(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	s0 := &isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
	s0.RunningRevision = "llama-70b-engine-" + testRevisionHash
	exit := int32(1)
	s0.LastFailure = &v1beta1.InstanceTermination{
		PodName:  "llama-70b-engine-0-default-0",
		Reason:   "Error",
		ExitCode: &exit,
	}
	// No pods exist → "pod count below desired" trigger.
	c := newFakeClient(t, isvc)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod count 0 below desired 1"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	s := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
	if s.LastFailure == nil || s.LastFailure.Reason != "Error" {
		t.Fatalf("prior LastFailure must be preserved when no pod yields a fresh signal; got %+v", s.LastFailure)
	}
}

// DetectRestartTrigger must return a rich, operator-readable reason for a
// failed pod (container + terminated reason + exit code), not the bare
// "pod X Failed" — and the Restart event carries it.
func TestDetectRestartTrigger_RichReasonForFailedPod(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	failed := failedPodOOM(isvc, 1)
	c := newFakeClient(t, isvc, failed)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	needs, reason, err := ops.DetectRestartTrigger(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("DetectRestartTrigger: %v", err)
	}
	if !needs {
		t.Fatalf("expected needsRestart=true for a Failed pod")
	}
	if !strings.Contains(reason, "OOMKilled") || !strings.Contains(reason, "137") {
		t.Errorf("reason must name the termination cause; got %q", reason)
	}
}

// The RestartTriggered Warning event must carry the rich reason so the
// cause is visible in `kubectl describe` even after the pod is gone.
func TestRestart_EmitsRichRestartTriggeredEvent(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].RunningRevision =
		"llama-70b-engine-" + testRevisionHash
	failed := failedPodOOM(isvc, 1)
	c := newFakeClient(t, isvc, failed)
	input := buildTestInputForDelete(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(isvc)

	rec := record.NewFakeRecorder(16)

	// Pass the rich reason (as the dispatcher would, via DetectRestartTrigger).
	_, reason, _ := ops.DetectRestartTrigger(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0])
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, plan, plan.Instances[0], reason); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	var events []string
	for drained := false; !drained; {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			drained = true
		}
	}
	found := false
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonRestartTriggered)) && strings.Contains(e, "OOMKilled") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a RestartTriggered event naming OOMKilled; got %v", events)
	}
}
