package ops_test

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

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
