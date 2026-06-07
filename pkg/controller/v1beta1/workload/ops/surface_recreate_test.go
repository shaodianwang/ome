package ops

import (
	"context"
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// seedRunningRevisionOnStatus sets InstanceStatus[idx].RunningRevision on
// the in-memory ISVC so legacyTestInput projects it into ObservedState
// (where recreateRevisionCause reads the "from" revision). Returns the
// revision name written.
func seedRunningRevisionOnStatus(isvc *v1beta1.InferenceService, idx int32, rev string) {
	cs := isvc.Status.Components[v1beta1.EngineComponent]
	for i := range cs.Lifecycle.InstanceStatuses {
		if cs.Lifecycle.InstanceStatuses[i].Index == idx {
			cs.Lifecycle.InstanceStatuses[i].RunningRevision = rev
		}
	}
	isvc.Status.Components[v1beta1.EngineComponent] = cs
}

// A revision-roll recreate must record a "revision <from> -> <to>" cause
// on Operation.Reason so the roll is distinguishable in status from a
// pod-failure Restart (which records a termination reason). This is the
// debuggability surface for "why did this gang recreate".
func TestRecreateUpdate_StampsRevisionCauseOnOperationReason(t *testing.T) {
	legacyResetExpectations(t)
	isvc := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	seedRunningRevisionOnStatus(isvc, 0, "llama-70b-engine-oldrev")

	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, pod, slice)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target); err != nil {
		t.Fatalf("Update: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("re-read ISVC: %v", err)
	}
	s := fresh.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0]
	if s.Operation == nil {
		t.Fatalf("Operation: nil, want recreate Update operation")
	}
	if s.Operation.Type != v1beta1.InstanceOperationUpdate {
		t.Errorf("Operation.Type: got %q want Update", s.Operation.Type)
	}
	want := "revision llama-70b-engine-oldrev -> " + tcr.Name
	if s.Operation.Reason != want {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, want)
	}
}

// The recreate's first-pass event must name the from->to revision so an
// operator watching `kubectl describe` sees the rollout cause even after
// the old pods are drained.
func TestRecreateUpdate_EmitsFromToRevisionEvent(t *testing.T) {
	legacyResetExpectations(t)
	isvc := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	seedRunningRevisionOnStatus(isvc, 0, "llama-70b-engine-oldrev")

	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, pod, slice)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	rec := record.NewFakeRecorder(16)
	deps := legacyTestDeps(c)
	deps.Recorder = rec

	if _, err := Update(context.Background(), deps, input, plan, plan.Instances[0], tcr, target); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := drainEvents(rec)
	if !anyContains(got, string(workload.EventReasonRecreateUpdateStarted)) {
		t.Fatalf("no RecreateUpdateStarted event; got %v", got)
	}
	if !anyContains(got, "revision llama-70b-engine-oldrev -> "+tcr.Name) {
		t.Errorf("recreate event must name the from->to revision; got %v", got)
	}
}

// drainEvents pulls all buffered events off a FakeRecorder without blocking.
func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func anyContains(events []string, sub string) bool {
	for _, e := range events {
		if strings.Contains(e, sub) {
			return true
		}
	}
	return false
}
