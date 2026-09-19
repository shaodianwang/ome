package ops

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// The update strategy an attempt runs under is fixed when its operation
// opens. UpdateStrategy is not part of the revision payload, so editing it
// retargets nothing: the attempt in flight keeps its mechanism and the edit
// is picked up by the next attempt this Instance is admitted for. The tests
// below drive each in-flight step with the strategy already flipped and
// assert the attempt does not change mechanism.

// TestUpdateWithPods_InPlacePatchIgnoresAStrategyEdit: a patch already
// applied to a live pod cannot be taken back, so dispatching the recreate
// machine over it would tear down a pod that is already converging — and
// bump its Incarnation out from under the patch.
//
// Cells: instance/Update/InPlace/spec.strategy/before
func TestUpdateWithPods_InPlacePatchIgnoresAStrategyEdit(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	ir.Status.InstanceStatuses[0].Operation.Strategy = string(v1beta1.UpdateStrategyInPlaceIfPossible)
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	// The attempt is already converging on the target, so the in-place stamp
	// recognizes its own state and the attempt's identity is observable.
	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	live.Status.InstanceStatuses[0].TargetRevision = tcr.Name
	live.Status.InstanceStatuses[0].Operation.TargetRevision = tcr.Name
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("seed the attempt's target: %v", err)
	}

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	// The desired strategy now says recreate; the pinned one still says
	// in place.
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != updateStepInPlace {
		t.Fatalf("Operation: got %+v, want the patch still on Step=%s", s.Operation, updateStepInPlace)
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (the same attempt)", s.Operation.ID, op.ID)
	}
	if s.Operation.Strategy != string(v1beta1.UpdateStrategyInPlaceIfPossible) {
		t.Errorf("Operation.Strategy: got %q, want the pin the attempt opened with", s.Operation.Strategy)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation: got %d want 1 — a recreate ran over the in-flight patch", s.Incarnation)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Errorf("the patched pod is gone (%v): the edit flipped the attempt to recreate", err)
	}
}

// TestUpdateWithPods_FailedRowPicksUpTheEditedStrategy: a Failed row holds
// no attempt, so there is nothing to keep on a mechanism. Editing the
// strategy is the operator's rescue lever out of a mode that cannot make
// progress, and the next admitted attempt has to honour it.
//
// Cells: instance/Failed/spec.strategy/before
func TestUpdateWithPods_FailedRowPicksUpTheEditedStrategy(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceFailed,
		Operation: &v1beta1.InstanceOperation{
			ID:             "update-0-1",
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           updateStepSurge,
			Strategy:       string(v1beta1.UpdateStrategySurgeThenDrain),
			StartedAt:      metav1.NewTime(time.Now().Add(-time.Hour)),
			LastProgressAt: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
	}
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil {
		t.Fatalf("no attempt was admitted off the Failed row")
	}
	if s.Operation.Strategy != string(v1beta1.UpdateStrategyRecreatePod) {
		t.Errorf("Operation.Strategy: got %q, want the edited %q",
			s.Operation.Strategy, v1beta1.UpdateStrategyRecreatePod)
	}
	if s.Operation.Step != updateStepDrain {
		t.Errorf("Operation.Step: got %q, want the recreate's %q", s.Operation.Step, updateStepDrain)
	}
}

// TestUpdateWithPods_RecreateDrainIgnoresAStrategyEdit: past the Incarnation
// bump the old materialization is already being torn down. Handing the row to
// the in-place patcher now would patch a pod that is on its way out and leave
// the recreate's replacement unbuilt.
//
// Cells: instance/Update/Drain/spec.strategy/before
func TestUpdateWithPods_RecreateDrainIgnoresAStrategyEdit(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 2)
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	live.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	live.Status.InstanceStatuses[0].Incarnation = 2
	live.Status.InstanceStatuses[0].TargetRevision = tcr.Name
	live.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		ID:             "update-0-1",
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           updateStepDrain,
		TargetRevision: tcr.Name,
		Strategy:       string(v1beta1.UpdateStrategyRecreatePod),
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Minute)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Minute)),
		Deadline:       metav1.NewTime(time.Now().Add(29 * time.Minute)),
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("seed the in-flight recreate: %v", err)
	}

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != updateStepDrain {
		t.Fatalf("Operation: got %+v, want the recreate still on Step=%s", s.Operation, updateStepDrain)
	}
	if s.Operation.ID != "update-0-1" {
		t.Errorf("Operation.ID: got %q want %q (the same attempt)", s.Operation.ID, "update-0-1")
	}
	if s.Operation.Strategy != string(v1beta1.UpdateStrategyRecreatePod) {
		t.Errorf("Operation.Strategy: got %q, want the pin the attempt opened with", s.Operation.Strategy)
	}
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err == nil {
		if got := fresh.Spec.Containers[0].Image; got != "llama:v1" {
			t.Errorf("the draining pod was patched to %q: the edit flipped the attempt to in place", got)
		}
	}
}
