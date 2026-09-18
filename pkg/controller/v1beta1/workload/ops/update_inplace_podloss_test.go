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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// inPlaceRollWithoutPods seeds an Instance whose in-place roll is in
// flight — Phase=Updating, Op{Update, InPlace} at targetRev with its own
// id and deadline — and whose only pod has been lost.
func inPlaceRollWithoutPods(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, *v1beta1.InstanceOperation) {
	t.Helper()
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	op := &v1beta1.InstanceOperation{
		ID:             "update-0-1",
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           updateStepInPlace,
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Minute)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Minute)),
		Deadline:       metav1.NewTime(time.Now().Add(29 * time.Minute)),
	}
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation:   op.DeepCopy(),
	}
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyInPlaceIfPossible},
	}
	return isvc, ir, op
}

// TestUpdateWithPods_InPlaceRollLosingItsOnlyPodReResolvesToRecreate:
// an in-place step has no repair for an empty pod set — the update pass
// owns the index so no create reaches it, and the restart trigger
// declines below Ready — so the roll would poll nothing until its
// deadline disposed it. Losing the pod re-resolves the roll to recreate
// in the same pass: the step becomes Drain, the replacement is rendered
// at the target revision, and the attempt keeps its identity and its
// deadline, so this is a continuation rather than a new attempt.
//
// Cells: instance/Update/InPlace/pod.deleted[source]/before
func TestUpdateWithPods_InPlaceRollLosingItsOnlyPodReResolvesToRecreate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	var blocked []workload.RetryBlockDisposition
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		blocked = append(blocked, mutate(&workload.RetryBlock{TargetRevision: rev}))
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	done, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target, nil)
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	if done {
		t.Fatalf("the roll must stay in flight while the replacement comes up")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Fatalf("Phase: got %q want Updating (no Failed on a re-resolution)", s.Phase)
	}
	if s.Operation == nil || s.Operation.Step != updateStepDrain {
		t.Fatalf("Operation: got %+v want Step=%s", s.Operation, updateStepDrain)
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (same attempt)", s.Operation.ID, op.ID)
	}
	if s.Operation.Deadline.Unix() != op.Deadline.Unix() {
		t.Errorf("Operation.Deadline: got %v want %v (same attempt)", s.Operation.Deadline, op.Deadline)
	}
	if s.Incarnation <= 1 {
		t.Errorf("Incarnation: got %d want a bump (the recreate's new materialization)", s.Incarnation)
	}
	for _, d := range blocked {
		if d == workload.RetryBlockPersist {
			t.Errorf("a re-resolution must charge no RetryBlock")
		}
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected the replacement to be rendered, got %d pods", len(pods.Items))
	}
	replacement := pods.Items[0]
	if got := replacement.Labels[query.LabelRevisionHash]; got != query.RevisionHashFromControllerRevisionName(tcr.Name) {
		t.Errorf("replacement revision hash: got %q want the target's", got)
	}
	if got := replacement.Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("replacement incarnation label: got %q want 2", got)
	}
}

// TestUpdateWithPods_InPlaceRollKeepsPatchingWhileItsPodLives pins the
// negative: the re-resolution is keyed on the pod set being empty, so a
// roll whose pod is merely not ready yet stays in place.
//
// Cells: instance/Update/InPlace/pod.deleted[source]/before
func TestUpdateWithPods_InPlaceRollKeepsPatchingWhileItsPodLives(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, _ := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != updateStepInPlace {
		t.Fatalf("Operation: got %+v want Step=%s", s.Operation, updateStepInPlace)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation: got %d want 1 (in-place keeps the materialization)", s.Incarnation)
	}
}

// TestUpdateWithPods_InPlaceRollWaitsForItsOwnExpectations: an empty pod
// set is evidence of loss only once this Instance's own creates and
// deletes have been observed. Before that it can be a read that predates
// them, and converting the roll on one would bump the Incarnation out
// from under a pod that does exist.
//
// Cells: instance/Update/InPlace/pod.deleted[source]/before
func TestUpdateWithPods_InPlaceRollWaitsForItsOwnExpectations(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	workload.DefaultExpectations.ExpectCreates(input.Key.Namespace, input.Key.OwnerName, workload.ComponentEngine, 0, 1)

	done, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target, nil)
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	if done {
		t.Fatalf("the roll must stay in flight while its own writes are unobserved")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != updateStepInPlace {
		t.Fatalf("Operation: got %+v want Step=%s (no conversion on an unsettled read)", s.Operation, updateStepInPlace)
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (same attempt)", s.Operation.ID, op.ID)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation: got %d want 1 (nothing has been re-materialized)", s.Incarnation)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("pods: got %d want none rendered", len(pods.Items))
	}
}

// TestUpdateWithPods_InPlaceRollReResolvedOnceStaysOneAttempt: the
// update mode is re-resolved every pass, so the pass after a
// re-resolution can pick in-place again over the replacement pod. That
// is a step move inside an attempt that never ended — it must not stamp
// a new operation id, hand the row a second full deadline window, or
// announce itself again.
//
// Cells: instance/Update/InPlace/pod.deleted[source]/before
func TestUpdateWithPods_InPlaceRollReResolvedOnceStaysOneAttempt(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	recorder := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: recorder}

	if _, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, target, nil); err != nil {
		t.Fatalf("UpdateWithPods (first pass): %v", err)
	}
	firstPassEvents := len(recorder.Events)

	// Second pass: the replacement the re-resolution rendered is now the
	// Instance's pod set, and the mode resolves back to in-place.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected the replacement to be rendered, got %d pods", len(pods.Items))
	}
	legacyResetExpectations(t)
	next := legacyTestInput(isvc, c, workload.ComponentEngine)
	if _, err := UpdateWithPods(context.Background(), deps, next, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{&pods.Items[0]}); err != nil {
		t.Fatalf("UpdateWithPods (second pass): %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil {
		t.Fatalf("Operation: got none want the attempt still in flight")
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (one attempt, two mechanisms)", s.Operation.ID, op.ID)
	}
	if s.Operation.Deadline.Unix() != op.Deadline.Unix() {
		t.Errorf("Operation.Deadline: got %v want %v (no second window)", s.Operation.Deadline, op.Deadline)
	}
	if s.Operation.StartedAt.Unix() != op.StartedAt.Unix() {
		t.Errorf("Operation.StartedAt: got %v want %v (the attempt did not restart)", s.Operation.StartedAt, op.StartedAt)
	}
	if len(recorder.Events) != firstPassEvents {
		t.Errorf("events: got %d want %d; a step move announces nothing", len(recorder.Events), firstPassEvents)
	}
}
