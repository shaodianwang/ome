package ops

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// A pause holds an attempt at a STEP boundary: the step under way runs
// to its end, the next one does not start. The surge machine is where
// that distinction is visible — its cycle crosses three steps and each
// boundary leaves a different pair of pods behind.

// pausedSurgeFixture builds the single-Instance surge at the point where
// the replacement has reached the promote bar and the source is still
// serving: the boundary between the surge step and the drain.
func pausedSurgeFixture(t *testing.T) (*v1beta1.InferenceService, client.Client, workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *corev1.Pod, *corev1.Pod) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b-engine-rev-v2hash", Namespace: isvc.Namespace,
	}}
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	podReadyAt(surgePod, minReadyWindowStart.Add(-time.Minute))
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(minReadyWindowStart)
	plan := surgePlan()
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, target.Name,
		workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	return isvc, c, input, plan, target, oldPod, surgePod
}

// TestSurgeUpdate_PausedHoldsAtTheSurgeStepBoundary: the replacement is
// created and reaches the promote bar under a pause — that is the step
// already under way — but the drain is a new step and does not start, so
// the source keeps serving for the length of the hold. Clearing the
// pause runs the drain.
//
// Cells: instance/Update/Surge/spec.pauseTrue/before, instance/Update/Surge/spec.pauseFreeze/before, instance/Update/Surge/spec.unpause/before
func TestSurgeUpdate_PausedHoldsAtTheSurgeStepBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		freeze bool
	}{{name: "standard pause"}, {name: "frozen pause", freeze: true}} {
		t.Run(tc.name, func(t *testing.T) {
			isvc, c, input, plan, target, oldPod, surgePod := pausedSurgeFixture(t)
			plan.Paused = true
			plan.PauseFreeze = tc.freeze

			done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
				plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod})
			if err != nil {
				t.Fatalf("surgeUpdate paused: %v", err)
			}
			if done {
				t.Fatalf("a held surge must not report the roll complete")
			}
			freshOld := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld); err != nil {
				t.Fatalf("source missing under pause: %v", err)
			}
			if !podreadiness.IsServing(freshOld) {
				t.Errorf("the source left rotation while paused; a pause may not cost the Instance capacity")
			}
			status := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if status.Operation == nil || status.Operation.Step != updateStepSurge {
				t.Errorf("step under pause: got %+v want %s (the drain is a new step)", status.Operation, updateStepSurge)
			}

			// The replacement still had to be finished: it is serving and
			// holds the surge ordinal.
			freshSurge := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(surgePod), freshSurge); err != nil {
				t.Fatalf("replacement missing under pause: %v", err)
			}
			if !podreadiness.IsServing(freshSurge) {
				t.Errorf("the replacement never joined rotation; the surge step did not run to its boundary")
			}

			plan.Paused, plan.PauseFreeze = false, false
			if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
				plan.Instances[0], target, []*corev1.Pod{freshOld, freshSurge}); err != nil {
				t.Fatalf("surgeUpdate unpaused: %v", err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Errorf("the drain did not resume on unpause: %v", err)
			}
		})
	}
}

// TestSurgeUpdate_PausedDrainDeletesAndPromotes: a pause taken while the
// drain step is under way does not freeze the operation in flight — it
// starts no NEW one. The drain runs to completion: the source leaves
// rotation, is deleted, and the row promotes onto the replacement's
// slot, which served throughout.
//
// Cells: instance/Update/SurgeDrain/spec.pauseTrue/before
func TestSurgeUpdate_PausedDrainDeletesAndPromotes(t *testing.T) {
	isvc, c, input, plan, target, oldPod, surgePod := pausedSurgeFixture(t)
	if err := patchInstanceStatusSurgeStepDrain(context.Background(), input, 0); err != nil {
		t.Fatalf("pre-stamp drain: %v", err)
	}
	// The observation this pass reads is the persisted row of the pass
	// that stamped the drain.
	input.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseUpdating
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		Type: workload.InstanceOperationUpdate, Step: updateStepSurgeDrain, TargetRevision: target.Name,
	}
	plan.Paused = true

	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
		plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod}); err != nil {
		t.Fatalf("surgeUpdate paused at drain: %v", err)
	}

	freshOld := &corev1.Pod{}
	err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld)
	if err == nil && freshOld.DeletionTimestamp == nil {
		t.Fatalf("the drain must delete its source under a pause too; got %+v", freshOld.ObjectMeta)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("read source: %v", err)
	}
	if podreadiness.IsServing(freshOld) {
		t.Errorf("the source left rotation before the delete")
	}

	// The promote lands on the pass that observes the source gone.
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		Type: workload.InstanceOperationUpdate, Step: updateStepSurgeDrain, TargetRevision: target.Name,
	}
	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
		plan.Instances[0], target, []*corev1.Pod{surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate promote pass: %v", err)
	}
	if !done {
		t.Fatalf("the promote must complete the roll once the source is gone")
	}
	status := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if status.Phase != v1beta1.OMENativeInstanceReady || status.ActiveOrdinal != 1 {
		t.Fatalf("promote under pause: got phase=%s activeOrdinal=%d want Ready on ordinal 1",
			status.Phase, status.ActiveOrdinal)
	}
}

// TestUpdate_InPlacePausedCompletesThePatch: an in-place patch is one
// step, so a pause taken while it runs does not interrupt it — the
// images reach the target rather than the Instance being left with a
// half-applied revision.
//
// Cells: instance/Update/InPlace/spec.pauseTrue/before
func TestUpdate_InPlacePausedCompletesThePatch(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	plan.Paused = true
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	for pass := 1; pass <= 2; pass++ {
		if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target); err != nil {
			t.Fatalf("Update pass %d under pause: %v", pass, err)
		}
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v2" {
		t.Errorf("image under pause: got %q want llama:v2 (the patch step must finish)", got.Spec.Containers[0].Image)
	}
}

// TestSurgeUpdate_PausedSourceOutOfRotationTakesTheToken: the pause is the
// weakest authority on the row. A source taken out of rotation by
// something outside the rollout means the Instance is serving nothing,
// which is the more urgent fact, so the surge's report replaces the
// pause token — and the row keeps reporting it after the unpause, where
// the escalation pass can act on it.
//
// Cells: instance/Update/Surge/spec.pauseTrue/before
func TestSurgeUpdate_PausedSourceOutOfRotationTakesTheToken(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	// Ready but out of rotation: nobody drained it, so the Instance is
	// serving nothing while its replacement comes up.
	oldPod := surgePodAtOrdinal(isvc, 0, 1, 0, true, false)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, true, true)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b-engine-rev-v2hash", Namespace: isvc.Namespace,
	}}
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	podReadyAt(surgePod, minReadyWindowStart.Add(-time.Minute))
	c := legacyNewFakeClient(t, isvc, ir, oldPod, surgePod, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(minReadyWindowStart)
	plan := surgePlan()
	plan.Paused = true
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, target.Name,
		workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	// The pause got there first, on the row and in the observation.
	held := func(s *workload.InstanceStatus) bool {
		op := *s.Operation
		op.Waiting = workload.WaitingReasonPaused
		s.Operation = &op
		return true
	}
	if err := input.MutateInstance(context.Background(), 0, held); err != nil {
		t.Fatalf("pre-stamp pause hold: %v", err)
	}
	input.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseUpdating
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		Type: workload.InstanceOperationUpdate, Step: updateStepSurge,
		TargetRevision: target.Name, Waiting: workload.WaitingReasonPaused,
	}

	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
		plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod}); err != nil {
		t.Fatalf("surgeUpdate paused with an unrouted source: %v", err)
	}

	got := legacyInstanceStatuses(c, isvc, workload.ComponentEngine)[0]
	if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonSourceUnrouted {
		t.Fatalf("token = %+v, want %s (the unrouted source outranks the pause)",
			got.Operation, workload.WaitingReasonSourceUnrouted)
	}
	if got.LastFailure == nil || got.LastFailure.PodName != oldPod.Name {
		t.Errorf("evidence = %+v, want the unrouted source named", got.LastFailure)
	}
}
