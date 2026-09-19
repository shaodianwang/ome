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

// TestSurgeUpdate_PausedFinishesTheDrainAndHoldsBeforeSettle: a pause
// taken while the drain step is under way lets it reach its boundary —
// the source leaves rotation, which is where it was already headed — and
// stops there: the settle is a new step, so the source stays alive and
// the promote waits.
//
// Cells: instance/Update/SurgeDrain/spec.pauseTrue/before
func TestSurgeUpdate_PausedFinishesTheDrainAndHoldsBeforeSettle(t *testing.T) {
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

	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan,
		plan.Instances[0], target, []*corev1.Pod{oldPod, surgePod})
	if err != nil {
		t.Fatalf("surgeUpdate paused at drain: %v", err)
	}
	if done {
		t.Fatalf("a held drain must not report the roll complete")
	}
	freshOld := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldPod), freshOld); err != nil {
		t.Fatalf("source deleted while paused: %v", err)
	}
	if podreadiness.IsServing(freshOld) {
		t.Errorf("the drain step did not reach its boundary: the source is still in rotation")
	}
	status := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if status.Operation == nil || status.Operation.Step != updateStepSurgeDrain {
		t.Errorf("step under pause: got %+v want %s (the settle is a new step)", status.Operation, updateStepSurgeDrain)
	}
}

// TestWaitForSurgeDrainSettle_PausedParksGraceAndRearmsOnRelease: the
// settle grace measures how long connections have had to age out, so a
// hold parks it rather than spending it unwatched, and the release
// starts a full interval from the unpause instead of crediting the hold.
//
// Cells: instance/Update/SurgeDrainSettle/spec.pauseTrue/before, instance/Update/SurgeDrainSettle/spec.unpause/before
func TestWaitForSurgeDrainSettle_PausedParksGraceAndRearmsOnRelease(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	now := minReadyWindowStart
	clk := clocktesting.NewFakeClock(now)
	input.Clock = clk
	plan := surgePlan()
	grace := int32(30)
	plan.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds = &grace

	// Persist an attempt already on the settle step, then observe it
	// held: the grace started an hour before this pass runs.
	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x",
		workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepSettle(context.Background(), input, 0); err != nil {
		t.Fatalf("pre-stamp settle: %v", err)
	}
	status := &input.ObservedState.InstanceStatuses[0]
	status.Phase = workload.InstancePhaseUpdating
	status.Operation = &workload.InstanceOperation{
		Type:           workload.InstanceOperationUpdate,
		Step:           updateStepSurgeDrainSettle,
		TargetRevision: "rev-x",
		Waiting:        workload.WaitingReasonPaused,
		LastProgressAt: metav1.NewTime(now),
	}
	release := now.Add(time.Hour)
	clk.SetTime(release)

	plan.Paused = true
	settled, err := waitForSurgeDrainSettle(context.Background(), input, plan, 0)
	if err != nil {
		t.Fatalf("settle while paused: %v", err)
	}
	if settled {
		t.Fatalf("an elapsed grace must not complete while the pair is held")
	}

	plan.Paused = false
	settled, err = waitForSurgeDrainSettle(context.Background(), input, plan, 0)
	if err != nil {
		t.Fatalf("settle on release: %v", err)
	}
	if settled {
		t.Fatalf("the release must re-arm the grace, not spend the hold's time on it")
	}
	rearmed := legacyInstanceStatuses(c, isvc, workload.ComponentEngine)[0].Operation
	if rearmed == nil || !rearmed.LastProgressAt.Time.Equal(release) {
		t.Fatalf("grace start after release: got %+v want %v", rearmed, release)
	}

	// The re-armed interval runs from the release, so the row settles a
	// full grace later and not before.
	status.Operation.Waiting = ""
	status.Operation.LastProgressAt = metav1.NewTime(release)
	clk.SetTime(release.Add(time.Duration(grace-1) * time.Second))
	if settled, err = waitForSurgeDrainSettle(context.Background(), input, plan, 0); err != nil || settled {
		t.Fatalf("inside the re-armed grace: settled=%v err=%v", settled, err)
	}
	clk.SetTime(release.Add(time.Duration(grace) * time.Second))
	if settled, err = waitForSurgeDrainSettle(context.Background(), input, plan, 0); err != nil || !settled {
		t.Fatalf("at the end of the re-armed grace: settled=%v err=%v", settled, err)
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

// TestWaitForSurgeDrainSettle_HeldByAnotherAuthorityRestartsTheGrace: the
// re-arm asks about the hold, not about who owns the row. A pair held
// through a pause while another authority reports the wait has spent
// just as little of its grace, so it resumes into a full interval rather
// than completing the moment the hold ends.
//
// Cells: instance/Update/SurgeDrainSettle/spec.pauseTrue/before, instance/Update/SurgeDrainSettle/spec.unpause/before
func TestWaitForSurgeDrainSettle_HeldByAnotherAuthorityRestartsTheGrace(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	now := minReadyWindowStart
	clk := clocktesting.NewFakeClock(now)
	input.Clock = clk
	plan := surgePlan()
	grace := int32(30)
	plan.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds = &grace

	if err := patchInstanceStatusSurgingForUpdate(context.Background(), input, 0, "rev-x",
		workload.UpdateStrategySurgeThenDrain, plan.InstanceReadyTimeout); err != nil {
		t.Fatalf("pre-stamp surge: %v", err)
	}
	if err := patchInstanceStatusSurgeStepSettle(context.Background(), input, 0); err != nil {
		t.Fatalf("pre-stamp settle: %v", err)
	}
	status := &input.ObservedState.InstanceStatuses[0]
	status.Phase = workload.InstancePhaseUpdating
	status.Operation = &workload.InstanceOperation{
		Type:           workload.InstanceOperationUpdate,
		Step:           updateStepSurgeDrainSettle,
		TargetRevision: "rev-x",
		// The quota refusal got to the row first, so the pause never
		// owned the token.
		Waiting:        workload.RejectionReasonQuotaExceeded,
		LastProgressAt: metav1.NewTime(now),
	}
	release := now.Add(time.Hour)
	clk.SetTime(release)

	plan.Paused = true
	if settled, err := waitForSurgeDrainSettle(context.Background(), input, plan, 0); err != nil || settled {
		t.Fatalf("while paused: settled=%v err=%v", settled, err)
	}

	plan.Paused = false
	settled, err := waitForSurgeDrainSettle(context.Background(), input, plan, 0)
	if err != nil {
		t.Fatalf("settle on release: %v", err)
	}
	if settled {
		t.Fatalf("the grace elapsed under the hold and was spent; it must start over")
	}
	rearmed := legacyInstanceStatuses(c, isvc, workload.ComponentEngine)[0].Operation
	if rearmed == nil || !rearmed.LastProgressAt.Time.Equal(release) {
		t.Fatalf("grace start after the hold: got %+v want %v", rearmed, release)
	}

	// Inside the restarted interval the row is quiet: a wait that lasts
	// costs one write per interval, not one per pass.
	status.Operation.LastProgressAt = metav1.NewTime(release)
	clk.SetTime(release.Add(time.Duration(grace-1) * time.Second))
	before := readRV(t, c, isvc)
	if settled, err = waitForSurgeDrainSettle(context.Background(), input, plan, 0); err != nil || settled {
		t.Fatalf("inside the restarted interval: settled=%v err=%v", settled, err)
	}
	if after := readRV(t, c, isvc); after != before {
		t.Errorf("a held settle wrote inside its interval: rv %s -> %s", before, after)
	}
}
