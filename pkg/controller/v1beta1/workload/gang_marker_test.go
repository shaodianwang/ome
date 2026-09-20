package workload_test

// The gang surge target marker is an index of its own, but no attempt of
// its own: its whole lifecycle is driven by the source's gang surge, and
// the source is where the governing operation, the deadline and the
// budget accounting live. These tests pin what that means for the events
// that reach the marker — which ones the pair answers together, and which
// ones it has no reader for.

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// gangSurgePair is the source (index 0, Step=Surge, pinned to the marker)
// and the replacement gang's marker at markerIndex running markerStep.
func gangSurgePair(now time.Time, markerIndex int32, markerStep string) []workload.InstanceStatus {
	return []workload.InstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           workload.InstancePhaseUpdating,
			PodCount:        2,
			RunningRevision: gangPairSource,
			TargetRevision:  gangPairTarget,
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepSurge,
				SurgeIndex:     &markerIndex,
				TargetRevision: gangPairTarget,
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
		{
			Index:          markerIndex,
			Incarnation:    1,
			Phase:          workload.InstancePhaseCreating,
			PodCount:       2,
			TargetRevision: gangPairTarget,
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           markerStep,
				TargetRevision: gangPairTarget,
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
	}
}

const (
	gangPairSource = "llama-70b-engine-priorrev"
	gangPairTarget = "llama-70b-engine-targetrv"
)

// TestEscalation_WedgedGangReplacementFailsBothHalvesOfThePair: all
// seven terminal waiting reasons say the replacement gang will never
// start, so the escalation ends the attempt — and it ends it on BOTH
// halves. The marker is where the wedged pods are, the source is where
// the governing operation lives, and the source's Failed-with-operation
// is what routes the dispatcher into the abandon path that tears the
// replacement gang down.
//
// Cells: instance/Update/GangSurgeTarget/pod.waiting[ImagePullBackOff]/before, instance/Update/GangSurgeTarget/pod.waiting[ErrImagePull]/before, instance/Update/GangSurgeTarget/pod.waiting[InvalidImageName]/before, instance/Update/GangSurgeTarget/pod.waiting[CreateContainerConfigError]/before, instance/Update/GangSurgeTarget/pod.waiting[CreateContainerError]/before, instance/Update/GangSurgeTarget/pod.waiting[CrashLoopBackOff]/before, instance/Update/GangSurgeTarget/pod.waiting[RunContainerError]/before
func TestEscalation_WedgedGangReplacementFailsBothHalvesOfThePair(t *testing.T) {
	for _, reason := range []string{
		"ImagePullBackOff", "ErrImagePull", "InvalidImageName",
		"CreateContainerConfigError", "CreateContainerError",
		"CrashLoopBackOff", "RunContainerError",
	} {
		t.Run(reason, func(t *testing.T) {
			now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
			fc := clocktesting.NewFakeClock(now)
			insts := gangSurgePair(now, 1, workload.UpdateStepGangSurgeTarget)
			input, _ := dispositionFixtureInput(fc, &insts, gangPairTarget, nil, ledgerOwnerCM())
			input.ObservedState.InstanceStatuses = append([]workload.InstanceStatus(nil), insts...)
			input.StuckPodGrace = time.Second

			// The source keeps serving; the replacement gang is wedged.
			wedged := []*corev1.Pod{
				waitingPod("engine-1-leader-0", reason, "node-a", now.Add(-time.Minute)),
				waitingPod("engine-1-worker-0", reason, "node-b", now.Add(-time.Minute)),
			}
			pods := map[int32][]*corev1.Pod{
				0: {servingPod("engine-0-leader-0"), servingPod("engine-0-worker-0")},
				1: wedged,
			}

			plan := workload.ComponentPlan{Instances: []workload.InstancePlan{
				{Index: 0, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
				{Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
			}}
			if err := runEscalationPass(t, workload.Deps{Client: fakeLedgerClient(t), Clock: fc}, input, plan, pods); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			for _, s := range insts {
				if s.Phase != workload.InstancePhaseFailed {
					t.Errorf("instance %d Phase: got %q want Failed", s.Index, s.Phase)
				}
				if s.Operation == nil {
					t.Errorf("instance %d Operation: got none want it preserved (the abandon path reads it)", s.Index)
				}
			}
			if insts[1].LastFailure == nil || insts[1].LastFailure.Reason != reason {
				t.Errorf("marker LastFailure: got %+v want the wedged pod's %s", insts[1].LastFailure, reason)
			}
		})
	}
}

// TestEscalation_GangFailedReadingLeavesTheSurgeMarkerUntouched: the
// replacement group's Failed phase is absorbing, so the top-level
// PodGroup pass deletes it and rebuilds it under the same deterministic
// name — a recoverable reading, not a verdict on the attempt. The
// marker's own row therefore takes no phase change, no waiting token and
// no RetryBlock from it, and the deadline keeps running.
//
// Cells: instance/Update/GangSurgeTarget/gang.podGroup[PhaseFailed]/before
func TestEscalation_GangFailedReadingLeavesTheSurgeMarkerUntouched(t *testing.T) {
	now := time.Now()
	insts := gangSurgePair(now, 1, workload.UpdateStepGangSurgeTarget)
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(1, workload.GangStateFailed, gangFailedMessage)

	plan := workload.ComponentPlan{Instances: []workload.InstancePlan{
		{Index: 0, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
	}}
	pods := map[int32][]*corev1.Pod{1: {
		{ObjectMeta: metav1.ObjectMeta{Name: "engine-1-leader-0"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		{ObjectMeta: metav1.ObjectMeta{Name: "engine-1-worker-0"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
	}}
	if err := runEscalationPass(t, workload.Deps{}, input, plan, pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	marker := rec.store[1]
	if marker.Phase != workload.InstancePhaseCreating {
		t.Errorf("marker Phase: got %q want Creating (the group resets itself)", marker.Phase)
	}
	if marker.Operation == nil || marker.Operation.Waiting != "" {
		t.Errorf("marker Operation: got %+v want no waiting token", marker.Operation)
	}
	if marker.LastFailure != nil {
		t.Errorf("marker LastFailure: got %+v want none", marker.LastFailure)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (no revision is blamed for a group that rebuilds)", rec.blocks)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestPlan_MigrationRequestWaitsForAGangSurgeToResolve: a fresh
// migration record adds one more surge Instance, so it is not driven
// while another surge is already counted. The source carries the Surge
// step for as long as the replacement gang exists — through the
// marker's own retirement — so the request waits for the pair to
// resolve rather than stacking a second lot of extra capacity.
//
// Cells: instance/Update/GangSurgeTarget/spec.migrateRequest/before, instance/Update/GangSurgeTargetCleanup/spec.migrateRequest/before
func TestPlan_MigrationRequestWaitsForAGangSurgeToResolve(t *testing.T) {
	for _, step := range []string{
		workload.UpdateStepGangSurgeTarget,
		workload.UpdateStepGangSurgeTargetCleanup,
	} {
		t.Run(step, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = gangSurgePair(time.Now(), 1, step)
			in.ObservedState.Migrations = []workload.MigrationRecord{{
				RequestUUID: "request-a",
				Trigger:     workload.MigrationTriggerManual,
				Phase:       workload.MigrationPhaseAccepted,
			}}
			plan, err := workload.BuildPlan(workload.ComponentEngine, in.DesiredSpec, in.ObservedState)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			plan.MigrationMode = workload.MigrationModeSurge

			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: gangPairTarget,
			}}
			sourcePod := enginePod("llama-70b", "prod", 0)
			surgePod := enginePod("llama-70b", "prod", 1)
			surgePod.Labels[query.LabelRevisionHash] = query.RevisionFromName(gangPairTarget).Hash()
			d := planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {sourcePod}, 1: {surgePod}}))

			if a := findAction(d, workload.ActionMigrate); a != nil {
				t.Fatalf("a migration was driven while the gang surge is in flight: %+v", a.Migration)
			}

			// Counter-case: the same record with no surge counted is
			// driven, so the deferral above is the surge's doing and not
			// a record the selector would have skipped anyway.
			in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: gangPairTarget},
			}
			free, err := workload.BuildPlan(workload.ComponentEngine, in.DesiredSpec, in.ObservedState)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			free.MigrationMode = workload.MigrationModeSurge
			d = planTargetOrFail(t, in, free, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {sourcePod}}))
			if findAction(d, workload.ActionMigrate) == nil {
				t.Fatalf("with no surge counted the same record must be driven; got %v", actionKinds(d))
			}
		})
	}
}

// TestEscalation_TerminatingMarkerPodEndsNothing: the stuck classifier
// skips a pod carrying a deletionTimestamp — a pod on its way out is not
// a pod wedged coming up — so a marker member stuck Terminating raises
// no verdict. The name it still occupies means no replacement is
// created either, and the source's pass never lists the marker's pods,
// so the wait is bounded only by the operation deadline.
//
// Cells: instance/Update/GangSurgeTarget/pod.stuckTerminating/before, instance/Update/GangSurgeTargetCleanup/pod.stuckTerminating/before
func TestEscalation_TerminatingMarkerPodEndsNothing(t *testing.T) {
	for _, step := range []string{
		workload.UpdateStepGangSurgeTarget,
		workload.UpdateStepGangSurgeTargetCleanup,
	} {
		t.Run(step, func(t *testing.T) {
			now := time.Now()
			insts := gangSurgePair(now, 1, step)
			input, rec := escalationFixture(insts)
			input.StuckPodGrace = time.Second

			deleting := waitingPod("engine-1-leader-0", "ContainerCreating", "node-a", now.Add(-time.Hour))
			dt := metav1.NewTime(now.Add(-time.Hour))
			deleting.DeletionTimestamp = &dt
			plan := workload.ComponentPlan{Instances: []workload.InstancePlan{
				{Index: 0, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
				{Index: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 2}}},
			}}
			pods := map[int32][]*corev1.Pod{1: {deleting}}

			if err := runEscalationPass(t, workload.Deps{}, input, plan, pods); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			marker := rec.store[1]
			if marker.Phase != workload.InstancePhaseCreating {
				t.Errorf("marker Phase: got %q want Creating (a pod on its way out is not a wedge)", marker.Phase)
			}
			if marker.Operation == nil {
				t.Fatalf("marker Operation: got none want the attempt still open")
			}
			if !marker.Operation.Deadline.Equal(&insts[1].Operation.Deadline) {
				t.Errorf("marker Deadline: got %v want it still running", marker.Operation.Deadline)
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
		})
	}
}
