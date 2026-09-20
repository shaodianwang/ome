package ops

// The replacement gang's marker index is driven entirely by the source's
// gang surge. These tests pin the three consequences that show up inside
// the ops package: the update strategy the pair runs under is the one
// pinned when it started, the restart trigger does not treat the marker
// as a row of its own, and the promote is what hands the index back to
// the ordinary repair path.

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// gangMarkerRevision is the replacement gang's target.
const gangMarkerRevision = "llama-70b-engine-gangtgt1"

// gangMarkerRow is the surge target marker at markerIndex: Creating with
// an Update operation whose step says whose lifecycle owns it.
func gangMarkerRow(markerIndex int32, step string) *workload.InstanceStatus {
	return &workload.InstanceStatus{
		Index:          markerIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		PodCount:       2,
		TargetRevision: gangMarkerRevision,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: gangMarkerRevision,
		},
	}
}

// TestEffectiveUpdateStrategy_GangSurgePairStaysOnThePinnedStrategy: the
// strategy is pinned on the operation when the attempt starts, so a
// strategy edit mid-surge does not switch the pair into another mode
// halfway through. The gang surge stays in control and finishes under
// SurgeThenDrain; the edit reaches the pair at its next admitted
// attempt.
//
// Cells: instance/Update/GangSurgeTarget/spec.strategy/before
func TestEffectiveUpdateStrategy_GangSurgePairStaysOnThePinnedStrategy(t *testing.T) {
	surgeIndex := int32(2)
	source := &workload.InstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "llama-70b-engine-priorrev",
		TargetRevision:  gangMarkerRevision,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			Strategy:       workload.UpdateStrategySurgeThenDrain,
			SurgeIndex:     &surgeIndex,
			TargetRevision: gangMarkerRevision,
		},
	}
	for _, edited := range []workload.UpdateStrategyType{
		workload.UpdateStrategyRecreatePod,
		workload.UpdateStrategyInPlaceOnly,
	} {
		t.Run(string(edited), func(t *testing.T) {
			if got := effectiveUpdateStrategy(source, edited); got != workload.UpdateStrategySurgeThenDrain {
				t.Errorf("effective strategy: got %q want the pinned SurgeThenDrain", got)
			}
		})
	}

	// Counter-case: with no attempt pinning it, the edit takes effect at
	// once — so the pin above is the operation's doing, not a constant.
	idle := &workload.InstanceStatus{Index: 0, Phase: workload.InstancePhaseReady}
	if got := effectiveUpdateStrategy(idle, workload.UpdateStrategyRecreatePod); got != workload.UpdateStrategyRecreatePod {
		t.Errorf("idle row effective strategy: got %q want the edited RecreatePod", got)
	}
}

// TestDetectRestartTrigger_GangSurgeMarkerIsNotRepairedOnItsOwn: the
// restart trigger declines a row whose non-Create operation is
// preserved, so a terminal replacement pod is not recreated behind the
// surge's back. The marker converges only through the stuck or deadline
// escalation, and the same holds once it has been retired.
//
// Cells: instance/Update/GangSurgeTarget/pod.phaseTerminal[Failed]/before, instance/Update/GangSurgeTarget/pod.phaseTerminal[Succeeded]/before
func TestDetectRestartTrigger_GangSurgeMarkerIsNotRepairedOnItsOwn(t *testing.T) {
	for _, step := range []string{
		workload.UpdateStepGangSurgeTarget,
		workload.UpdateStepGangSurgeTargetCleanup,
	} {
		for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
			t.Run(step+"/"+string(phase), func(t *testing.T) {
				runGangMarkerTerminalPodCase(t, step, phase)
			})
		}
	}
}

// runGangMarkerTerminalPodCase drives one (marker step, terminal phase)
// pair through the restart trigger.
func runGangMarkerTerminalPodCase(t *testing.T, step string, phase corev1.PodPhase) {
	t.Helper()
	markerIndex := int32(2)
	marker := gangMarkerRow(markerIndex, step)
	input := workload.ReconcileInput{
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{*marker},
			CurrentRevision:  gangMarkerRevision,
		},
		StuckPodGrace: time.Minute,
	}
	plan := workload.ComponentPlan{
		RestartPolicy: workload.RestartPolicyRecreateInstance,
		Instances: []workload.InstancePlan{{
			Index: markerIndex, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		}},
	}
	terminal := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "llama-70b-engine-2-leader-0",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{Phase: phase},
	}

	if needs, reason := DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{terminal}); needs {
		t.Fatalf("the marker's pods belong to the surge, not the repair; got reason %q", reason)
	}
}

// TestDetectRestartTrigger_PromotedGangTargetOwnsAMissingMember: the
// promote clears the operation and stamps the index Ready, which is an
// ordinary Instance in every respect. Losing a member as that lands, or
// at any point after it, is therefore the restart pass's business — the
// surge has no claim on the index any more.
//
// Cells: instance/Update/GangSurgeTarget/pod.deleted[target]/boundary@T-gangtarget-ready, instance/Update/GangSurgeTarget/pod.deleted[target]/after@T-gangtarget-ready
func TestDetectRestartTrigger_PromotedGangTargetOwnsAMissingMember(t *testing.T) {
	markerIndex := int32(2)
	promoted := workload.InstanceStatus{
		Index:           markerIndex,
		Incarnation:     1,
		Phase:           workload.InstancePhaseReady,
		PodCount:        2,
		RunningRevision: gangMarkerRevision,
		ReadySince:      &metav1.Time{Time: time.Now().Add(-time.Hour)},
	}
	input := workload.ReconcileInput{
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{promoted},
			CurrentRevision:  gangMarkerRevision,
		},
		StuckPodGrace: time.Minute,
	}
	plan := workload.ComponentPlan{
		RestartPolicy: workload.RestartPolicyRecreateInstance,
		Instances: []workload.InstancePlan{{
			Index: markerIndex, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		}},
	}
	survivor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "llama-70b-engine-2-leader-0",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	needs, reason := DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{survivor})
	if !needs {
		t.Fatalf("a promoted gang index missing a member must be repaired by the restart pass")
	}
	if reason == "" {
		t.Errorf("the repair must name why it was opened")
	}
}
