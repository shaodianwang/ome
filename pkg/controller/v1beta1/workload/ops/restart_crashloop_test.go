package ops_test

// Op-less crash-loop repair: a Ready row whose pod is wedged in a
// terminal kubelet waiting reason on the revision it is supposed to be
// running has no other repair path, so the restart trigger opens one
// whatever the restart policy says.

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

const crashLoopRevision = "llama-70b-engine-" + testRevisionHash

// crashLoopingPod is a pod of instance 0 whose runner is parked in a
// terminal waiting reason, created `age` ago.
func crashLoopingPod(isvc *v1beta1.InferenceService, reason string, age time.Duration) *corev1.Pod {
	pod := podForInstance(isvc, 0, false /* ready */, false /* serving */)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-age))
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  constants.MainContainerName,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	return pod
}

// crashLoopInput wires a Ready, operation-free row on crashLoopRevision
// with the stuck-pod grace configured.
func crashLoopInput(t *testing.T, grace time.Duration) (workload.ReconcileInput, workload.ComponentPlan, *corev1.Pod) {
	t.Helper()
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	pod := crashLoopingPod(isvc, "CrashLoopBackOff", time.Hour)
	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = grace
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	return input, plan, pod
}

// A Ready row with no operation whose pod has been wedged in a terminal
// waiting reason longer than the grace opens a Restart even under
// RestartPolicy=None: the pod cannot recover on its own and nothing else
// repairs it.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestDetectRestartTrigger_OpLessCrashLoopRepairsUnderPolicyNone(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)

	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod})
	if !needs {
		t.Fatalf("expected a restart trigger for an operation-less crash loop under RestartPolicy=None")
	}
	if !strings.Contains(reason, "CrashLoopBackOff") || !strings.Contains(reason, pod.Name) {
		t.Errorf("reason must name the pod and the kubelet reason; got %q", reason)
	}
}

// The repair is paced by the grace: evidence younger than
// lifecycle.stuckPodGracePeriod is an ordinary backoff the kubelet may
// still resolve.
//
// Cells: instance/Ready/timer.stuckPodGrace/before
func TestDetectRestartTrigger_CrashLoopWithinGraceStaysSilent(t *testing.T) {
	input, plan, pod := crashLoopInput(t, 2*time.Hour)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a wedge inside the grace must not trigger; got reason %q", reason)
	}
}

// An unconfigured grace disables the repair outright, exactly as it
// disables the stuck-pod fast escalation.
//
// Cells: instance/Ready/timer.stuckPodGrace/before
func TestDetectRestartTrigger_UnconfiguredGraceDisablesCrashLoopRepair(t *testing.T) {
	input, plan, pod := crashLoopInput(t, 0)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("no configured grace means no repair; got reason %q", reason)
	}
}

// A wedged pod whose revision-hash label disagrees with the Component's
// current revision is a leftover, not this row's workload: there is no
// revision for a repair to rebuild on, so it belongs to the escalation
// pass's wedged-pod edge to Failed.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestDetectRestartTrigger_OffRevisionWedgeLeftToEscalation(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	pod.Labels[query.LabelRevisionHash] = "otherrev"

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("an off-revision leftover must not open a repair; got reason %q", reason)
	}
}

// A serving pod set is never repaired on the strength of a container
// waiting reason: the workload is answering traffic and a recycle would
// take it out.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestDetectRestartTrigger_CrashLoopOnServingPodSetStaysSilent(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a fully serving pod set must not be recycled; got reason %q", reason)
	}
}

// The repair answers to the same RetryBlock authority every rebuild does:
// a revision held after repeated failures is not re-materialized on a
// loop. A Held block is the one gate verdict with no time bound: it
// denies every fresh attempt at that target until a different target
// arrives.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before, instance/Ready/gate.deny[Held]/before
func TestDetectRestartTrigger_CrashLoopDeniedByHeldRetryBlock(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: crashLoopRevision,
		State:          workload.RetryBlockHeld,
	}}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a held RetryBlock must deny the repair; got reason %q", reason)
	}
}

// RestartPolicy=None keeps its meaning for mere container restarts: a
// runner that died and came back inside the same pod is not a wedge, and
// the operator asked for it to be left alone.
//
// Cells: instance/Ready/pod.containerRestart/before
func TestDetectRestartTrigger_ContainerRestartStillHonoursPolicyNone(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	readySince := metav1.NewTime(time.Now().Add(-time.Hour))
	ir.Status.InstanceStatuses[0].ReadySince = &readySince

	pod := podForInstance(isvc, 0, true, true)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		runnerStatus(constants.MainContainerName, readySince.Add(10*time.Minute), &corev1.ContainerStateTerminated{
			Reason:     "OOMKilled",
			ExitCode:   137,
			FinishedAt: metav1.NewTime(readySince.Add(9 * time.Minute)),
		}),
	}

	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("RestartPolicy=None must still ignore a bare container restart; got reason %q", reason)
	}
}

// Once the repair is open, the row is re-selected on every pass so the
// Restart state machine advances — a repair that stalls at Phase=Drain
// under RestartPolicy=None would be worse than never opening one.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-crashloop-restart
func TestDetectRestartTrigger_OpenRepairKeepsDrivingUnderPolicyNone(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationRestart, Step: "Drain"},
	})
	c := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	if needs, _ := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); !needs {
		t.Fatalf("an in-flight Restart must keep being driven under RestartPolicy=None")
	}
}

// A row below Ready is not this trigger's: an Instance still forming
// boots through waiting states the kubelet resolves on its own, and the
// phases before Ready are owned by the passes that materialize them.
//
// Cells: instance/Pending/pod.waiting[CrashLoopBackOff]/before
func TestDetectRestartTrigger_PendingRowIsNotACrashLoopRepair(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	for i := range input.ObservedState.InstanceStatuses {
		input.ObservedState.InstanceStatuses[i].Phase = workload.InstancePhasePending
	}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a Pending row belongs to another pass; got reason %q", reason)
	}
}

// A gang is repaired as a unit: one wedged member opens ONE Restart for
// the Instance, which is what rebuilds the whole pod set. The repair is
// selected per Instance, so the healthy members are neither separately
// triggered nor separately counted.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestDetectRestartTrigger_GangWedgedOnOneMemberOpensOneRepair(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	ir.Status.InstanceStatuses[0].PodCount = 2

	healthy := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	wedged := crashLoopingPod(isvc, "CrashLoopBackOff", time.Hour)
	wedged.Name += "-1"
	wedged.Labels[query.LabelPodOrdinal] = "1"

	c := newFakeClient(t, isvc, ir, healthy, wedged)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	plan.Instances[0].Runners = []workload.RunnerPlan{{Name: "default", Size: 2}}

	pods := []*corev1.Pod{healthy, wedged}
	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], pods)
	if !needs {
		t.Fatal("a gang wedged on one member must open a repair for the Instance")
	}
	if !strings.Contains(reason, wedged.Name) {
		t.Errorf("the repair must name the wedged member; got %q", reason)
	}
	if !ops.RestartOpensUnavailability(input, plan.Instances[0], pods) {
		t.Error("the gang repair takes the Instance offline, so it must be admitted like any fresh attempt")
	}
}

// Once the repair is stamped it owns the pod set, and the stamp is
// idempotent: the same wedged pod re-observed on the next pass is the
// same evidence the repair already acted on, so nothing is armed a
// second time — no fresh incarnation, no second operation.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-crashloop-restart, instance/Ready/pod.waiting[CrashLoopBackOff]/after@T-ready-crashloop-restart
func TestRestart_ReObservedCrashLoopArmsNothingASecondTime(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	pod := crashLoopingPod(isvc, "CrashLoopBackOff", time.Hour)
	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	// The stamp lands: the row leaves Ready at a bumped incarnation.
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart (stamp): %v", err)
	}
	armed := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if armed.Phase != v1beta1.OMENativeInstanceRestarting || armed.Incarnation != 2 {
		t.Fatalf("stamp: got Phase=%q Incarnation=%d want Restarting at 2", armed.Phase, armed.Incarnation)
	}

	// The same reason is read again on the next pass, off the same pod.
	resetExpectations(t)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan = buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart (re-observation): %v", err)
	}

	again := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if again.Incarnation != armed.Incarnation {
		t.Errorf("Incarnation: got %d want %d (the stamp is idempotent)", again.Incarnation, armed.Incarnation)
	}
	if again.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting (the repair still owns the row)", again.Phase)
	}
	if again.Operation == nil || again.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("Operation: got %+v want the one open repair", again.Operation)
	}
}

// Losing the wedged pod in the same pass that stamps the repair only
// completes work the repair was going to do: the drain has nothing left
// to flip or delete, so the stamp lands unchanged. From there the
// repair owns the pod set and recreates the missing member at the
// bumped incarnation, which is the same answer whether the pod was lost
// at the stamp or after it.
//
// Cells: instance/Ready/pod.deleted[source]/boundary@T-ready-crashloop-restart, instance/Ready/pod.deleted[source]/after@T-ready-crashloop-restart
func TestRestart_PodLostAsTheRepairStampCommitsStillDrains(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	// The pod the crash-loop trigger read is gone by the time the stamp
	// commits, so the pass sees the index with no pods at all.
	c := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting (the stamp lands whatever happened to the pod)", s.Phase)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2", s.Incarnation)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("Operation: got %+v want an open repair", s.Operation)
	}

	// The repair owns the pod set from here: the next pass rebuilds the
	// missing member at the bumped incarnation.
	resetExpectations(t)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan = buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart (rebuild): %v", err)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods: got %d want 1 (the repair recreates the lost member)", len(pods.Items))
	}
	if got := pods.Items[0].Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("new pod %s incarnation label: got %q want 2", pods.Items[0].Name, got)
	}
}

// A new target revision arriving while the repair is stamped or open
// waits for it: the update trigger declines any row below Ready, so the
// roll re-enters from Ready — or from Failed — once the repair resolves.
//
// Cells: instance/Ready/spec.revision/boundary@T-ready-crashloop-restart, instance/Ready/spec.revision/after@T-ready-crashloop-restart
func TestDetectUpdateTrigger_NewTargetWaitsForAnOpenCrashLoopRepair(t *testing.T) {
	for _, tc := range []struct {
		name string
		step string
	}{
		{name: "stamp landing", step: ""},
		{name: "repair open", step: "Drain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc := minimalISVC("llama-70b", "prod", 1)
			op := &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationRestart, Reason: "pod CrashLoopBackOff"}
			op.Step = tc.step
			ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
				Index:           0,
				Incarnation:     2,
				Phase:           v1beta1.OMENativeInstanceRestarting,
				RunningRevision: crashLoopRevision,
				Operation:       op,
			})
			c := newFakeClient(t, isvc, ir)
			input := buildTestInput(isvc, c, workload.ComponentEngine)
			input.ObservedState.CurrentRevision = crashLoopRevision
			plan := buildPlanSinglePodEngineForRestart(c, isvc)

			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: "llama-70b-engine-newtarget",
			}}
			trigger, _, err := ops.DetectUpdateTriggerWithPods(context.Background(), workload.Deps{Client: c},
				input, plan, plan.Instances[0], target, input.DesiredSpec.PodSpec, nil)
			if err != nil {
				t.Fatalf("DetectUpdateTriggerWithPods: %v", err)
			}
			if trigger {
				t.Fatalf("a row below Ready must not be rolled; the new target waits for the repair")
			}
		})
	}
}
