package ops_test

// Op-less crash-loop repair: a Ready row whose pod is wedged in a
// terminal kubelet waiting reason on the revision it is supposed to be
// running has no other repair path, so the restart trigger opens one
// whatever the restart policy says.

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
// loop.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
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
