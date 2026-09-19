package workload_test

// Gate limbo: every container runs and passes its probes, but the pod's
// Ready condition never follows because a readiness gate it declares stays
// unsatisfied. The pod is not eligible for its Service, so every promote bar
// it stands behind waits forever and the operation deadline is the only exit.
// The record that exit leaves must name the gate rather than the clock.

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// gateNotFoldedPod builds the gate-limbo shape: phase Running, the container
// running and Ready, ContainersReady=True, and a declared readiness gate
// whose condition nobody has satisfied, so Ready stays False.
func gateNotFoldedPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Spec: corev1.PodSpec{
			ReadinessGates: []corev1.PodReadinessGate{{ConditionType: podreadiness.ConditionType}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.ContainersReady,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(since),
				},
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionFalse,
					Reason:             "ReadinessGatesNotReady",
					Message:            "corresponding condition of pod readiness gate \"ome.io/serving\" does not exist",
					LastTransitionTime: metav1.NewTime(since),
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// TestPodGateNotFolded pins the detector and its disjointness from the
// containers-never-ready limbo: this shape requires ContainersReady True and
// Ready not True, so a pod is evidence for at most one of the two.
//
// Cells: instance/Create/CreatePods/timer.operationDeadline/before
func TestPodGateNotFolded(t *testing.T) {
	now := time.Now()
	pod := gateNotFoldedPod("engine-0-default-0", now)
	if !workloadtypes.PodGateNotFolded(pod) {
		t.Errorf("containers ready with Ready=False: got false want true")
	}
	if workloadtypes.PodRunningNotReady(pod) {
		t.Errorf("gate limbo also read as a containers-not-ready limbo: the two shapes must be disjoint")
	}
	if workloadtypes.PodGateNotFolded(runningNotReadyPod("engine-0-default-0", now)) {
		t.Errorf("containers-not-ready pod: got true want false")
	}
	if workloadtypes.PodGateNotFolded(servingPod("engine-0-default-0")) {
		t.Errorf("serving pod: got true want false")
	}

	backoff := gateNotFoldedPod("engine-0-default-0", now)
	backoff.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
	}
	if workloadtypes.PodGateNotFolded(backoff) {
		t.Errorf("CrashLoopBackOff pod: got true want false (a kubelet wedge, not a gate limbo)")
	}
	if workloadtypes.PodGateNotFolded(nil) {
		t.Errorf("nil pod: got true want false")
	}
}

// TestEscalation_DeadlineWithGateNotFoldedPod_NamesTheGate: the deadline
// expires on a Create attempt whose pod passed every probe but never became
// eligible for its Service. The record names the unsatisfied gate. Evidence
// only — the blame is the one the attempt would have taken with no evidence
// at all, because an unsatisfied gate is ambiguous between a gate nobody
// owns and a second writer deliberately holding one.
//
// Cells: instance/Create/CreatePods/timer.operationDeadline/before
func TestEscalation_DeadlineWithGateNotFoldedPod_NamesTheGate(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index:    0,
		Phase:    workload.InstancePhaseCreating,
		PodCount: 1,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationCreate,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {gateNotFoldedPod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != workloadtypes.ReasonReadinessGateNotSatisfied {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, workloadtypes.ReasonReadinessGateNotSatisfied)
	}
	if !strings.Contains(got.LastFailure.Message, string(podreadiness.ConditionType)) {
		t.Errorf("LastFailure.Message: got %q want the unsatisfied gate named", got.LastFailure.Message)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (an unsatisfied gate is ambiguous, not revision-scoped)", rec.blocks)
	}
}

// TestEscalation_RestartDeadlineWithGateNotFoldedPod_NamesTheGate: the same
// evidence on the repair path. A restart whose rebuilt pod passes its probes
// but never returns to its Service fails on the deadline with the gate named,
// and the attempt keeps the blame it would have had without the evidence.
//
// Cells: instance/Restart/Drain/timer.operationDeadline/before
func TestEscalation_RestartDeadlineWithGateNotFoldedPod_NamesTheGate(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index:       0,
		Incarnation: 2,
		Phase:       workload.InstancePhaseRestarting,
		PodCount:    1,
		Operation: &workload.InstanceOperation{
			Type:     workload.InstanceOperationRestart,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {gateNotFoldedPod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != workloadtypes.ReasonReadinessGateNotSatisfied {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, workloadtypes.ReasonReadinessGateNotSatisfied)
	}
	if !strings.Contains(got.LastFailure.Message, string(podreadiness.ConditionType)) {
		t.Errorf("LastFailure.Message: got %q want the unsatisfied gate named", got.LastFailure.Message)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (an unsatisfied gate is ambiguous, not revision-scoped)", rec.blocks)
	}
}
