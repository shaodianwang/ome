package workload_test

// Running-but-never-Ready limbo: the pod runs every container, the
// kubelet reports no waiting reason, and ContainersReady simply never
// flips. The operation deadline is the only exit, and the record it
// leaves must say what was never ready rather than "the clock ran out".

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

const probeMessage = "containers with unready status: [main]"

// runningNotReadyPod builds the limbo shape: phase Running, every
// container started, ContainersReady=False with the kubelet's own reason
// and message.
func runningNotReadyPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.ContainersReady,
				Status:             corev1.ConditionFalse,
				Reason:             workloadtypes.ReasonContainersNotReady,
				Message:            probeMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// TestPodRunningNotReady pins the detector: only a Running pod whose
// containers have all started yet never report ContainersReady counts.
// A pod with a container parked in a waiting state is a kubelet wedge
// the existing evidence already classifies, not this shape.
//
// Cells: instance/Create/CreatePods/pod.runningNotReady[source]/before
func TestPodRunningNotReady(t *testing.T) {
	now := time.Now()
	if !workloadtypes.PodRunningNotReady(runningNotReadyPod("engine-0-default-0", now)) {
		t.Errorf("running pod with ContainersReady=False: got false want true")
	}
	if workloadtypes.PodRunningNotReady(servingPod("engine-0-default-0")) {
		t.Errorf("serving pod: got true want false")
	}

	backoff := runningNotReadyPod("engine-0-default-0", now)
	backoff.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
	}
	if workloadtypes.PodRunningNotReady(backoff) {
		t.Errorf("CrashLoopBackOff pod: got true want false (a kubelet wedge, not a readiness limbo)")
	}
	if workloadtypes.PodRunningNotReady(nil) {
		t.Errorf("nil pod: got true want false")
	}
}

// TestEscalation_DeadlineWithRunningNotReadyPod_NamesTheProbe: when the
// deadline expires on a single-pod Create attempt whose pod runs but
// never reports ready, the failure record names the unready container
// and the readiness condition. Evidence only — the disposition is the
// one the attempt would have taken with no evidence at all, because a
// pod that runs yet never passes its probes is ambiguous between the
// revision and the node it landed on.
//
// Cells: instance/Create/CreatePods/timer.operationDeadline/before, instance/Create/CreatePods/pod.runningNotReady[source]/before
func TestEscalation_DeadlineWithRunningNotReadyPod_NamesTheProbe(t *testing.T) {
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
		map[int32][]*corev1.Pod{0: {runningNotReadyPod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != workloadtypes.ReasonContainersNotReady {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, workloadtypes.ReasonContainersNotReady)
	}
	if got.LastFailure.ContainerName != "main" {
		t.Errorf("LastFailure.ContainerName: got %q want the unready container \"main\"", got.LastFailure.ContainerName)
	}
	if !strings.Contains(got.LastFailure.Message, probeMessage) {
		t.Errorf("LastFailure.Message: got %q want the readiness condition message", got.LastFailure.Message)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (readiness limbo is ambiguous, not revision-scoped)", rec.blocks)
	}
}

// TestEscalation_DeadlineWithCrashLoopPod_KeepsAmbiguousBlame: a pod
// backing off is ambiguous between a broken binary and broken hardware,
// so the deadline must not start blaming the revision for it just
// because the pod is also not ready.
//
// Cells: instance/Create/CreatePods/timer.operationDeadline/before
func TestEscalation_DeadlineWithCrashLoopPod_KeepsAmbiguousBlame(t *testing.T) {
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

	pod := runningNotReadyPod("engine-0-default-0", now.Add(-time.Hour))
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off 5m0s"},
	}
	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", rec.store[0].Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (CrashLoopBackOff stays ambiguous)", rec.blocks)
	}
}

// TestEscalation_RunningNotReadyBeforeDeadline_Untouched: the readiness
// wait itself changes nothing. Only the deadline ends it, so a live
// deadline leaves the attempt alone however long the probes have failed.
//
// Cells: instance/Create/CreatePods/pod.runningNotReady[source]/before
func TestEscalation_RunningNotReadyBeforeDeadline_Untouched(t *testing.T) {
	now := time.Now()
	insts := []workload.InstanceStatus{{
		Index:    0,
		Phase:    workload.InstancePhaseCreating,
		PodCount: 1,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationCreate,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {runningNotReadyPod("engine-0-default-0", now.Add(-24*time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workload.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (the deadline is the only exit)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 || len(rec.blocks) != 0 {
		t.Errorf("writes before the deadline: warns=%v blocks=%v want none", rec.warns, rec.blocks)
	}
}

// TestEscalation_GangDeadlineWithRunningNotReadyPods_NamesTheProbe: a
// gang surge keeps its Operation for the abandon path, and the record it
// leaves behind must still name the readiness failure — that record is
// what the abandon charges the revision's ladder on.
//
// Cells: instance/Update/Surge/timer.operationDeadline/before, instance/Update/Surge/pod.runningNotReady[target]/before
func TestEscalation_GangDeadlineWithRunningNotReadyPods_NamesTheProbe(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []workload.InstanceStatus{{
		Index:    0,
		Phase:    workload.InstancePhaseUpdating,
		PodCount: 2,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			SurgeIndex:     &surge,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{1: {
			runningNotReadyPod("engine-1-leader-0", now.Add(-time.Hour)),
			runningNotReadyPod("engine-1-worker-0", now.Add(-time.Hour)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.Operation == nil {
		t.Errorf("Operation: got nil want preserved (the abandon path consumes the continuation)")
	}
	if got.LastFailure == nil || got.LastFailure.Reason != workloadtypes.ReasonContainersNotReady {
		t.Fatalf("LastFailure.Reason: got %+v want %q", got.LastFailure, workloadtypes.ReasonContainersNotReady)
	}
	if workload.IsWorkloadCausedReason(got.LastFailure.Reason) {
		t.Errorf("IsWorkloadCausedReason(%q): got true want false — the abandon must not charge the ladder for an ambiguous failure", got.LastFailure.Reason)
	}
}

// TestEscalation_SurgeDeadlineBlamesTheTargetNotTheSource: a single-pod
// surge shares its Instance index with the source it is replacing, so
// the deadline sees both pods. When the replacement is in readiness
// limbo and the source's probes are flapping too, the EVIDENCE must name
// the replacement — the pod the attempt is about — rather than a source
// that belongs to a superseded revision.
//
// Cells: instance/Update/Surge/timer.operationDeadline/before, instance/Update/Surge/pod.runningNotReady[target]/before
func TestEscalation_SurgeDeadlineBlamesTheTargetNotTheSource(t *testing.T) {
	now := time.Now()
	const targetRev = "own-engine-newhash"
	insts := []workload.InstanceStatus{{
		Index:           0,
		Phase:           workload.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "own-engine-oldhash",
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: targetRev,
			Deadline:       metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.ObservedState.UpdateRevision = targetRev

	// The source is listed first, so a set-wide scan reaches it first.
	source := runningNotReadyPod("engine-0-default-0", now.Add(-2*time.Hour))
	source.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName("own-engine-oldhash").Hash()}
	target := runningNotReadyPod("engine-0-default-1", now.Add(-time.Hour))
	target.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(targetRev).Hash()}

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {source, target}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (an expired deadline must resolve)", got.Phase)
	}
	if got.Operation != nil {
		t.Errorf("Operation: got %+v want cleared (a single-pod surge is a disposable attempt)", got.Operation)
	}
	if got.LastFailure == nil || got.LastFailure.PodName != target.Name {
		t.Errorf("LastFailure: got %+v want the replacement %s blamed", got.LastFailure, target.Name)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (readiness limbo blames nobody)", rec.blocks)
	}
}
