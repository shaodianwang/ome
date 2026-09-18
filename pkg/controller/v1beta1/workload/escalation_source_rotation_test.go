package workload_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// wedgedButGatedPod is the shape the failed-while-serving guard used to
// read as healthy capacity: conditions still say ContainersReady and the
// serving gate is still True, while the container is parked in a
// terminal waiting reason.
func wedgedButGatedPod(name, reason string) *corev1.Pod {
	pod := servingPod(name)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	return pod
}

func surgingInstanceWithWaiting(waiting string) []workload.InstanceStatus {
	return []workload.InstanceStatus{{
		Index:    0,
		Phase:    workload.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: "own-engine-targethash",
			Deadline:       metav1.NewTime(time.Now().Add(30 * time.Minute)),
			Waiting:        waiting,
		},
	}}
}

// TestEscalation_UnroutedSourceDoesNotExemptAWedgedSurge: the
// failed-while-serving guard exempts an Instance whose pod set reads
// healthy, and pod conditions alone can read healthy while nothing is in
// rotation. A surge that has reported its source out of rotation is
// exactly that case, so the exemption must not apply and the wedged pod
// must still escalate.
//
// Cells: instance/Update/Surge/pod.servingOff[source]/before, instance/Update/Surge/endpoint.rotationOut[source]/before
func TestEscalation_UnroutedSourceDoesNotExemptAWedgedSurge(t *testing.T) {
	insts := surgingInstanceWithWaiting(workload.WaitingReasonSourceUnrouted)
	input, rec := escalationFixture(insts)
	input.StuckPodGrace = time.Second

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {wedgedButGatedPod("engine-0-default-0", "ImagePullBackOff")}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (nothing is in rotation to exempt the row)", rec.store[0].Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_RoutedSourceKeepsTheServingExemption pins the other
// side: with no report of an unrouted source the guard stands, and a row
// whose pods read healthy is left alone whatever the evidence says.
//
// Cells: instance/Update/Surge/pod.servingOff[source]/before
func TestEscalation_RoutedSourceKeepsTheServingExemption(t *testing.T) {
	insts := surgingInstanceWithWaiting("")
	input, rec := escalationFixture(insts)
	input.StuckPodGrace = time.Second

	if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {wedgedButGatedPod("engine-0-default-0", "ImagePullBackOff")}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != workload.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (a serving pod set is never stamped Failed)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// The hold arms judge an Instance on pod health alone. A row whose pods
// are all serving releases the token it was parked behind — the PodGroup
// bookkeeping may still say the name is being collected, but a gang
// already in rotation is not waiting on it, and a row left holding a
// stale token has its own deadline parked with it. The stricter capacity
// question, which a source out of rotation answers no, belongs to the
// failed-while-serving exemption alone: it decides what is exempt from
// escalating, never who owns the Waiting token.
//
// Cells: instance/Update/Surge/pod.servingOff[source]/before
func TestEscalation_ServingRowKeepsTheHoldArmsOnPodHealth(t *testing.T) {
	t.Run("gang hold released", func(t *testing.T) {
		input, rec := escalationFixture(surgingInstanceWithWaiting(workload.WaitingReasonPodGroupTerminating))
		input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)

		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {servingPod("engine-0-default-0")}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}

		got := rec.store[0]
		if got.Operation == nil || got.Operation.Waiting != "" {
			t.Errorf("Operation.Waiting: got %+v want cleared; serving pods are not waiting on a name", got.Operation)
		}
		if got.Phase != workload.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating", got.Phase)
		}
	})
	t.Run("rotation report kept", func(t *testing.T) {
		input, rec := escalationFixture(surgingInstanceWithWaiting(workload.WaitingReasonSourceUnrouted))
		input.Gangs = gangObservations(0, workload.GangStateTerminating, gangTerminatingMessage)

		if err := runEscalationPass(t, workload.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {servingPod("engine-0-default-0")}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}

		got := rec.store[0]
		if got.Operation == nil || got.Operation.Waiting != workload.WaitingReasonSourceUnrouted {
			t.Errorf("Operation.Waiting: got %+v want the report kept; no arm may park a serving row", got.Operation)
		}
		if len(rec.warns) != 0 {
			t.Errorf("WarnInstanceFailed: got %v want none (the attempt has time left)", rec.warns)
		}
	})
}
