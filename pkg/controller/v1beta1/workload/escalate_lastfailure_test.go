package workload_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// Stuck-pod escalation (#398) must preserve the wedged pod's diagnostics
// into InstanceStatus.LastFailure when it flips the Instance to Failed —
// the escalation is followed by a recreate / teardown that deletes the
// wedged pod, so this is the surviving trace operators read.
func TestEscalateStuckPodFailures_CapturesLastFailure(t *testing.T) {
	prev := workload.StuckPodFailureGracePeriod
	workload.StuckPodFailureGracePeriod = 1 * time.Millisecond
	t.Cleanup(func() { workload.StuckPodFailureGracePeriod = prev })

	now := time.Now()
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-leader-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "rev-new"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "back-off pulling image"}},
			}},
		},
	}

	insts := []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseUpdating, RunningRevision: "rev-old", Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: "Drain"}}}
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			for i := range insts {
				if insts[i].Index == idx {
					mutate(&insts[i])
					return nil
				}
			}
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) {},
	}

	if err := workload.EscalateStuckPodFailures(context.Background(), input, workload.EscalationParams{
		Instances:           insts,
		PodsByIndex:         map[int32][]*corev1.Pod{0: {stuck}},
		CurrentRevisionHash: "rev-old",
		RevisionHashLabel:   "ome.io/revision-hash",
	}); err != nil {
		t.Fatalf("EscalateStuckPodFailures: %v", err)
	}

	if insts[0].Phase != workload.InstancePhaseFailed {
		t.Fatalf("Phase: got %q want Failed", insts[0].Phase)
	}
	lf := insts[0].LastFailure
	if lf == nil {
		t.Fatalf("LastFailure: nil, want the wedged pod's diagnostics")
	}
	if lf.PodName != stuck.Name {
		t.Errorf("LastFailure.PodName: got %q want %q", lf.PodName, stuck.Name)
	}
	if lf.Reason != "ImagePullBackOff" {
		t.Errorf("LastFailure.Reason: got %q want ImagePullBackOff", lf.Reason)
	}
	if lf.ContainerName != "main" {
		t.Errorf("LastFailure.ContainerName: got %q want main", lf.ContainerName)
	}
	// A stuck waiting-state wedge never ran a process → no exit code.
	if lf.ExitCode != nil {
		t.Errorf("LastFailure.ExitCode: got %v want nil for a waiting-state wedge", lf.ExitCode)
	}
}
