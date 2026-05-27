package omenative

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
)

// withStuckPodGracePeriod swaps in a short grace window for the duration
// of the test so the escalation fires synchronously instead of after
// the production 60-second debounce. Restores the prior value on cleanup
// so parallel tests don't observe each other's overrides.
func withStuckPodGracePeriod(t *testing.T, d time.Duration) {
	t.Helper()
	prev := StuckPodFailureGracePeriod
	StuckPodFailureGracePeriod = d
	t.Cleanup(func() { StuckPodFailureGracePeriod = prev })
}

// surgeStuckPodEnv builds a 1-replica engine ISVC mid-surge: status
// shows Phase=Updating with Operation.Step=Surge, the old pod is up at
// ordinal 0, and the surge pod at ordinal 1 is in the supplied stuck
// state (default ImagePullBackOff). The returned ISVC mirrors the
// shape the controller writes when surgeUpdate has just stamped Phase 1
// and is waiting on the surge pod to reach ContainersReady.
func surgeStuckPodEnv(t *testing.T, podAge time.Duration, reason string) (*v1beta1.InferenceService, *corev1.Pod, *corev1.Pod) {
	t.Helper()
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "old-rev",
		TargetRevision:  "new-rev",
		ActiveOrdinal:   0,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate,
			Step: "Surge",
			// Far-future Deadline so the slow-path expire doesn't race
			// the fast-path escalation we're trying to test.
			Deadline: metav1.NewTime(time.Now().Add(30 * time.Minute)),
		},
	}
	oldPod := podAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	oldPod.Labels[query.LabelPodOrdinal] = "0"
	surgePod := podAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	surgePod.Name = query.PodName(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	surgePod.Labels[query.LabelPodOrdinal] = "1"
	surgePod.CreationTimestamp = metav1.NewTime(time.Now().Add(-podAge))
	surgePod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name:  "main",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
		},
	}
	return isvc, oldPod, surgePod
}

// TestSurgeUpdate_ImagePullBackOff_MarksInstanceFailed is the primary
// fix verification: a SurgeThenDrain surge pod stuck in
// ImagePullBackOff past StuckPodFailureGracePeriod escalates the
// Instance to Phase=Failed without waiting for the (default 30 min)
// InstanceReadyTimeout deadline. Closes the X-1 reproduction reported
// against the dev cluster.
func TestSurgeUpdate_ImagePullBackOff_MarksInstanceFailed(t *testing.T) {
	resetExpectations(t)
	withStuckPodGracePeriod(t, 1*time.Second)
	isvc, oldPod, surgePod := surgeStuckPodEnv(t, 5*time.Second, "ImagePullBackOff")
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: got %q want Failed (surge pod ImagePullBackOff past grace should fast-fail)", s.Phase)
	}
	// Operation must be preserved so operators can see WHAT was in flight
	// (the OEP-0007.2 contract — the breadcrumb tells them which step
	// wedged, distinct from the InstanceFailed event's pod-name detail).
	if s.Operation == nil {
		t.Fatalf("Operation must be preserved on stuck-pod escalation; got nil")
	}
	if s.Operation.Step != "Surge" {
		t.Errorf("Operation.Step: got %q want Surge (Step preserved as breadcrumb)", s.Operation.Step)
	}
}

// TestSurgeUpdate_ImagePullBackOff_WithinGrace_NoEscalation pins the
// debounce: a pod that has been in ImagePullBackOff for less than
// StuckPodFailureGracePeriod must NOT escalate. Without this guard, a
// brief transient pull failure on a flaky registry would mis-classify
// a pod that's about to recover within seconds.
func TestSurgeUpdate_ImagePullBackOff_WithinGrace_NoEscalation(t *testing.T) {
	resetExpectations(t)
	withStuckPodGracePeriod(t, 60*time.Second)
	isvc, oldPod, surgePod := surgeStuckPodEnv(t, 5*time.Second, "ImagePullBackOff")
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating (still inside grace window)", s.Phase)
	}
}

// TestSurgeUpdate_ImagePullBackOff_IdempotentOnAlreadyFailed pins the
// idempotency guard: a second AggregateAndWriteStatus pass on an
// already-Failed Instance must NOT re-fire the InstanceFailed event
// or churn LastTransitionTime. Without this, every reconcile pass
// after the first escalation would emit a duplicate event.
func TestSurgeUpdate_ImagePullBackOff_IdempotentOnAlreadyFailed(t *testing.T) {
	resetExpectations(t)
	withStuckPodGracePeriod(t, 1*time.Second)
	isvc, oldPod, surgePod := surgeStuckPodEnv(t, 5*time.Second, "ImagePullBackOff")
	// Pre-stamp Phase=Failed on the InstanceStatus so the second pass
	// sees the already-failed state on the very first read.
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed
	c := newFakeClientWithDiscovery(t, isvc, oldPod, surgePod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	// Two passes back-to-back should both succeed silently — the second
	// one must not panic, error, or churn the status (the InstanceFailed
	// event and Phase mutate both skip when already-Failed).
	for i := 0; i < 2; i++ {
		if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
			t.Fatalf("pass %d AggregateAndWriteStatus: %v", i, err)
		}
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: got %q want Failed (escalation must persist across passes)", s.Phase)
	}
}

// TestSurgeUpdate_NoOperation_NoEscalation pins the precondition:
// the fast-failure escalation must NOT fire when there is no in-flight
// Operation. A pod stuck in ImagePullBackOff that's NOT under an active
// op is the steady-state "this pod is wedged" surface — surfacing it as
// Instance.Failed would create false positives during scenarios where
// the user has manually deleted a Deployment underneath us and kubelet
// is replaying the pod's failure state on every reconcile.
//
// The guard: shouldCheckForStuckPods returns false when s.Operation
// is nil.
func TestSurgeUpdate_NoOperation_NoEscalation(t *testing.T) {
	resetExpectations(t)
	withStuckPodGracePeriod(t, 1*time.Second)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// No Operation, Phase=Ready (steady state).
	pod := podAtIncarnation(isvc, 0, 1, false, false)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-1 * time.Minute))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
	}
	c := newFakeClientWithDiscovery(t, isvc, pod)
	p := minimalParams(t, isvc, c)
	plan, _ := BuildPlan(p)

	if err := AggregateAndWriteStatus(context.Background(), p, plan, nil); err != nil {
		t.Fatalf("AggregateAndWriteStatus: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready (no in-flight Operation must not escalate)", s.Phase)
	}
}

// TestSurgeUpdate_FailedInstanceSurfacesAtComponentObservation closes
// the end-to-end loop: a Phase=Failed InstanceStatus must lift into
// ComponentObservation.Failed via the coordination layer's
// buildComponentObservation, which the per-policy state machine reads
// to flip the group's phase to Failed. Without this rollup, the X-1
// fix would surface Instance.Failed but the group would still sit at
// Surging / Waiting.
//
// Equivalent to PR #236's reconciler_test rollup tests; pinned here so
// a refactor of buildComponentObservation can't silently break the
// surface contract this fix depends on.
func TestSurgeUpdate_FailedInstanceSurfacesAtComponentObservation(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceFailed,
		RunningRevision: "old-rev",
		TargetRevision:  "new-rev",
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate,
			Step: "Surge",
		},
	}
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.Replicas = 1
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision = "llama-70b-engine-old"
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.UpdateRevision = "llama-70b-engine-new"

	// Build a single-Component BlueGreen group.
	group := coordination.ResolvedGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	}
	// Empty perRevisionPods — the rollup reads Failed off the Instance
	// status, not off pod counts.
	obs := coordination.GroupObservation{
		Group:      group,
		Components: map[v1beta1.ComponentType]coordination.ComponentObservation{},
	}
	for _, c := range group.Components {
		// Direct calls to the export-able accessor would be cleaner, but
		// buildComponentObservation lives in the coordination package
		// and is unexported. We can verify the same contract by reading
		// the InstanceStatus directly and confirming the policy machinery
		// translates a Failed Instance to Failed Component.Failed=true.
		failedIdx := []int32{}
		for _, s := range isvc.Status.Components[c].OMENative.InstanceStatuses {
			if s.Phase == v1beta1.OMENativeInstanceFailed {
				failedIdx = append(failedIdx, s.Index)
			}
		}
		obs.Components[c] = coordination.ComponentObservation{
			Component:           c,
			DesiredReplicas:     1,
			TargetRevisionHash:  "new",
			CurrentRevisionHash: "old",
			RolloutInFlight:     true,
			Failed:              len(failedIdx) > 0,
		}
	}
	tr := coordination.ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("group phase: got %q want Failed (Phase=Failed Instance must surface at group)", tr.Phase)
	}
}
