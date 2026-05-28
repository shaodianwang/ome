package workload_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// TestUniqueNodes locks the deterministic ordering + nil-preservation
// contract NodesOccupied callers rely on (status equality checks key on
// the rendered field; non-determinism would churn LastTransitionTime).
func TestUniqueNodes(t *testing.T) {
	pods := []*corev1.Pod{
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{NodeName: "b"}},
		{Spec: corev1.PodSpec{NodeName: "a"}},
		{Spec: corev1.PodSpec{}}, // unscheduled — skipped
	}
	got := workload.UniqueNodes(pods)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("UniqueNodes: got %v want [a b]", got)
	}
	if got := workload.UniqueNodes(nil); got != nil {
		t.Errorf("nil input should return nil; got %v", got)
	}
}

// TestInstanceMeetsThreshold pins the surge-tolerant classifier: the
// canonical pods always count when their satisfying count meets the
// desired threshold, even if PodCount briefly exceeds desired during a
// surge.
func TestInstanceMeetsThreshold(t *testing.T) {
	tests := []struct {
		name     string
		observed int32
		sat      int32
		desired  int32
		want     bool
	}{
		{name: "all observed satisfying (no plan info)", observed: 2, sat: 2, desired: 0, want: true},
		{name: "fewer observed satisfying than total (no plan info)", observed: 2, sat: 1, desired: 0, want: false},
		{name: "no pods observed", observed: 0, sat: 0, desired: 1, want: false},
		{name: "surge: satisfying < observed but >= desired", observed: 2, sat: 1, desired: 1, want: true},
		{name: "surge: satisfying matches desired exactly", observed: 3, sat: 2, desired: 2, want: true},
		{name: "surge: satisfying short of desired", observed: 3, sat: 1, desired: 2, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workload.InstanceMeetsThreshold(tt.observed, tt.sat, tt.desired); got != tt.want {
				t.Errorf("InstanceMeetsThreshold(observed=%d sat=%d desired=%d): got %v want %v",
					tt.observed, tt.sat, tt.desired, got, tt.want)
			}
		})
	}
}

// TestDesiredFor pins the lookup-with-fallback semantic: present in the
// map → use the value; absent (or nil map) → fall back to observed.
func TestDesiredFor(t *testing.T) {
	m := map[int32]int32{0: 2, 1: 3}
	if got := workload.DesiredFor(m, 0, 7); got != 2 {
		t.Errorf("present: got %d want 2", got)
	}
	if got := workload.DesiredFor(m, 99, 7); got != 7 {
		t.Errorf("absent: got %d want fallback 7", got)
	}
	if got := workload.DesiredFor(nil, 0, 7); got != 7 {
		t.Errorf("nil map: got %d want fallback 7", got)
	}
}

// TestRolloutComplete pins the CurrentRevision promotion gate: every
// Instance must be Phase=Ready AND on the target revision; any deviation
// holds CurrentRevision in place.
func TestRolloutComplete(t *testing.T) {
	allReady := []workload.InstanceStatus{
		{Phase: workload.InstancePhaseReady, RunningRevision: "rev"},
		{Phase: workload.InstancePhaseReady, RunningRevision: "rev"},
	}
	if !workload.RolloutComplete(allReady, "rev") {
		t.Errorf("all Ready on rev should be complete")
	}
	oneCreating := []workload.InstanceStatus{
		{Phase: workload.InstancePhaseCreating, RunningRevision: "rev"},
		{Phase: workload.InstancePhaseReady, RunningRevision: "rev"},
	}
	if workload.RolloutComplete(oneCreating, "rev") {
		t.Errorf("Phase=Creating should keep rollout incomplete")
	}
	stale := []workload.InstanceStatus{
		{Phase: workload.InstancePhaseReady, RunningRevision: "old"},
	}
	if workload.RolloutComplete(stale, "rev") {
		t.Errorf("stale RunningRevision should keep rollout incomplete")
	}
	if workload.RolloutComplete(nil, "rev") {
		t.Errorf("empty instance list should not be complete")
	}
}

// TestPodStuckPullFailure pins the wait-window + reason classifier:
// fires only on terminal kubelet waiting reasons, after grace, and
// not on freshly-created pods (which legitimately pass through
// transient ContainerCreating).
func TestPodStuckPullFailure(t *testing.T) {
	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	transient := corev1.ContainerStateWaiting{Reason: "ContainerCreating"}

	cases := []struct {
		name       string
		pod        *corev1.Pod
		wantStuck  bool
		wantReason string
	}{
		{
			name:      "nil pod returns (false, '')",
			pod:       nil,
			wantStuck: false,
		},
		{
			name: "pod with empty CreationTimestamp returns (false, '')",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck: false,
		},
		{
			name: "within grace: not stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck: false,
		},
		{
			name: "past grace, terminal reason: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck:  true,
			wantReason: "ImagePullBackOff",
		},
		{
			name: "past grace, transient reason: not stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &transient}}},
				},
			},
			wantStuck: false,
		},
		{
			name: "init container stuck counts too",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
				},
			},
			wantStuck:  true,
			wantReason: "ImagePullBackOff",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			stuck, reason := workload.PodStuckPullFailure(tt.pod, now, 2*time.Second)
			if stuck != tt.wantStuck {
				t.Errorf("stuck: got %v want %v", stuck, tt.wantStuck)
			}
			if reason != tt.wantReason {
				t.Errorf("reason: got %q want %q", reason, tt.wantReason)
			}
		})
	}
}

// TestFirstStuckPodForInstance pins the per-Instance escalator probe:
// skips nil pods + pods marked for deletion + non-stuck pods, returns
// the first eligible stuck pod + reason.
func TestFirstStuckPodForInstance(t *testing.T) {
	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	mkPod := func(name string, age time.Duration, deleting bool, reason *corev1.ContainerStateWaiting) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.NewTime(now.Add(-age)),
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: reason}}},
			},
		}
		if deleting {
			t := metav1.NewTime(now)
			p.ObjectMeta.DeletionTimestamp = &t
		}
		return p
	}

	pods := []*corev1.Pod{
		nil, // skipped
		mkPod("deleting", 10*time.Second, true, &terminal), // skipped (deletion)
		mkPod("ok", 10*time.Second, false, nil),            // skipped (no Waiting)
		mkPod("stuck", 10*time.Second, false, &terminal),   // first eligible
	}
	got, reason := workload.FirstStuckPodForInstance(pods, now, 2*time.Second)
	if got == nil || got.Name != "stuck" {
		t.Errorf("got %+v want pod 'stuck'", got)
	}
	if reason != "ImagePullBackOff" {
		t.Errorf("reason: got %q want ImagePullBackOff", reason)
	}

	// No stuck pods: returns (nil, "").
	none, noneReason := workload.FirstStuckPodForInstance(
		[]*corev1.Pod{mkPod("ok1", 10*time.Second, false, nil)},
		now, 2*time.Second,
	)
	if none != nil || noneReason != "" {
		t.Errorf("no stuck pods: got (%v, %q) want (nil, '')", none, noneReason)
	}
}

// TestHasWedgedPodAgainstCurrent pins the wedged-pod recovery probe:
// disagreement between pod's revision-hash label and currentHash flags
// the wedge; empty currentHash is the no-rollout-ever guard that
// suppresses false fires.
func TestHasWedgedPodAgainstCurrent(t *testing.T) {
	label := "ome.io/revision-hash"
	mk := func(hash string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{label: hash}}}
	}

	cases := []struct {
		name        string
		pods        []*corev1.Pod
		currentHash string
		labelKey    string
		want        bool
	}{
		{name: "empty currentHash never flags", pods: []*corev1.Pod{mk("anything")}, currentHash: "", labelKey: label, want: false},
		{name: "empty labelKey never flags", pods: []*corev1.Pod{mk("anything")}, currentHash: "x", labelKey: "", want: false},
		{name: "all pods match", pods: []*corev1.Pod{mk("x"), mk("x")}, currentHash: "x", labelKey: label, want: false},
		{name: "one pod disagrees", pods: []*corev1.Pod{mk("x"), mk("y")}, currentHash: "x", labelKey: label, want: true},
		{name: "pods missing label are skipped", pods: []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{}}}, currentHash: "x", labelKey: label, want: false},
		{name: "nil pods are skipped", pods: []*corev1.Pod{nil}, currentHash: "x", labelKey: label, want: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := workload.HasWedgedPodAgainstCurrent(tt.pods, tt.currentHash, tt.labelKey); got != tt.want {
				t.Errorf("got %v want %v", got, tt.want)
			}
		})
	}
}

// TestShouldCheckForStuckPods pins the two-path qualifier: transient
// phase + Operation qualifies; wedged-pod (label-hash disagreement)
// qualifies; Failed / Deleting never qualify; empty currentHash
// suppresses the wedged branch.
func TestShouldCheckForStuckPods(t *testing.T) {
	label := "ome.io/revision-hash"
	podWithHash := func(hash string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{label: hash}}}
	}

	cases := []struct {
		name        string
		status      *workload.InstanceStatus
		pods        []*corev1.Pod
		currentHash string
		want        bool
	}{
		{name: "nil status", status: nil, want: false},
		{name: "Failed never qualifies",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseFailed},
			want:   false,
		},
		{name: "Deleting never qualifies",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseDeleting},
			want:   false,
		},
		{name: "Creating + Operation qualifies",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseCreating, Operation: &workload.InstanceOperation{}},
			want:   true,
		},
		{name: "Updating + Operation qualifies",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseUpdating, Operation: &workload.InstanceOperation{}},
			want:   true,
		},
		{name: "Restarting + Operation qualifies",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseRestarting, Operation: &workload.InstanceOperation{}},
			want:   true,
		},
		{name: "Migrating + Operation qualifies",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseMigrating, Operation: &workload.InstanceOperation{}},
			want:   true,
		},
		{name: "Creating without Operation falls to wedged-pod check",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseCreating},
			pods:   []*corev1.Pod{podWithHash("x")}, currentHash: "y",
			want: true,
		},
		{name: "Ready + wedged-pod qualifies",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseReady},
			pods:   []*corev1.Pod{podWithHash("y")}, currentHash: "x",
			want: true,
		},
		{name: "Ready + matching pod hash does not qualify",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseReady},
			pods:   []*corev1.Pod{podWithHash("x")}, currentHash: "x",
			want: false,
		},
		{name: "Ready + wedged pod but empty currentHash does not qualify",
			status: &workload.InstanceStatus{Phase: workload.InstancePhaseReady},
			pods:   []*corev1.Pod{podWithHash("y")}, currentHash: "",
			want: false,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := workload.ShouldCheckForStuckPods(tt.status, tt.pods, tt.currentHash, label); got != tt.want {
				t.Errorf("got %v want %v", got, tt.want)
			}
		})
	}
}

// TestEscalateStuckPodFailures_EscalatesAndEmitsEvent pins the
// integration contract for the adapter-agnostic escalator: a wedged
// pod past grace flips the InstanceStatus Phase to Failed via
// MutateInstance and fires WarnInstanceFailed exactly once.
func TestEscalateStuckPodFailures_EscalatesAndEmitsEvent(t *testing.T) {
	prev := workload.StuckPodFailureGracePeriod
	workload.StuckPodFailureGracePeriod = 1 * time.Millisecond
	t.Cleanup(func() { workload.StuckPodFailureGracePeriod = prev })

	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "rev-new"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
		},
	}

	// Local mirror of the per-Instance status the closure mutates.
	insts := []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseReady, RunningRevision: "rev-old"}}
	var eventFired struct {
		idx    int32
		pod    string
		reason string
		count  int
	}
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
		WarnInstanceFailed: func(idx int32, podName, reason string) {
			eventFired.idx = idx
			eventFired.pod = podName
			eventFired.reason = reason
			eventFired.count++
		},
	}

	err := workload.EscalateStuckPodFailures(context.Background(), input, workload.EscalationParams{
		Instances:           insts,
		PodsByIndex:         map[int32][]*corev1.Pod{0: {stuck}},
		CurrentRevisionHash: "rev-old",
		RevisionHashLabel:   "ome.io/revision-hash",
	})
	if err != nil {
		t.Fatalf("EscalateStuckPodFailures: %v", err)
	}

	if insts[0].Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", insts[0].Phase)
	}
	if eventFired.count != 1 {
		t.Errorf("event count: got %d want 1", eventFired.count)
	}
	if eventFired.pod != stuck.Name {
		t.Errorf("event pod: got %q want %q", eventFired.pod, stuck.Name)
	}
	if eventFired.reason != "ImagePullBackOff" {
		t.Errorf("event reason: got %q want ImagePullBackOff", eventFired.reason)
	}
}

// TestEscalateStuckPodFailures_AlreadyFailedNoOp pins the idempotency:
// the escalator must not re-fire on an already-Failed Instance even
// though the wedged pod is still in front of it.
func TestEscalateStuckPodFailures_AlreadyFailedNoOp(t *testing.T) {
	prev := workload.StuckPodFailureGracePeriod
	workload.StuckPodFailureGracePeriod = 1 * time.Millisecond
	t.Cleanup(func() { workload.StuckPodFailureGracePeriod = prev })

	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
			Labels:            map[string]string{"ome.io/revision-hash": "rev-new"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
		},
	}

	insts := []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseFailed}}
	var eventFired int
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, _ int32, _ func(*workload.InstanceStatus) bool) error {
			t.Errorf("MutateInstance must not be called for already-Failed Instance")
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) { eventFired++ },
	}

	err := workload.EscalateStuckPodFailures(context.Background(), input, workload.EscalationParams{
		Instances:           insts,
		PodsByIndex:         map[int32][]*corev1.Pod{0: {stuck}},
		CurrentRevisionHash: "rev-old",
		RevisionHashLabel:   "ome.io/revision-hash",
	})
	if err != nil {
		t.Fatalf("EscalateStuckPodFailures: %v", err)
	}
	if eventFired != 0 {
		t.Errorf("event count: got %d want 0 (already-Failed must not re-fire)", eventFired)
	}
}

// TestEscalateStuckPodFailures_NoWarnCallbackTolerated pins the nil-safe
// contract: a missing WarnInstanceFailed callback must not panic; the
// Phase=Failed mutation still lands.
func TestEscalateStuckPodFailures_NoWarnCallbackTolerated(t *testing.T) {
	prev := workload.StuckPodFailureGracePeriod
	workload.StuckPodFailureGracePeriod = 1 * time.Millisecond
	t.Cleanup(func() { workload.StuckPodFailureGracePeriod = prev })

	now := time.Now()
	terminal := corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &terminal}}},
		},
	}
	insts := []workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseUpdating, Operation: &workload.InstanceOperation{}}}
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			for i := range insts {
				if insts[i].Index == idx {
					mutate(&insts[i])
				}
			}
			return nil
		},
		// WarnInstanceFailed deliberately nil — workload code must tolerate.
	}
	if err := workload.EscalateStuckPodFailures(context.Background(), input, workload.EscalationParams{
		Instances:   insts,
		PodsByIndex: map[int32][]*corev1.Pod{0: {stuck}},
	}); err != nil {
		t.Fatalf("EscalateStuckPodFailures with nil WarnInstanceFailed: %v", err)
	}
	if insts[0].Phase != workload.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (escalation must land even without warn callback)", insts[0].Phase)
	}
}

// TestComputeComponentCounters pins the entry-point aggregation
// contract: counts Replicas off the slice length, runs the four
// surge-tolerant counters, and conditionally fills Updated* when a
// targetRevName is supplied.
func TestComputeComponentCounters(t *testing.T) {
	insts := []workload.InstanceStatus{
		{Index: 0, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, RunningRevision: "rev-a"},
		{Index: 1, PodCount: 1, ReadyPodCount: 0, ServingPodCount: 0, AvailablePodCount: 0, RunningRevision: "rev-b"},
	}
	desired := map[int32]int32{0: 1, 1: 1}

	// No target → Updated* zeroed.
	got := workload.ComputeComponentCounters(insts, desired, "")
	if got.Replicas != 2 {
		t.Errorf("Replicas: got %d want 2", got.Replicas)
	}
	if got.ReadyReplicas != 1 || got.ServingReplicas != 1 || got.AvailableReplicas != 1 {
		t.Errorf("counters: %+v", got)
	}
	if got.UpdatedReplicas != 0 || got.UpdatedReadyReplicas != 0 {
		t.Errorf("Updated* should be zero with empty target: %+v", got)
	}

	// With target=rev-a → only inst-0 is on target, both Updated and UpdatedReady.
	got = workload.ComputeComponentCounters(insts, desired, "rev-a")
	if got.UpdatedReplicas != 1 || got.UpdatedReadyReplicas != 1 {
		t.Errorf("rev-a Updated*: got (%d,%d) want (1,1)", got.UpdatedReplicas, got.UpdatedReadyReplicas)
	}
}
