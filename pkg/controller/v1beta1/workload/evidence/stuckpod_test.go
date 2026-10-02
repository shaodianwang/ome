package evidence_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/evidence"
)

// TestPodStuckInTerminalWaiting pins the wait-window + reason classifier:
// fires only on terminal kubelet waiting reasons, after grace, and
// not on freshly-created pods (which legitimately pass through
// transient ContainerCreating).
func TestPodStuckInTerminalWaiting(t *testing.T) {
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
		// CrashLoopBackOff is the steady-state for a container
		// that pulls cleanly but exits immediately. kubelet's restart
		// backoff caps at ~5 min/attempt; without the carve-out the
		// rollout would sit at Phase=Updating until the Deadline fired.
		{
			name: "past grace, CrashLoopBackOff: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}},
				},
			},
			wantStuck:  true,
			wantReason: "CrashLoopBackOff",
		},
		// RunContainerError fires on runtime-rejected starts (missing
		// .so, exec-format mismatch). Same permanence as ImagePullBackOff.
		{
			name: "past grace, RunContainerError: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "RunContainerError"}}}},
				},
			},
			wantStuck:  true,
			wantReason: "RunContainerError",
		},
		{
			name: "past grace, CreateContainerError: stuck",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second))},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CreateContainerError"}}}},
				},
			},
			wantStuck:  true,
			wantReason: "CreateContainerError",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			reason, stuck := evidence.PodStuckInTerminalWaiting(tt.pod, now, 2*time.Second)
			if stuck != tt.wantStuck {
				t.Errorf("stuck: got %v want %v", stuck, tt.wantStuck)
			}
			if reason != tt.wantReason {
				t.Errorf("reason: got %q want %q", reason, tt.wantReason)
			}
		})
	}
}

// TestFirstStuckPodForInstance_SkipsDeletingPods pins the per-Instance escalator probe:
// skips nil pods + pods marked for deletion + non-stuck pods, returns
// the first eligible stuck pod + reason.
func TestFirstStuckPodForInstance_SkipsDeletingPods(t *testing.T) {
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
	got, reason := evidence.FirstStuckPodForInstance(pods, now, 2*time.Second)
	if got == nil || got.Name != "stuck" {
		t.Errorf("got %+v want pod 'stuck'", got)
	}
	if reason != "ImagePullBackOff" {
		t.Errorf("reason: got %q want ImagePullBackOff", reason)
	}

	// No stuck pods: returns (nil, "").
	none, noneReason := evidence.FirstStuckPodForInstance(
		[]*corev1.Pod{mkPod("ok1", 10*time.Second, false, nil)},
		now, 2*time.Second,
	)
	if none != nil || noneReason != "" {
		t.Errorf("no stuck pods: got (%v, %q) want (nil, '')", none, noneReason)
	}
}

// TestFirstCrashLoopingPod_ReadsBothFacesOfTheLoop pins the crash-loop
// reader on the shapes a pass can catch a looping container in: parked
// in CrashLoopBackOff, Running again between two crashes with the
// restarts and a non-zero last termination on its status, or just
// exited non-zero and awaiting the restart. A container that restarted
// and is Ready again has recovered; a clean restart proves nothing; a
// pod of another revision is not the attempt's.
func TestFirstCrashLoopingPod_ReadsBothFacesOfTheLoop(t *testing.T) {
	crashed := &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}
	clean := &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}
	podWith := func(name string, cs corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{cs}},
		}
	}
	running := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"parked in CrashLoopBackOff", podWith("a", corev1.ContainerStatus{Name: "main",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}), true},
		{"running between two crashes", podWith("b", corev1.ContainerStatus{Name: "main",
			State: running, RestartCount: 2, LastTerminationState: corev1.ContainerState{Terminated: crashed}}), true},
		{"exited non-zero, restart pending", podWith("c", corev1.ContainerStatus{Name: "main",
			State: corev1.ContainerState{Terminated: crashed}}), true},
		{"restarted and ready again", podWith("d", corev1.ContainerStatus{Name: "main", Ready: true,
			State: running, RestartCount: 2, LastTerminationState: corev1.ContainerState{Terminated: crashed}}), false},
		{"clean restart", podWith("e", corev1.ContainerStatus{Name: "main",
			State: running, RestartCount: 1, LastTerminationState: corev1.ContainerState{Terminated: clean}}), false},
		{"running, never restarted", podWith("f", corev1.ContainerStatus{Name: "main", State: running}), false},
		{"waiting on an image pull", podWith("g", corev1.ContainerStatus{Name: "main",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod, reason := evidence.FirstCrashLoopingPod([]*corev1.Pod{tc.pod}, "")
			if (pod != nil) != tc.want {
				t.Fatalf("crash-looping: got pod=%v reason=%q want found=%v", pod != nil, reason, tc.want)
			}
			if tc.want && reason != evidence.ReasonCrashLoop {
				t.Errorf("reason: got %q want %q", reason, evidence.ReasonCrashLoop)
			}
		})
	}

	// The attempt's scope: a crash on another revision is not its own.
	other := podWith("h", corev1.ContainerStatus{Name: "main",
		State: running, RestartCount: 2, LastTerminationState: corev1.ContainerState{Terminated: crashed}})
	other.Labels = map[string]string{"ome.io/revision-hash": "oldhash"}
	if pod, _ := evidence.FirstCrashLoopingPod([]*corev1.Pod{other}, "own-engine-newhash"); pod != nil {
		t.Errorf("a crash on another revision must not be read as the attempt's: got %s", pod.Name)
	}
	deleting := podWith("i", corev1.ContainerStatus{Name: "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}})
	now := metav1.NewTime(time.Now())
	deleting.DeletionTimestamp = &now
	if pod, _ := evidence.FirstCrashLoopingPod([]*corev1.Pod{deleting}, ""); pod != nil {
		t.Errorf("a pod on its way out is not evidence: got %s", pod.Name)
	}
}
