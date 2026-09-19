package query

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var promoteNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// promotePod builds a live pod carrying the requested conditions; a zero
// readyAt leaves the PodReady condition off entirely.
func promotePod(name string, containersReady bool, readyAt time.Time) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if containersReady {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
		})
	}
	if !readyAt.IsZero() {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt),
		})
	}
	return pod
}

// TestPodSetPromotable covers the one bar every Ready stamp shares:
// ContainersReady alone is below it, PodReady past the minReadySeconds
// window clears it, and a set still inside the window reports the
// remainder so the caller wakes on it instead of polling.
//
// Cells: instance/Create/CreatePods/pod.containersReady[source]/before, instance/Create/CreatePods/pod.ready[source]/before
func TestPodSetPromotable(t *testing.T) {
	cases := []struct {
		name            string
		pods            []*corev1.Pod
		minReadySeconds int32
		extra           []func(*corev1.Pod) bool
		want            bool
		wantWait        time.Duration
	}{
		{
			// Nothing running proves nothing: an Instance whose pods are all
			// gone must never be stamped Ready by a vacuously clear bar.
			name: "empty set is never promotable",
			want: false,
		},
		{
			name:            "empty set is never promotable under a window either",
			pods:            []*corev1.Pod{},
			minReadySeconds: 20,
			want:            false,
		},
		{
			name: "ContainersReady without PodReady is below the bar",
			pods: []*corev1.Pod{promotePod("a", true, time.Time{})},
			want: false,
		},
		{
			name: "PodReady with no window clears the bar",
			pods: []*corev1.Pod{promotePod("a", true, promoteNow.Add(-time.Second))},
			want: true,
		},
		{
			name:            "inside the window reports the remainder",
			pods:            []*corev1.Pod{promotePod("a", true, promoteNow.Add(-5*time.Second))},
			minReadySeconds: 20,
			want:            false,
			wantWait:        15 * time.Second,
		},
		{
			name: "the remainder is the last pod to clear the window",
			pods: []*corev1.Pod{
				promotePod("a", true, promoteNow.Add(-15*time.Second)),
				promotePod("b", true, promoteNow.Add(-5*time.Second)),
			},
			minReadySeconds: 20,
			want:            false,
			wantWait:        15 * time.Second,
		},
		{
			name:            "elapsed window clears the bar at the exact boundary",
			pods:            []*corev1.Pod{promotePod("a", true, promoteNow.Add(-20*time.Second))},
			minReadySeconds: 20,
			want:            true,
		},
		{
			name: "one pod short of PodReady holds the whole set with no remainder",
			pods: []*corev1.Pod{
				promotePod("a", true, promoteNow.Add(-time.Second)),
				promotePod("b", true, time.Time{}),
			},
			minReadySeconds: 20,
			want:            false,
		},
		{
			name: "a terminal pod is never promotable",
			pods: []*corev1.Pod{func() *corev1.Pod {
				p := promotePod("a", true, promoteNow.Add(-time.Second))
				p.Status.Phase = corev1.PodSucceeded
				return p
			}()},
			want: false,
		},
		{
			name:  "an unsatisfied extra predicate holds a PodReady set",
			pods:  []*corev1.Pod{promotePod("a", true, promoteNow.Add(-time.Second))},
			extra: []func(*corev1.Pod) bool{func(*corev1.Pod) bool { return false }},
			want:  false,
		},
		{
			name:  "a satisfied extra predicate leaves the bar to readiness",
			pods:  []*corev1.Pod{promotePod("a", true, promoteNow.Add(-time.Second))},
			extra: []func(*corev1.Pod) bool{func(*corev1.Pod) bool { return true }},
			want:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, wait := PodSetPromotable(tc.pods, tc.minReadySeconds, promoteNow, tc.extra...)
			if got != tc.want {
				t.Fatalf("promotable: got %t, want %t", got, tc.want)
			}
			if wait != tc.wantWait {
				t.Fatalf("wait: got %s, want %s", wait, tc.wantWait)
			}
		})
	}
}
