package executor

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	perflabels "github.com/ome-projects/ome/tests/perf/inferenceservice/runner/labels"
)

func TestReadyDelay(t *testing.T) {
	cases := []struct {
		name  string
		label string
		want  time.Duration
	}{
		{"absent", "", 0},
		{"zero", "0", 0},
		{"valid", "2500", 2500 * time.Millisecond},
		{"garbage", "not-a-number", 0},
		{"negative", "-5", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{}}
			if tc.label != "" {
				dep.Labels = map[string]string{perflabels.ReadyAfterMsLabel: tc.label}
			}
			if got := readyDelay(dep); got != tc.want {
				t.Fatalf("readyDelay = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeploymentAvailable(t *testing.T) {
	makeDep := func(ready, avail int32, cond corev1.ConditionStatus) *appsv1.Deployment {
		return &appsv1.Deployment{
			Status: appsv1.DeploymentStatus{
				ReadyReplicas:     ready,
				AvailableReplicas: avail,
				Conditions: []appsv1.DeploymentCondition{{
					Type:   appsv1.DeploymentAvailable,
					Status: cond,
				}},
			},
		}
	}

	if !deploymentAvailable(makeDep(3, 3, corev1.ConditionTrue), 3) {
		t.Errorf("expected available when ready==avail==desired and condition True")
	}
	if deploymentAvailable(makeDep(2, 3, corev1.ConditionTrue), 3) {
		t.Errorf("expected NOT available when ready < desired")
	}
	if deploymentAvailable(makeDep(3, 3, corev1.ConditionFalse), 3) {
		t.Errorf("expected NOT available when condition False")
	}
	// No condition at all → not available.
	if deploymentAvailable(&appsv1.Deployment{Status: appsv1.DeploymentStatus{ReadyReplicas: 1, AvailableReplicas: 1}}, 1) {
		t.Errorf("expected NOT available when Available condition is absent")
	}
}

func TestMarkAvailable(t *testing.T) {
	r := &Reconciler{}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 7},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(4))},
	}
	r.markAvailable(dep, 4)

	if dep.Status.ReadyReplicas != 4 || dep.Status.AvailableReplicas != 4 || dep.Status.UpdatedReplicas != 4 {
		t.Fatalf("replica counters not all 4: %+v", dep.Status)
	}
	if dep.Status.ObservedGeneration != 7 {
		t.Fatalf("ObservedGeneration = %d, want 7", dep.Status.ObservedGeneration)
	}
	if !deploymentAvailable(dep, 4) {
		t.Fatalf("markAvailable did not make the deployment available: %+v", dep.Status)
	}
	// markAvailable must be idempotent: a second call leaves a single
	// Available condition, not two.
	r.markAvailable(dep, 4)
	count := 0
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one Available condition after two markAvailable calls, got %d", count)
	}
}

func TestSeenClockSince(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := newSeenClock(func() time.Time { return now })

	// First observation records "now"; elapsed is zero.
	if d := clock.since("a/b"); d != 0 {
		t.Fatalf("first since = %v, want 0", d)
	}
	// Advance the clock; elapsed grows from the first-seen instant.
	now = now.Add(3 * time.Second)
	if d := clock.since("a/b"); d != 3*time.Second {
		t.Fatalf("since after 3s = %v, want 3s", d)
	}
	// forget resets the entry so the next since re-anchors at the new now.
	clock.forget("a/b")
	now = now.Add(10 * time.Second)
	if d := clock.since("a/b"); d != 0 {
		t.Fatalf("since after forget = %v, want 0", d)
	}
}
