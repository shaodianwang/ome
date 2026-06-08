package coordination

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// TestPodReadyAndServing covers the serving-readiness predicate the canary
// capacity gate relies on: only Running, non-deleting pods reporting
// PodReady=True count as serving.
func TestPodReadyAndServing(t *testing.T) {
	condTrue, condFalse := corev1.ConditionTrue, corev1.ConditionFalse
	mk := func(phase corev1.PodPhase, cond *corev1.ConditionStatus, deleting bool) *corev1.Pod {
		p := &corev1.Pod{}
		p.Status.Phase = phase
		if cond != nil {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: *cond}}
		}
		if deleting {
			p.DeletionTimestamp = &metav1.Time{}
		}
		return p
	}
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"running + ready", mk(corev1.PodRunning, &condTrue, false), true},
		{"running + ready=false", mk(corev1.PodRunning, &condFalse, false), false},
		{"running + no ready condition", mk(corev1.PodRunning, nil, false), false},
		{"pending (ContainerCreating)", mk(corev1.PodPending, nil, false), false},
		{"running + ready but terminating", mk(corev1.PodRunning, &condTrue, true), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := podReadyAndServing(tc.pod); got != tc.want {
				t.Errorf("podReadyAndServing(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestObservePerRevisionPods_TotalVsReady is the regression guard for the canary
// premature-cutover bug: total counts every pod carrying a revision hash, but the
// READY map excludes pods that merely exist (ContainerCreating/Pending). The
// canary capacity gate consumes the ready map, so it never shifts traffic / cuts
// over before the new revision is actually serving.
func TestObservePerRevisionPods_TotalVsReady(t *testing.T) {
	isvc := testReconcilerISVC()
	const stable, canary = "stablehash", "canaryhash"
	objs := []runtime.Object{
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 0, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, stable, 1, true),
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 2, true),  // serving
		buildReadyPod(isvc, v1beta1.EngineComponent, canary, 3, false), // exists, not serving yet
	}
	total, ready, err := observePerRevisionPods(context.Background(), testClient(objs...), isvc, []v1beta1.ComponentType{v1beta1.EngineComponent})
	if err != nil {
		t.Fatalf("observePerRevisionPods: %v", err)
	}
	eng := v1beta1.EngineComponent
	if total[eng][canary] != 2 {
		t.Errorf("total[canary] = %d, want 2 (both canary pods exist)", total[eng][canary])
	}
	if ready[eng][canary] != 1 {
		t.Errorf("ready[canary] = %d, want 1 (the not-yet-serving pod must be excluded)", ready[eng][canary])
	}
	if total[eng][stable] != 2 || ready[eng][stable] != 2 {
		t.Errorf("stable total/ready = %d/%d, want 2/2", total[eng][stable], ready[eng][stable])
	}
}

// buildReadyPod is buildPod with a serving status set (serving=true → Running +
// PodReady=True) or a not-yet-serving status (serving=false → Pending), for the
// total-vs-ready accounting test.
func buildReadyPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int, serving bool) runtime.Object {
	p := buildPod(isvc, component, revisionHash, idx).(*corev1.Pod)
	if serving {
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	} else {
		p.Status.Phase = corev1.PodPending
	}
	return p
}
