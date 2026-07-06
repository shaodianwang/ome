package gangpack

import (
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

func TestFactsFromPodGroup(t *testing.T) {
	pg := &schedv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{topologyKeyAnnotation: "nvidia.com/gpu.clique"}},
		Spec:       schedv1alpha1.PodGroupSpec{MinMember: 18, ScheduleTimeoutSeconds: ptr.To[int32](120)},
	}
	if mm, tk, to := factsFromPodGroup(pg); mm != 18 || tk != "nvidia.com/gpu.clique" || to != 120*time.Second {
		t.Fatalf("factsFromPodGroup = %d,%q,%v want 18,nvidia.com/gpu.clique,2m", mm, tk, to)
	}
	// No annotation -> empty key; no ScheduleTimeoutSeconds -> default timeout.
	if mm, tk, to := factsFromPodGroup(&schedv1alpha1.PodGroup{Spec: schedv1alpha1.PodGroupSpec{MinMember: 4}}); mm != 4 || tk != "" || to != defaultPermitTimeout {
		t.Fatalf("factsFromPodGroup(no annotation) = %d,%q,%v want 4,'',default", mm, tk, to)
	}
	if mm, tk, to := factsFromPodGroup(nil); mm != 0 || tk != "" || to != defaultPermitTimeout {
		t.Fatalf("factsFromPodGroup(nil) = %d,%q,%v want 0,'',default", mm, tk, to)
	}
}

func TestInformerReaderGet(t *testing.T) {
	pg := &schedv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "pf", Annotations: map[string]string{topologyKeyAnnotation: "clique"}},
		Spec:       schedv1alpha1.PodGroupSpec{MinMember: 8},
	}
	r := &informerReader{getPG: func(ns, name string) (*schedv1alpha1.PodGroup, error) {
		if ns == "team" && name == "pf" {
			return pg, nil
		}
		return nil, errors.New("not found")
	}}

	if mm, tk, _, ok := r.get("team", "pf"); !ok || mm != 8 || tk != "clique" {
		t.Fatalf("get(team/pf) = %d,%q,%v want 8,clique,true", mm, tk, ok)
	}
	if _, _, _, ok := r.get("team", "missing"); ok {
		t.Fatal("get(missing) should be not-found")
	}

	// A getPG that returns (nil, nil) is treated as not found, not a panic.
	rNil := &informerReader{getPG: func(string, string) (*schedv1alpha1.PodGroup, error) { return nil, nil }}
	if _, _, _, ok := rNil.get("x", "y"); ok {
		t.Fatal("nil PodGroup should be not-found")
	}
}
