package gangpack

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

func TestFactsFromPodGroup(t *testing.T) {
	pg := &schedv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{topologyKeyAnnotation: "nvidia.com/gpu.clique"}},
		Spec:       schedv1alpha1.PodGroupSpec{MinMember: 18},
	}
	if mm, tk := factsFromPodGroup(pg); mm != 18 || tk != "nvidia.com/gpu.clique" {
		t.Fatalf("factsFromPodGroup = %d,%q want 18,nvidia.com/gpu.clique", mm, tk)
	}
	// No topology-key annotation -> empty (resolveGang will treat it unresolvable).
	if mm, tk := factsFromPodGroup(&schedv1alpha1.PodGroup{Spec: schedv1alpha1.PodGroupSpec{MinMember: 4}}); mm != 4 || tk != "" {
		t.Fatalf("factsFromPodGroup(no annotation) = %d,%q want 4,''", mm, tk)
	}
	if mm, tk := factsFromPodGroup(nil); mm != 0 || tk != "" {
		t.Fatalf("factsFromPodGroup(nil) = %d,%q want 0,''", mm, tk)
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

	if mm, tk, ok := r.get("team", "pf"); !ok || mm != 8 || tk != "clique" {
		t.Fatalf("get(team/pf) = %d,%q,%v want 8,clique,true", mm, tk, ok)
	}
	if _, _, ok := r.get("team", "missing"); ok {
		t.Fatal("get(missing) should be not-found")
	}

	// A getPG that returns (nil, nil) is treated as not found, not a panic.
	rNil := &informerReader{getPG: func(string, string) (*schedv1alpha1.PodGroup, error) { return nil, nil }}
	if _, _, ok := rNil.get("x", "y"); ok {
		t.Fatal("nil PodGroup should be not-found")
	}
}
