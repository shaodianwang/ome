package omenative

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestRevisionTemplateMeta_NilStaysNilForBackCompat(t *testing.T) {
	// Empty input -> nil output -> JSON serializes as `"podMeta":null`,
	// matching the pre-PR-D hash shape so existing alpha CRs aren't
	// invalidated on first reconcile.
	cases := []struct {
		name string
		src  *metav1.ObjectMeta
	}{
		{"nil src", nil},
		{"empty labels and annotations", &metav1.ObjectMeta{}},
		{"empty map labels", &metav1.ObjectMeta{Labels: map[string]string{}}},
		{"empty map annotations", &metav1.ObjectMeta{Annotations: map[string]string{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := revisionTemplateMeta(tc.src)
			if got != nil {
				t.Errorf("expected nil for empty input, got %+v", got)
			}
		})
	}
}

func TestRevisionTemplateMeta_CapturesLabelsAndAnnotations(t *testing.T) {
	src := &metav1.ObjectMeta{
		Name:            "engine",
		Namespace:       "prod",
		UID:             "should-be-stripped",
		ResourceVersion: "12345",
		Labels: map[string]string{
			"app":           "llama-70b",
			"runtime":       "sglang",
			"models.ome.io": "Ready",
		},
		Annotations: map[string]string{
			"ome.io/deploymentMode": "OMENative",
		},
	}
	got := revisionTemplateMeta(src)
	if got == nil {
		t.Fatalf("expected non-nil output for src with labels/annotations")
	}
	if got.Name != "" || got.Namespace != "" || got.UID != "" || got.ResourceVersion != "" {
		t.Errorf("operational fields leaked into revision meta: %+v", got)
	}
	if len(got.Labels) != 3 {
		t.Errorf("Labels not captured: %v", got.Labels)
	}
	if got.Annotations["ome.io/deploymentMode"] != "OMENative" {
		t.Errorf("Annotations not captured: %v", got.Annotations)
	}
}

func TestRevisionHash_LabelChangeProducesDifferentHash(t *testing.T) {
	// REV-8: an edit to user template labels must produce a new
	// revision hash so AggregateAndWriteStatus's diff detection
	// fires a rollout.
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	metaV1 := &metav1.ObjectMeta{Labels: map[string]string{"tier": "free"}}
	metaV2 := &metav1.ObjectMeta{Labels: map[string]string{"tier": "premium"}}

	h1, _, err := revisionHash(ps, metaV1, nil)
	if err != nil {
		t.Fatalf("revisionHash v1: %v", err)
	}
	h2, _, err := revisionHash(ps, metaV2, nil)
	if err != nil {
		t.Fatalf("revisionHash v2: %v", err)
	}
	if h1 == h2 {
		t.Errorf("hash unchanged after label edit: %s == %s", h1, h2)
	}
}

func TestRevisionHash_AnnotationChangeProducesDifferentHash(t *testing.T) {
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	metaV1 := &metav1.ObjectMeta{Annotations: map[string]string{"sidecar.istio.io/inject": "false"}}
	metaV2 := &metav1.ObjectMeta{Annotations: map[string]string{"sidecar.istio.io/inject": "true"}}

	h1, _, _ := revisionHash(ps, metaV1, nil)
	h2, _, _ := revisionHash(ps, metaV2, nil)
	if h1 == h2 {
		t.Errorf("hash unchanged after annotation edit: %s == %s", h1, h2)
	}
}

func TestRevisionHash_NoMetaAndEmptyMetaProduceSameHash(t *testing.T) {
	// nil and empty input MUST hash identically so pre-PR-D
	// revisions (computed with no PodMeta) reuse the same hash
	// after this PR lands for ISVCs that don't have template
	// labels/annotations.
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	hNil, _, _ := revisionHash(ps, nil, nil)
	hEmpty, _, _ := revisionHash(ps, &metav1.ObjectMeta{}, nil)
	if hNil != hEmpty {
		t.Errorf("hash should be stable across nil vs empty meta: %s vs %s", hNil, hEmpty)
	}
}

func TestCollectLiveRevisionNames_UnionAcrossInstanceStatuses(t *testing.T) {
	// A5/REV-5: retention must protect revisions referenced by
	// in-flight migrations whose source RunningRevision points to
	// older CRs.
	cs := &v1beta1.OMENativeComponentStatus{
		CurrentRevision: "rev-current",
		UpdateRevision:  "rev-update",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, RunningRevision: "rev-old-source", TargetRevision: ""},
			{Index: 2, RunningRevision: "rev-current", TargetRevision: "rev-update"},
			{Index: 3, RunningRevision: "", TargetRevision: ""},
		},
	}
	got := collectLiveRevisionNames(cs)
	want := map[string]bool{
		"rev-current":    true,
		"rev-update":     true,
		"rev-old-source": true,
	}
	if len(got) != len(want) {
		t.Errorf("count mismatch: got %d want %d (%v)", len(got), len(want), got)
	}
	for _, name := range got {
		if !want[name] {
			t.Errorf("unexpected name in output: %s", name)
		}
	}
}

func TestCollectLiveRevisionNames_HandlesNilStatus(t *testing.T) {
	if got := collectLiveRevisionNames(nil); got != nil {
		t.Errorf("expected nil for nil status, got %v", got)
	}
	if got := collectLiveRevisionNames(&v1beta1.OMENativeComponentStatus{}); got != nil {
		t.Errorf("expected nil for empty status (no live names), got %v", got)
	}
}

func TestCollectLiveRevisionNames_DropsEmpty(t *testing.T) {
	cs := &v1beta1.OMENativeComponentStatus{
		CurrentRevision: "",
		UpdateRevision:  "rev-a",
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, RunningRevision: "", TargetRevision: ""},
		},
	}
	got := collectLiveRevisionNames(cs)
	if len(got) != 1 || got[0] != "rev-a" {
		t.Errorf("expected only [rev-a], got %v", got)
	}
}
