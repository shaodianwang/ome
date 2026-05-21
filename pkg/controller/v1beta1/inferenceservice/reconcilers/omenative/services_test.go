package omenative

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func TestBuildHeadlessService_NilISVC(t *testing.T) {
	if _, err := BuildHeadlessService(nil, v1beta1.EngineComponent); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
}

func TestBuildHeadlessService_Engine(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			UID:       types.UID("uid-1"),
		},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if svc.Name != "llama-70b-engine-headless" {
		t.Errorf("Name: got %q want llama-70b-engine-headless", svc.Name)
	}
	if svc.Namespace != "prod" {
		t.Errorf("Namespace: got %q want prod", svc.Namespace)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("ClusterIP: got %q want %q (none)", svc.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("PublishNotReadyAddresses: want true")
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Type: got %q want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 0 {
		t.Errorf("Ports: got %d want 0 (headless DNS-only service)", len(svc.Spec.Ports))
	}
}

func TestBuildHeadlessService_SelectorAndLabels(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.DecoderComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "decoder",
		labelManagedBy:                        managedByOMENative,
	}
	if diff := cmp.Diff(want, svc.Spec.Selector); diff != "" {
		t.Errorf("Selector (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(want, svc.Labels); diff != "" {
		t.Errorf("Labels (-want +got):\n%s", diff)
	}
}

func TestBuildHeadlessService_OwnerReference(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: types.UID("uid-1")},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(svc.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(svc.OwnerReferences))
	}
	ref := svc.OwnerReferences[0]
	if ref.Name != "llama-70b" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-70b", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
}

func TestBuildHeadlessService_NameVariesWithComponent(t *testing.T) {
	tests := []struct {
		component v1beta1.ComponentType
		wantName  string
	}{
		{v1beta1.EngineComponent, "fleet-engine-headless"},
		{v1beta1.DecoderComponent, "fleet-decoder-headless"},
		{v1beta1.RouterComponent, "fleet-router-headless"},
	}
	for _, tt := range tests {
		t.Run(string(tt.component), func(t *testing.T) {
			svc, err := BuildHeadlessService(&v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "fleet", Namespace: "prod"},
			}, tt.component)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if svc.Name != tt.wantName {
				t.Errorf("got %q want %q", svc.Name, tt.wantName)
			}
		})
	}
}

func TestBuildHeadlessService_SelectorMatchesRenderedPodLabels(t *testing.T) {
	// Pins the cross-file invariant: pod labels stamped by Render() are a
	// superset of the headless Service selector, so the Service publishes
	// every OMENative-managed pod of the same Component.
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	podLabelSet := podLabels(isvc.Name, v1beta1.EngineComponent, 0, "default", 1)
	for k, v := range svc.Spec.Selector {
		if got, ok := podLabelSet[k]; !ok || got != v {
			t.Errorf("selector %s=%q not present in pod labels (got %q, ok=%v)", k, v, got, ok)
		}
	}
}
