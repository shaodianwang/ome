package omenative

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// TestReconcileComponent_NilClientRejected pins that the entry function
// short-circuits when Client is missing.
func TestReconcileComponent_NilClientRejected(t *testing.T) {
	_, err := ReconcileComponent(context.Background(), ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "nil client") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestReconcileComponent_MultiPodPropagatesPlanError pins that a multi-pod
// dispatch surfaces ErrMultiPodNotYetSupported wrapped in a build-plan
// error.
func TestReconcileComponent_MultiPodPropagatesPlanError(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc)
	_, err := ReconcileComponent(context.Background(), ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{},
		MultiPod:     true,
		Client:       c,
	})
	if !errors.Is(err, ErrMultiPodNotYetSupported) {
		t.Fatalf("expected ErrMultiPodNotYetSupported in chain, got %v", err)
	}
}

// TestReconcileComponent_DispatchesToCreate pins the wiring: with a valid
// single-pod plan and a fake client, ReconcileComponent reaches Create
// and the empty cluster gets the desired pod.
func TestReconcileComponent_DispatchesToCreate(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc)
	result, err := ReconcileComponent(context.Background(), minimalParams(t, isvc, c))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("expected Requeue (Create is multi-pass), got %+v", result)
	}
}
