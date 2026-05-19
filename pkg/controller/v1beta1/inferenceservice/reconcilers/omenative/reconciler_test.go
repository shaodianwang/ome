package omenative

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// TestReconcileComponent_NotImplementedAfterSuccessfulPlan covers the
// happy plan-builder path: ComponentExt is valid + MultiPod=false, so
// BuildPlan succeeds. ReconcileComponent then returns the
// not-implemented sentinel because no operations are wired yet.
func TestReconcileComponent_NotImplementedAfterSuccessfulPlan(t *testing.T) {
	for _, comp := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		t.Run(string(comp), func(t *testing.T) {
			_, err := ReconcileComponent(context.Background(), ReconcileParams{
				Component:    comp,
				ComponentExt: &v1beta1.ComponentExtensionSpec{},
			})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "OMENative") ||
				!strings.Contains(err.Error(), "not implemented") ||
				!strings.Contains(err.Error(), string(comp)) {
				t.Fatalf("unexpected error message: %v", err)
			}
		})
	}
}

// TestReconcileComponent_MultiPodPropagatesPlanError pins that a multi-pod
// dispatch surfaces ErrMultiPodNotYetSupported wrapped in a build-plan
// error, not the not-implemented sentinel.
func TestReconcileComponent_MultiPodPropagatesPlanError(t *testing.T) {
	_, err := ReconcileComponent(context.Background(), ReconcileParams{
		Component:    v1beta1.EngineComponent,
		ComponentExt: &v1beta1.ComponentExtensionSpec{},
		MultiPod:     true,
	})
	if !errors.Is(err, ErrMultiPodNotYetSupported) {
		t.Fatalf("expected ErrMultiPodNotYetSupported in chain, got %v", err)
	}
}
