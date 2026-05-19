package omenative

import (
	"context"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestReconcileComponent_NotImplemented(t *testing.T) {
	for _, comp := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		t.Run(string(comp), func(t *testing.T) {
			_, err := ReconcileComponent(context.Background(), ReconcileParams{Component: comp})
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
