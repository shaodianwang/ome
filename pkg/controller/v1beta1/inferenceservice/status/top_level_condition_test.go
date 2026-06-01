package status

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// TestTopLevelComponentReadyFromLifecycle locks the contract that
// AggregateAndWriteStatus relies on to flip EngineReady / DecoderReady /
// RouterReady. Without it, the ingress reconciler and `kubectl wait
// --for=condition=Ready` perpetually see False/Initializing — even when
// pods are healthy and serving.
func TestTopLevelComponentReadyFromLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		component  v1beta1.ComponentType
		cs         *v1beta1.LifecycleStatus
		wantType   apis.ConditionType
		wantStatus corev1.ConditionStatus
		wantReason string
		wantNil    bool
	}{
		{
			name:       "engine ready: ReadyReplicas == Replicas",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 2},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "engine partial: 1/2 ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 1},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "NotAllReady",
		},
		{
			name:       "engine zero replicas",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 0},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "NoReplicas",
		},
		{
			name:       "engine nil status",
			component:  v1beta1.EngineComponent,
			cs:         nil,
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "NoReplicas",
		},
		{
			name:       "decoder ready",
			component:  v1beta1.DecoderComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 3, ReadyReplicas: 3},
			wantType:   v1beta1.DecoderReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "router ready",
			component:  v1beta1.RouterComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 1, ReadyReplicas: 1},
			wantType:   v1beta1.RouterReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:      "unknown component returns nil",
			component: v1beta1.ComponentType("predictor"),
			cs:        &v1beta1.LifecycleStatus{Replicas: 1, ReadyReplicas: 1},
			wantNil:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TopLevelComponentReadyFromLifecycle(tt.component, tt.cs)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("want nil condition, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("want condition, got nil")
			}
			if got.Type != tt.wantType {
				t.Errorf("Type: got %q want %q", got.Type, tt.wantType)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status: got %q want %q", got.Status, tt.wantStatus)
			}
			if got.Reason != tt.wantReason {
				t.Errorf("Reason: got %q want %q", got.Reason, tt.wantReason)
			}
		})
	}
}
