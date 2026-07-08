package status

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func extMaxUnavail(s string) *v1beta1.ComponentExtensionSpec {
	v := intstr.FromString(s)
	return &v1beta1.ComponentExtensionSpec{MaxUnavailable: &v}
}

func extMinAvail(i int) *v1beta1.ComponentExtensionSpec {
	v := intstr.FromInt(i)
	return &v1beta1.ComponentExtensionSpec{MinAvailable: &v}
}

func extRolloutMaxUnavail(s string) *v1beta1.ComponentExtensionSpec {
	v := intstr.FromString(s)
	return &v1beta1.ComponentExtensionSpec{
		Lifecycle: &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &v},
			},
		},
	}
}

// TestTopLevelComponentReadyFromLifecycle locks the availability-based readiness
// contract: Ready when serving Instances are at or above the Component's
// disruption floor (MinAvailable/MaxUnavailable, else the rolling-update budget),
// not the old strict all-Instances-ready equality — so a Component serving within
// budget (e.g. 9/10) reports Ready instead of flapping to NotReady.
func TestTopLevelComponentReadyFromLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		component  v1beta1.ComponentType
		cs         *v1beta1.LifecycleStatus
		ext        *v1beta1.ComponentExtensionSpec
		wantType   apis.ConditionType
		wantStatus corev1.ConditionStatus
		wantReason string
		wantNil    bool
	}{
		{
			name:       "engine fully ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 2, ServingReplicas: 2},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "engine partial, no budget: strict → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 1, ServingReplicas: 1},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "engine 9/10 within 25% maxUnavailable → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 9, ServingReplicas: 9},
			ext:        extMaxUnavail("25%"),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			name:       "engine 6/10 below 25% maxUnavailable → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 6, ServingReplicas: 6},
			ext:        extMaxUnavail("25%"),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "engine minAvailable met → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 8, ServingReplicas: 8},
			ext:        extMinAvail(8),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			name:       "engine minAvailable not met → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 7, ServingReplicas: 7},
			ext:        extMinAvail(8),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "engine rollout-budget fallback (25%) → ready at 3/4",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 4, ReadyReplicas: 3, ServingReplicas: 3},
			ext:        extRolloutMaxUnavail("25%"),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
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
			name:       "decoder fully ready",
			component:  v1beta1.DecoderComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 3, ReadyReplicas: 3, ServingReplicas: 3},
			wantType:   v1beta1.DecoderReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "router fully ready",
			component:  v1beta1.RouterComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1},
			wantType:   v1beta1.RouterReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:      "unknown component returns nil",
			component: v1beta1.ComponentType("predictor"),
			cs:        &v1beta1.LifecycleStatus{Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1},
			wantNil:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TopLevelComponentReadyFromLifecycle(tt.component, tt.cs, tt.ext)
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
