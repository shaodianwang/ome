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

func extMinAvailPercent(s string) *v1beta1.ComponentExtensionSpec {
	v := intstr.FromString(s)
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

func replicas(i int32) *int32 { return &i }

// TestTopLevelComponentReadyFromLifecycle locks the availability-based readiness
// contract: Ready when serving Instances are at or above the Component's
// disruption floor (MinAvailable/MaxUnavailable, else the rolling-update budget),
// not the old strict all-Instances-ready equality — so a Component serving within
// budget (e.g. 9/10) reports Ready instead of flapping to NotReady.
//
// The floor scales against the desired count, so rollout surge cannot raise it.
func TestTopLevelComponentReadyFromLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		component  v1beta1.ComponentType
		cs         *v1beta1.LifecycleStatus
		ext        *v1beta1.ComponentExtensionSpec
		desired    *int32
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

		// --- rollout surge: the floor must follow `desired`, not the surged live count ---
		{
			// 16 desired surged to 32 live, all 16 serving. Floor is 25% of 16 = 12,
			// not of 32 (= 24).
			name:       "surged 16->32, all desired serving → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 16, ServingReplicas: 16},
			ext:        extMaxUnavail("25%"),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Real unavailability during a surge must still be caught: 8 < floor 12.
			name:       "surged 16->32, serving below desired floor → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 8, ServingReplicas: 8},
			ext:        extMaxUnavail("25%"),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			// Mid-drain: new Instances ready on top of the old set.
			name:       "surged 16->32, 24 serving → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 24, ServingReplicas: 24},
			ext:        extMaxUnavail("25%"),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Converged: surge drained.
			name:       "post-drain 16/16 ready → fully ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 16, ReadyReplicas: 16, ServingReplicas: 16},
			ext:        extMaxUnavail("25%"),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			// Rolling-update budget as the only source (no PDB).
			name:       "surged with rollout-budget fallback → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 8, ReadyReplicas: 4, ServingReplicas: 4},
			ext:        extRolloutMaxUnavail("25%"),
			desired:    replicas(4),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Clamp: floor is 25% of the 8 live, not of the not-yet-real 16.
			name:       "scale-up 8 live of 16 desired → clamped to live floor, ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 8, ReadyReplicas: 6, ServingReplicas: 6},
			ext:        extMaxUnavail("25%"),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// nil desired keeps the old live-count floor.
			name:       "surged with nil desired → legacy live-count floor",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 16, ServingReplicas: 16},
			ext:        extMaxUnavail("25%"),
			desired:    nil,
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "zero desired → falls back to live count",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 9, ServingReplicas: 9},
			ext:        extMaxUnavail("25%"),
			desired:    replicas(0),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Percentage MinAvailable also scales against desired.
			name:       "surged with percentage minAvailable → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 16, ServingReplicas: 16},
			ext:        extMinAvailPercent("75%"),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Router surges too (MaxSurge=25%).
			name:       "router surged 3->4, all desired serving → ready",
			component:  v1beta1.RouterComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 4, ReadyReplicas: 3, ServingReplicas: 3},
			ext:        extMaxUnavail("25%"),
			desired:    replicas(3),
			wantType:   v1beta1.RouterReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TopLevelComponentReadyFromLifecycle(tt.component, tt.cs, tt.ext, tt.desired)
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

// TestSurgeDoesNotInflateAvailabilityFloor pins the reported regression: a
// 16-Instance engine (MaxSurge=100%, MaxUnavailable=25%) surged to 32 while all
// 16 desired Instances served, and reported
// `EngineReady=False InsufficientAvailable "16/32 Instances serving, need 24"`
// for the whole rollout, gating IngressReady behind it. The floor must be 12.
func TestSurgeDoesNotInflateAvailabilityFloor(t *testing.T) {
	cs := &v1beta1.LifecycleStatus{
		Replicas:        32, // 16 desired + 100% surge
		ReadyReplicas:   16,
		ServingReplicas: 16,
		CurrentRevision: "engine-ed3867a3",
		UpdateRevision:  "engine-66ed9807",
	}
	ext := extMaxUnavail("25%")

	got := TopLevelComponentReadyFromLifecycle(v1beta1.EngineComponent, cs, ext, replicas(16))
	if got == nil {
		t.Fatal("want condition, got nil")
	}
	if got.Status != corev1.ConditionTrue {
		t.Errorf("Status: got %q want %q (all 16 desired Instances are serving)", got.Status, corev1.ConditionTrue)
	}
	if got.Reason != "MinimumAvailable" {
		t.Errorf("Reason: got %q want %q", got.Reason, "MinimumAvailable")
	}
	if want := "16/32 Instances serving (min 12)"; got.Message != want {
		t.Errorf("Message: got %q want %q", got.Message, want)
	}
}

func TestFloorBasis(t *testing.T) {
	tests := []struct {
		name    string
		actual  int32
		desired *int32
		want    int32
	}{
		{name: "surge: desired below live count wins", actual: 32, desired: replicas(16), want: 16},
		{name: "steady state: equal", actual: 16, desired: replicas(16), want: 16},
		{name: "scale-up: clamped to live count", actual: 8, desired: replicas(16), want: 8},
		{name: "nil desired falls back to live", actual: 32, desired: nil, want: 32},
		{name: "zero desired falls back to live", actual: 32, desired: replicas(0), want: 32},
		{name: "negative desired falls back to live", actual: 32, desired: replicas(-1), want: 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := floorBasis(tt.actual, tt.desired); got != tt.want {
				t.Errorf("floorBasis(%d, %v) = %d, want %d", tt.actual, tt.desired, got, tt.want)
			}
		})
	}
}
