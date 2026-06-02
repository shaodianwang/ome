package coordination

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// mkSymmetricRatioFixture builds a symmetric engine 4 : decoder 4
// RatioBalanced group with an ObservedRatio snapshot of 4:4 and both
// Components fully serving (4/4). This is the p32/p42 deadlock shape:
// a 1:1 ratio where, at tolerance 25%, the discrete pod granularity
// leaves no room for a single -1 drain.
func mkSymmetricRatioFixture(tol int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: int32Ptr(tol),
					},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
			},
		},
	}
}

// TestCheckRatio_SurgeVsDrainProjection_SymmetricBand pins the
// projection-sign fix at the gate level. Symmetric 4:4, tol=25% → band
// [0.75, 1.25]:
//   - a drain (-1) projects max/min = 4/3 = 1.333 > 1.25 → correctly
//     denied (a genuine drain on a 1:1 pair has no slack at tol 25%);
//   - a surge (+1) projects 5/4 = 1.25, in band → MUST be allowed.
//
// The pre-fix code hardcoded -1 for ALL strategies, so a SurgeThenDrain
// roll hit the drain math and BOTH members were denied at the start →
// the permanent deadlock #400 exposed by activating the gate on the
// default IR-managed path.
func TestCheckRatio_SurgeVsDrainProjection_SymmetricBand(t *testing.T) {
	isvc := mkSymmetricRatioFixture(25)
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)

	if allowed, _ := ctx.CheckRatio(0, -1); allowed {
		t.Errorf("drain projection (-1) on symmetric 4:4 tol=25%% should be denied (4/3=1.333 > 1.25)")
	}
	if allowed, reason := ctx.CheckRatio(0, +1); !allowed {
		t.Errorf("surge projection (+1) on symmetric 4:4 tol=25%% must be allowed (5/4=1.25 <= 1.25); got denied: %s", reason)
	}
}

// TestEvaluateUpdateGate_RatioBalancedSurgeNotDeadlocked is the
// end-to-end regression for the p32/p42 deadlock: a SurgeThenDrain roll
// on a symmetric 4:4 RatioBalanced group must NOT be blocked by the gate
// stack (it was, pre-fix, because CheckRatio projected -1 for it).
func TestEvaluateUpdateGate_RatioBalancedSurgeNotDeadlocked(t *testing.T) {
	isvc := mkSymmetricRatioFixture(25)

	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.EngineComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Errorf("SurgeThenDrain RatioBalanced 4:4 must be allowed (no deadlock); got denied: %s", reason)
	}

	// RecreatePod genuinely drops capacity (-1) first, so on a symmetric
	// pair at tol=25%% it is still gated. This documents the boundary the
	// Part-2 (discrete-granularity tiebreaker) follow-up addresses; it is
	// intentionally NOT fixed by the projection-sign change.
	if allowed, _ := EvaluateUpdateGate(isvc, v1beta1.EngineComponent, nil,
		workloadtypes.UpdateStrategyRecreatePod, 0, 0); allowed {
		t.Errorf("RecreatePod on symmetric 4:4 tol=25%% is expected to still be gated (Part-2 follow-up); got allowed")
	}
}
