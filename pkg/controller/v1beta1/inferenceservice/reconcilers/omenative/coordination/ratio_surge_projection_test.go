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
			Rollout: &v1beta1.RolloutSpec{Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: int32Ptr(tol),
					},
				}},
			}},
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

// TestCheckRatio_SurgeVsDrainProjection_SymmetricBand pins that a
// symmetric 4:4 RatioBalanced pair (tol=25% → band [0.75, 1.25]) is not
// deadlocked in EITHER direction:
//   - surge (+1) projects 5/4 = 1.25, in band → allowed (#401
//     projection-sign fix);
//   - drain (-1) projects 4/3 = 1.333, which the STRICT band rejects, but
//     the Part-2 discrete-granularity tiebreaker allows one rounding-error
//     pod from a balanced baseline (1.333 <= 2x band 1.5) → allowed.
//
// Pre-#401 the code hardcoded -1 for ALL strategies, so even a
// SurgeThenDrain roll hit the drain math and BOTH members deadlocked at
// the start (the gate-projection bug exposed by activating the gate on the default
// IR-managed path). Asymmetric large skews still stay denied — see
// ratio_drain_tiebreaker_test.go.
func TestCheckRatio_SurgeVsDrainProjection_SymmetricBand(t *testing.T) {
	isvc := mkSymmetricRatioFixture(25)
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)

	if allowed, reason := ctx.CheckRatio(0, +1); !allowed {
		t.Errorf("surge projection (+1) on symmetric 4:4 tol=25%% must be allowed (5/4=1.25 <= 1.25); got denied: %s", reason)
	}
	if allowed, reason := ctx.CheckRatio(0, -1); !allowed {
		t.Errorf("drain (-1) on symmetric 4:4 tol=25%% must be allowed by the discrete-granularity tiebreaker (1.333 <= 2x band 1.5); got denied: %s", reason)
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
	// (RecreatePod on a symmetric pair is now also cleared by the Part-2
	// tiebreaker — see TestEvaluateUpdateGate_RecreatePodSymmetricCleared.)
}

// TestEvaluateSurge_SurgeTiebreaker pins the surge-side tiebreaker. When the
// band is narrower than one whole pod, a surge-first roll deadlocks because the
// discrete (N+1):N step exceeds the band. A surge only ADDS capacity (it can
// NEVER starve a peer) and SurgeThenDrain drains it back, so ONE minimal +1
// surge from a balanced full-original baseline is allowed through regardless of
// overshoot magnitude. Only mid-roll / already-skewed / zero-peer baselines
// stay denied (guards (a) full-original + (b) balanced). The overshoot is NOT
// capped: the earlier 2x-band cap permanently wedged the unavoidable minimal
// surge at small N / tight band (N=1 2:1, tol 10% at N=4 5:4) — a roll that can
// only proceed by surging. Playground: cleared shapes p54 (N=3), p52/p56
// (asymmetric), and now p53/p55 (single-pod) + p80/p81/p96 (gangs).
func TestEvaluateSurge_SurgeTiebreaker(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	cases := []struct {
		name        string
		tol         int32
		original    map[v1beta1.ComponentType]int32
		serving     map[v1beta1.ComponentType]int32
		comp        v1beta1.ComponentType
		wantAllowed bool
	}{
		// Cleared — rounding-error overshoot within 2x band:
		{ // p54: 3:3 -> 4:3 = 1.333 <= 2x band 1.5
			name: "N=3 symmetric (ratio_n3)", tol: 25, comp: eng, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 3, dec: 3},
			serving:  map[v1beta1.ComponentType]int32{eng: 3, dec: 3},
		},
		{ // p52: decoder 2->3, 4:3 = 1.333 within orig-2.0 2x band [1.0,3.0]
			name: "asymmetric 4:2, small decoder surges", tol: 25, comp: dec, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
			serving:  map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
		},
		{ // p56: decoder 2->3, 8:3 = 2.667 within orig-4.0 2x band [2.0,6.0]
			name: "asymmetric 8:2, small decoder surges", tol: 25, comp: dec, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 8, dec: 2},
			serving:  map[v1beta1.ComponentType]int32{eng: 8, dec: 2},
		},
		// Now CLEARED — a minimal +1 surge on a balanced full-original
		// baseline is allowed regardless of overshoot magnitude: a surge
		// can't starve a peer and SurgeThenDrain drains it back. The prior
		// 2x-band cap left these as permanent deadlocks, wedging a roll that
		// can ONLY proceed by surging — caught on the cluster as p53/p55
		// (single-pod) and p80/p81/p96 (gangs).
		{ // p55/p81: 1:1 -> 2:1 — N=1 can only roll by surging
			name: "N=1 minimal surge (cleared)", tol: 25, comp: eng, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
			serving:  map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
		},
		{ // p53/p80: 4:4 tol=10 -> 5:4 — band narrower than one pod
			name: "tight tol=10 at N=4 minimal surge (cleared)", tol: 10, comp: eng, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			serving:  map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pacing := v1beta1.CoordinationPacing{
				Type:           v1beta1.CoordinationPacingRatioBalanced,
				RatioTolerance: int32Ptr(tc.tol),
			}
			d := EvaluateSurge(pacing, RatioState{Original: tc.original, Serving: tc.serving}, tc.comp, +1)
			got := d.AllowedSurgeDelta != 0
			if got != tc.wantAllowed {
				t.Errorf("surge allowed=%v want %v (delta=%d, reason=%q)", got, tc.wantAllowed, d.AllowedSurgeDelta, d.Reason)
			}
		})
	}
}

// TestEvaluateSurge_DrainStaysStrictAfterSurgeTiebreaker guards that the
// surge tiebreaker did NOT loosen the DRAIN direction: a -1 drain that
// exceeds 2x band on a small asymmetric side stays denied — a drain is the
// real capacity-shortfall risk the band exists for. (4:2, decoder drains to
// 1 → 4:1 = 4.0 > 2x-band ceiling 3.0.)
func TestEvaluateSurge_DrainStaysStrictAfterSurgeTiebreaker(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: int32Ptr(25),
	}
	d := EvaluateSurge(pacing, RatioState{
		Original: map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
		Serving:  map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
	}, dec, -1)
	if d.AllowedSurgeDelta != 0 {
		t.Errorf("a drain exceeding 2x band (4:1 from orig 2:1) must stay DENIED; the surge tiebreaker must not loosen drains; got allowed (reason=%q)", d.Reason)
	}
}
