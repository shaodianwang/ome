package coordination

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// TestEvaluateSurge_DrainTiebreaker pins the Part-2 discrete-granularity
// tiebreaker: a drain-first roll (RecreatePod / in-place) gets exactly ONE
// rounding-error pod through when a tight band otherwise permits zero
// whole-pod moves, but large asymmetric skews stay denied and a second
// pod (from an already-skewed baseline) is refused.
func TestEvaluateSurge_DrainTiebreaker(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: int32Ptr(25), // band [0.75, 1.25]; 2x band [0.5, 1.5]
	}
	cases := []struct {
		name        string
		original    map[v1beta1.ComponentType]int32
		serving     map[v1beta1.ComponentType]int32
		comp        v1beta1.ComponentType
		wantAllowed bool
	}{
		{
			// 4/3 = 1.333 > 1.25 (strict) but <= 1.5 (2x band) from a
			// balanced 4:4 → tiebreaker allows the one pod. Clears p51.
			name:        "symmetric 4:4 drain — tiebreaker allows one pod",
			original:    map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			serving:     map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			comp:        eng,
			wantAllowed: true,
		},
		{
			// 4:1 = 4.0 > 3.0 (2x band on orig 2.0) → too large an overshoot,
			// stays an honest deadlock (operator widens tolerance).
			name:        "asymmetric 4:2 decoder drain — large skew denied",
			original:    map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
			serving:     map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
			comp:        dec,
			wantAllowed: false,
		},
		{
			// Baseline already 3:4 (one pod out) → not in band → tiebreaker
			// refuses, bounding in-flight drains to ONE pod at a time.
			name:        "bounded — second pod from skewed 3:4 baseline denied",
			original:    map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			serving:     map[v1beta1.ComponentType]int32{eng: 3, dec: 4},
			comp:        eng,
			wantAllowed: false,
		},
		{
			// Draining the last pod would zero a side → always refused
			// (mass-drain guard), even from a balanced 1:1 baseline.
			name:        "zero-guard — draining the last pod denied",
			original:    map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
			serving:     map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
			comp:        eng,
			wantAllowed: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := EvaluateSurge(pacing, RatioState{Original: tc.original, Serving: tc.serving}, tc.comp, -1)
			gotAllowed := d.AllowedSurgeDelta != 0
			if gotAllowed != tc.wantAllowed {
				t.Errorf("allowed=%v want %v (delta=%d, skewRejected=%v, reason=%q)",
					gotAllowed, tc.wantAllowed, d.AllowedSurgeDelta, d.SkewRejected, d.Reason)
			}
		})
	}
}

// TestEvaluateUpdateGate_RecreatePodSymmetricCleared is the end-to-end
// Part-2 regression for p51: a RecreatePod (drain-first) roll on a
// symmetric 4:4 RatioBalanced group, with a MaxUnavailable budget so the
// unavailability gate permits the pod, must now be allowed — the ratio
// tiebreaker clears the drain that previously deadlocked.
func TestEvaluateUpdateGate_RecreatePodSymmetricCleared(t *testing.T) {
	isvc := mkSymmetricRatioFixture(25)
	mu := intstr.FromInt(3)
	isvc.Spec.RolloutCoordination.Groups[0].Pacing.MaxUnavailable = &mu

	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.EngineComponent, nil,
		workloadtypes.UpdateStrategyRecreatePod, 0, 0); !allowed {
		t.Errorf("RecreatePod on symmetric 4:4 (MaxUnavailable=3) must be allowed via the tiebreaker; got denied: %s", reason)
	}
}
