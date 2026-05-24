package coordination

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestEvaluateSurge_NoSurgeRequested(t *testing.T) {
	d := EvaluateSurge(v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced}, RatioState{}, v1beta1.EngineComponent, 0)
	if d.AllowedSurgeDelta != 0 {
		t.Errorf("zero delta: got %d want 0", d.AllowedSurgeDelta)
	}
	if d.SkewRejected {
		t.Errorf("zero delta should not be a rejection")
	}
}

func TestEvaluateSurge_PerComponentPassThrough(t *testing.T) {
	d := EvaluateSurge(v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingPerComponent}, RatioState{}, v1beta1.EngineComponent, 3)
	if d.AllowedSurgeDelta != 3 {
		t.Errorf("PerComponent should pass through: got %d want 3", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_SingleComponentGroupAllowsSurge(t *testing.T) {
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 10},
		NewPods:  map[v1beta1.ComponentType]int32{},
	}
	d := EvaluateSurge(
		v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced},
		state,
		v1beta1.EngineComponent,
		5,
	)
	if d.AllowedSurgeDelta != 5 {
		t.Errorf("single component: got %d want 5", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_2to1RatioWithinTolerance(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
	}
	// engine 40 : decoder 20 = 2:1. 10 new engine + 5 new decoder
	// keeps the ratio exact.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  0,
			v1beta1.DecoderComponent: 5,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 10)
	if d.AllowedSurgeDelta != 10 {
		t.Errorf("balanced 10 engine vs 5 decoder: got %d want 10", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_AlreadySkewedRejectsAnyFurtherSurge(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
	}
	// 40 engine : 20 decoder. The component being surged already
	// has 15 new pods (37.5%) while decoder has only 5 (25%). The
	// skew is 12.5% > 5% band before any new surge. Any further
	// engine surge can only increase the skew, so every back-off
	// step also gets rejected — the function returns zero with
	// SkewRejected=true.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  15,
			v1beta1.DecoderComponent: 5,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if !d.SkewRejected {
		t.Errorf("expected skew rejection: got %+v", d)
	}
	if d.AllowedSurgeDelta != 0 {
		t.Errorf("rejected surge should produce zero delta: got %d", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_BacksOffEvenWhenPartiallyAhead(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
	}
	// 40 engine : 20 decoder. With 10 new engine + 5 new decoder,
	// the current ratio is exact (25% : 25%). Asking for 5 more
	// engine pushes engine to 37.5% vs decoder's 25% — 12.5%
	// skew, rejected. Back off: delta=2 → engine 30% vs 25% = 5%
	// skew, which is at the inclusive band boundary, accepted.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  10,
			v1beta1.DecoderComponent: 5,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if d.AllowedSurgeDelta != 2 {
		t.Errorf("partial back-off: got %d want 2", d.AllowedSurgeDelta)
	}
	if d.SkewRejected {
		t.Errorf("partial back-off should not be flagged as a full rejection: got %+v", d)
	}
}

func TestEvaluateSurge_BacksOffToHighestAllowedDelta(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
	}
	// 40 engine : 20 decoder. No new pods yet. Propose 5 engine —
	// 5/40 = 12.5% skew against decoder's 0%, out of the 5% band.
	// Back off step by step: 2 engine = 5% skew, which is *equal*
	// to the band and so is allowed (band is inclusive). 3 engine
	// would be 7.5%, rejected.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if d.AllowedSurgeDelta != 2 {
		t.Errorf("back-off: got %d want 2", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_3WayRatioIsPairwise(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
	}
	// router 1 : engine 40 : decoder 20. Propose 4 new router pods
	// (fraction = 400%) — should be rejected wholly because router
	// is so small that any surge skews.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.RouterComponent:  1,
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{},
	}
	d := EvaluateSurge(pacing, state, v1beta1.RouterComponent, 4)
	if !d.SkewRejected {
		t.Errorf("router surge should be rejected (out of pairwise band): got %+v", d)
	}
}

func TestEvaluateSurge_ZeroOriginalReplicasIgnored(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
	}
	// One component has zero original replicas — it shouldn't
	// anchor ratio enforcement. Treating it as a valid pair would
	// reject all surges.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent: 10,
			v1beta1.RouterComponent: 0,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 3)
	if d.AllowedSurgeDelta != 3 {
		t.Errorf("zero-original component should not block: got %d want 3", d.AllowedSurgeDelta)
	}
}

func TestSnapshotOriginal(t *testing.T) {
	in := map[v1beta1.ComponentType]int32{
		v1beta1.EngineComponent:  40,
		v1beta1.DecoderComponent: 0,
	}
	out := SnapshotOriginal(in)
	if out[v1beta1.EngineComponent] != 40 {
		t.Errorf("engine: got %d want 40", out[v1beta1.EngineComponent])
	}
	if out[v1beta1.DecoderComponent] != 1 {
		t.Errorf("zero clamped to 1: got %d want 1", out[v1beta1.DecoderComponent])
	}
	// Original input should not be mutated.
	if in[v1beta1.DecoderComponent] != 0 {
		t.Errorf("input mutated: %v", in)
	}
}
