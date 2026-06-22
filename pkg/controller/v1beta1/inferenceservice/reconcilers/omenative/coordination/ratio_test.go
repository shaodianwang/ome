package coordination

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
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
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
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
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
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

func TestEvaluateSurge_BacksOffWhenLiveRatioWouldExceedBand(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// Live serving ratio (larger/smaller).
	// 40 engine : 20 decoder original → orig_ratio = 2.0,
	// band [1.9, 2.1] at tol=5%.
	// Serving snapshot: engine=10, decoder=5 → live = 2.0 (in band).
	// Surge engine +1 → 11/5 = 2.2 → out of band → reject.
	// Surge decoder +1 → 10/6 = 1.67 → out of band → reject.
	// All deltas rejected at this tight tolerance.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  10,
			v1beta1.DecoderComponent: 5,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if d.AllowedSurgeDelta != 0 || !d.SkewRejected {
		t.Errorf("tight tolerance should reject all engine surges from balanced state: got %+v", d)
	}
}

func TestEvaluateSurge_AllowsSurgeWhenLiveRatioStaysInBand(t *testing.T) {
	tol := int32(20)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// 40 engine : 20 decoder original → orig_ratio = 2.0,
	// band [1.6, 2.4] at tol=20%.
	// Serving: engine=40, decoder=20 → live = 2.0 (in band).
	// Surge engine: +1 → 41/20=2.05 in. +2 → 42/20=2.1 in. +8 → 48/20=2.4 boundary in.
	// +9 → 49/20=2.45 out.
	// Propose 10 → backs off to 8.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 10)
	if d.AllowedSurgeDelta != 8 {
		t.Errorf("partial back-off to band ceiling: got %d want 8 (49/20=2.45 out, 48/20=2.4 boundary)", d.AllowedSurgeDelta)
	}
	if d.SkewRejected {
		t.Errorf("partial back-off should NOT be flagged as full rejection: got %+v", d)
	}
}

func TestEvaluateSurge_RejectsWhenPeerSerivngWouldBeZero(t *testing.T) {
	tol := int32(50)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// Peer's serving count is zero → ratio undefined → any surge of
	// the other component is rejected. This is the mass-drain
	// failure mode the gate must block.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  10,
			v1beta1.DecoderComponent: 0,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if !d.SkewRejected || d.AllowedSurgeDelta != 0 {
		t.Errorf("zero-peer-serving must reject all surges (would compute infinite ratio): got %+v", d)
	}
}

func TestEvaluateSurge_3WayRatioIsPairwise(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
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
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
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

// =============================================================================
// CheckRatioGate
// =============================================================================

func TestCheckRatioGate_NilISVC(t *testing.T) {
	allowed, _ := CheckRatioGate(nil, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("nil ISVC should be allowed (no-op gate)")
	}
}

func TestCheckRatioGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no coord block should be allowed: %s", reason)
	}
}

func TestCheckRatioGate_PerComponentPacingBypasses(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("PerComponent pacing should bypass the gate: %s", reason)
	}
}

func TestCheckRatioGate_FirstReconcileAllowed(t *testing.T) {
	// RatioBalanced configured but no ObservedRatio snapshot yet.
	// First reconcile must allow the surge so SnapshotOriginal can run
	// next pass.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: 5},
				}},
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("first reconcile (no ObservedRatio yet) must allow surge")
	}
}

func TestCheckRatioGate_PDScenarioEngineMustWaitForDecoder(t *testing.T) {
	// Canonical PD: engine 20 pods, decoder 5 pods (4:1, decoder=25%).
	// Tolerance 5%. After engine surges 1 pod (1/20 = 5%) and decoder is
	// still at 0/5 (0%), the next engine surge would push 2/20 = 10%
	// against 0% → 10pp diff > 5pp tolerance → MUST be denied.
	isvc := mkPDFixture(int32Ptr(5))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        20,
				UpdatedReplicas: 1,
			},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				UpdatedReplicas: 0,
			},
		},
	}

	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("engine should be gated when decoder hasn't moved (1/20=5%% vs 0/5=0%%, asking for 2/20=10%%): got allowed=true reason=%s", reason)
	}
}

func TestCheckRatioGate_PDScenarioDecoderAllowedToCatchUp(t *testing.T) {
	// Same PD setup; ask whether decoder can move. decoder going from
	// 0/5 to 1/5 = 20% is a 15pp leap from 5% (engine), beyond 5%
	// tolerance — so the strict band-check rejects decoder too. This is
	// the "ratio convergence freezes" failure mode the operator must
	// address by widening tolerance or restructuring replicas. The gate
	// reports it honestly.
	isvc := mkPDFixture(int32Ptr(5))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        20,
				UpdatedReplicas: 1,
			},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				UpdatedReplicas: 0,
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.DecoderComponent, 0)
	// At tolerance=5, the discrete granularity prevents decoder from
	// moving. The test pins this honest deadlock so a reviewer can see
	// it; widening tolerance fixes it.
	if allowed {
		t.Errorf("expected decoder gated at tolerance=5: discrete granularity (decoder pod = 20%% jump) cannot satisfy band; got allowed=true")
	}
}

func TestCheckRatioGate_PDScenarioWiderToleranceAllowsDrain(t *testing.T) {
	// Wider tolerance lets an engine drain through that tighter tolerance
	// would block. Setup: engine 20, decoder 5 → orig_ratio=4.0,
	// tol=20% → band [3.2, 4.8].
	// Live state: full capacity (engine=20, decoder=5).
	// Engine drain –1 → projected (19, 5), ratio = 19/5 = 3.8 → in band.
	// (Tighter tol=5% → band [3.8, 4.2] → 3.8 sits at boundary, still in;
	// at tol=2% → band [3.92, 4.08] → 3.8 < 3.92 → out → reject. The
	// 20% tolerance test pins the "wider band gives more drain headroom"
	// property the redesign promises.)
	isvc := mkPDFixture(int32Ptr(20))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        20,
				ServingReplicas: 20,
			},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ServingReplicas: 5,
			},
		},
	}
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("expected engine drain allowed at tolerance=20: projected ratio 19/5=3.8 in band [3.2, 4.8]; got reason=%s", reason)
	}
}

func TestCheckRatioGate_SingleComponentGroupBypasses(t *testing.T) {
	// A single-Component group has no peer to ratio-balance against, so the
	// gate bypasses. (v2: single-Component groups can't carry MaintainRatio
	// meaningfully — and rollingUpdate, the MaintainRatio carrier, requires
	// 2+ Components — so a lone Component is a plain blueGreen group.)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("single-component group must bypass the gate")
	}
}

func TestCheckRatioGate_ComponentNotInAnyGroup(t *testing.T) {
	// router not in declared group → no gate, allowed.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: 5},
				}},
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.RouterComponent, 0)
	if !allowed {
		t.Errorf("component outside any coord group must bypass the gate")
	}
}

// TestCheckRatioGate_InFlightProjectionTightensBand pins the
// in-wake-up stale-snapshot fix. status.ServingReplicas reports 30
// (snapshot from the prior reconcile) but the dispatcher has already
// pulled 3 engine pods from rotation this wake-up — projected serving
// is 27. Tolerance 10% on a 30:10 fleet → band [2.7, 3.3]. Without
// the in-flight delta, the gate sees 30/10 = 3.0 → allowed. With
// inFlightDelta=3, it sees (30-3)/10 = 2.7 → at the boundary. The
// caller asks for one more drain; gate must REJECT because
// (27-1)/10 = 2.6 sits outside the band.
func TestCheckRatioGate_InFlightProjectionTightensBand(t *testing.T) {
	tol := int32(10)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        30,
						ServingReplicas: 30,
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        10,
						ServingReplicas: 10,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  30,
							v1beta1.DecoderComponent: 10,
						},
					},
				}},
			},
		},
	}

	// Baseline: no in-flight → projected serving stays at status (30/10=3.0
	// → in band) → allowed.
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0); !allowed {
		t.Fatalf("baseline (0 in-flight) should be allowed: %s", reason)
	}

	// At the band boundary: 3 already drained → projected serving 27,
	// asking for +1 puts us at (27-1)/10 = 2.6 → out of band → MUST reject.
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 3); allowed {
		t.Fatalf("with 3 in-flight, one more drain would cross the band; expected reject, got allowed (reason=%s)", reason)
	}

	// Sanity: at 2 in-flight, (28-1)/10 = 2.7 → exactly the band edge →
	// still allowed (band is inclusive).
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 2); !allowed {
		t.Fatalf("with 2 in-flight, projected 2.7 sits at band edge and should remain allowed: %s", reason)
	}
}

// TestCheckRatioGate_BlueGreenRatioBalancedDoesNotDeadlock pins the
// fix for the BlueGreen + RatioBalanced deadlock observed in KIND tests.
//
// Reproduction shape: 4 engine + 2 decoder, BlueGreen + RatioBalanced
// (tol 25%, band [1.5, 2.5]). The original ObservedRatio anchor must
// reflect the 4:2 fleet — NOT the bogus {1, 1} that SnapshotOriginal
// produced on the very first reconcile (when status.OMENative.Replicas
// hadn't been written yet, so DesiredReplicas defaulted to 0 → clamped
// to 1 by SnapshotOriginal). With the bogus 1:1 anchor, every drain
// projection produced a ratio outside the 0.75-1.25 band derived from
// 1.0 * (1 ± 25%), even though the LIVE 4:2 = 2.0 ratio sat perfectly
// inside the intended 2.0 * (1 ± 25%) = [1.5, 2.5] band.
//
// This test asserts the gate ALLOWS the first drain when Original
// matches the cluster's actual 4:2 shape — the property that broke
// when buildRatioState snapshotted from an empty status.
func TestCheckRatioGate_BlueGreenRatioBalancedDoesNotDeadlock(t *testing.T) {
	tol := int32(25)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        4,
						ServingReplicas: 4,
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 2,
						},
					},
				}},
			},
		},
	}

	// Engine drain (-1): projected serving = {eng: 3, dec: 2}, ratio
	// 3/2 = 1.5, lower band 2.0 * (1 - 0.25) = 1.5 → boundary, in band
	// (inclusive) → MUST be allowed. The deadlock manifested as this
	// gate rejecting EVERY surge because Original was set to {1, 1}
	// rather than {4, 2}.
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0); !allowed {
		t.Fatalf("BlueGreen + RatioBalanced with 4:2 fleet and 25%% tolerance: first engine drain should be allowed (projected 3/2=1.5 sits at band lower edge [1.5, 2.5]); got allowed=false reason=%s", reason)
	}
}

// TestCheckRatioGate_BogusOnePerComponentAnchorReproducesDeadlock pins
// the BAD BEHAVIOR side of the same fix: with the broken {1, 1} anchor
// (what buildRatioState used to produce when status.OMENative.Replicas
// was 0 at snapshot time), the gate rejects every drain on a fleet
// whose live ratio differs from 1:1. This test documents the failure
// mode so a future regression that re-introduces the empty-status
// snapshot will trip here AND in the ratiobalanced_kind / loadtest_kind
// KIND specs together.
func TestCheckRatioGate_BogusOnePerComponentAnchorReproducesDeadlock(t *testing.T) {
	tol := int32(25)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        4,
						ServingReplicas: 4,
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					// Bogus 1:1 anchor — what the old buildRatioState
					// snapshotted when status.OMENative.Replicas was 0
					// at first reconcile (SnapshotOriginal clamps zero
					// to one).
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  1,
							v1beta1.DecoderComponent: 1,
						},
					},
				}},
			},
		},
	}

	// With anchor {1, 1}, band = 1.0 * (1 ± 0.25) = [0.75, 1.25].
	// Live ratio 4/2 = 2.0 is OUT of band. Engine drain projects
	// 3/2 = 1.5 → still out of band → REJECT.
	// (This is the deadlock the bug report captures: the gate rejects
	// every drain because the anchor doesn't match the cluster shape.)
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0); allowed {
		t.Fatalf("with bogus 1:1 anchor against 4:2 live fleet, gate should reject (projection 3:2=1.5 is outside band [0.75, 1.25] derived from 1.0); got allowed=true reason=%q — if this test PASSES with the bogus anchor it means the band semantics changed and the deadlock can't be reproduced from this anchor any more", reason)
	}
}

// mkPDFixture returns a coordinated ISVC with engine+decoder in one
// RatioBalanced group + an ObservedRatio snapshot of 20:5 (4:1, decoder
// = 25% of engine). Caller sets Status.Components.
//
// v2: cross-Component ratio guarding rides on MaintainRatio on a
// multi-Component rollingUpdate group (resolveGroup only honors
// MaintainRatio on rollingUpdate). A nil tolerance maps to the v1 default
// of 5.
func mkPDFixture(tolerance *int32) *v1beta1.InferenceService {
	tol := int32(5)
	if tolerance != nil {
		tol = *tolerance
	}
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  20,
							v1beta1.DecoderComponent: 5,
						},
					},
				}},
			},
		},
	}
}

func int32Ptr(v int32) *int32 { return &v }

// =============================================================================
// CheckUnavailabilityGate
// =============================================================================

func TestCheckUnavailabilityGate_NilISVC(t *testing.T) {
	allowed, _ := CheckUnavailabilityGate(nil, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("nil ISVC should bypass the gate")
	}
}

func TestCheckUnavailabilityGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no coord block should bypass the gate")
	}
}

func TestCheckUnavailabilityGate_ComponentNotInAnyGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.RouterComponent, 0)
	if !allowed {
		t.Errorf("component outside any coord group must bypass the gate")
	}
}

func TestCheckUnavailabilityGate_NoObservationYet(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	// No Status.Components populated.
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("missing OMENative observation must bypass the gate (first reconcile)")
	}
}

func TestCheckUnavailabilityGate_WithinBudget(t *testing.T) {
	// MaxUnavailable=1, 5 replicas, 5 Ready → currentUnavailable=0,
	// projected=1, budget=1 → ALLOWED.
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ReadyReplicas:   5,
				ServingReplicas: 5,
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("0 current unavailable + budget=1 must allow: %s", reason)
	}
}

func TestCheckUnavailabilityGate_BudgetExhausted(t *testing.T) {
	// MaxUnavailable=1, 5 replicas, 4 Ready → currentUnavailable=1,
	// projected=2, budget=1 → DENIED.
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ReadyReplicas:   4,
				ServingReplicas: 4,
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("1 current unavailable + budget=1 must deny next termination: %s", reason)
	}
}

func TestCheckUnavailabilityGate_ZeroBudgetDeadlocks(t *testing.T) {
	// MaxUnavailable=0 + 5 Ready/5 Replicas → projected=1 > budget=0 → DENIED.
	// This is the documented honest semantic: zero maxUnavailable without
	// surge-then-drain support deadlocks rollouts.
	isvc := mkUnavailFixture(iosInt(0))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ReadyReplicas:   5,
				ServingReplicas: 5,
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("MaxUnavailable=0 + all Ready must STILL deny (no surge-then-drain support)")
	}
}

func TestCheckUnavailabilityGate_PercentBudget(t *testing.T) {
	// MaxUnavailable=20%, 10 replicas → budget=2. 0 unavailable, projected=1
	// → ALLOWED.
	pct := intstr.FromString("20%")
	isvc := mkUnavailFixture(&pct)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        10,
				ReadyReplicas:   10,
				ServingReplicas: 10,
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("20%% of 10 = 2 budget; first termination must be allowed")
	}
}

func TestCheckUnavailabilityGate_InFlightDeltaCharges(t *testing.T) {
	// MaxUnavailable=2, 10 replicas, all serving. Budget=2.
	// inFlightDelta=0 → projected=1 → allowed.
	// inFlightDelta=1 → projected=2 → allowed (boundary).
	// inFlightDelta=2 → projected=3 → DENIED.
	// Proves the wake-up-counter closes the within-pass stale-state hole
	// that previously let the dispatcher fire every instance at once.
	pct := intstr.FromString("20%")
	isvc := mkUnavailFixture(&pct)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        10,
				ReadyReplicas:   10,
				ServingReplicas: 10,
			},
		},
	}
	cases := []struct {
		inFlight int32
		want     bool
	}{
		{0, true},
		{1, true}, // boundary
		{2, false},
		{5, false},
	}
	for _, c := range cases {
		got, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, c.inFlight)
		if got != c.want {
			t.Errorf("inFlight=%d: got allowed=%v want %v (%s)", c.inFlight, got, c.want, reason)
		}
	}
}

func TestCheckUnavailabilityGate_ServingDivergesFromReady(t *testing.T) {
	// ReadyReplicas=10 but ServingReplicas=7 — simulates the in-place
	// drain mid-rollout state where containers are still Ready but the
	// controller has flipped serving=False on 3 pods. The gate must
	// see 3 currently unavailable (from serving count), not 0 (from
	// ready count). MaxUnavailable=3 → projected=4 > budget=3 → DENY.
	// Before the fix, this scenario was the mass-outage bug.
	isvc := mkUnavailFixture(iosInt(3))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        10,
				ReadyReplicas:   10, // misleading — kubelet sees containers Ready
				ServingReplicas: 7,  // truth — 3 pods are out of rotation
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("gate must use ServingReplicas, not ReadyReplicas, so the drained-but-Ready state denies more drains: %s", reason)
	}
}

func iosInt(v int) *intstr.IntOrString {
	x := intstr.FromInt(v)
	return &x
}

// mkUnavailFixture returns a coordinated ISVC with engine in one group
// carrying the specified MaxUnavailable budget. Caller sets
// Status.Components.
//
// v2: the surge / unavailable budget rides on a rollingUpdate group (the
// only v2 progression that carries MaxSurge / MaxUnavailable — blueGreen
// drops them). The gate path reads the resolved pacing directly and does
// not run ValidateGroupShape, so a single-Component rollingUpdate fixture
// exercises the budget math (which is what these tests pin).
func mkUnavailFixture(maxUnavail *intstr.IntOrString) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{
						MaxUnavailable: maxUnavail,
					},
				}},
			},
		},
	}
}

// ----------------------------------------------------------------------
// CheckSurgeGate tests — parallel of CheckUnavailabilityGate, but
// counts in-flight surge operations against the MaxSurge budget rather
// than offline pods against the MaxUnavailable budget.
// ----------------------------------------------------------------------

// mkSurgeFixture is the surge analog of mkUnavailFixture: a rollingUpdate
// group carrying the specified MaxSurge budget. Caller sets
// Status.Components. (v2: MaxSurge rides on rollingUpdate; see
// mkUnavailFixture for why a single-Component rollingUpdate fixture is the
// right gate-test shape.)
func mkSurgeFixture(maxSurge *intstr.IntOrString) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{
						MaxSurge: maxSurge,
					},
				}},
			},
		},
	}
}

func TestCheckSurgeGate_NilISVC(t *testing.T) {
	allowed, _ := CheckSurgeGate(nil, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("nil ISVC: allow (bypass)")
	}
}

func TestCheckSurgeGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no coord: allow (bypass)")
	}
}

func TestCheckSurgeGate_NoObservationYet(t *testing.T) {
	isvc := mkSurgeFixture(iosInt(1))
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("first reconcile (no status): allow")
	}
}

func TestCheckSurgeGate_NoInFlight_WithinBudget(t *testing.T) {
	// MaxSurge=1, 5 replicas, no in-flight surge → projected=1, budget=1
	// → ALLOWED.
	isvc := mkSurgeFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
	}
	allowed, reason := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no in-flight + budget=1: allow; got reason=%s", reason)
	}
}

func TestCheckSurgeGate_OneInFlight_BudgetExhausted(t *testing.T) {
	// MaxSurge=1, 5 replicas, ONE instance already on Step=Surge → projected=2 > budget=1
	// → DENIED.
	isvc := mkSurgeFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{
			Replicas: 5,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
				Index:     0,
				Phase:     v1beta1.OMENativeInstanceUpdating,
				Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "Surge"},
			}},
		}},
	}
	allowed, reason := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("1 in-flight Surge + budget=1: deny; got allow reason=%s", reason)
	}
}

func TestCheckSurgeGate_SurgeDrainStepAlsoCounts(t *testing.T) {
	// SurgeDrain is the Phase 2 step (surge ready, draining old). Pod
	// still counts as in-flight surge for budget purposes — the extra
	// pod is alive until old is deleted.
	isvc := mkSurgeFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{
			Replicas: 5,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
				Index:     0,
				Phase:     v1beta1.OMENativeInstanceUpdating,
				Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "SurgeDrain"},
			}},
		}},
	}
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("SurgeDrain step must count toward in-flight surge budget")
	}
}

func TestCheckSurgeGate_OtherStepsDoNotCount(t *testing.T) {
	// Step=Drain (recreate) and Step=InPlace are NOT surge — they don't
	// add an extra pod. Gate must not count them.
	for _, step := range []string{"Drain", "InPlace"} {
		isvc := mkSurgeFixture(iosInt(1))
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{
				Replicas: 5,
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
					Index:     0,
					Phase:     v1beta1.OMENativeInstanceUpdating,
					Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: step},
				}},
			}},
		}
		allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
		if !allowed {
			t.Errorf("step=%s (not surge) must not consume surge budget", step)
		}
	}
}

func TestCheckSurgeGate_InFlightDeltaProjection(t *testing.T) {
	// No status in-flight, but dispatcher's wake-up tally says we already
	// kicked off 1 surge this wake-up. With MaxSurge=1, projected=2 → DENIED.
	isvc := mkSurgeFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
	}
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 1)
	if allowed {
		t.Errorf("inFlightDelta=1 + budget=1 must deny the next surge")
	}
}

func TestCheckSurgeGate_PercentBudget(t *testing.T) {
	// MaxSurge=25%, 8 replicas → ceil(8*0.25)=2 budget. 0 in-flight,
	// projected=1 → ALLOWED. inFlightDelta=2 would project=3 → DENIED.
	pct := intstr.FromString("25%")
	isvc := mkSurgeFixture(&pct)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 8}},
	}
	if allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0); !allowed {
		t.Errorf("25%% of 8 = 2, 0 in-flight, projected=1: allow")
	}
	if allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 2); allowed {
		t.Errorf("25%% of 8 = 2, in-flight=2, projected=3: deny")
	}
}

// mkSymmetricRatioFixture builds a symmetric engine 4 : decoder 4
// RatioBalanced group with an ObservedRatio snapshot of 4:4 and both
// Components fully serving (4/4). This is the p32/p42 deadlock shape:
// a 1:1 ratio where, at tolerance 25%, the discrete pod granularity
// leaves no room for a single -1 drain.
func mkSymmetricRatioFixture(tol int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: tol},
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
				Type:                  v1beta1.CoordinationPacingRatioBalanced,
				RatioTolerancePercent: int32Ptr(tc.tol),
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
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(25),
	}
	d := EvaluateSurge(pacing, RatioState{
		Original: map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
		Serving:  map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
	}, dec, -1)
	if d.AllowedSurgeDelta != 0 {
		t.Errorf("a drain exceeding 2x band (4:1 from orig 2:1) must stay DENIED; the surge tiebreaker must not loosen drains; got allowed (reason=%q)", d.Reason)
	}
}

// TestEvaluateSurge_DrainTiebreaker pins the Part-2 discrete-granularity
// tiebreaker: a drain-first roll (RecreatePod / in-place) gets exactly ONE
// rounding-error pod through when a tight band otherwise permits zero
// whole-pod moves, but large asymmetric skews stay denied and a second
// pod (from an already-skewed baseline) is refused.
func TestEvaluateSurge_DrainTiebreaker(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(25), // band [0.75, 1.25]; 2x band [0.5, 1.5]
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
	isvc.Spec.Rollout.Groups[0].RollingUpdate.MaxUnavailable = &mu

	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.EngineComponent, nil,
		workloadtypes.UpdateStrategyRecreatePod, 0, 0); !allowed {
		t.Errorf("RecreatePod on symmetric 4:4 (MaxUnavailable=3) must be allowed via the tiebreaker; got denied: %s", reason)
	}
}

// TestEvaluateSurge_DrainTiebreaker_InFlightOpBoundsAcrossWakeups pins the
// p51 cross-wake-up fix: when a prior recreate's drain is still in flight
// (operation-tracked InFlightUnavail=1) but the cache-derived Serving still
// over-reports a full 4:4 (the watch lag), the tiebreaker must NOT fire — it
// would let a SECOND pod drain (2:4, past the band) during a recreate burst.
// The fresh op count is what catches it; stale Serving alone does not.
func TestEvaluateSurge_DrainTiebreaker_InFlightOpBoundsAcrossWakeups(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(25),
	}
	// Serving STALE-high at 4:4 (cache hasn't observed the prior drain), but
	// InFlightUnavail=1 (op-tracked: one engine pod is already mid-recreate).
	state := RatioState{
		Original:        map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
		Serving:         map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
		InFlightUnavail: map[v1beta1.ComponentType]int32{eng: 1},
	}
	if d := EvaluateSurge(pacing, state, eng, -1); d.AllowedSurgeDelta != 0 {
		t.Errorf("a second drain must be denied while a prior drain is in flight "+
			"(InFlightUnavail=1), even though stale Serving shows a full 4:4; got allowed (reason=%q)", d.Reason)
	}

	// Sanity: with InFlightUnavail=0 (no prior drain), the FIRST pod IS allowed.
	state.InFlightUnavail = map[v1beta1.ComponentType]int32{}
	if d := EvaluateSurge(pacing, state, eng, -1); d.AllowedSurgeDelta == 0 {
		t.Errorf("the first drain (no in-flight op) must be allowed by the tiebreaker; got denied (reason=%q)", d.Reason)
	}
}

// Regression for the OMENative playground p226 / p227 verdict
// "decoder peak 5 > ceiling 4".
//
// Shape (both cases): a PD [engine, decoder] coordination group, engine
// maxSurge=50% (per-Component) and decoder SurgeThenDrain maxSurge=1, with
// the group's pacing.maxSurge left unset → defaulted to "25%". Both
// Components are bumped (the harness perturbs every group member), so each
// rolls off its OWN revision and surges per its OWN budget — the documented
// per-Component surge model (see the p62 header: "each component paces on its
// OWN budget, no cross-ratio", and the ~40 sibling rollout cases p27/p30/p32/
// p33/p41/p48/p62/p80/p243/p244 that all expect a bumped SurgeThenDrain
// maxSurge=1 decoder to surge to N+1=5).
//
// The correct decoder peak is therefore N(4) + maxSurge(1) = 5. These tests
// lock that in at the gate level: the decoder is permitted EXACTLY ONE surge
// (one extra pod alive → peak 5) and is gated from a SECOND (peak never
// reaches 6). A future change that wrongly clamped the decoder to peak 4
// (surge 0) — the mis-authored p226/p227 ceil_d=4 expectation — would make
// the "allowed" assertions fail; a change that let it surge twice would make
// the "denied" assertions fail.

// mkPDGroupSurge builds the p226/p227 group shape for the DECODER's surge
// gate: a 2-member [engine, decoder] group with the given pacing, the group's
// pacing.MaxSurge defaulted to "25%" (= budget 1 at N=4), and a 4:4
// ObservedRatio anchor (needed by RatioBalanced; harmless for PerComponent).
// decoderSurgesInFlight controls how many decoder Instances are already on a
// surge step (Surge/SurgeDrain) from a prior wake-up.
func mkPDGroupSurge(pacingType v1beta1.CoordinationPacingType, decoderSurgesInFlight int) *v1beta1.InferenceService {
	groupSurge := intstr.FromString("25%") // the webhook default when maxSurge is unset
	group := v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{MaxSurge: &groupSurge},
	}
	// v2: RatioBalanced pacing is expressed as MaintainRatio on the
	// rollingUpdate group; PerComponent is the absence of MaintainRatio.
	if pacingType == v1beta1.CoordinationPacingRatioBalanced {
		group.MaintainRatio = &v1beta1.MaintainRatio{Tolerance: 25}
	}

	decoderInstances := make([]v1beta1.OMENativeInstanceStatus, 0, decoderSurgesInFlight)
	for i := 0; i < decoderSurgesInFlight; i++ {
		decoderInstances = append(decoderInstances, v1beta1.OMENativeInstanceStatus{
			Index:     int32(i),
			Phase:     v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "Surge"},
		})
	}

	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{group},
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
				v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{
					Replicas:         4,
					ServingReplicas:  4,
					InstanceStatuses: decoderInstances,
				}},
			},
		},
	}
}

// TestDecoderSurgeBudget_P226_RatioBalanced pins the p226 shape: a bumped
// SurgeThenDrain maxSurge=1 decoder under RatioBalanced pacing surges EXACTLY
// once (peak N+1 = 5) and no more.
func TestDecoderSurgeBudget_P226_RatioBalanced(t *testing.T) {
	// First surge: no decoder surge in flight → the gate stack (CheckRatio
	// +1 projects 5:4 = 1.25, in band [0.75,1.25]; CheckSurge 0+0+1 <=
	// budget 1) must ALLOW. This is the surge that takes the decoder to
	// peak 5.
	isvc := mkPDGroupSurge(v1beta1.CoordinationPacingRatioBalanced, 0)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("p226: decoder's FIRST surge (→ peak N+1=5) must be allowed; got denied: %s", reason)
	}

	// Second surge: one decoder Instance already surging → CheckSurge
	// projects 1+0+1 = 2 > budget 1 → must DENY. This is what keeps the
	// decoder peak at 5 and not 6. (If the decoder were wrongly clamped to
	// peak 4, the first-surge assertion above would already have failed.)
	isvc = mkPDGroupSurge(v1beta1.CoordinationPacingRatioBalanced, 1)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); allowed {
		t.Fatalf("p226: decoder's SECOND surge (would be peak 6 > maxSurge=1 budget) must be denied; got allowed (reason=%s)", reason)
	}
}

// TestDecoderSurgeBudget_P227_PerComponent pins the p227 shape: identical to
// p226 but PerComponent pacing (no cross-ratio gate). The decoder still
// surges exactly once to peak 5 — governed solely by the group MaxSurge
// budget (defaulted "25%" = 1 at N=4).
func TestDecoderSurgeBudget_P227_PerComponent(t *testing.T) {
	isvc := mkPDGroupSurge(v1beta1.CoordinationPacingPerComponent, 0)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("p227: decoder's FIRST surge (→ peak N+1=5) must be allowed; got denied: %s", reason)
	}

	isvc = mkPDGroupSurge(v1beta1.CoordinationPacingPerComponent, 1)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); allowed {
		t.Fatalf("p227: decoder's SECOND surge (would be peak 6 > maxSurge=1 budget) must be denied; got allowed (reason=%s)", reason)
	}
}

// TestDecoderSurgeBudget_InWakeUpDeltaGatesSecondSurge guards the
// within-wake-up tally path: even before status reflects the first surge, the
// dispatcher threads inFlightSurge into the gate, so a second surge in the
// SAME wake-up (inFlightSurge=1) is denied. This is the other half of
// "decoder peak never exceeds 5" — the stale-snapshot hole that would
// otherwise let the dispatcher fire two decoder surges in one pass.
func TestDecoderSurgeBudget_InWakeUpDeltaGatesSecondSurge(t *testing.T) {
	for _, pt := range []v1beta1.CoordinationPacingType{
		v1beta1.CoordinationPacingRatioBalanced,
		v1beta1.CoordinationPacingPerComponent,
	} {
		isvc := mkPDGroupSurge(pt, 0)
		// inFlightSurge=1 → CheckSurge projects 0+1+1 = 2 > budget 1.
		if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
			workloadtypes.UpdateStrategySurgeThenDrain, 1, 0); allowed {
			t.Errorf("pacing=%s: a second decoder surge in one wake-up (inFlightSurge=1, budget=1) must be denied; got allowed (reason=%s)", pt, reason)
		}
	}
}

// mkGangSurgeFixture builds a symmetric engine 2 : decoder 2 RatioBalanced
// group (tol 25%, band [0.75, 1.25]) where ENGINE is mid multi-node (gang)
// SurgeThenDrain: its surge-target Instance has a full, serving gang, so the
// status aggregator counts it and engine ServingReplicas reads N+1=3 — the
// transient surge PEAK. Decoder is at steady N=2.
//
// For a single-pod Component the surge flip is atomic within one reconcile,
// so ServingReplicas never reports N+1 across a status write; for a gang the
// surge gang lingers serving alongside the source, so the gate genuinely
// observes the peak. The gate must treat that peak as the transient surge
// headroom it is — NOT durable serving capacity a drain may eat into.
func mkGangSurgeFixture() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: 25},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 3, // N=2 source + 1 surge gang = peak
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
								PodCount: 2, ServingPodCount: 2,
								Operation: &v1beta1.InstanceOperation{
									Type: v1beta1.InstanceOperationUpdate, Step: "Surge",
									SurgeIndex: int32Ptr(2)}},
							{Index: 1, Phase: v1beta1.OMENativeInstanceReady,
								PodCount: 2, ServingPodCount: 2},
							// Surge-target gang: full pod set, all serving →
							// counted in ServingReplicas (the N+1 peak).
							{Index: 2, Phase: v1beta1.OMENativeInstanceCreating,
								PodCount: 2, ServingPodCount: 2,
								Operation: &v1beta1.InstanceOperation{
									Type: v1beta1.InstanceOperationUpdate,
									Step: workloadtypes.UpdateStepGangSurgeTarget}},
						},
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  2,
							v1beta1.DecoderComponent: 2,
						},
					},
				}},
			},
		},
	}
}

// TestCheckRatioGate_GangSurgePeakDoesNotAuthorizeDrain reproduces the
// multi-node RatioBalanced mis-pacing found on the cluster (p72/78/79/80/81/
// 83/87/95/96, all confirmed isolated). Engine is mid gang SurgeThenDrain so
// its ServingReplicas reads the transient PEAK N+1=3 against decoder's steady
// N=2. A drain of one engine Instance would, once the surge settles back to
// N, leave engine at 1 : decoder 2 = ratio 2.0 vs the 1:1 anchor — far past
// the 25% band. The gate must REJECT the drain.
//
// Before the fix the gate counted the surge peak as durable serving (3:2 → a
// -1 drain projects 2:2, in band) and ALLOWED it. After the fix the gate
// nets the in-flight gang-surge peak out, sees the steady 2:2, and a -1 drain
// projects 1:2 (out of band) → DENIED. Single-pod is unaffected: it has no
// GangSurgeTarget Instance, so nothing is netted.
func TestCheckRatioGate_GangSurgePeakDoesNotAuthorizeDrain(t *testing.T) {
	isvc := mkGangSurgeFixture()
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Fatalf("a drain on an engine whose serving is inflated by an in-flight "+
			"gang surge (peak 3 over steady 2) must be DENIED — once the surge "+
			"settles the drain leaves 1:2 (ratio 2.0 vs 1:1 anchor, >25%% band); "+
			"got allowed (reason=%q)", reason)
	}
}

// TestUnavailableInFlight_GangSurgeTargetNotCountedUnavailable pins that a
// gang surge TARGET Instance (Step=GangSurgeTarget) is NOT counted as
// unavailable-in-flight — it ADDS a replacement gang, it does not pull one
// from rotation. Counting it would poison the drain-side tiebreaker's
// "no pod already out of rotation" guard during every gang surge.
func TestUnavailableInFlight_GangSurgeTargetNotCountedUnavailable(t *testing.T) {
	statuses := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Operation: &v1beta1.InstanceOperation{Step: "Surge"}},
		{Index: 1, Operation: &v1beta1.InstanceOperation{Step: workloadtypes.UpdateStepGangSurgeTarget}},
		{Index: 2, Operation: &v1beta1.InstanceOperation{Step: "Drain"}},
	}
	if got := unavailableInFlight(statuses); got != 1 {
		t.Fatalf("only the Drain Instance is unavailable-in-flight; Surge and "+
			"GangSurgeTarget add/keep capacity: want 1, got %d", got)
	}
}

// CheckRatioGate, CheckUnavailabilityGate, and CheckSurgeGate are
// test-only shims preserving the (isvc, component, inFlightDelta) call
// shape the gate unit tests exercise. The production package-level
// wrappers were removed (zero production callers — the dispatcher uses
// the GateContext-aware methods via EvaluateUpdateGate); these helpers
// keep the per-gate band-math coverage by delegating to the same
// GateContext methods the wrappers did.
func CheckRatioGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32) (bool, string) {
	return ResolveGateContext(isvc, component).CheckRatio(inFlightDelta, -1)
}

func CheckUnavailabilityGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32) (bool, string) {
	return ResolveGateContext(isvc, component).CheckUnavailability(inFlightDelta)
}

func CheckSurgeGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32) (bool, string) {
	return ResolveGateContext(isvc, component).CheckSurge(inFlightDelta)
}
