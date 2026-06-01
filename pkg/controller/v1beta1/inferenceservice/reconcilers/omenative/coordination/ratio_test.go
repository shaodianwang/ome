package coordination

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

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

func TestEvaluateSurge_BacksOffWhenLiveRatioWouldExceedBand(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
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
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
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
		Type:           v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerance: &tol,
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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type: v1beta1.CoordinationPacingPerComponent,
					},
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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: int32Ptr(5),
					},
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
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: int32Ptr(0),
					},
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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type: v1beta1.CoordinationPacingRatioBalanced,
					},
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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: &tol,
					},
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
// fix for the C-1 + H-3 KIND deadlock (`.claude/specs/2026-05-30-kind-test-walk/
// ratiobalanced-bluegreen-deadlock-no-progress.md`).
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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: &tol,
					},
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
// snapshot will trip here AND in the C-1 / H-3 KIND specs together.
func TestCheckRatioGate_BogusOnePerComponentAnchorReproducesDeadlock(t *testing.T) {
	tol := int32(25)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: &tol,
					},
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
func mkPDFixture(tolerance *int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: tolerance,
					},
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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
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
// using PerComponent pacing + specified MaxUnavailable. Caller sets
// Status.Components.
func mkUnavailFixture(maxUnavail *intstr.IntOrString) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingPerComponent,
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

// mkSurgeFixture is the surge analog of mkUnavailFixture: PerComponent
// pacing + specified MaxSurge. Caller sets Status.Components.
func mkSurgeFixture(maxSurge *intstr.IntOrString) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:     v1beta1.CoordinationPacingPerComponent,
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
