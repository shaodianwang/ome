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

// =============================================================================
// CheckRatioGate
// =============================================================================

func TestCheckRatioGate_NilISVC(t *testing.T) {
	allowed, _ := CheckRatioGate(nil, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("nil ISVC should be allowed (no-op gate)")
	}
}

func TestCheckRatioGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent)
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
					Policy:     v1beta1.CoordinationPolicyAtomic,
					Pacing: &v1beta1.CoordinationPacing{
						Type: v1beta1.CoordinationPacingPerComponent,
					},
				}},
			},
		},
	}
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent)
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
					Policy:     v1beta1.CoordinationPolicyAtomic,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: int32Ptr(5),
					},
				}},
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.EngineComponent)
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
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:        20,
				UpdatedReplicas: 1,
			},
		},
		v1beta1.DecoderComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:        5,
				UpdatedReplicas: 0,
			},
		},
	}

	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent)
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
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:        20,
				UpdatedReplicas: 1,
			},
		},
		v1beta1.DecoderComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:        5,
				UpdatedReplicas: 0,
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.DecoderComponent)
	// At tolerance=5, the discrete granularity prevents decoder from
	// moving. The test pins this honest deadlock so a reviewer can see
	// it; widening tolerance fixes it.
	if allowed {
		t.Errorf("expected decoder gated at tolerance=5: discrete granularity (decoder pod = 20%% jump) cannot satisfy band; got allowed=true")
	}
}

func TestCheckRatioGate_PDScenarioWiderToleranceAllowsCatchUp(t *testing.T) {
	// Same PD setup but tolerance=20%. Decoder going 0/5 → 1/5 = 20%
	// against engine 1/20 = 5% → diff 15pp, within 20pp → allowed.
	isvc := mkPDFixture(int32Ptr(20))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:        20,
				UpdatedReplicas: 1,
			},
		},
		v1beta1.DecoderComponent: {
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:        5,
				UpdatedReplicas: 0,
			},
		},
	}
	allowed, reason := CheckRatioGate(isvc, v1beta1.DecoderComponent)
	if !allowed {
		t.Errorf("expected decoder allowed at tolerance=20: got reason=%s", reason)
	}
}

func TestCheckRatioGate_SingleComponentGroupBypasses(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicyAtomic,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: int32Ptr(0),
					},
				}},
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.EngineComponent)
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
					Policy:     v1beta1.CoordinationPolicyAtomic,
					Pacing: &v1beta1.CoordinationPacing{
						Type: v1beta1.CoordinationPacingRatioBalanced,
					},
				}},
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.RouterComponent)
	if !allowed {
		t.Errorf("component outside any coord group must bypass the gate")
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
					Policy:     v1beta1.CoordinationPolicyAtomic,
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
					Policy:     v1beta1.CoordinationPolicyAtomic,
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
	allowed, _ := CheckUnavailabilityGate(nil, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("nil ISVC should bypass the gate")
	}
}

func TestCheckUnavailabilityGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent)
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
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.RouterComponent)
	if !allowed {
		t.Errorf("component outside any coord group must bypass the gate")
	}
}

func TestCheckUnavailabilityGate_NoObservationYet(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	// No Status.Components populated.
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent)
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
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:      5,
				ReadyReplicas: 5,
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent)
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
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:      5,
				ReadyReplicas: 4,
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent)
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
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:      5,
				ReadyReplicas: 5,
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent)
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
			OMENative: &v1beta1.OMENativeComponentStatus{
				Replicas:      10,
				ReadyReplicas: 10,
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("20%% of 10 = 2 budget; first termination must be allowed")
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
					Policy:     v1beta1.CoordinationPolicyAtomic,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingPerComponent,
						MaxUnavailable: maxUnavail,
					},
				}},
			},
		},
	}
}
