package coordination

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// helper: build an idle observation set (no rollout in flight).
func idleObs(g ResolvedGroup) GroupObservation {
	obs := GroupObservation{
		Group:      g,
		Components: map[v1beta1.ComponentType]ComponentObservation{},
	}
	for _, c := range g.Components {
		obs.Components[c] = ComponentObservation{
			Component:            c,
			DesiredReplicas:      2,
			TotalPods:            2,
			ReadyPods:            2,
			NewRevisionPods:      2,
			NewRevisionReadyPods: 2,
			TargetRevisionHash:   "rev1",
			CurrentRevisionHash:  "rev1",
			RolloutInFlight:      false,
		}
	}
	return obs
}

func TestComputeTransition_PausedGlobal(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyAtomic}
	obs := idleObs(g)
	obs.PausedGlobal = true
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhasePaused {
		t.Errorf("global pause: phase got %q want Paused", tr.Phase)
	}
}

func TestComputeTransition_IndependentIdle(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyIndependent}
	tr := ComputeTransition(idleObs(g))
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("idle: got %q want Idle", tr.Phase)
	}
}

func TestComputeTransition_IndependentInFlight(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyIndependent}
	obs := idleObs(g)
	c := obs.Components[v1beta1.EngineComponent]
	c.RolloutInFlight = true
	obs.Components[v1beta1.EngineComponent] = c
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseShifting {
		t.Errorf("Independent in-flight: got %q want Shifting", tr.Phase)
	}
}

func TestComputeTransition_IndependentFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyIndependent}
	obs := idleObs(g)
	c := obs.Components[v1beta1.EngineComponent]
	c.Failed = true
	obs.Components[v1beta1.EngineComponent] = c
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("Independent failed: got %q want Failed", tr.Phase)
	}
}

func TestComputeTransition_AtomicSurging(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic}
	obs := idleObs(g)
	// One Component is rolling but has zero new pods (not surged yet).
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.NewRevisionPods = 0
	engine.NewRevisionReadyPods = 0
	engine.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("Atomic surging: got %q want Surging", tr.Phase)
	}
}

func TestComputeTransition_AtomicWaiting(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic}
	obs := idleObs(g)
	// Both rolling, both have surge pods, but engine's new pods not Ready.
	for _, c := range g.Components {
		comp := obs.Components[c]
		comp.RolloutInFlight = true
		comp.NewRevisionPods = 2
		comp.NewRevisionReadyPods = 2
		comp.TotalPods = 4 // 2 new + 2 old still up
		comp.TargetRevisionHash = "rev2"
		obs.Components[c] = comp
	}
	engine := obs.Components[v1beta1.EngineComponent]
	engine.NewRevisionReadyPods = 1
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseWaiting {
		t.Errorf("Atomic Waiting: got %q want Waiting", tr.Phase)
	}
}

func TestComputeTransition_AtomicShifting(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic}
	obs := idleObs(g)
	// Both rolling, all new pods Ready, but old pods still up.
	for _, c := range g.Components {
		comp := obs.Components[c]
		comp.RolloutInFlight = true
		comp.NewRevisionPods = 2
		comp.NewRevisionReadyPods = 2
		comp.TotalPods = 4 // 2 new + 2 old still up
		comp.TargetRevisionHash = "rev2"
		obs.Components[c] = comp
	}
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseShifting {
		t.Errorf("Atomic Shifting: got %q want Shifting", tr.Phase)
	}
}

func TestComputeTransition_AtomicScalingDown(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic}
	obs := idleObs(g)
	// All rolling, new pods Ready, old pods gone (TotalPods == NewRevisionPods).
	for _, c := range g.Components {
		comp := obs.Components[c]
		comp.RolloutInFlight = true
		comp.NewRevisionPods = 2
		comp.NewRevisionReadyPods = 2
		comp.TotalPods = 2
		comp.TargetRevisionHash = "rev2"
		obs.Components[c] = comp
	}
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseScalingDown {
		t.Errorf("Atomic ScalingDown: got %q want ScalingDown", tr.Phase)
	}
}

func TestComputeTransition_AtomicFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyAtomic}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.Failed = true
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("Atomic any-failure: got %q want Failed", tr.Phase)
	}
}

func TestComputeTransition_AtomicPaused(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyAtomic}
	obs := idleObs(g)
	c := obs.Components[v1beta1.EngineComponent]
	c.Paused = true
	obs.Components[v1beta1.EngineComponent] = c
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhasePaused {
		t.Errorf("Atomic with paused Component: got %q want Paused", tr.Phase)
	}
}

func TestComputeTransition_PairedMixableSurging(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyPairedMixable}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.NewRevisionPods = 0
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("PairedMixable any-surging: got %q want Surging", tr.Phase)
	}
}

func TestComputeTransition_PairedMixableIdle(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyPairedMixable}
	tr := ComputeTransition(idleObs(g))
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("PairedMixable idle: got %q want Idle", tr.Phase)
	}
}

func TestComputeTransition_PairedMixableFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyPairedMixable}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.Failed = true
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("PairedMixable failure: got %q want Failed", tr.Phase)
	}
}

func TestComputeTransition_SequentialActiveComponent(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := idleObs(g)
	// Decoder is rolling; engine is idle.
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.RolloutInFlight = true
	decoder.NewRevisionPods = 0
	decoder.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.DecoderComponent] = decoder
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("Sequential active: phase got %q want Surging", tr.Phase)
	}
	if tr.CompositePhase != "decoder.Surging" {
		t.Errorf("composite phase: got %q want decoder.Surging", tr.CompositePhase)
	}
	if tr.CurrentComponent != v1beta1.DecoderComponent {
		t.Errorf("currentComponent: got %q want decoder", tr.CurrentComponent)
	}
}

func TestComputeTransition_SequentialAwaitingNext(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	// All Components idle (neither has rolled yet) — group is Idle
	// at the base layer; composite is plain Idle since no Component
	// has advanced.
	obs := idleObs(g)
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("all-idle: phase got %q want Idle", tr.Phase)
	}
	// Now decoder is on a new revision but engine isn't rolling.
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.CurrentRevisionHash = "rev2"
	decoder.TargetRevisionHash = "rev2"
	decoder.RolloutInFlight = false
	obs.Components[v1beta1.DecoderComponent] = decoder
	// Engine still on rev1, no rollout in flight.
	tr = ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("Sequential awaiting: phase got %q want Idle", tr.Phase)
	}
}

func TestComputeTransition_SequentialFailedBlocks(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := idleObs(g)
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.Failed = true
	decoder.FailureMessage = "ready timeout"
	obs.Components[v1beta1.DecoderComponent] = decoder
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("Sequential failure: got %q want Failed", tr.Phase)
	}
	if tr.CompositePhase != v1beta1.CompositePhaseSequentialFailed {
		t.Errorf("composite Sequential.Failed: got %q", tr.CompositePhase)
	}
	if tr.CurrentComponent != v1beta1.DecoderComponent {
		t.Errorf("CurrentComponent: got %q want decoder", tr.CurrentComponent)
	}
}

func TestComputeTransition_SequentialNextStartsAfterPrevious(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
	}
	obs := idleObs(g)
	// Decoder done (Idle on new rev), engine now starts rolling.
	decoder := obs.Components[v1beta1.DecoderComponent]
	decoder.RolloutInFlight = false
	obs.Components[v1beta1.DecoderComponent] = decoder
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.NewRevisionPods = 0
	engine.TargetRevisionHash = "rev2"
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.CurrentComponent != v1beta1.EngineComponent {
		t.Errorf("CurrentComponent: got %q want engine", tr.CurrentComponent)
	}
	if tr.PreviousComponent != v1beta1.DecoderComponent {
		t.Errorf("PreviousComponent: got %q want decoder", tr.PreviousComponent)
	}
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("phase: got %q want Surging", tr.Phase)
	}
}
