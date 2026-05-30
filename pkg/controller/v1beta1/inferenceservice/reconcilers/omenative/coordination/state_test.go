package coordination

import (
	"testing"
	"time"

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
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
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

func TestComputeTransition_BlueGreenSurging(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
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
		t.Errorf("BlueGreen surging: got %q want Surging", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenWaiting(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
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
		t.Errorf("BlueGreen Waiting: got %q want Waiting", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenShifting(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
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
		t.Errorf("BlueGreen Shifting: got %q want Shifting", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenScalingDown(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
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
		t.Errorf("BlueGreen ScalingDown: got %q want ScalingDown", tr.Phase)
	}
}

func TestComputeTransition_BlueGreenFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.Failed = true
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("BlueGreen any-failure: got %q want Failed", tr.Phase)
	}
}

// TestComputeTransition_BlueGreenPaused asserts BlueGreen honors the
// ISVC-wide PausedGlobal signal (the only pause path coordination
// supports today). Per-Component pause was never wired in production;
// it was removed in favor of PausedGlobal exclusively.
func TestComputeTransition_BlueGreenPaused(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
	obs := idleObs(g)
	obs.PausedGlobal = true
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhasePaused {
		t.Errorf("BlueGreen with global pause: got %q want Paused", tr.Phase)
	}
}

func TestComputeTransition_RollingUpdateSurging(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.RolloutInFlight = true
	engine.NewRevisionPods = 0
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseSurging {
		t.Errorf("RollingUpdate any-surging: got %q want Surging", tr.Phase)
	}
}

func TestComputeTransition_RollingUpdateIdle(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	tr := ComputeTransition(idleObs(g))
	if tr.Phase != v1beta1.CoordinationPhaseIdle {
		t.Errorf("RollingUpdate idle: got %q want Idle", tr.Phase)
	}
}

func TestComputeTransition_RollingUpdateFailed(t *testing.T) {
	g := ResolvedGroup{Name: "0", Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, Policy: v1beta1.CoordinationPolicyRollingUpdate}
	obs := idleObs(g)
	engine := obs.Components[v1beta1.EngineComponent]
	engine.Failed = true
	obs.Components[v1beta1.EngineComponent] = engine
	tr := ComputeTransition(obs)
	if tr.Phase != v1beta1.CoordinationPhaseFailed {
		t.Errorf("RollingUpdate failure: got %q want Failed", tr.Phase)
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

// TestComputeTransition_SequentialSoakHolds verifies the soak gate
// blocks the next Component from starting until the configured Soak
// duration has elapsed since the previous Component completed (as
// recorded in PreviousPhaseEnteredAt).
func TestComputeTransition_SequentialSoakHolds(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Soak:       5 * time.Second,
	}
	now := time.Now()
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {
				Component:           v1beta1.DecoderComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev2",
				RolloutInFlight:     false,
			},
			v1beta1.EngineComponent: {
				Component:           v1beta1.EngineComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev1",
				RolloutInFlight:     true,
			},
		},
		Now:                    now,
		PreviousPhaseEnteredAt: now.Add(-2 * time.Second), // 2s into a 5s soak
	}
	tr := ComputeTransition(obs)
	if tr.CompositePhase != v1beta1.CompositePhaseSequentialAwaiting {
		t.Errorf("soak should hold engine at Sequential.Awaiting; got composite=%q phase=%q msg=%q",
			tr.CompositePhase, tr.Phase, tr.Message)
	}
	if tr.CurrentComponent != v1beta1.EngineComponent {
		t.Errorf("CurrentComponent during soak: got %q want engine", tr.CurrentComponent)
	}
	if tr.PreviousComponent != v1beta1.DecoderComponent {
		t.Errorf("PreviousComponent during soak: got %q want decoder", tr.PreviousComponent)
	}
}

// TestComputeTransition_SequentialSoakReleases verifies the soak gate
// releases once the configured Soak duration has elapsed, letting the
// next Component begin its rollout.
func TestComputeTransition_SequentialSoakReleases(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Soak:       5 * time.Second,
	}
	now := time.Now()
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {
				Component:           v1beta1.DecoderComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev2",
				RolloutInFlight:     false,
			},
			v1beta1.EngineComponent: {
				Component:           v1beta1.EngineComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev1",
				RolloutInFlight:     true,
			},
		},
		Now:                    now,
		PreviousPhaseEnteredAt: now.Add(-10 * time.Second), // soak elapsed (10s > 5s)
	}
	tr := ComputeTransition(obs)
	if tr.CompositePhase == v1beta1.CompositePhaseSequentialAwaiting {
		t.Errorf("soak elapsed: must NOT be Awaiting; got composite=%q", tr.CompositePhase)
	}
	if tr.CurrentComponent != v1beta1.EngineComponent {
		t.Errorf("CurrentComponent post-soak: got %q want engine", tr.CurrentComponent)
	}
}

// TestComputeTransition_SequentialNoSoakRunsImmediately verifies the
// default behavior (Soak=0): the next Component starts the moment
// the previous Component finishes, no time-based hold.
func TestComputeTransition_SequentialNoSoakRunsImmediately(t *testing.T) {
	g := ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		Soak:       0,
	}
	now := time.Now()
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {
				Component:           v1beta1.DecoderComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev2",
				RolloutInFlight:     false,
			},
			v1beta1.EngineComponent: {
				Component:           v1beta1.EngineComponent,
				DesiredReplicas:     1,
				TargetRevisionHash:  "rev2",
				CurrentRevisionHash: "rev1",
				RolloutInFlight:     true,
			},
		},
		Now:                    now,
		PreviousPhaseEnteredAt: now.Add(-1 * time.Millisecond), // immediately after completion
	}
	tr := ComputeTransition(obs)
	if tr.CompositePhase == v1beta1.CompositePhaseSequentialAwaiting {
		t.Errorf("Soak=0 must NOT engage Awaiting; got composite=%q", tr.CompositePhase)
	}
}
