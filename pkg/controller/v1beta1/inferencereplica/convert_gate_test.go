package inferencereplica

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// mkSequentialParent returns an ISVC named "llama" (the parent every
// baselineIR points at) declaring a Sequential rollout over [decoder,
// engine] in that order. In v2 (OEP-0017) Sequential is spelled as a run
// of single-Component blueGreen groups, in list order; the controller's
// collapseSequential folds them back into one Sequential group whose Order
// is the list order (decoder→engine). Generation is 2; callers stamp
// per-Component Status to model in-flight vs converged via
// ObservedGeneration skew (gen < 2 ⇒ in flight, the moment-of-bump
// signal observeSequentialComponentsForGate keys on).
func mkSequentialParent() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 2},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
}

// mkIR creates an InferenceReplica for the given component of parent "llama".
// Callers stamp status fields to set up test scenarios.
func mkIR(c v1beta1.ComponentType, replicas int32) *v1beta1.InferenceReplica {
	return baselineIR("llama-"+string(c), "default", replicas)
}

func setObservedGen(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, gen int64) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.ObservedGeneration = gen
	isvc.Status.Components[c] = cs
}

// TestBuildReconcileInput_WiresSequentialGate is the regression guard for
// the IR-managed coordination bug. The IR controller used to leave
// ReconcileInput.UpdateGate nil, so the default (IR-managed) path skipped
// CheckSequential entirely and both Components of a Sequential group
// rolled concurrently (the playground caught this as "Sequential overlap:
// engine started before decoder finished"). The gate must now be wired
// AND deny the engine while the decoder (first in Order) is in flight.
func TestBuildReconcileInput_WiresSequentialGate(t *testing.T) {
	g := gomega.NewWithT(t)
	// Moment-of-bump: both Components lag isvc.Generation=2 ⇒ both in
	// flight. The active selector picks decoder (first in Order); the
	// engine must wait. Seed IR objects with ObservedGeneration=1.
	decoderIR := mkIR(v1beta1.DecoderComponent, 1)
	decoderIR.Status.ObservedGeneration = 1
	engineIR := mkIR(v1beta1.EngineComponent, 1)
	engineIR.Status.ObservedGeneration = 1
	r, _ := newReconciler(t, decoderIR, engineIR)

	parent := mkSequentialParent()

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil(),
		"UpdateGate must be wired on the IR path when the parent declares coordination")

	allowed, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must be denied while decoder (first in Sequential Order) is in flight")
	g.Expect(reason).To(gomega.ContainSubstring("Sequential waiting on decoder"))
}

// TestBuildReconcileInput_SequentialReleasesActiveComponent proves the
// gate is not a blanket block: once the decoder has converged, the engine
// becomes the active Sequential Component and is allowed to roll.
func TestBuildReconcileInput_SequentialReleasesActiveComponent(t *testing.T) {
	g := gomega.NewWithT(t)
	// Decoder converged (ObservedGeneration caught up to parent Generation=2)
	// ⇒ not in flight. Engine still lagging (gen=1) ⇒ it is the active
	// Sequential Component. Seed BOTH IRs so the gate reads their fresh status
	// (the group is not idle — engine is genuinely in flight — so this exercises
	// the active-component release path, not the "Sequential group idle" bypass).
	decoderIR := mkIR(v1beta1.DecoderComponent, 1)
	decoderIR.Status.ObservedGeneration = 2
	engineIR := mkIR(v1beta1.EngineComponent, 1)
	engineIR.Status.ObservedGeneration = 1
	r, _ := newReconciler(t, decoderIR, engineIR)

	parent := mkSequentialParent()

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, _ := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"engine is the active Sequential Component once decoder converged; it must be allowed")
}

// TestBuildReconcileInput_NilParentLeavesGateNil pins that without a
// resolvable parent there is no RolloutCoordination block to enforce, so
// the gate stays nil and the dispatcher's "always allowed" fallback
// applies (matching the documented EventTarget fallback behavior).
func TestBuildReconcileInput_NilParentLeavesGateNil(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)

	input := r.buildReconcileInput(context.Background(), engineIR, nil, nil)
	g.Expect(input.UpdateGate).To(gomega.BeNil(),
		"no parent ⇒ no coordination to enforce ⇒ gate stays nil")
}

// TestBuildReconcileInput_WiresMigrationWhenParentSet pins Layer 2: with
// a resolvable parent, the IR-managed path wires DetectMigrationRequest
// (so explicit migration requests fire on the production-default path)
// and points the migration audit ledger at the
// parent ISVC via LedgerOwner — so the ledger Migrate reads/writes is the
// same ISVC-owned ledger detection uses, while the IR still owns the pods.
// Nil parent leaves both nil (migration dormant), matching the gate/
// EventTarget fallback.
func TestBuildReconcileInput_WiresMigrationWhenParentSet(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)

	parent := mkSequentialParent() // any non-nil ISVC named "llama"
	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil)

	g.Expect(input.DetectMigrationRequest).NotTo(gomega.BeNil(),
		"DetectMigrationRequest must be wired on the IR path when the parent is resolvable")
	g.Expect(input.LedgerOwner).NotTo(gomega.BeNil(),
		"the migration ledger must be owned by the parent ISVC, not the IR")
	g.Expect(input.LedgerOwner.GetName()).To(gomega.Equal(parent.Name))
	g.Expect(input.LedgerOwnerGVK).To(gomega.Equal(isvcGVK))

	// Nil parent ⇒ migration dormant.
	nilInput := r.buildReconcileInput(context.Background(), engineIR, nil, nil)
	g.Expect(nilInput.DetectMigrationRequest).To(gomega.BeNil(),
		"no parent ⇒ migration has no ISVC to read ⇒ DetectMigrationRequest stays nil")
	g.Expect(nilInput.LedgerOwner).To(gomega.BeNil())
}

// mkRatioParent returns an ISVC named "llama" declaring a RatioBalanced
// (MaintainRatio) rollingUpdate group over [engine, decoder] anchored at a
// symmetric 4:4 original ratio. The per-Component projected Status reflects
// whatever the caller stamps — modeling the lagged irprojector rollup the
// gate reads for PEER Components.
func mkRatioParent(tol int32, engServing, decServing int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 1},
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
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: engServing}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: decServing}},
			},
		},
	}
}

// peerIR returns a sibling IR (e.g. the decoder) with the given fresh
// serving count stamped on its status — the authoritative count the
// gate should read for a PEER Component, not the parent's lagged
// projection.
func peerIR(component v1beta1.ComponentType, replicas, serving int32) *v1beta1.InferenceReplica {
	engineIR := baselineIR("llama-"+string(component), "default", replicas)
	engineIR.Spec.Component = component
	engineIR.Status.Replicas = replicas
	engineIR.Status.ServingReplicas = serving
	return engineIR
}

// TestBuildReconcileInput_GateReadsFreshPeerStatus is the regression guard
// for the cross-Component coordination stale-status race. The gate the IR
// path wires reads the GATED Component's counts from the IR's own fresh
// status, but it must ALSO read every PEER Component's counts from that
// peer's fresh IR status — not the parent ISVC's lagged projection.
//
// Scenario (RatioBalanced 4:4, tol 25% → band [0.75, 1.25]): the engine is
// at full serving (4/4) and asks to surge one new-revision pod. The decoder
// is actually mid-roll with one pod already out of rotation (fresh decoder
// IR: serving 3/4), but the parent ISVC's projected status still reports the
// decoder fully serving (4/4) because the irprojector rollup lags.
//
//   - Reading the STALE projected decoder (4/4): engine surge projects
//     5/4 = 1.25, inside the strict band → engine ALLOWED to run ahead.
//   - Reading the FRESH decoder (3/4): engine surge projects 5/3 = 1.667,
//     past the band; the surge tiebreaker also refuses (the baseline 4/3 is
//     already out of band) → engine correctly DENIED until the decoder
//     catches up.
//
// Before the fix the gate reads the stale projection and lets the engine
// outrun the decoder (the playground caught this as RatioBalanced
// "[engine 2 : decoder 1] dev > tol" violations). The fix overlays each
// peer's fresh IR status onto the gate's view, so the engine is held.
func TestBuildReconcileInput_GateReadsFreshPeerStatus(t *testing.T) {
	g := gomega.NewWithT(t)

	// Decoder is genuinely behind: fresh IR serving 3/4 (one pod out).
	decoder := peerIR(v1beta1.DecoderComponent, 4, 3)

	// Engine IR being reconciled: at full serving 4/4, about to surge. Seed it
	// into the client too, so the gate reads the engine's OWN fresh status (not
	// nil) and the denial is genuinely driven by the decoder's fresh 3/4.
	engineIR := baselineIR("llama-engine", "default", 4)
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 4
	r, _ := newReconciler(t, decoder, engineIR)

	// Parent projection is STALE: it still reports the decoder fully
	// serving (4/4), which is the lagged irprojector rollup.
	parent := mkRatioParent(25, 4, 4)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must be held while the decoder is genuinely behind (fresh decoder serving 3/4 → "+
			"engine surge 5/3 = 1.667 out of band); got allowed — the gate read the stale parent "+
			"projection (decoder 4/4) instead of the decoder's fresh IR status: "+reason)
}

// TestBuildReconcileInput_GateFreshPeerReleasesWhenBalanced is the GREEN
// companion: when the decoder's fresh IR status shows it back in balance
// (4/4), the same engine surge projects 5/4 = 1.25 (in band) and must be
// ALLOWED. This pins that the peer-freshness overlay does not turn into a
// blanket block — it tracks the peer's true position both ways.
func TestBuildReconcileInput_GateFreshPeerReleasesWhenBalanced(t *testing.T) {
	g := gomega.NewWithT(t)

	// Decoder is caught up: fresh IR serving 4/4.
	decoder := peerIR(v1beta1.DecoderComponent, 4, 4)
	engineIR := baselineIR("llama-engine", "default", 4)
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 4
	r, _ := newReconciler(t, decoder, engineIR)

	// Parent projection is irrelevant here (stamp it stale-low to prove the
	// gate prefers the fresh peer IR): decoder projected at 3/4.
	parent := mkRatioParent(25, 4, 3)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeTrue(),
		"engine surge must be allowed once the decoder's FRESH status is balanced (4/4 → engine 5/4 = "+
			"1.25 in band), even though the parent projection lags at 3/4: "+reason)
}

// TestBuildReconcileInput_SequentialGateReadsFreshPeerStatus is the
// Sequential analogue of the RatioBalanced peer-freshness guard. The
// decoder is first in Order; the engine must not start until the decoder
// finishes. Here the parent ISVC's projected status reports the decoder
// fully CONVERGED (UpdateRevision == CurrentRevision, ObservedGeneration
// caught up) because the irprojector rollup lags, but the decoder's OWN
// fresh IR status shows it still mid-rollout (UpdateRevision !=
// CurrentRevision).
//
//   - Reading the STALE projection: the decoder looks done, no Component
//     is in flight, the gate reports "Sequential group idle" → the engine
//     starts EARLY (the playground caught this as "engine started before
//     decoder finished").
//   - Reading the decoder's FRESH IR status: the decoder is in flight and
//     is the active Sequential Component → the engine is correctly DENIED.
func TestBuildReconcileInput_SequentialGateReadsFreshPeerStatus(t *testing.T) {
	g := gomega.NewWithT(t)

	// Fresh decoder IR: revision skew (v2 target, v1 current) ⇒ still
	// rolling, even though the parent projection will say it's done.
	decoder := baselineIR("llama-decoder", "default", 1)
	decoder.Spec.Component = v1beta1.DecoderComponent
	decoder.Status.ObservedGeneration = 2
	decoder.Status.CurrentRevision = "llama-decoder-v1"
	decoder.Status.UpdateRevision = "llama-decoder-v2"
	r, _ := newReconciler(t, decoder)

	engineIR := baselineIR("llama-engine", "default", 1) // Component=engine

	parent := mkSequentialParent() // Generation=2, Order=[decoder, engine]
	// STALE projection: both Components look converged (decoder "done").
	setObservedGen(parent, v1beta1.DecoderComponent, 2)
	setObservedGen(parent, v1beta1.EngineComponent, 2)

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, reason := input.UpdateGate(workload.UpdateStrategySurgeThenDrain, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"engine must wait while the decoder is genuinely still rolling (fresh decoder IR has "+
			"UpdateRevision != CurrentRevision); got allowed — the gate read the stale parent "+
			"projection (decoder converged) instead of the decoder's fresh IR status: "+reason)
	g.Expect(reason).To(gomega.ContainSubstring("Sequential waiting on decoder"))
}

// TestBuildReconcileInput_ThreadsGangSchedulingAvailable pins the IR-path
// multi-node gang fix: buildReconcileInput must thread the controller's
// GangSchedulingAvailable flag into DesiredSpec, because EnsurePodGroups gates
// per-Instance PodGroup creation on it. Without this, IR-managed multi-node
// pods render the gang reference but no PodGroup object is ever created, so the
// gang stays Pending forever ("PodGroup not found") — the bug this fixes.
func TestBuildReconcileInput_ThreadsGangSchedulingAvailable(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	engineIR := baselineIR("llama-engine", "default", 1)

	r.GangSchedulingAvailable = true
	g.Expect(r.buildReconcileInput(context.Background(), engineIR, nil, nil).DesiredSpec.GangSchedulingAvailable).To(gomega.BeTrue(),
		"DesiredSpec.GangSchedulingAvailable must follow the controller flag (true) so EnsurePodGroups runs")

	r.GangSchedulingAvailable = false
	g.Expect(r.buildReconcileInput(context.Background(), engineIR, nil, nil).DesiredSpec.GangSchedulingAvailable).To(gomega.BeFalse(),
		"flag false ⇒ DesiredSpec false ⇒ EnsurePodGroups skips (CRD absent / degradation surface)")
}

func TestBuildReconcileInput_ParentPauseAnnotationIsAuthoritative(t *testing.T) {
	tests := []struct {
		name       string
		irPaused   bool
		parent     *v1beta1.InferenceService
		wantPaused bool
	}{
		{
			name:       "readable parent pause overrides stale false IR projection",
			parent:     &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.PausedRolloutAnnotation: "true"}}},
			wantPaused: true,
		},
		{
			name:       "readable parent removal overrides stale true IR projection",
			irPaused:   true,
			parent:     &v1beta1.InferenceService{},
			wantPaused: false,
		},
		{
			name:       "unreadable parent falls back to projected IR value",
			irPaused:   true,
			wantPaused: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newReconciler(t)
			engineIR := baselineIR("llama-engine", "default", 1)
			engineIR.Spec.Paused = tc.irPaused
			got := r.buildReconcileInput(context.Background(), engineIR, tc.parent, nil).DesiredSpec.Paused
			if got != tc.wantPaused {
				t.Fatalf("DesiredSpec.Paused: got %v want %v", got, tc.wantPaused)
			}
		})
	}
}

// TestBuildReconcileInput_GateUsesFreshIRStatus pins the IR-path
// gate-staleness fix (p44/p51): the gate must read the GATED Component's
// counts from the IR's OWN fresh status, not the parent ISVC's lagged
// projection. Here the IR's fresh status shows the engine already one pod
// down (serving 3/4); a RatioBalanced drain (RecreatePod → -1) must be
// DENIED — the tiebreaker bounds in-flight to one pod — even though the
// parent's projected status still reports a full 4/4 (which, if the gate
// read it, would let the tiebreaker fire again and over-drain).
func TestBuildReconcileInput_GateUsesFreshIRStatus(t *testing.T) {
	g := gomega.NewWithT(t)
	engineIR := baselineIR("llama-engine", "default", 4) // engine, parent llama
	engineIR.Status.Replicas = 4
	engineIR.Status.ServingReplicas = 3 // FRESH: one engine pod already out of rotation
	// Peer decoder is balanced at 4/4 (fresh); only the engine is down. Seed
	// both so the gate reads the engine's OWN fresh 3/4 (not nil) — the drain
	// denial must be driven by that, not by the engine being absent.
	decoderIR := peerIR(v1beta1.DecoderComponent, 4, 4)
	r, _ := newReconciler(t, engineIR, decoderIR)

	tol := int32(25)
	parent := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "default", Generation: 1},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					// RatioBalanced pacing is spelled as MaintainRatio in v2;
					// tolerance 25 preserves the original RatioTolerancePercent.
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
				// STALE projection: still reports engine fully serving (4/4).
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
			},
		},
	}

	input := r.buildReconcileInput(context.Background(), engineIR, parent, nil)
	g.Expect(input.UpdateGate).NotTo(gomega.BeNil())

	allowed, reason := input.UpdateGate(workload.UpdateStrategyRecreatePod, 0, 0)
	g.Expect(allowed).To(gomega.BeFalse(),
		"with the IR's fresh status (engine serving 3/4 = one pod already out), the "+
			"RatioBalanced tiebreaker must refuse a second drain; got allowed — the gate "+
			"read the stale parent projection (4/4) instead of the IR's fresh status: "+reason)
}
