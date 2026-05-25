package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Sequential policy state-machine specs for OEP-0007.1.
//
// Sequential walks the group's `Order` one Component at a time. The
// next Component cannot enter Surging until the previous Component is
// back at Idle on its new revision. Composite phase carries the active
// Component name (e.g. "engine.Surging", "Sequential.AwaitingNextComponent",
// "Sequential.Failed").
//
// Most cases run as pure-function `It`s. End-to-end PIts depend on:
//   - G1: reconciler does not invoke ComputeTransition
//   - G5: SequentialNextSpecBumpDeferred event not emitted
//   - G8: spec-revert recovery from Sequential.Failed not implemented
//
// Reference: state.go sequentialTransition + previousCompletedComponent
// + completedSequentialComponents.

// seqObs builds an Ordered Sequential observation. `order` is both the
// Components slice AND the Order slice (Sequential requires Order to
// cover every Component). Defaults: 2 replicas per Component, all on
// rev1, no rollout in flight.
func seqObs(order ...v1beta1.ComponentType) coordination.GroupObservation {
	if len(order) == 0 {
		order = []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	}
	g := coordination.ResolvedGroup{
		Name:       "0",
		Components: order,
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      order,
	}
	obs := coordination.GroupObservation{
		Group:      g,
		Components: map[v1beta1.ComponentType]coordination.ComponentObservation{},
	}
	for _, c := range order {
		obs.Components[c] = coordination.ComponentObservation{
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

var _ = ginkgo.Describe("Coordination Sequential state machine (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-seq-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs
	// ----------------------------------------------------------------

	ginkgo.It("S-1 Order [decoder, engine]: spec bump → only decoder rolls (engine stays Idle)", func() {
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		// Decoder is the FIRST in Order. Bump only decoder.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		// Active Component is decoder; sub-phase is Surging (no new pods yet).
		Expect(tr.CurrentComponent).To(Equal(v1beta1.DecoderComponent))
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(tr.CompositePhase).To(Equal("decoder.Surging"))
		Expect(tr.PreviousComponent).To(BeEmpty(),
			"first Component in Order has no predecessor")
	})

	ginkgo.It("S-2 Decoder reaches Idle on new revision → engine becomes eligible", func() {
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		// Decoder done (CurrentRevisionHash flipped, RolloutInFlight false).
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.CurrentRevisionHash = "rev2"
			o.TargetRevisionHash = "rev2"
			o.RolloutInFlight = false
		})
		// Engine bumped, no surge pods yet — it's the active Component now.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.EngineComponent))
		Expect(tr.CompositePhase).To(Equal("engine.Surging"))
		Expect(tr.PreviousComponent).To(Equal(v1beta1.DecoderComponent),
			"PreviousComponent must point at the just-completed predecessor")
	})

	ginkgo.It("S-3 active Component's sub-phase becomes the CompositePhase suffix", func() {
		obs := seqObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 4 // old pods still up → Shifting
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseShifting))
		Expect(tr.CompositePhase).To(Equal("engine.Shifting"))
	})

	ginkgo.It("S-4 CurrentComponent/PreviousComponent tracked across the Order", func() {
		// 3-Component order: A → B → C. A done, B rolling. Expect
		// CurrentComponent=B, PreviousComponent=A.
		obs := seqObs(v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent)
		mutate(&obs, v1beta1.RouterComponent, func(o *coordination.ComponentObservation) {
			o.CurrentRevisionHash = "rev2"
			o.TargetRevisionHash = "rev2"
			o.RolloutInFlight = false
		})
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.EngineComponent))
		Expect(tr.PreviousComponent).To(Equal(v1beta1.RouterComponent))
	})

	ginkgo.It("S-5 inter-Component soak (transient): CompositePhase = Sequential.AwaitingNextComponent when next Component not yet observed", func() {
		// The AwaitingNextComponent composite phase is only reachable
		// in a transient observation state: an earlier Component has
		// completed (RolloutInFlight=false) AND the next Component is
		// missing from obs.Components — i.e. the reconciler hasn't yet
		// populated an observation for it. In steady state both
		// Components are observed and `completedSequentialComponents`
		// reaches the end of Order → "complete" branch.
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.CurrentRevisionHash = "rev2"
			o.TargetRevisionHash = "rev2"
			o.RolloutInFlight = false
		})
		// Drop engine from observations to simulate the transient
		// "next-Component-not-yet-observed" case.
		delete(obs.Components, v1beta1.EngineComponent)

		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle),
			"soak window keeps base Phase=Idle until next Component is bumped")
		Expect(tr.CompositePhase).To(Equal(v1beta1.CompositePhaseSequentialAwaiting))
		Expect(tr.CurrentComponent).To(Equal(v1beta1.EngineComponent),
			"CurrentComponent during soak points at the next Component awaiting observation")
		Expect(tr.PreviousComponent).To(Equal(v1beta1.DecoderComponent))
	})

	ginkgo.It("S-5b decoder done + engine present + both at rest → 'complete' (NOT Awaiting)", func() {
		// Codifies the actual state machine semantics: once both
		// Components are observed and neither is RolloutInFlight, the
		// group collapses to 'complete' even if the user has only
		// bumped one Component. The Awaiting composite is unreachable
		// in this steady-observation case. If we ever want a true
		// inter-Component soak window with all Components observed,
		// the state machine would need a group-level target-revision
		// concept (it currently relies on per-Component RolloutInFlight).
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.CurrentRevisionHash = "rev2"
			o.TargetRevisionHash = "rev2"
			o.RolloutInFlight = false
		})
		// Engine kept at rev1 / not rolling.
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(tr.CompositePhase).To(Equal(string(v1beta1.CoordinationPhaseIdle)),
			"steady-state with all Components observed collapses to Idle 'complete' regardless of per-Component target")
		Expect(tr.Message).To(ContainSubstring("complete"))
	})

	ginkgo.It("S-7 Failed Component blocks the group; subsequent Components never start", func() {
		// Order [decoder, engine]; decoder fails mid-rollout. Engine
		// must not roll, regardless of its own state.
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 0 // would be Waiting if not Failed
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
			o.Failed = true
			o.FailureMessage = "Ready timeout"
		})
		// Engine COULD roll but Sequential blocks on decoder's failure.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(tr.CompositePhase).To(Equal(v1beta1.CompositePhaseSequentialFailed))
		Expect(tr.CurrentComponent).To(Equal(v1beta1.DecoderComponent),
			"CurrentComponent points at the Failed Component, not the blocked successor")
		Expect(tr.Message).To(ContainSubstring("decoder"))
	})

	ginkgo.It("S-8 3-Component happy path walks Order: A active → B active → C active → complete", func() {
		// Natural Sequential flow under steady-state observation: each
		// Component is observed throughout, and the active loop walks
		// the Order one at a time. Awaiting composite isn't reached
		// because all Components are present in obs (see S-5/S-5b).
		obs := seqObs(v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent)

		// Phase 0: router rolling (active).
		mutate(&obs, v1beta1.RouterComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.RouterComponent))
		Expect(tr.PreviousComponent).To(BeEmpty())
		Expect(tr.CompositePhase).To(Equal("router.Surging"))

		// Phase 1: router done; engine starts (RolloutInFlight=true on
		// engine), decoder still Idle. Active loop skips router (not
		// rolling) and catches engine.
		mutate(&obs, v1beta1.RouterComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = false
			o.CurrentRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr = coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.EngineComponent))
		Expect(tr.PreviousComponent).To(Equal(v1beta1.RouterComponent))
		Expect(tr.CompositePhase).To(Equal("engine.Surging"))

		// Phase 2: engine done; decoder starts.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = false
			o.CurrentRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr = coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.DecoderComponent))
		Expect(tr.PreviousComponent).To(Equal(v1beta1.EngineComponent))
		Expect(tr.CompositePhase).To(Equal("decoder.Surging"))

		// Phase 3: decoder done → all done → Idle complete.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = false
			o.CurrentRevisionHash = "rev2"
		})
		tr = coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(tr.CompositePhase).To(Equal(string(v1beta1.CoordinationPhaseIdle)))
		Expect(tr.Message).To(ContainSubstring("complete"),
			"all-done case must surface 'complete' in the operator message")
	})

	ginkgo.It("S-10 first Component in Order fails → PreviousComponent empty", func() {
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.CompositePhase).To(Equal(v1beta1.CompositePhaseSequentialFailed))
		Expect(tr.PreviousComponent).To(BeEmpty(),
			"first Component in Order has no predecessor — PreviousComponent must be empty even on failure")
	})

	ginkgo.It("S-11 last Component reaches Idle → group Idle (no successor to await)", func() {
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		// All Components done on new revision.
		for _, c := range obs.Group.Order {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = false
				o.CurrentRevisionHash = "rev2"
				o.TargetRevisionHash = "rev2"
			})
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		// Composite is the plain Idle when all-done (not soak).
		Expect(tr.CompositePhase).To(Equal(string(v1beta1.CoordinationPhaseIdle)))
		Expect(tr.CurrentComponent).To(BeEmpty(),
			"no active or pending Component on completion")
		Expect(tr.PreviousComponent).To(BeEmpty(),
			"PreviousComponent is only populated mid-rollout or in soak windows")
	})

	ginkgo.It("S-15 default observation (all RolloutInFlight=false, all present) → 'complete' branch", func() {
		// Codifies actual state machine behavior: the initial state
		// where Components are observed but no rollout has happened
		// is INDISTINGUISHABLE from "all done" — both have RolloutInFlight
		// false on every Component, so `completedSequentialComponents`
		// counts to len(Order) and returns the "complete" branch. A
		// semantically distinct "awaiting first" state would require
		// the state machine to track a group-level target revision (or
		// for Components to carry a queued-but-not-started signal).
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(tr.Message).To(ContainSubstring("complete"))
	})

	ginkgo.It("S-15b 'awaiting first' branch fires when no Component in Order has been observed yet", func() {
		// Reach the `completed == 0` branch by leaving obs.Components
		// empty (the reconciler hasn't yet populated any Component
		// observations for the Sequential group's Order).
		obs := coordination.GroupObservation{
			Group: coordination.ResolvedGroup{
				Name:       "0",
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
			},
			Components: map[v1beta1.ComponentType]coordination.ComponentObservation{},
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(tr.Message).To(ContainSubstring("awaiting first"),
			"empty observations → 'awaiting first Component spec bump' message")
	})

	ginkgo.It("S-Pure-Aux Failed Component earlier in Order wins over active rolling later Component", func() {
		// 3-Component order: A → B → C. A is Failed; B is rolling.
		// Sequential.Failed must report A as CurrentComponent (not B).
		obs := seqObs(v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent)
		mutate(&obs, v1beta1.RouterComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.CompositePhase).To(Equal(v1beta1.CompositePhaseSequentialFailed))
		Expect(tr.CurrentComponent).To(Equal(v1beta1.RouterComponent),
			"Failed-block must dominate; later rolling Component is ignored")
	})

	ginkgo.It("S-Pure-Aux Components-not-in-Order are silently ignored by Sequential transition", func() {
		// Sequential's loops iterate over Group.Order, not
		// Group.Components. If observations contain Components that
		// aren't in Order, they don't influence transitions.
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		// Inject a stray router observation (rolling + failed) that
		// isn't in Order.
		obs.Components[v1beta1.RouterComponent] = coordination.ComponentObservation{
			Component:       v1beta1.RouterComponent,
			RolloutInFlight: true,
			Failed:          true,
		}
		// Decoder bumped (first in Order, rolling).
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		// Group is Surging on decoder, NOT Failed (router's Failed is
		// outside Order so ignored).
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(tr.CurrentComponent).To(Equal(v1beta1.DecoderComponent))
	})

	// ----------------------------------------------------------------
	// Known-gap PIts
	// ----------------------------------------------------------------

	ginkgo.PIt("G5: S-6 SequentialNextSpecBumpDeferred event when next Component spec changes while previous still rolling", func() {
		// User bumps engine's spec while decoder is still rolling.
		// Sequential MUST NOT start engine's surge — and should emit
		// a SequentialNextSpecBumpDeferred event so the operator knows
		// the bump is queued, not applied.
		fix := newCoordFixture(ns.Name, WithISVCName("s6"),
			WithSequentialGroup(v1beta1.DecoderComponent, v1beta1.EngineComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events)).To(Succeed())
			var saw bool
			for _, e := range events.Items {
				if e.InvolvedObject.Name == "s6" && e.Reason == "SequentialNextSpecBumpDeferred" {
					saw = true
				}
			}
			g.Expect(saw).To(BeTrue(),
				"SequentialNextSpecBumpDeferred event must fire when bumping engine while decoder still rolling")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("S-9 operator reverts decoder spec → group exits Failed back to Idle (closes G8 pure-function path)", func() {
		// Closes G8 at the state-machine level: sequentialTransition is
		// stateless w.r.t. Failed — when the caller clears Component.Failed
		// (on spec revert), the next transition naturally falls through
		// to Idle. No "explicit recovery branch" is needed; the spec's
		// G8 framing assumed sticky Failed state that doesn't exist.
		// Upstream caller responsibility: clear Component.Failed when
		// observing a spec revert. The state machine reflects whatever
		// the caller passes.
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))

		// Operator reverts: Failed clears, RolloutInFlight false.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.Failed = false
			o.RolloutInFlight = false
			o.TargetRevisionHash = o.CurrentRevisionHash
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle),
				"stateless state machine: clearing Failed must let group fall through to Idle")
	})

	ginkgo.It("S-12 Failed recovers in 3-Component Order → group falls through to 'complete' (closes G8)", func() {
		// 3-Component: router done, engine was Failed, decoder never
		// started. After clearing engine.Failed and marking it done on
		// rev2, the state machine sees all Components RolloutInFlight=false
		// and returns 'complete'. (The AwaitingNextComponent composite
		// only fires for transient observation gaps — see S-5/S-5b for
		// the codified behavior.)
		obs := seqObs(v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent)
		mutate(&obs, v1beta1.RouterComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = false
			o.CurrentRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))

		// Recover engine: clear Failed, mark done on rev2.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = false
			o.RolloutInFlight = false
			o.CurrentRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(tr.Message).To(ContainSubstring("complete"),
			"all Components RolloutInFlight=false → 'complete' branch (stateless recovery)")
	})

	ginkgo.PIt("G1: S-13 Sequential Order reordered mid-rollout (operator edit) → coordination handles gracefully", func() {
		// Operator edits Order mid-rollout: from [decoder, engine] to
		// [engine, decoder]. Reconciler must not panic; the active
		// Component logic must re-evaluate against the new Order.
		fix := newCoordFixture(ns.Name, WithISVCName("s13"),
			WithSequentialGroup(v1beta1.DecoderComponent, v1beta1.EngineComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "s13")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		cur := freshISVC(Default, ns.Name, "s13")
		cur.Spec.RolloutCoordination.Groups[0].Order =
			[]v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "s13")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Phase).
				NotTo(Equal(v1beta1.CoordinationPhaseFailed),
					"reordering Order must not trip Failed")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G3+G6: S-14 Sequential + RatioBalanced pacing → ratio computed per-Component-window", func() {
		// Sequential rolls one Component at a time, so RatioBalanced
		// pacing has to compute ratio against a per-Component window
		// rather than across the whole group. Today neither the
		// RatioBalanced pacing path (G3) nor the ObservedRatio status
		// writer (G6) is wired in.
		fix := newCoordFixture(ns.Name, WithISVCName("s14"),
			WithSequentialGroup(v1beta1.DecoderComponent, v1beta1.EngineComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "s14")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedRatio).NotTo(BeNil(),
				"RatioBalanced pacing must populate ObservedRatio")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
