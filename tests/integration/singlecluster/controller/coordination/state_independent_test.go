package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Independent policy state-machine specs for OEP-0007.1.
//
// Independent is the default (also the fallback when no group is
// declared at all). Each Component runs its own per-Component
// transitions; the GROUP phase is the trivial union:
//
//   any Failed       → Failed
//   not rolling      → Idle
//   any rolling      → Shifting   (no Surging/Waiting/ScalingDown sub-phases)
//
// Independent intentionally collapses to a single "rolling" phase
// because the group does not gate transitions across Components — each
// Component reports its own per-Component status via the OMENative
// subfield; the group-level Phase is just a roll-up.
//
// Reference: state.go independentTransition.

// indepObs builds an N-Component Independent observation. Defaults
// match other state-machine builders: 2 replicas, all at rev1, no
// rollout in flight.
func indepObs(components ...v1beta1.ComponentType) coordination.GroupObservation {
	if len(components) == 0 {
		components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	}
	g := coordination.ResolvedGroup{
		Name:       "0",
		Components: components,
		Policy:     v1beta1.CoordinationPolicyIndependent,
	}
	obs := coordination.GroupObservation{
		Group:      g,
		Components: map[v1beta1.ComponentType]coordination.ComponentObservation{},
	}
	for _, c := range components {
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

var _ = ginkgo.Describe("Coordination Independent state machine (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-indep-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs
	// ----------------------------------------------------------------

	ginkgo.It("I-1 implicit Independent (no policy set) collapses to independentTransition", func() {
		// Policy=="" is the fallthrough/default branch in ComputeTransition
		// (`case independent; fallthrough; default:`). Codify that an
		// empty/unspecified policy follows Independent semantics.
		obs := indepObs()
		obs.Group.Policy = "" // unset
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 1
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseShifting),
			"unset Policy must default to independentTransition (any rolling → Shifting)")
	})

	ginkgo.It("I-2 explicit Independent group: status reports state but applies no gating", func() {
		// Independent's job is reporting, not gating. Even with one
		// Component "Surging-shaped" and one "ScalingDown-shaped"
		// (which BlueGreen/RollingUpdate would distinguish into sub-phases),
		// Independent reports a single Shifting.
		obs := indepObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0 // not yet surged
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 4 // would be ScalingDown in Atomic
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseShifting),
			"Independent flattens the Surging/Waiting/ScalingDown distinction into Shifting")
	})

	ginkgo.It("I-3 each Component rolls on its own pace; group remains Shifting", func() {
		// One Component still rolling, the other already done. Group
		// stays Shifting until ALL Components stop rolling.
		obs := indepObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.CurrentRevisionHash = "rev2"
			o.TargetRevisionHash = "rev2"
			o.RolloutInFlight = false
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 1
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting))

		// Decoder finishes too → Idle.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.CurrentRevisionHash = "rev2"
			o.TargetRevisionHash = "rev2"
			o.RolloutInFlight = false
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	ginkgo.It("I-4 ANY Component failed → group Failed", func() {
		obs := indepObs()
		// Decoder rolling, engine fails.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 1
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(tr.Message).To(ContainSubstring("Independent"))
	})

	ginkgo.It("I-5 ALL Components paused → group Paused (matches RollingUpdate G12 semantics)", func() {
		// Independent has per-Component independence like RollingUpdate,
		// so the Paused gate uses "ALL paused → Paused" (not BlueGreen's
		// "ANY paused"). Mirrors the G12 fix shipped for
		// rollingUpdateTransition.
		obs := indepObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 1
				o.TargetRevisionHash = "rev2"
				o.Paused = true
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused),
				"Independent with ALL Components paused must return Paused")
	})

	ginkgo.It("I-5b one Component paused, others rolling → group keeps reporting Shifting (NOT Paused)", func() {
		// Mirror of I-5: with only some Components paused, Independent
		// continues to report the group as rolling. Codifies that a
		// per-Component pause doesn't gang-stop independent rollouts.
		obs := indepObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 1
			o.TargetRevisionHash = "rev2"
			o.Paused = true
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 1
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting))
	})

	ginkgo.It("I-6 Independent never returns Surging/Waiting/ScalingDown — only Idle/Shifting/Failed/Paused", func() {
		// Exhaustive: walk through observation shapes that would yield
		// Surging/Waiting/ScalingDown under BlueGreen/RollingUpdate, and
		// confirm Independent collapses them all to Shifting.
		shouldBeShifting := []func(*coordination.ComponentObservation){
			func(o *coordination.ComponentObservation) {
				// BlueGreen-Surging shape: rolling, no surge pods yet.
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			},
			func(o *coordination.ComponentObservation) {
				// BlueGreen-Waiting shape: surge pods exist but not Ready.
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 1
				o.TotalPods = 4
				o.TargetRevisionHash = "rev2"
			},
			func(o *coordination.ComponentObservation) {
				// BlueGreen-Shifting shape: surge ready + old still up.
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2
				o.TotalPods = 4
				o.TargetRevisionHash = "rev2"
			},
			func(o *coordination.ComponentObservation) {
				// BlueGreen-ScalingDown shape: surge ready + old drained.
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2
				o.TotalPods = 2
				o.TargetRevisionHash = "rev2"
			},
		}
		for _, shape := range shouldBeShifting {
			obs := indepObs()
			mutate(&obs, v1beta1.EngineComponent, shape)
			Expect(coordination.ComputeTransition(obs).Phase).
				To(Equal(v1beta1.CoordinationPhaseShifting),
					"Independent must collapse Surging/Waiting/ScalingDown into Shifting")
		}
	})

	ginkgo.It("I-7 spec revert on one Component leaves the other unaffected", func() {
		// Decoder rolling on rev2; engine spec is reverted (no rollout).
		// Group must reflect decoder's rolling state — Shifting — even
		// after engine's revert.
		obs := indepObs()
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 1
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			// Reverted: TargetRevisionHash matches CurrentRevisionHash.
			o.RolloutInFlight = false
			o.TargetRevisionHash = "rev1"
			o.CurrentRevisionHash = "rev1"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting),
				"engine's revert is observed; decoder still rolling keeps group at Shifting")
	})

	ginkgo.It("I-8 Component recovers from Failed → group reflects recovery", func() {
		// Engine failed → group Failed. Then operator action clears the
		// failure (Failed=false). Group must reflect the recovery —
		// independentTransition has no sticky Failed state.
		obs := indepObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))

		// Clear the failure; engine is now at rest on its current rev.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = false
			o.RolloutInFlight = false
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle),
				"clearing Failed must allow the group to return to Idle (no sticky failure state)")
	})

	ginkgo.It("I-Pure-Aux Failed beats Paused in Independent (same precedence as RollingUpdate)", func() {
		obs := indepObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 1
				o.TargetRevisionHash = "rev2"
				o.Paused = true
			})
		}
		// One Component also Failed.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed),
				"Failed must beat Paused regardless of how many Components are paused")
	})
})
