package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// RollingUpdate policy state-machine specs for OEP-0007.1.
//
// RollingUpdate lets each Component walk its own state machine in
// parallel. The GROUP phase is the union of per-Component states:
//
//   anySurging       → Surging
//   anyWaiting       → Waiting
//   anyScalingDown   → ScalingDown
//   default (rolling but past the above) → Shifting
//   not rolling at all → Idle
//   any Failed       → Failed
//
// Most specs run as pure-function `It`s. End-to-end PIts (PM-5, PM-8)
// are blocked on G5 (MixedPairingObserved event emission) and G1
// (reconciler not invoking ComputeTransition / pacing math).
//
// Reference: state.go rollingUpdateTransition.

// pmObs builds an N-Component RollingUpdate observation. Defaults: 2
// replicas per Component, all live + Ready on rev1, no rollout in
// flight. Specs mutate as needed.
func pmObs(components ...v1beta1.ComponentType) coordination.GroupObservation {
	if len(components) == 0 {
		components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	}
	g := coordination.ResolvedGroup{
		Name:       "0",
		Components: components,
		Policy:     v1beta1.CoordinationPolicyRollingUpdate,
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

var _ = ginkgo.Describe("Coordination RollingUpdate state machine (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-pm-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs
	// ----------------------------------------------------------------

	ginkgo.It("PM-1 default Shifting: all rolling Components past Surging/Waiting/ScalingDown", func() {
		// To reach the default Shifting branch every rolling Component
		// must have all new pods ready AND old pods drained.
		obs := pmObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2
				o.TotalPods = 2 // old already drained
				o.TargetRevisionHash = "rev2"
			})
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseShifting))
		Expect(tr.Message).To(ContainSubstring("traffic"),
			"Shifting message must reference the traffic-write step")
	})

	ginkgo.It("PM-1a Surging wins over Waiting and ScalingDown in the union", func() {
		obs := pmObs()
		// Engine: not surged yet (anySurging=true).
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		// Decoder: surge pods exist but not all Ready (anyWaiting=true).
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 1
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"Surging must take precedence over Waiting in RollingUpdate union")
	})

	ginkgo.It("PM-1b Waiting wins over ScalingDown in the union", func() {
		obs := pmObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 1 // anyWaiting
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 4 // anyScalingDown
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseWaiting),
				"Waiting must take precedence over ScalingDown in RollingUpdate union")
	})

	ginkgo.It("PM-2 per-Component independence: idle Components don't influence the union", func() {
		obs := pmObs()
		// Engine done (RolloutInFlight=false); decoder mid-Surging.
		// The group phase should reflect decoder's state.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(obs.Components[v1beta1.EngineComponent].RolloutInFlight).To(BeFalse(),
			"engine remains Idle while decoder rolls — that's the point of RollingUpdate")
	})

	ginkgo.It("PM-3 single Component failed → whole group Failed", func() {
		obs := pmObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(tr.Message).To(ContainSubstring("RollingUpdate"))
	})

	ginkgo.It("PM-3a Failed beats every other state in the union", func() {
		obs := pmObs()
		// Decoder mid-Surging; engine failed. Failed must still win.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))
	})

	ginkgo.It("PM-4 mixed v1+v2 engine + v1+v2 decoder allowed (no auto-rollback)", func() {
		// Both Components at intermediate state: 2 new pods + 2 old still
		// up. anyScalingDown=true so group is ScalingDown — never
		// RollingBack. RollingUpdate explicitly permits cross-revision
		// pod coexistence.
		obs := pmObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2
				o.TotalPods = 4 // 2 new + 2 old coexisting
				o.TargetRevisionHash = "rev2"
			})
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseScalingDown),
			"RollingUpdate must NOT auto-rollback when old + new pods coexist")
		Expect(tr.Phase).NotTo(Equal(v1beta1.CoordinationPhaseRollingBack))
	})

	ginkgo.It("PM-6 all Components on same revision → Idle (no rollout in flight)", func() {
		obs := pmObs() // default: not rolling
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(coordination.ComputeTransition(obs).Message).
			To(ContainSubstring("no rollout"))
	})

	ginkgo.It("PM-7 3-Component RollingUpdate: each Component rolls independently in the union", func() {
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent)
		// Engine: surging. Decoder: waiting. Router: scaling down.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 1
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.RouterComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
		})
		// Surging wins.
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))

		// Advance engine past Surging → Waiting wins (decoder Waiting,
		// router ScalingDown).
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 2
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseWaiting))
	})

	ginkgo.It("PM-12 empty Components map → Idle (degenerate observation)", func() {
		// Empty RollingUpdate group: no Components observed, no rollout
		// in flight. BlueGreen and Independent handle this; RollingUpdate
		// must too.
		obs := coordination.GroupObservation{
			Group: coordination.ResolvedGroup{
				Name:       "0",
				Components: []v1beta1.ComponentType{},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
			},
			Components: map[v1beta1.ComponentType]coordination.ComponentObservation{},
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle),
				"empty Components map must collapse to Idle, not nil-deref or panic")
	})

	// ----------------------------------------------------------------
	// Known-gap PIts
	// ----------------------------------------------------------------

	ginkgo.It("PM-9 ALL Components paused (not just one) → group Paused", func() {
		// RollingUpdate's Paused semantics differ from BlueGreen: gang
		// policies short-circuit on any-paused, but RollingUpdate lets
		// each Component progress independently. So we only Pause the
		// group when EVERY Component is paused.
		obs := pmObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 1
				o.NewRevisionReadyPods = 0
				o.TargetRevisionHash = "rev2"
				o.Paused = true
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused),
				"RollingUpdate with ALL Components Paused must return Paused")
	})

	ginkgo.It("PM-5 MixedPairingObserved event fires when distinct (engine-rev, decoder-rev) pairs observed", func() {
		// RollingUpdate explicitly permits running engine and decoder
		// on different revisions simultaneously. When the controller
		// observes this state (e.g. engine on rev1+rev2, decoder on
		// rev1+rev2), it should emit a MixedPairingObserved event for
		// operator visibility. Today no such event exists — coordination
		// has no event recorder wired in. Gap G5.
		fix := newCoordFixture(ns.Name, WithISVCName("pm5"),
			WithRollingUpdateGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events)).To(Succeed())
			var saw bool
			for _, e := range events.Items {
				if e.InvolvedObject.Name == "pm5" && e.Reason == "MixedPairingObserved" {
					saw = true
				}
			}
			g.Expect(saw).To(BeTrue(),
				"MixedPairingObserved event must fire when distinct cross-Component revisions coexist")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PM-8 RollingUpdate + per-Component pacing — each Component honors its own pacing budget", func() {
		// RollingUpdate's pacing block is interpreted PER COMPONENT
		// (vs. RatioBalanced's cross-Component snap). With MaxSurge=1
		// on each Component, engine and decoder each get exactly 1
		// surge pod regardless of the other's progress.
		fix := newCoordFixture(ns.Name, WithISVCName("pm8"),
			WithRollingUpdateGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		ms := intstr.FromInt(1)
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:     v1beta1.CoordinationPacingPerComponent,
			MaxSurge: &ms,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pm8")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge.IntValue()).To(Equal(1))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
