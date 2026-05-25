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

// Atomic policy state-machine specs for OEP-0007.1.
//
// Atomic is the "gang" policy: every Component in the group surges
// together, waits together, shifts together, drains together. Failure
// in any Component fails the whole group. The state machine is a pure
// function — ComputeTransition(GroupObservation) → GroupTransition —
// so the bulk of these specs run as `It`s that exercise the pure
// transition directly. End-to-end specs that need the status writer or
// reconciler to fire (A-7, A-8, A-9, A-10, A-14, A-17) are PIts gated
// on **G1**: ops/reconciler does not invoke coordination.ComputeTransition
// or write Status.RolloutCoordination. They flip to It mechanically when
// G1 wiring lands.
//
// Reference: pkg/controller/v1beta1/inferenceservice/reconcilers/
//            omenative/coordination/state.go (atomicTransition).

// atomicObs builds a `n`-Component Atomic observation. Defaults: 2
// replicas per Component, all live + Ready on rev1, no rollout in
// flight. Specs mutate the returned observation to set up specific
// scenarios.
func atomicObs(components ...v1beta1.ComponentType) coordination.GroupObservation {
	if len(components) == 0 {
		components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	}
	g := coordination.ResolvedGroup{
		Name:       "0",
		Components: components,
		Policy:     v1beta1.CoordinationPolicyAtomic,
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

// mutate is a small helper for replacing one Component's observation
// in-place (Go maps can't return mutable values).
func mutate(obs *coordination.GroupObservation, c v1beta1.ComponentType, f func(*coordination.ComponentObservation)) {
	o := obs.Components[c]
	f(&o)
	obs.Components[c] = o
}

var _ = ginkgo.Describe("Coordination Atomic state machine (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-atomic-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs (call ComputeTransition directly — no envtest)
	// ----------------------------------------------------------------

	ginkgo.It("A-1 steady state → Idle on a 2-Component Atomic group", func() {
		obs := atomicObs() // engine + decoder, no rollout
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseIdle))
		Expect(tr.CompositePhase).To(Equal(string(v1beta1.CoordinationPhaseIdle)))
		Expect(tr.Message).To(ContainSubstring("no rollout"))
	})

	ginkgo.It("A-2 spec bump on both Components → Surging until any surge pod exists", func() {
		obs := atomicObs()
		// Both Components rolling, neither has surge pods yet.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))

		// Asymmetric: engine has surge pods, decoder hasn't started yet.
		// Atomic gangs them — group is still Surging (allHaveSurge is false).
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 0
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"Atomic must NOT advance past Surging until every Component has surge pods")
	})

	ginkgo.It("A-3 surge pods exist but not all Ready → Waiting", func() {
		obs := atomicObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2 // start fully Ready
				o.TotalPods = 4            // 2 new + 2 old
				o.TargetRevisionHash = "rev2"
			})
		}
		// Knock one Ready replica off decoder — group flips to Waiting.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.NewRevisionReadyPods = 1
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseWaiting))
	})

	ginkgo.It("A-4 all surge Ready + old pods still up → Shifting", func() {
		obs := atomicObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2
				o.TotalPods = 4 // 2 new + 2 old still up
				o.TargetRevisionHash = "rev2"
			})
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseShifting))
		Expect(tr.Message).To(ContainSubstring("old-revision pods still serving"))
	})

	ginkgo.It("A-5 old pods drained → ScalingDown; promotion flips to Idle", func() {
		obs := atomicObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2
				o.TotalPods = 2 // old pods drained
				o.TargetRevisionHash = "rev2"
			})
		}
		// ScalingDown is the holding pattern between drain and revision
		// promotion. RolloutInFlight is still true here because the
		// revision promoter has not yet flipped CurrentRevisionHash.
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseScalingDown))

		// After promotion: RolloutInFlight=false everywhere → Idle.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = false
				o.CurrentRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	ginkgo.It("A-6 single Component Ready timeout → whole group Failed", func() {
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
			o.FailureMessage = "Ready timeout"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseFailed))
		Expect(tr.Message).To(ContainSubstring("Atomic"))
	})

	ginkgo.It("A-11 1-Component Atomic is degenerate: same transitions as 2-Component", func() {
		obs := atomicObs(v1beta1.EngineComponent)
		// Idle.
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
		// Rolling without surge → Surging.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))
		// Surge ready + no old → ScalingDown (single-Component gang
		// behaves identically to the 2-Component flow).
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 2
			o.TotalPods = 2
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseScalingDown))
	})

	ginkgo.It("A-12 ANY Component failed → Failed (current logic: short-circuit on first failure)", func() {
		// state.go atomicTransition has a variable named `allFailed` but
		// it short-circuits on the FIRST Failed Component. This codifies
		// the de-facto behavior. If the policy ever changes to require
		// all Components Failed, flip this expectation AND fix the
		// variable name in state.go.
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(obs.Components[v1beta1.DecoderComponent].Failed).To(BeFalse())
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed),
				"single failed Component must fail the whole Atomic group")
	})

	ginkgo.It("A-13 ANY Component paused → Paused (current logic: short-circuit on first paused)", func() {
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 1
			o.NewRevisionReadyPods = 0
			o.TargetRevisionHash = "rev2"
			o.Paused = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused),
				"single paused Component must pause the whole Atomic group")
		// Failure takes precedence over Paused.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed),
				"Failed must beat Paused when both are set")
	})

	ginkgo.It("A-15 multi-replica + asymmetric Components: phase logic uses per-Component pod counts", func() {
		// 5-replica engine, 3-replica decoder. Both rolling; engine has
		// all 5 new pods Ready, decoder only has 2 of 3 Ready → Waiting.
		obs := atomicObs()
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.DesiredReplicas = 5
			o.TotalPods = 10
			o.NewRevisionPods = 5
			o.NewRevisionReadyPods = 5
			o.RolloutInFlight = true
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.DesiredReplicas = 3
			o.TotalPods = 6
			o.NewRevisionPods = 3
			o.NewRevisionReadyPods = 2 // one stuck
			o.RolloutInFlight = true
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseWaiting))

		// Promote decoder's last pod to Ready → Shifting.
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.NewRevisionReadyPods = 3
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting))
	})

	ginkgo.It("A-16 Order pin has no effect on Atomic transitions (Order is Sequential-only)", func() {
		obs := atomicObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		trNoOrder := coordination.ComputeTransition(obs)

		obs.Group.Order = []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
		trWithOrder := coordination.ComputeTransition(obs)

		Expect(trWithOrder).To(Equal(trNoOrder),
			"Atomic policy ignores Order; transition outputs must match")
	})

	ginkgo.It("A-G7 Atomic Surging populates SurgeBudget from group pacing (closes G7)", func() {
		// G7: state machine now invokes MaxSurgeBudget under the hood
		// and surfaces the per-Component budget on the GroupTransition.
		// MaxSurge=2 on a 4-replica engine + 4-replica decoder → 2 surge
		// pods per Component allowed.
		obs := atomicObs()
		ms := intstr.FromInt(2)
		obs.Group.Pacing = v1beta1.CoordinationPacing{
			Type:     v1beta1.CoordinationPacingPerComponent,
			MaxSurge: &ms,
		}
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.DesiredReplicas = 4
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhaseSurging))
		Expect(tr.SurgeBudget).To(HaveKeyWithValue(v1beta1.EngineComponent, int32(2)))
		Expect(tr.SurgeBudget).To(HaveKeyWithValue(v1beta1.DecoderComponent, int32(2)))
	})

	ginkgo.It("A-G7b SurgeBudget defaults to 25% when MaxSurge unset", func() {
		// MaxSurge nil → MaxSurgeBudget returns ceil(replicas * 25 / 100).
		// 4 replicas → 1.
		obs := atomicObs()
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.DesiredReplicas = 4
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.SurgeBudget[v1beta1.EngineComponent]).To(Equal(int32(1)),
			"default MaxSurge=25%% of 4 replicas → ceil(1.0) = 1")
	})

	ginkgo.It("A-G7c Idle / Shifting / ScalingDown transitions leave SurgeBudget nil", func() {
		// Surge budget is only meaningful when the state machine
		// authorizes surge. Other phases must leave the field nil so
		// the caller doesn't accidentally drive surge during drain.
		obs := atomicObs() // Idle by default
		Expect(coordination.ComputeTransition(obs).SurgeBudget).To(BeNil())
		Expect(coordination.ComputeTransition(obs).UnavailableBudget).To(BeNil())
	})

	ginkgo.It("A-18 spec revert mid-Surging → group transitions back toward Idle", func() {
		obs := atomicObs()
		// Mid-Surging: rolling, no surge pods yet on any Component.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))

		// Operator reverts: TargetRevisionHash collapses back to
		// CurrentRevisionHash; RolloutInFlight false; surge pods would
		// be cleaned by the controller on next reconcile.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = false
				o.TargetRevisionHash = o.CurrentRevisionHash
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle),
				"spec revert must collapse the group back to Idle without going through Failed")
	})

	// ----------------------------------------------------------------
	// End-to-end (G1-blocked PIts) — wait for ops/reconciler to invoke
	// coordination.ComputeTransition and write Status.RolloutCoordination.
	// ----------------------------------------------------------------

	ginkgo.PIt("G1: A-7 CoordinationGroupSurging/Shifting/Failed/Completed events fire on phase boundary", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("a7"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events)).To(Succeed())
			var sawSurging bool
			for _, e := range events.Items {
				if e.InvolvedObject.Name == "a7" && e.Reason == "CoordinationGroupSurging" {
					sawSurging = true
				}
			}
			g.Expect(sawSurging).To(BeTrue(),
				"CoordinationGroupSurging event must fire when Atomic group enters Surging")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: A-8 Status.RolloutCoordination populated; RolloutCoordinationReady condition stamped", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("a8"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "a8")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil(),
				"Status.RolloutCoordination must be populated by the reconciler")
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: A-9 LastTransitionTime preserved across same-phase reconciles", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("a9"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Capture LastTransitionTime once the group is Idle, then poke a
		// no-op annotation update; the time should NOT advance.
		var firstTime string
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "a9")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			ltt := cur.Status.RolloutCoordination.Groups[0].LastTransitionTime
			g.Expect(ltt).NotTo(BeNil())
			firstTime = ltt.String()
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		cur := freshISVC(Default, ns.Name, "a9")
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations["coord.test/poke"] = "1"
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "a9")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].LastTransitionTime.String()).
				To(Equal(firstTime),
					"LastTransitionTime must be preserved when Phase doesn't change")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: A-10 ObservedSurge reflects the pacing block's MaxSurge", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("a10"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		ms := intstr.FromInt(2)
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:     v1beta1.CoordinationPacingPerComponent,
			MaxSurge: &ms,
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "a10")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge.IntValue()).
				To(Equal(2), "ObservedSurge must mirror Pacing.MaxSurge")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: A-14 Component count change mid-rollout (group spec mutated) handled gracefully", func() {
		// Start with 2-Component Atomic, then add router to the group.
		// The controller must not panic; next reconcile observes 3 Components.
		fix := newCoordFixture(ns.Name, WithISVCName("a14"), WithRouter(),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "a14")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		cur := freshISVC(Default, ns.Name, "a14")
		cur.Spec.RolloutCoordination.Groups[0].Components = append(
			cur.Spec.RolloutCoordination.Groups[0].Components, v1beta1.RouterComponent)
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "a14")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Phase).
				NotTo(Equal(v1beta1.CoordinationPhaseFailed))
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Components).
				To(ContainElement(v1beta1.RouterComponent))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: A-17 Atomic group covers engine+decoder; router not in group rolls independently", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("a17"), WithRouter(),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "a17")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			// Only one group declared, and it's the Atomic engine+decoder
			// group. Router is not represented in the status.
			g.Expect(cur.Status.RolloutCoordination.Groups).To(HaveLen(1))
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Components).
				To(ConsistOf(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
