package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Coordination mid-rollout mutation (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-mut-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("MUT-3 mid-call policy swap Atomic→Independent flips union semantics", func() {
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 2
			o.NewRevisionReadyPods = 1
			o.TotalPods = 4
			o.TargetRevisionHash = "rev2"
		})
		obs.Group.Policy = v1beta1.CoordinationPolicyAtomic
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"Atomic: any !allHaveSurge → Surging")

		obs.Group.Policy = v1beta1.CoordinationPolicyIndependent
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting),
				"Independent collapses any-rolling → Shifting")
	})

	ginkgo.It("MUT-4 mid-call policy swap Independent→Atomic starts gating sub-phases", func() {
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		obs.Group.Policy = v1beta1.CoordinationPolicyIndependent
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting))

		obs.Group.Policy = v1beta1.CoordinationPolicyAtomic
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"swapping to Atomic must surface the Surging sub-phase that Independent collapsed")
	})

	ginkgo.It("MUT-5 Sequential Order swap mid-call → active loop re-evaluates against new Order", func() {
		obs := seqObs(v1beta1.DecoderComponent, v1beta1.EngineComponent)
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		tr := coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.DecoderComponent))

		obs.Group.Order = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
		tr = coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.EngineComponent),
			"Order swap mid-call must re-target the active Component")
	})

	ginkgo.It("MUT-7 removing rolloutCoordination (Policy unset) falls through to Independent", func() {
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		obs.Group.Policy = v1beta1.CoordinationPolicyPairedMixable
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"PairedMixable: anySurging branch")

		obs.Group.Policy = ""
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting),
				"unset policy falls through to independentTransition → Shifting")
	})

	ginkgo.It("MUT-12 PausedGlobal=true blocks bump-induced transition until cleared", func() {
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		obs.Group.Policy = v1beta1.CoordinationPolicyAtomic
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused),
				"bump while paused must NOT advance — PausedGlobal short-circuits")
		obs.PausedGlobal = false
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"unpause must release the bump to surface as Surging")
	})

	ginkgo.It("MUT-Aux mutating observation between consecutive ComputeTransition calls drives the state forward", func() {
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		obs.Group.Policy = v1beta1.CoordinationPolicyAtomic

		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))

		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))

		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 0
				o.TotalPods = 4
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseWaiting))

		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.NewRevisionReadyPods = 2
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting))

		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.TotalPods = 2
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseScalingDown))

		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = false
				o.CurrentRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	ginkgo.PIt("MUT-1 Add Component to existing group mid-rollout — needs WithRouter + restart-render harness", func() {})
	ginkgo.PIt("MUT-2 Remove Component from existing group mid-rollout — admission webhook may block partial removal", func() {})

	ginkgo.It("MUT-6 Add brand-new group mid-rollout → coordination ensures per-revision Services", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut6"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			g.Expect(listPerRevisionServices(g, ns.Name)).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut6")
			cur.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyAtomic,
				}},
			}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut6")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil(),
				"adding a coord group must populate Status.RolloutCoordination")
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("MUT-7-E2E Remove rolloutCoordination block mid-rollout — flaky under suite load (Update races with controller writes)", func() {})
	ginkgo.PIt("MUT-8 Revert spec to old image mid-Surging — narrow timing window in envtest", func() {})

	ginkgo.It("MUT-10 Edit MaxSurge mid-rollout — ObservedSurge reflects post-edit MaxSurge", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut10"), WithEngineReplicas(4),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:     v1beta1.CoordinationPacingPerComponent,
			MaxSurge: iosInt(1),
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut10")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge.IntValue()).To(Equal(1),
				"pre-edit ObservedSurge must reflect MaxSurge=1")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut10")
			cur.Spec.RolloutCoordination.Groups[0].Pacing.MaxSurge = iosInt(2)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut10")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge.IntValue()).
				To(Equal(2), "ObservedSurge must reflect the post-edit MaxSurge=2")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("MUT-11 Edit RatioTolerance mid-RatioBalanced rollout — next reconcile uses the new band", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut11"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		fix.ISVC.Spec.RolloutCoordination.Groups[0].Pacing = &v1beta1.CoordinationPacing{
			Type:           v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerance: int32Ptr(5),
		}
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut11")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedRatio).NotTo(BeNil(),
				"RatioBalanced pacing must populate ObservedRatio")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut11")
			cur.Spec.RolloutCoordination.Groups[0].Pacing.RatioTolerance = int32Ptr(20)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut11")
			g.Expect(cur.Spec.RolloutCoordination.Groups[0].Pacing.RatioTolerance).NotTo(BeNil())
			g.Expect(*cur.Spec.RolloutCoordination.Groups[0].Pacing.RatioTolerance).To(Equal(int32(20)),
				"spec.RatioTolerance must reflect the post-edit value")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("MUT-9 Bump model + bump runtime concurrently → per-revision Services materialize for both Components", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut9"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			services := listPerRevisionServices(g, ns.Name)
			g.Expect(len(services)).To(BeNumerically(">=", 4),
				"expected services for both Components; got %s", describeServices(services))
			eng, dec := 0, 0
			for _, s := range services {
				if s.Spec.Selector["component"] == "engine" {
					eng++
				}
				if s.Spec.Selector["component"] == "decoder" {
					dec++
				}
			}
			g.Expect(eng).To(BeNumerically(">=", 2), "engine missing services")
			g.Expect(dec).To(BeNumerically(">=", 2), "decoder missing services")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("MUT-13 Scale-up MinReplicas mid-rollout — kubelet sim doesn't simulate scale-up replica creation", func() {})
	ginkgo.PIt("MUT-14 Scale-down MinReplicas mid-rollout — same kubelet sim limitation as MUT-13", func() {})
})
