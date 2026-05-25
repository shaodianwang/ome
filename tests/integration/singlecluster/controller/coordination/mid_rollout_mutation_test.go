package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Mid-rollout mutation specs for OEP-0007.1.
//
// Most MUT-* are inherently end-to-end (operator edits the ISVC mid-rollout
// and the controller must react). With G1 unfixed, the reconciler never
// invokes coordination.ComputeTransition during requeues — every E2E
// mutation test is PIt'd until G1 lands.
//
// Pure-function tests cover the small set of "what does ComputeTransition
// return when X changes between calls" semantics: policy swap, Order
// change, mutation of the observation between consecutive calls. These
// pin the contract that the state machine is stateless and the next
// reconcile fully reflects the latest observation.

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

	// ----------------------------------------------------------------
	// Pure-function specs: state machine reacts to observation mutations
	// ----------------------------------------------------------------

	ginkgo.It("MUT-3 mid-call policy swap Atomic→Independent flips union semantics", func() {
		// Same observation; just change Group.Policy and re-call
		// ComputeTransition. Atomic gangs surge → Surging if any not
		// yet surged; Independent collapses to Shifting if any rolling.
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		// One Component surging (no surge pods), one with surge pods +
		// not Ready (Waiting-shape).
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

		// Swap to Independent on the same observation.
		obs.Group.Policy = v1beta1.CoordinationPolicyIndependent
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting),
				"Independent collapses any-rolling → Shifting")
	})

	ginkgo.It("MUT-4 mid-call policy swap Independent→Atomic starts gating sub-phases", func() {
		// Independent only reports Idle/Shifting/Failed/Paused. Atomic
		// adds Surging/Waiting/ScalingDown distinctions. Same observation
		// produces different phase shape under the two policies.
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
		// Order [decoder, engine]: decoder rolls first. Swap Order to
		// [engine, decoder]: now engine should be the active Component.
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
		// With Order [decoder, engine], active loop picks decoder.
		tr := coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.DecoderComponent))

		// Swap Order: [engine, decoder]. Active loop picks engine first.
		obs.Group.Order = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
		tr = coordination.ComputeTransition(obs)
		Expect(tr.CurrentComponent).To(Equal(v1beta1.EngineComponent),
			"Order swap mid-call must re-target the active Component")
	})

	ginkgo.It("MUT-7 removing rolloutCoordination (Policy unset) falls through to Independent", func() {
		// When the user removes the rolloutCoordination block, the
		// controller's call site sees no policy → fallthrough/default
		// independentTransition. The state machine has no sticky
		// state, so this is a pure-function call:
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		// Start with PairedMixable.
		obs.Group.Policy = v1beta1.CoordinationPolicyPairedMixable
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"PairedMixable: anySurging branch")

		// Clear the policy.
		obs.Group.Policy = ""
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting),
				"unset policy falls through to independentTransition → Shifting")
	})

	ginkgo.It("MUT-12 PausedGlobal=true blocks bump-induced transition until cleared", func() {
		// Existing observation is Idle. Operator bumps spec (engine
		// gets new TargetRevisionHash, RolloutInFlight=true) and pause
		// is set simultaneously. PausedGlobal wins.
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		obs.Group.Policy = v1beta1.CoordinationPolicyAtomic
		// Bump engine.
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
		// Pause set.
		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused),
				"bump while paused must NOT advance — PausedGlobal short-circuits")
		// Unpause: bump now takes effect.
		obs.PausedGlobal = false
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"unpause must release the bump to surface as Surging")
	})

	ginkgo.It("MUT-Aux mutating observation between consecutive ComputeTransition calls drives the state forward", func() {
		// Walk through a full Atomic surge progression by mutating the
		// observation between calls. Pins that the state machine is
		// stateless and the latest observation fully drives the phase.
		obs := pmObs(v1beta1.EngineComponent, v1beta1.DecoderComponent)
		obs.Group.Policy = v1beta1.CoordinationPolicyAtomic

		// Step 0: nothing rolling → Idle.
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))

		// Step 1: rolling, no surge → Surging.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))

		// Step 2: surge pods exist, not Ready → Waiting.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 0
				o.TotalPods = 4
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseWaiting))

		// Step 3: surge Ready, old still up → Shifting.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.NewRevisionReadyPods = 2
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseShifting))

		// Step 4: old drained → ScalingDown.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.TotalPods = 2
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseScalingDown))

		// Step 5: revision promoted → Idle.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = false
				o.CurrentRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseIdle))
	})

	// ----------------------------------------------------------------
	// End-to-end PIts (G1-blocked + others)
	// ----------------------------------------------------------------

	ginkgo.PIt("G1: MUT-1 Add Component to existing group mid-rollout → coordination picks up next reconcile", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut1"), WithRouter(),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Wait for initial Idle (currently won't write Status w/o G1).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut1")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Add router to the group.
		cur := freshISVC(Default, ns.Name, "mut1")
		cur.Spec.RolloutCoordination.Groups[0].Components = append(
			cur.Spec.RolloutCoordination.Groups[0].Components, v1beta1.RouterComponent)
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut1")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Components).
				To(ContainElement(v1beta1.RouterComponent))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: MUT-2 Remove Component from existing group mid-rollout → coordination releases", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut2"), WithRouter(),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		cur := freshISVC(Default, ns.Name, "mut2")
		cur.Spec.RolloutCoordination.Groups[0].Components =
			[]v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut2")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Components).
				NotTo(ContainElement(v1beta1.RouterComponent))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: MUT-6 Add brand-new group mid-rollout → coordination ensures per-revision Services", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut6"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		cur := freshISVC(Default, ns.Name, "mut6")
		cur.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyAtomic,
			}},
		}
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			services := listPerRevisionServices(g, ns.Name)
			g.Expect(services).NotTo(BeEmpty(),
				"adding a coord group must trigger per-revision Service creation")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: MUT-7 Remove rolloutCoordination block mid-rollout → groups drop from status", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut7"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		cur := freshISVC(Default, ns.Name, "mut7")
		cur.Spec.RolloutCoordination = nil
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut7")
			if cur.Status.RolloutCoordination != nil {
				g.Expect(cur.Status.RolloutCoordination.Groups).To(BeEmpty(),
					"removing the coord block must drop group statuses")
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: MUT-8 Revert spec to old image mid-Surging → coordination cancels rollout", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mut8"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut8")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Bump.
		cur := freshISVC(Default, ns.Name, "mut8")
		cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		// Revert immediately.
		cur = freshISVC(Default, ns.Name, "mut8")
		cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:test"
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut8")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"revert during Surging must collapse the group back to Idle")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G7: MUT-10 Edit MaxSurge mid-rollout → next reconcile uses new budget", func() {
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

		// Bump MaxSurge mid-rollout.
		cur := freshISVC(Default, ns.Name, "mut10")
		cur.Spec.RolloutCoordination.Groups[0].Pacing.MaxSurge = iosInt(2)
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "mut10")
			g.Expect(cur.Status.RolloutCoordination.Groups[0].ObservedSurge.IntValue()).
				To(Equal(2), "ObservedSurge must reflect the post-edit MaxSurge=2")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G3: MUT-11 Edit RatioTolerance mid-RatioBalanced rollout → next reconcile uses new band", func() {
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

		// Widen tolerance mid-rollout.
		cur := freshISVC(Default, ns.Name, "mut11")
		cur.Spec.RolloutCoordination.Groups[0].Pacing.RatioTolerance = int32Ptr(20)
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		// Hard to assert behavior without G3+G6 status; PIt'd.
	})

	ginkgo.PIt("G1: MUT-9 Bump model + bump runtime concurrently → both CR pairs generated", func() {
		// Two simultaneous spec edits → controller generates two new
		// ControllerRevisions (one per Component) and creates both
		// pairs of per-revision Services. Today no per-revision
		// Services are created at all (G1).
		fix := newCoordFixture(ns.Name, WithISVCName("mut9"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			services := listPerRevisionServices(g, ns.Name)
			g.Expect(services).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: MUT-13 Scale-up MinReplicas mid-rollout → new pods carry current target hash", func() {
		// Bump MinReplicas during rollout. The new pods scheduled by
		// OMENative must carry the target revision hash, not the old one.
		fix := newCoordFixture(ns.Name, WithISVCName("mut13"), WithEngineReplicas(2),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		cur := freshISVC(Default, ns.Name, "mut13")
		five := 5
		cur.Spec.Engine.MinReplicas = &five
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "mut13")
			engineCount := 0
			for _, p := range pods {
				if p.Labels["component"] == string(v1beta1.EngineComponent) {
					engineCount++
				}
			}
			g.Expect(engineCount).To(BeNumerically(">=", 5),
				"scale-up must produce >= 5 engine pods")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1: MUT-14 Scale-down MinReplicas mid-rollout → old-revision pods preferentially scaled down", func() {
		// Inverse of MUT-13. After scaling down, the surviving pods
		// should be on the new target revision (old-revision pods are
		// drained first to favor the rollout direction).
		fix := newCoordFixture(ns.Name, WithISVCName("mut14"), WithEngineReplicas(5),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		cur := freshISVC(Default, ns.Name, "mut14")
		two := 2
		cur.Spec.Engine.MinReplicas = &two
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "mut14")
			engineCount := 0
			for _, p := range pods {
				if p.Labels["component"] == string(v1beta1.EngineComponent) {
					engineCount++
				}
			}
			g.Expect(engineCount).To(BeNumerically("<=", 2))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// MUT-15: covered by WU-10 (deploymentMode flip rejected at admission)
	//         — cross-ref, no test here.
})
