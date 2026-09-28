package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Regression test — the ISVC component projection must keep converging on
// an ISVC that requeues on every reconcile pass.
//
// AggregateIRStatus is the sole writer of status.components.<c>.lifecycle
// and of the top-level EngineReady / DecoderReady / RouterReady
// conditions. The controller short-circuits Reconcile as soon as anything
// asks to requeue, and a canary parked on a manual gate requeues on EVERY
// pass indefinitely. Running the projector only on the no-requeue tail
// therefore freezes the projection for as long as the rollout is parked —
// and because the rollout engines gate on component readiness, the freeze
// sustains itself: stale NotReady conditions hold the rollout, the held
// rollout keeps requeueing, and the requeue keeps the projection stale.
//
// Production signature: four ISVCs pinned Ready=False/Initializing for
// seven days whose live InferenceReplicas were ten to eleven generations
// ahead and fully Ready, with zero reconcile errors and no recovery across
// controller restarts.
//
// The probe is a scale-up performed while the canary is parked. It bumps
// ISVC.metadata.generation and moves the IR's replica counters, and
// neither reaches the ISVC status surface without a projector run — a
// frozen projection stays pinned at the pre-scale snapshot forever.
//
// A manual-gate pause is used rather than the ome.io/rollout-paused
// freeze: the global pause additionally suspends Create work, so the
// scale-up would never reach the IR.
var _ = ginkgo.Describe("regression — component projection under a permanently requeueing rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "proj-freeze-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("tracks the live InferenceReplica while a parked canary requeues every pass", func() {
		const isvcName = "proj-freeze1"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Converge on v1 from a settled IR: the probe is the parked rollout,
		// not the initial roll.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: open a canary whose step 0 holds on a bare pause — a manual
		// gate waiting for a promote annotation that never arrives — and bump
		// the image. From here every reconcile pass returns the held step's
		// requeue.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{
					Steps: []v1beta1.RolloutGroupStep{
						{Capacity: intstr.FromString("50%"), Traffic: 20, Pause: &v1beta1.RolloutPause{}},
						{Capacity: intstr.FromString("100%"), Traffic: 100},
					},
				},
			}}}
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the parked step: split serving at 20%, step 0, Paused.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)))
			g.Expect(c.Status.Canary.ObservedTrafficWeight).To(Equal(int32(20)))
			g.Expect(c.Status.Components[v1beta1.EngineComponent].RolloutPhase).
				To(Equal(v1beta1.RolloutPhasePaused))
			expectIRStagedAt(g, ns.Name, isvcName, v1beta1.EngineComponent, 1)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Action: scale the engine while the canary is parked.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			three := 3
			cur.Spec.Engine.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		var scaledGeneration int64
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Spec.Engine.MinReplicas).To(HaveValue(Equal(3)))
			scaledGeneration = c.Generation
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the canary stays parked — nothing below is allowed to
		// pass because the rollout quietly finished and stopped requeueing.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.Canary).NotTo(BeNil())
			g.Expect(c.Status.Canary.CurrentStep).To(Equal(int32(0)),
				"premise: a manual-gate step must stay parked, requeueing every pass")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the projection converges on the live IR even though the
		// rollout never stops requeueing. A frozen projector leaves the
		// Lifecycle block pinned at the pre-scale snapshot indefinitely.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			lc := c.Status.Components[v1beta1.EngineComponent].Lifecycle
			g.Expect(lc).NotTo(BeNil())
			g.Expect(lc.Replicas).To(Equal(int32(3)),
				"the scale-up must reach the projection while the rollout is parked")
			g.Expect(lc.Replicas).To(Equal(ir.Status.Replicas),
				"projected replicas must track the live IR (%d), got %d",
				ir.Status.Replicas, lc.Replicas)
			g.Expect(lc.ReadyReplicas).To(Equal(ir.Status.ReadyReplicas),
				"projected readyReplicas must track the live IR (%d), got %d",
				ir.Status.ReadyReplicas, lc.ReadyReplicas)
			g.Expect(lc.ObservedGeneration).To(Equal(scaledGeneration),
				"the projection must report the post-scale ISVC generation (%d), got %d; "+
					"a stale value means AggregateIRStatus stopped running for this object",
				scaledGeneration, lc.ObservedGeneration)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Invariant: the top-level readiness condition the projector owns
		// stays live too. This is what pinned the production ISVCs NotReady,
		// and what the rollout engines gate on — which is why a stale
		// projection can never heal itself.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			engineReady := c.Status.GetCondition(v1beta1.EngineReady)
			g.Expect(engineReady).NotTo(BeNil(),
				"EngineReady must exist; the projector is its only writer")
			g.Expect(engineReady.Status).To(Equal(corev1.ConditionTrue),
				"EngineReady must reflect the live IR while the rollout is parked; reason=%q message=%q",
				engineReady.Reason, engineReady.Message)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
