package coordination

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test H — load test that exercises RatioBalanced + the
// MaxUnavailable gate at scale. Original redesign quoted 30 prefill +
// 10 decode with tolerance 10%; on the buildkite envtest CI that
// fleet sustains a >5min rollout under suite contention and the
// package-level 10min default test timeout panics before convergence.
// Scaled down to 15:5 with tolerance 20%:
//
//	ratio 15/5 = 3.0, band [2.4, 3.6]
//	1st drain: 14/5 = 2.8 ✓
//	2nd drain: 13/5 = 2.6 ✓
//	3rd drain: 12/5 = 2.4 ✓ (boundary)
//	4th drain: 11/5 = 2.2 ✗
//
// Same "3 parallel drains gated by RatioBalanced" property as the
// 30:10 / 10% version — the gate fix is what makes it work — at half
// the pod count and a 5-cycle (not 10-cycle) convergence.
//
// What the fix did to make this pass:
//   - CheckRatioGate now takes inFlightDelta and subtracts it from
//     state.Serving[component] before projecting. Before, parallel
//     drains in one wake-up all saw the same stale ServingReplicas
//     snapshot and the gate over-approved.
//   - The gate now projects the DRAIN direction (–1) instead of the
//     SURGE direction (+1). The redesign's "projection = current – 1"
//     promise from ratio.go was always the intent; the implementation
//     accidentally used +1 because EvaluateSurge was written for a real
//     surge-then-drain path that doesn't ship yet — and at +1 the gate
//     never trips at any scale until ServingReplicas is already below
//     the band.
//
// MaxUnavailable=3 is set explicitly so the RatioBalanced gate is the
// binding constraint (default 25% on 15 = 3 floor, would match the
// ratio gate; setting it explicit makes the intent obvious and pins
// it against future default changes).
var _ = ginkgo.Describe("behavior — load test (15 prefill + 5 decode, tol 20%)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "load-h-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("H-1 + H-2 + H-3 RatioBalanced at scale: no mass drain, ratio in band, no deadlock", func() {
		// Config: 15 prefill + 5 decode, RollingUpdate,
		// sync: RatioBalanced, ratioTolerance: 20, maxUnavailable: 3.
		// Original ratio 15/5 = 3.0; band = [2.4, 3.6].
		//
		// H-1 (no mass drain): on every 100ms sample,
		//        Serving(engine)  >= 12   (within 20% of 15)
		//        Serving(decoder) >= 4    (within 20% of 5)
		// H-2 (ratio band): on every sample,
		//        Serving(engine)/Serving(decoder) ∈ [2.4, 3.6]
		// H-3 (no deadlock): rollout reaches Idle within 5 minutes.
		tol := int32(20)
		maxUnavail := intstr.FromInt(3)
		fix := newCoordFixture(ns.Name, WithISVCName("load-h"),
			WithEngineReplicas(15),
			WithDecoderReplicas(5),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingRatioBalanced,
					RatioTolerance: &tol,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence. 20 pods is heavier than the rest of
		// the suite; 4 minutes for initial bring-up on buildkite envtest
		// where per-pod apiserver throughput is far below dev.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "load-h")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(15)))
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(5)))
		}).WithTimeout(4 * time.Minute).WithPolling(util.PollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "load-h")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision

		// Sampler: H-1 floor on each side + H-2 ratio in band.
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "load-h")
			g.Expect(err).NotTo(HaveOccurred())
			eng := snap.Serving(v1beta1.EngineComponent)
			dec := snap.Serving(v1beta1.DecoderComponent)
			g.Expect(eng).To(BeNumerically(">=", 12),
				"H-1: engine serving must stay >= 12 (within 20%% of 15); got %d", eng)
			g.Expect(dec).To(BeNumerically(">=", 4),
				"H-1: decoder serving must stay >= 4 (within 20%% of 5); got %d", dec)
			if dec == 0 {
				return
			}
			ratio := float64(eng) / float64(dec)
			g.Expect(ratio).To(BeNumerically(">=", 2.4),
				"H-2: live ratio must stay >= 2.4 (orig 3.0 - 20%%); got %.3f", ratio)
			g.Expect(ratio).To(BeNumerically("<=", 3.6),
				"H-2: live ratio must stay <= 3.6 (orig 3.0 + 20%%); got %.3f", ratio)
		})
		defer stop()

		// Bump engine image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "load-h")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// H-3: rollout converges within the deadline. If RatioBalanced
		// over-pauses, this never reaches Idle and the timeout fires.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "load-h")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"H-3: engine CurrentRevision must advance off v1")
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(15)),
				"H-3: engine returns to full 15 serving after rollout")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(5 * time.Minute).WithPolling(util.PollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">=", 2),
			"sampler should observe at least 2 ticks during the rollout — proves the invariant block actually ran")
	})
})
