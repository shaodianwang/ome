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

// Behavior test C — BlueGreen + RatioBalanced pacing under the
// serving-ratio semantic (live ratio between Components stays within
// original_ratio * (1 ± tolerance), not per-Component progress drift).
// Uses BlueGreen here because the RatioBalanced math is policy-agnostic
// (it applies via the pacing.Type field).
var _ = ginkgo.Describe("behavior — BlueGreen + RatioBalanced", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rbal-c-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.PIt("C-1 + C-4 live serving ratio stays in band; rollout converges — PIt: hangs in envtest (StartSampler waits indefinitely for pod state transitions PodKubeletSimulator doesn't reliably emit under suite load). Real-cluster equivalents in .claude/specs/2026-05-25-real-cluster-tests/ cover these invariants. Re-enable when envtest's simulator can match real kubelet sampling fidelity", func() {
		// Config: 4 prefill + 2 decode, BlueGreen + RatioBalanced
		// pacing, tolerance 25%. Original ratio = 4/2 = 2.0,
		// band = [1.5, 2.5]. The default updateStrategy
		// (SurgeThenDrain → recreate delegate) preserves serving
		// capacity at original counts, so the live ratio stays at 2.0
		// throughout.
		tol := int32(25)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("ratio-c"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingRatioBalanced,
					RatioTolerance: &tol,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-c")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(4)))
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "ratio-c")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision

		// Start sampler asserting C-1: live serving ratio in band [1.5, 2.5].
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "ratio-c")
			g.Expect(err).NotTo(HaveOccurred())
			eng := snap.Serving(v1beta1.EngineComponent)
			dec := snap.Serving(v1beta1.DecoderComponent)
			if dec == 0 {
				// Ratio undefined when peer hits zero — that's the
				// mass-drain failure mode the gate must block.
				g.Expect(dec).To(BeNumerically(">=", 1),
					"C-1: decoder serving must never drop to 0 (would violate band)")
				return
			}
			ratio := float64(eng) / float64(dec)
			g.Expect(ratio).To(BeNumerically(">=", 1.5),
				"C-1: live engine/decoder ratio must stay >= 1.5 (orig 2.0 - 25%%)")
			g.Expect(ratio).To(BeNumerically("<=", 2.5),
				"C-1: live engine/decoder ratio must stay <= 2.5 (orig 2.0 + 25%%)")
		})
		defer stop()

		// Bump engine image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "ratio-c")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// C-4: rollout converges (engine CurrentRevision advances off v1).
		// 4-prefill + 2-decode with RatioBalanced pacing produces ~12 pod
		// transitions through the recreate-via-SurgeThenDrain path; under
		// concurrent suite load the standard LongTimeout (90s) is too
		// tight. 3 minutes gives convergence headroom without hiding a
		// genuine deadlock — a real regression would never converge.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-c")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(3 * time.Minute).WithPolling(util.PollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">=", 2),
			"sampler should observe at least 2 ticks during the rollout — proves the invariant block actually ran")
	})

	ginkgo.PIt("C-2 mass-drain blocked — covered by unit test TestEvaluateSurge_RejectsWhenPeerSerivngWouldBeZero plus C-1 sampler invariant", func() {})

	ginkgo.PIt("C-3 leading side pauses while peer catches up — narrow timing window in envtest; covered structurally by unit-level back-off tests in ratio_test.go", func() {})
})
