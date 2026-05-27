package coordination_kind

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
// MaxUnavailable gate at scale on KIND.
//
// Config: 15 prefill + 5 decode, RollingUpdate + RatioBalanced (tol
// 20%), MaxUnavailable=3. Original ratio 15/5 = 3.0, band [2.4, 3.6].
// The same "3 parallel drains gated by RatioBalanced" property the
// envtest sibling exercises, against real kubelet so the gate's
// in-flight projection actually matters (envtest's instant-Ready
// transitions complete each cycle in microseconds).
//
// Why this is the heaviest spec in the suite: 20 pods on a single-node
// KIND cluster takes minutes to schedule + reach ContainersReady, and
// the rollout itself recreates 15 pods sequentially-ish (gated to
// MaxUnavailable=3 concurrent). Budget for ~6-8 minutes wall.
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
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence. 20 pods on a single-node KIND can
		// take a few minutes to all reach Ready; budget generously.
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "load-h")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision

		// Sampler: H-1 floor on each side + H-2 ratio in band.
		// Failure messages name the observed counts so a band violation
		// is debuggable from the test log.
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
				"H-2: live ratio must stay >= 2.4 (orig 3.0 - 20%%); got %.3f (eng=%d dec=%d)", ratio, eng, dec)
			g.Expect(ratio).To(BeNumerically("<=", 3.6),
				"H-2: live ratio must stay <= 3.6 (orig 3.0 + 20%%); got %.3f (eng=%d dec=%d)", ratio, eng, dec)
		})
		defer stop()

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "load-h")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// H-3: rollout converges. 15 sequential-ish recreates on a
		// single-node cluster — budget generously.
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		stop()
	})
})
