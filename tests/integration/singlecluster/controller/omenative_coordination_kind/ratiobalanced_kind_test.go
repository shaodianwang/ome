package omenative_coordination_kind

import (
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Behavior test C — BlueGreen + RatioBalanced pacing.
//
// The serving-ratio semantic says the live engine:decoder ratio must
// stay within the original ratio × (1 ± tolerance). On real kubelet
// the pod-lifecycle window is long enough that the sampler actually
// observes the gate at work.
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

	ginkgo.It("C-1 + C-4 live serving ratio stays in band; rollout converges", func() {
		// Config: 4 prefill + 2 decode, BlueGreen + RatioBalanced
		// pacing, tolerance 25%. Original ratio = 4/2 = 2.0,
		// band = [1.5, 2.5].
		tol := int32(25)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("ratio-c"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					Type:                  v1beta1.CoordinationPacingRatioBalanced,
					RatioTolerancePercent: &tol,
					MaxUnavailable:        &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-c")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
			g.Expect(decCS.Lifecycle.ServingReplicas).To(Equal(int32(2)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// CurrentRevision is promoted on a later reconcile than the one the wait
		// above gates on, so poll for it: a bare read races the promotion and
		// captures the empty pre-promotion value.
		var v1EngineRev string
		Eventually(func(g Gomega) {
			preBump := freshISVC(g, ns.Name, "ratio-c")
			g.Expect(preBump.Status.Components[v1beta1.EngineComponent].Lifecycle).NotTo(BeNil())
			v1EngineRev = preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
			g.Expect(v1EngineRev).NotTo(BeEmpty(),
				"engine CurrentRevision must be promoted before the bump")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Sampler asserts C-1: live serving ratio in band [1.5, 2.5].
		// Failure messages name the observed counts so a band violation
		// is debuggable from the test log (no need to re-instrument).
		var samples atomic.Int64
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			// Tolerant snapshot: a late tick can race the ISVC deletion
			// during teardown — skip rather than fail the spec on a
			// transient list error.
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "ratio-c")
			if err != nil {
				return
			}
			eng := snap.Serving(v1beta1.EngineComponent)
			dec := snap.Serving(v1beta1.DecoderComponent)
			samples.Add(1)
			g.Expect(dec).To(BeNumerically(">=", 1),
				"C-1: decoder serving=%d (would zero-out and violate band)", dec)
			if dec == 0 {
				return
			}
			ratio := float64(eng) / float64(dec)
			g.Expect(ratio).To(BeNumerically(">=", 1.5),
				"C-1: live engine/decoder ratio %.2f (eng=%d dec=%d) below band lower 1.5 (orig 2.0 - 25%%)", ratio, eng, dec)
			g.Expect(ratio).To(BeNumerically("<=", 2.5),
				"C-1: live engine/decoder ratio %.2f (eng=%d dec=%d) above band upper 2.5 (orig 2.0 + 25%%)", ratio, eng, dec)
		})
		defer stop()

		// Bump engine image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "ratio-c")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// C-4: rollout converges. 4-prefill + 2-decode under
		// RatioBalanced + MaxUnavailable=1 produces ~6 sequential
		// recreate cycles on real kubelet.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "ratio-c")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"engine CurrentRevision must advance off v1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Stop the sampler. A band violation would have surfaced via the
		// GinkgoRecover→Fail path inside the goroutine. We also assert the
		// sampler actually observed the rollout: without this, a sampler
		// that never ticked during the in-flight window would pass the
		// spec WITHOUT ever checking the ratio band (vacuous). The count
		// is tracked via an atomic incremented per sample — NOT stop()'s
		// return, which can spuriously report 0 via its 2×interval timeout
		// escape under load. The rollout is ~6 sequential recreate cycles
		// (seconds) at 100ms cadence, so >=3 samples is a wide margin.
		stop()
		Expect(samples.Load()).To(BeNumerically(">=", 3),
			"C-1: sampler observed only %d samples during the rollout; the serving-ratio band was not meaningfully checked in-flight", samples.Load())
	})

	// PIt: covered by unit test TestEvaluateSurge_RejectsWhenPeerSerivngWouldBeZero plus C-1 sampler invariant.
	ginkgo.PIt("C-2 mass-drain blocked", func() {})

	// PIt: narrow timing window; covered structurally by unit-level back-off tests in ratio_test.go.
	ginkgo.PIt("C-3 leading side pauses while peer catches up", func() {})
})
