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

// Behavior test B — RollingUpdate + RecreatePod.
//
// RecreatePod is the explicit drain-then-create path; the
// MaxUnavailable gate caps how many pods can be off-rotation at once,
// MaxSurge=0 forbids the controller from creating an extra pod
// alongside the old. This is the conservative opt-in for clusters with
// no GPU surge budget.
var _ = ginkgo.Describe("behavior — RollingUpdate + RecreatePod", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "recreate-b-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.PIt("B-1 + B-2 + B-3 RecreatePod respects unavailable budget, no surge, converges — PIt: hangs in envtest (StartSampler waits indefinitely for pod state transitions PodKubeletSimulator doesn't reliably emit under suite load). Real-cluster equivalents in .claude/specs/2026-05-25-real-cluster-tests/ cover these invariants. Re-enable when envtest's simulator can match real kubelet sampling fidelity", func() {
		// Config: 4 prefill + 2 decode, RollingUpdate, sync: PerComponent,
		// maxUnavailable: 1, maxSurge: 0, updateStrategy.type: RecreatePod.
		// Action: bump engine image v1 -> v2.
		//
		// B-1 (unavailable budget): on every 100ms sample,
		//        (4 - Serving(engine))  <= 1
		//        (2 - Serving(decoder)) <= 1
		// B-2 (no surge): on every sample,
		//        Alive(engine)  <= 4
		//        Alive(decoder) <= 2
		// B-3 (converges): engine CurrentRevision advances off v1; both
		//        components return to full serving on v2.
		maxUnavail := intstr.FromInt(1)
		maxSurge := intstr.FromInt(0)
		fix := newCoordFixture(ns.Name, WithISVCName("recreate-b"),
			WithEngineReplicas(4),
			WithDecoderReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxSurge:       &maxSurge,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		// RecreatePod is opt-in; the default updateStrategy is
		// SurgeThenDrain. Override per-component so B-2's no-surge
		// invariant is exercised against the explicit recreate path.
		fix.ISVC.Spec.Engine.OMENative = &v1beta1.OMENativeComponentSpec{
			UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{
				Type: v1beta1.OMENativeUpdateStrategyRecreatePod,
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for v1 convergence: both components fully serving.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-b")
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

		preBump := freshISVC(Default, ns.Name, "recreate-b")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision

		// Sampler asserts B-1 + B-2 on every 100ms tick.
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, "recreate-b")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(4-snap.Serving(v1beta1.EngineComponent)).To(BeNumerically("<=", 1),
				"B-1: engine MaxUnavailable=1 — at most 1 of 4 prefill pods missing from serving")
			g.Expect(2-snap.Serving(v1beta1.DecoderComponent)).To(BeNumerically("<=", 1),
				"B-1: decoder MaxUnavailable=1 — at most 1 of 2 decode pods missing from serving")
			g.Expect(snap.Alive(v1beta1.EngineComponent)).To(BeNumerically("<=", 4),
				"B-2: RecreatePod must not surge — engine alive count <= replicas")
			g.Expect(snap.Alive(v1beta1.DecoderComponent)).To(BeNumerically("<=", 2),
				"B-2: RecreatePod must not surge — decoder alive count <= replicas")
		})
		defer stop()

		// Bump engine image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "recreate-b")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// B-3: convergence. Engine recreates 4 pods at MaxUnavailable=1
		// cadence → ~4 drain/create cycles. Decoder unchanged.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "recreate-b")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"B-3: engine CurrentRevision must advance off v1")
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(4)),
				"B-3: engine returns to full serving on v2")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(3 * time.Minute).WithPolling(util.PollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">=", 2),
			"sampler should observe at least 2 ticks during the rollout — proves the invariant block actually ran")
	})
})
