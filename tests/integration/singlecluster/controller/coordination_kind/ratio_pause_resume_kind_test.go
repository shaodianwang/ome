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

// Pause mid-RatioBalanced rollout, then resume.
//
// pause_kind covers pause-at-Idle for BlueGreen. pause_midrollout_kind
// covers pause-mid for BlueGreen. sequential_pause_mid_kind covers
// Sequential. This spec covers the RatioBalanced pacing path: while the
// gate is evaluating live engine:decoder ratios, the pause annotation
// must freeze the group, and unset must let it finish.
//
// The RatioBalanced gate logic is conceptually orthogonal to pause —
// pause is checked before the gate fires — but exercising the
// combination flushes out any wiring regression where the gate's
// requeue cadence interferes with the pause observer.
var _ = ginkgo.Describe("permutation — pause mid-RatioBalanced rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-ratio-pause-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("pause annotation freezes a RatioBalanced rollout; unset resumes", func() {
		tol := int32(25)
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-ratio-pause"),
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
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-pause")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-ratio-pause")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Bump engine image — the RatioBalanced gate will start
		// pacing the engine rollout.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ratio-pause")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for non-Idle so we land the pause mid-rollout.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-pause")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				NotTo(Equal(v1beta1.CoordinationPhaseIdle),
					"rollout must enter non-Idle before pause is applied")
		}).WithTimeout(kindMediumTimeout).WithPolling(50 * time.Millisecond).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ratio-pause")
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations["ome.io/rollout-paused"] = "true"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-pause")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused),
					"pause annotation must drive RatioBalanced group to Paused")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-pause")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused))
		}, 2*time.Second, 200*time.Millisecond).Should(Succeed())

		// Unpause.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ratio-pause")
			delete(cur.Annotations, "ome.io/rollout-paused")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ratio-pause")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"after unpause, engine CurrentRevision must advance to v2")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"RatioBalanced rollout must return to Idle after unpause")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
