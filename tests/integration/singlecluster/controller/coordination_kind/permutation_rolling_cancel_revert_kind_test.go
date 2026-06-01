package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KP-17 — Revert mid-rollout under RollingUpdate coord (the
// operator panic-button workflow under RollingUpdate).
//
// KCG-4 covers the same cancel-via-revert workflow under BlueGreen.
// KP-17 covers it under RollingUpdate (parallel per-Component
// rollouts, no atomic gang flip). The contract: after a v1 → v2
// rollout is mid-flight, reverting the image back to v1 must drive
// the cluster state machine BACK to v1 with no v2 stragglers.
//
// Setup: engine MinReplicas=3 + MaxUnavailable=1 under RollingUpdate
// — multiple recreate cycles per Component give the revert plenty of
// time to land mid-rollout.
var _ = ginkgo.Describe("permutation — RollingUpdate cancel via revert", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-ru-revert-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KP-17 RollingUpdate v1 → v2 → v1 revert converges back to v1 with no v2 stragglers", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-ru-revert"),
			WithEngineReplicas(3),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{
					v1beta1.EngineComponent, v1beta1.DecoderComponent,
				},
				Policy: v1beta1.CoordinationPolicyRollingUpdate,
				Pacing: &v1beta1.CoordinationPacing{
					Type:           v1beta1.CoordinationPacingPerComponent,
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-revert")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping engine v1 → v2 (start the rollout we'll cancel)")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ru-revert")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for non-Idle so we revert mid-rollout.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-revert")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				NotTo(Equal(v1beta1.CoordinationPhaseIdle),
					"KP-17: rollout must enter non-Idle before revert is applied")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("reverting engine image back to v1 mid-rollout")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-ru-revert")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v1"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Converge back to 3 alive engine pods, all on v1.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-revert")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(3)),
				"KP-17: engine must be back to 3 serving replicas after revert")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"KP-17: rollout must return to Idle after revert converges")

			pods := listOMENativePods(g, ns.Name, "perm-ru-revert")
			engCount, v1Count := 0, 0
			for _, pod := range pods {
				if string(v1beta1.ComponentType(pod.Labels["component"])) != string(v1beta1.EngineComponent) {
					continue
				}
				if pod.DeletionTimestamp != nil {
					continue
				}
				engCount++
				for _, ctr := range pod.Spec.Containers {
					if ctr.Name == "ome-container" && ctr.Image == "fake-serving:v1" {
						v1Count++
					}
				}
			}
			g.Expect(engCount).To(Equal(3),
				"KP-17: expected 3 alive engine pods, got %d", engCount)
			g.Expect(v1Count).To(Equal(3),
				"KP-17: every alive engine pod must be on fake-serving:v1 after revert (got %d/%d v1 pods); a non-v1 pod = v2 straggler", v1Count, engCount)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Final no-flap check: 2s of stable Idle.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-ru-revert")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"KP-17: post-revert state must be stable at Idle")
		}, kindShortTimeout, kindPollInterval).Should(Succeed())
	})
})
