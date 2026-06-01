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

// KP-15 — Pause mid-Sequential rollout, then resume.
//
// D-7 (existing) covers pause mid-BlueGreen. KP-15 pins the same
// pause-then-resume contract for the Sequential coord policy: the
// pause annotation must freeze a Sequential rollout that is between
// Component handoffs, and removing it must let the chain finish.
//
// Setup: decoder + engine, each with 2 replicas, Sequential
// Order=[decoder, engine]. Pause is applied AFTER the rollout has
// entered a non-Idle phase. Then unpause and assert convergence to
// v2 on both Components.
var _ = ginkgo.Describe("permutation — pause mid-Sequential rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-seq-pause-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("KP-15 pause annotation freezes a Sequential rollout; unset resumes the chain", func() {
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("perm-seq-pause"),
			WithEngineReplicas(2),
			WithDecoderReplicas(2),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
				Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-pause")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "perm-seq-pause")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision
		v1DecoderRev := preBump.Status.Components[v1beta1.DecoderComponent].OMENative.CurrentRevision

		// Bump BOTH images — Sequential will roll decoder first.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-seq-pause")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			cur.Spec.Decoder.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the rollout to enter a non-Idle phase, then pause.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-pause")
			phase := c.Status.RolloutCoordination.Groups[0].Phase
			g.Expect(phase).NotTo(Equal(v1beta1.CoordinationPhaseIdle),
				"KP-15: rollout must enter non-Idle before pause is applied")
		}).WithTimeout(kindMediumTimeout).WithPolling(50 * time.Millisecond).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-seq-pause")
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations["ome.io/rollout-paused"] = "true"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Phase reaches Paused.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-pause")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused),
					"KP-15: pause must drive Sequential group to Paused mid-rollout")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Hold Paused for 2s.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-pause")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused),
					"KP-15: paused phase must hold while annotation is set")
		}, 2*time.Second, 200*time.Millisecond).Should(Succeed())

		// Unpause. Sequential chain resumes and finishes.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "perm-seq-pause")
			delete(cur.Annotations, "ome.io/rollout-paused")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "perm-seq-pause")
			g.Expect(c.Status.Components[v1beta1.EngineComponent].OMENative.CurrentRevision).NotTo(Equal(v1EngineRev),
				"KP-15: after unpause, engine CurrentRevision must advance to v2")
			g.Expect(c.Status.Components[v1beta1.DecoderComponent].OMENative.CurrentRevision).NotTo(Equal(v1DecoderRev),
				"KP-15: after unpause, decoder CurrentRevision must advance to v2")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"KP-15: Sequential rollout must return to Idle after unpause")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
