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

// pause/resume MID-ROLLOUT (not Idle-pause).
//
// The Idle-pause spec (pause_kind) verifies the annotation drives a
// converged group to Paused. This spec verifies the same annotation
// freezes a group that's IN the middle of a rollout — the case
// operators actually care about ("hit pause if a bad bump is in
// progress"). Real kubelet's seconds-long rollout window makes this
// case observable.
var _ = ginkgo.Describe("behavior — pause mid-rollout", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pause-mid-d7-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("pause annotation freezes a mid-rollout group; unset resumes", func() {
		// Config: 4 engine + 1 decoder, BlueGreen, MaxUnavailable=1.
		// 4 engine recreates @ ~3s each = ~12s window — plenty of time
		// to land the pause annotation in the middle.
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("pause-mid-d7"),
			WithEngineReplicas(4),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
				Pacing: &v1beta1.CoordinationPacing{
					MaxUnavailable: &maxUnavail,
				},
			}),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-mid-d7")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.ServingReplicas).To(Equal(int32(4)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		preBump := freshISVC(Default, ns.Name, "pause-mid-d7")
		v1EngineRev := preBump.Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision

		// Bump engine to v2.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-mid-d7")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the rollout to enter a non-Idle phase (Surging
		// during BlueGreen). This is the "mid-rollout" gate — without
		// it the test races the rollout completion and pauses too
		// late.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-mid-d7")
			phase := c.Status.RolloutCoordination.Groups[0].Phase
			g.Expect(phase).NotTo(Equal(v1beta1.CoordinationPhaseIdle),
				"rollout should have entered a non-Idle phase before pause is applied")
		}).WithTimeout(kindMediumTimeout).WithPolling(50 * time.Millisecond).Should(Succeed())

		// Land the pause annotation mid-rollout.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-mid-d7")
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations["ome.io/rollout-paused"] = "true"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Phase reaches Paused.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-mid-d7")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused),
					"pause annotation must drive group to Paused even mid-rollout")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Hold Paused for 2s. The intent is "freeze further rollout
		// activity" — Phase must not flip out of Paused while the
		// annotation is set.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-mid-d7")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused),
					"paused phase must hold while annotation is set")
		}, 2*time.Second, 200*time.Millisecond).Should(Succeed())

		// Unpause. Rollout resumes and converges.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-mid-d7")
			delete(cur.Annotations, "ome.io/rollout-paused")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-mid-d7")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(Equal(v1EngineRev),
				"after unpause, engine CurrentRevision must advance to v2")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"rollout must finish and return to Idle after unpause")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
