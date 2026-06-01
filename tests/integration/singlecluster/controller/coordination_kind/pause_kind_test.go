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

// Behavior test G — pause / resume.
//
// The ome.io/rollout-paused annotation drives a group's phase to
// Paused, and removing it lets the group resume.
var _ = ginkgo.Describe("behavior — pause / resume", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pause-g-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("paused annotation freezes group phase; unset resumes", func() {
		// Config: 2 prefill + 1 decode, BlueGreen.
		maxUnavail := intstr.FromInt(1)
		fix := newCoordFixture(ns.Name, WithISVCName("pause-g"),
			WithEngineReplicas(2),
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

		// Wait for v1 convergence.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-g")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Apply pause annotation.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-g")
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations["ome.io/rollout-paused"] = "true"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// group phase reaches Paused.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-g")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused),
					"pause annotation must drive group to Paused")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Confirm pause holds: phase remains Paused across the polling window.
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-g")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused),
					"paused annotation must hold the group at Paused")
		}, 2*time.Second, 200*time.Millisecond).Should(Succeed())

		// remove pause annotation, group resumes to Idle.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-g")
			delete(cur.Annotations, "ome.io/rollout-paused")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pause-g")
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle),
					"removing pause annotation must let the group return to Idle")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
