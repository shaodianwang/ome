package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// LIFE-BASIC — the simplest possible pilot. Creates a 2 engine + 1
// decoder BlueGreen-coordinated ISVC and waits for the group to reach
// Idle. Pre-requisite for every coordination test; if this fails the
// foundation isn't ready for more complex specs.
var _ = ginkgo.Describe("LIFE-BASIC — ISVC converges to Idle on KIND", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "life-basic-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("creates an ISVC and converges Status.RolloutCoordination.Groups[0].Phase to Idle", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("life-basic"),
			WithEngineReplicas(2),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "life-basic")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil(),
				"Status.RolloutCoordination is nil — controller hasn't run the coordination reconciler yet")
			g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty(),
				"Status.RolloutCoordination.Groups is empty — group resolution failed")
			phase := c.Status.RolloutCoordination.Groups[0].Phase
			g.Expect(phase).To(Equal(v1beta1.CoordinationPhaseIdle),
				"expected Idle, got %q", phase)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
