package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A-6 mirror — PD baseline without a rolloutCoordination block.
//
// Asserts the OMENative reconciler runs to convergence WITHOUT a
// coordination spec: per-Instance status populates, OMENative-level
// AvailableReplicas hits 1 per Component, and status.rolloutCoordination
// stays nil. Catches regressions where the controller mistakenly
// requires a coord block to advance.
var _ = ginkgo.Describe("baseline — PD without rolloutCoordination", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "nocoord-a6-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("A-6 engine + decoder converge with no rolloutCoordination block; status.rolloutCoordination stays nil", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("nocoord-a6"),
			WithEngineImage("fake-serving:v1"))
		// No WithCoordinationGroup → fixture leaves Spec.RolloutCoordination
		// nil. Both Components are still OMENative; the coordination layer
		// just doesn't engage.
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "nocoord-a6")
			g.Expect(c.Status.RolloutCoordination).To(BeNil(),
				"status.rolloutCoordination must stay nil when spec omits it")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ReadyReplicas).To(Equal(int32(1)))
			g.Expect(decCS.OMENative.ReadyReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
