package scaleout_kind

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/multicluster"
)

// Fan-out onto real member clusters.
var _ = ginkgo.Describe("Multi-cluster fan-out (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	// All mode on purpose. The claim under test is that a fan-out reaches BOTH
	// members and each brings up real pods; in Single mode the placer sweeps the
	// loser as soon as a winner emerges, so "both members ran the workload" is
	// racy to observe by construction. All keeps every home that admits, which
	// makes the assertion deterministic. Single-mode winner election and loser
	// sweep are covered by the lifecycle specs.
	ginkgo.It("fans the derived ISVC out to both members and each brings up running pods", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeAll, 1)

		ginkgo.By("asserting the placer created a derived on BOTH members, stamped with this source's origin UID")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				derived, ok := getDerivedOn(m.c, env.isvcNS, env.isvcName)
				g.Expect(ok).To(gomega.BeTrue(), "no derived ISVC on %s", m.wcName)
				g.Expect(multicluster.OriginUID(derived)).NotTo(gomega.BeEmpty(),
					"derived on %s must carry the origin-UID stamp", m.wcName)
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"derived ISVC should appear on %s", m.wcName)
		}

		ginkgo.By("asserting a real kubelet took an engine pod to Running on BOTH members")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).
					To(gomega.BeNumerically(">=", 1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"expected a Running engine pod on %s", m.wcName)
		}

		ginkgo.By("asserting the control plane reports both members as placement candidates")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(multicluster.CandidateClusters(ps)).To(gomega.ConsistOf(env.memberNames()))
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
