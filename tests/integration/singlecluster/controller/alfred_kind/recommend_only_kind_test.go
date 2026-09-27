package alfred_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred recommend-only mode", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-ro-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("recommends an evacuation but writes nothing", func() {
		src := workers[0]
		setAlfredMode("recommend-only")
		fix := newAlfredFixture(ns.Name, withName("ro-single"))
		readyOn(fix, src, 1)
		markUnhealthy(src)

		// In recommend-only mode alfred turns every executable candidate into
		// an advisory before arbitration, so the evacuation surfaces as a
		// produced recommendation with an advisory outcome, never as an
		// admitted-but-withheld one.
		ginkgo.By("the recommendation surfaces as events, a cycle entry and a node record")
		waitEvent(ns.Name, fix.ISVC.Name, "EvacuationRecommendationProduced", kindMediumTimeout)
		waitNodeEvent(src, "NodeRepairNeeded", kindMediumTimeout)
		Eventually(func(g Gomega) {
			cycle := recommendations(g)
			g.Expect(cycle.Mode).To(Equal("recommend-only"))
			recs := recsFor(cycle, workloadID(fix))
			g.Expect(recs).NotTo(BeEmpty())
			g.Expect(recs[0].Policy).To(Equal(nodeHealthPolicy))
			g.Expect(recs[0].FromNode).To(Equal(src))
			g.Expect(recs[0].Outcome).To(Equal(advisoryOutcome))
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("nothing is dispatched and the pod stays put")
		Consistently(func(g Gomega) {
			g.Expect(assertNoDispatch(g, fix, src)).To(HaveLen(1), "%s pod left %s", workloadID(fix), src)
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
	})
})
