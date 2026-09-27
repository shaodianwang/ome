package alfred_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred advisory-only cases", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-advisory-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("a RawDeployment workload is advisory only", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("adv-raw"), withRawEngine())
		readyOn(fix, src, 1)
		markUnhealthy(src)

		// Node health classifies a RawDeployment Instance before arbitration:
		// no executor exists for it, so the finding is surfaced to the
		// operator as an advisory and never becomes a migration request.
		ginkgo.By("the finding surfaces as an unsupported-migration advisory")
		waitEvent(ns.Name, fix.ISVC.Name, "RawDeploymentMigrationUnsupported", kindMediumTimeout)
		Eventually(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).NotTo(BeEmpty())
			g.Expect(recs[0].Policy).To(Equal(nodeHealthPolicy))
			g.Expect(recs[0].FromNode).To(Equal(src))
			g.Expect(recs[0].Outcome).To(Equal(advisoryOutcome))
			g.Expect(recs[0].AdvisoryReason).To(Equal("RawDeploymentMigrationUnsupported"))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("it stays advisory and nothing is dispatched")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			for _, r := range recsFor(recommendations(g), workloadID(fix)) {
				g.Expect(r.Outcome).To(Equal(advisoryOutcome), "RawDeployment finding for %s", workloadID(fix))
			}
			g.Expect(assertNoDispatch(g, fix, src)).To(HaveLen(1), "%s pod left %s", workloadID(fix), src)
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})

	ginkgo.It("a non-OME GPU pod is counted but never moved", func() {
		src := workers[0]
		filler := createFiller(Default, ns.Name, "adv-filler", src, 2)

		ginkgo.By("its GPUs count toward the node's allocated capacity")
		Eventually(func(g Gomega) {
			v, _ := scrapeLeader(g).value("alfred_gpu_capacity", map[string]string{"node": src, "status": "allocated"})
			g.Expect(v).To(BeNumerically(">=", 2))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		markUnhealthy(src)

		// Only OME-managed GPU pods are occupants node health can act on; a
		// plain pod is capacity, not a workload, so the node is reported as
		// holding nothing alfred could evacuate.
		ginkgo.By("the node is reported with no OME occupant")
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(BeEmpty())
			g.Expect(rec.OMEGPUOccupantsPresent).To(BeFalse())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the node keeps reporting no OME occupant and the pod is untouched")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(BeEmpty())
			g.Expect(rec.OMEGPUOccupantsPresent).To(BeFalse())
			fresh := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(filler), fresh)).To(Succeed())
			g.Expect(fresh.UID).To(Equal(filler.UID))
			g.Expect(fresh.DeletionTimestamp).To(BeNil())
			g.Expect(fresh.Spec.NodeName).To(Equal(src))
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})

	ginkgo.It("alfred.ome.io/movable=false opts a workload out", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("adv-optout"), withISVCAnnotation(movableAnnotation, "false"))
		readyOn(fix, src, 1)
		markUnhealthy(src)

		// The opt-out is part of the policy's eligibility baseline, so the
		// finding is produced as an ineligible advisory and never reaches
		// arbitration (whose own not-movable gate backs up other policies).
		expectIneligibleAdvisory(fix, src)
	})

	ginkgo.It("signalOnly reports the node without evacuating", func() {
		src := workers[0]
		// Registered first so its restore runs last: the policy returns to
		// normal only after the node condition is cleared and the fixture is
		// gone.
		patchAlfredConfig(setConfig("policies.nodeHealth.signalOnly", true))
		fix := newAlfredFixture(ns.Name, withName("adv-signal"))
		readyOn(fix, src, 1)

		waitNodeRecordGone(src)
		markUnhealthy(src)

		ginkgo.By("the node is signaled for repair with the workload listed")
		waitNodeEvent(src, "NodeRepairNeeded", kindMediumTimeout)
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// With signalOnly the policy returns its remediation markers and no
		// findings, so nothing reaches the arbiter or the dispatcher.
		ginkgo.By("no evacuation is recommended or dispatched")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			g.Expect(recsFor(recommendations(g), workloadID(fix))).To(BeEmpty())
			g.Expect(eventsFor(g, ns.Name, fix.ISVC.Name, "EvacuationRecommendationProduced")).To(BeEmpty())
			g.Expect(assertNoDispatch(g, fix, src)).To(HaveLen(1), "%s pod left %s", workloadID(fix), src)
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
