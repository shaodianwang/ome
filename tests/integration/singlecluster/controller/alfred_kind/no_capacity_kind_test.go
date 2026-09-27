package alfred_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred with no target capacity", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-nocap-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("declines to dispatch and leaves the source alone", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("nocap"))
		readyOn(fix, src, 1)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(1))
		sourceUID := onSrc[0].UID

		// Plain GPU pods hold every GPU of both healthy workers, so the
		// replacement has nowhere to land while the source still holds its
		// own GPU. Each is bound to its node directly and waited for, so
		// alfred counts them as occupancy before the node turns unhealthy.
		for i, w := range otherWorkers(src) {
			waitNodeGPUsFree(w)
			createFiller(Default, ns.Name, fmt.Sprintf("nocap-filler-%d", i), w, gpusPerWorker)
		}

		episodeStart := beginUnhealthyEpisode(src)

		// The policy plans the surge before it emits a finding. With no node
		// able to take the footprint the finding is produced as a headroom
		// advisory: it names no target, never enters arbitration and never
		// consults the scheduling simulation.
		ginkgo.By("the finding surfaces as a no-headroom advisory")
		Eventually(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).NotTo(BeEmpty())
			g.Expect(recs[0].Policy).To(Equal(nodeHealthPolicy))
			g.Expect(recs[0].Instance).To(Equal(int32(0)))
			g.Expect(recs[0].FromNode).To(Equal(src))
			g.Expect(recs[0].Outcome).To(Equal(advisoryOutcome))
			g.Expect(recs[0].AdvisoryReason).To(Equal("NoSurgeHeadroom"))
			g.Expect(recs[0].HintTargets).To(BeEmpty(), "a headroom advisory names no target")
			var messages []string
			for _, e := range alfredEventsFor(g, ns.Name, fix.ISVC.Name, "EvacuationRecommendationProduced") {
				messages = append(messages, e.Message)
			}
			g.Expect(messages).To(ContainElement(ContainSubstring("NoSurgeHeadroom")))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// A workload that cannot move is exactly what the node record is
		// for: the node is signaled for repair with the workload listed and
		// is not reported drained while the workload occupies it.
		ginkgo.By("the node is signaled with the workload still on it")
		waitNodeEvent(src, "NodeRepairNeeded", kindMediumTimeout)
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
			g.Expect(rec.OMEGPUOccupantsPresent).To(BeTrue())
			g.Expect(rec.SignaledAt).NotTo(BeNil())
			g.Expect(rec.DrainedAt).To(BeNil())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("it stays advisory, nothing is dispatched and the source pod is untouched")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).NotTo(BeEmpty())
			for _, r := range recs {
				g.Expect(r.Outcome).To(Equal(advisoryOutcome), "no-capacity finding for %s", workloadID(fix))
			}
			live := assertNoDispatch(g, fix, src)
			g.Expect(live).To(HaveLen(1))
			g.Expect(live[0].UID).To(Equal(sourceUID))
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
			g.Expect(rec.DrainedAt).To(BeNil(), "%s reported drained with the workload still on it", src)
			g.Expect(nodeEventsSince(g, src, "NodeDrainedForRepair", episodeStart)).To(BeEmpty(), "%s reported drained with the workload still on it", src)
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
