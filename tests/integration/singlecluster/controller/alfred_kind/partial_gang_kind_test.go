package alfred_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// expectParked pins the parked Instance: the controller's create attempt is
// still open over both members (Phase Creating with its Operation), and the
// InferenceReplica, current for its generation, counts no Ready replica.
func expectParked(g Gomega, ns, name string) {
	rows := instanceRows(g, ns, name, v1beta1.EngineComponent)
	g.ExpectWithOffset(1, rows).To(HaveLen(1))
	g.ExpectWithOffset(1, rows[0].Index).To(Equal(int32(0)))
	g.ExpectWithOffset(1, rows[0].PodCount).To(Equal(int32(2)))
	g.ExpectWithOffset(1, rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceCreating), "the create attempt was closed")
	g.ExpectWithOffset(1, rows[0].Operation).NotTo(BeNil(), "the create attempt was closed")
	ir := freshIR(g, ns, name, v1beta1.EngineComponent)
	g.ExpectWithOffset(1, ir.Status.ObservedGeneration).To(Equal(ir.Generation), "%s/%s status is stale", ns, name)
	g.ExpectWithOffset(1, ir.Status.ReadyReplicas).To(BeZero(), "%s/%s is steady", ns, name)
}

var _ = ginkgo.Describe("alfred withholds a gang whose members are not all Ready", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-partial-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("a Running-but-unready gang is advisory only", func() {
		src := workers[0]
		// Every container of the fixture carries the node-scoped fault, so the
		// leader and the worker both park Running-but-unready on the node and
		// the Instance never reaches Ready. Cleanup is registered before the
		// create so a failed create still tears down.
		release := pinToNode(src)
		fix := newAlfredFixture(ns.Name, withName("partial-gang"), withGang(1), withNodeScopedFailure(src))
		deferFixtureCleanup(fix)
		applyFixture(Default, fix)

		ginkgo.By("leader and worker park Running-but-unready on the node")
		var leader, worker corev1.Pod
		Eventually(func(g Gomega) {
			leader, worker = expectGangShape(g, livePodsByRunner(livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)))
			for _, p := range []corev1.Pod{leader, worker} {
				g.Expect(p.Status.Phase).To(Equal(corev1.PodRunning), "%s phase", p.Name)
				g.Expect(podIsReady(&p)).To(BeFalse(), "%s is Ready", p.Name)
			}
			expectParked(g, ns.Name, fix.ISVC.Name)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		oldUIDs := map[types.UID]struct{}{leader.UID: {}, worker.UID: {}}
		release()
		waitPlacementCooldown()

		episodeStart := beginUnhealthyEpisode(src)

		// Node health emits one finding per Instance and classifies it before
		// arbitration: the InferenceReplica counts no Ready replica and neither
		// member is Ready, so the shared eligibility baseline keeps the finding
		// advisory. The event is checked once, here: the recorder's per-object
		// spam budget drops repeats of the per-pass advisory later on.
		ginkgo.By("the finding is one ineligible advisory for the whole Instance")
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			cycle := recommendations(g)
			g.Expect(cycle.Mode).To(Equal("execute"))
			recs := recsFor(cycle, workloadID(fix))
			g.Expect(recs).To(HaveLen(1), "one finding for the gang, not one per member: %s", describeRecs(recs))
			g.Expect(recs[0].Instance).To(Equal(int32(0)))
			expectSteadinessAdvisory(g, recs[0], v1beta1.EngineComponent, src)
			var messages []string
			for _, e := range alfredEventsFor(g, ns.Name, fix.ISVC.Name, "EvacuationRecommendationProduced") {
				messages = append(messages, e.Message)
			}
			g.Expect(messages).To(ContainElement(SatisfyAll(
				ContainSubstring(ineligibleAdvisory),
				ContainSubstring("footprint 2 GPUs"),
			)), "the advisory event names the reason and both members' GPUs")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// A gang that cannot move is what the node record is for: the node is
		// signaled for repair with the workload listed and is not reported
		// drained while the members occupy it.
		ginkgo.By("the node is signaled with the gang still on it")
		waitNodeEvent(src, "NodeRepairNeeded", kindMediumTimeout)
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.State).To(Equal(nodeHealthUnhealthy))
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
			g.Expect(rec.OMEAcceleratorOccupantsPresent).To(BeTrue())
			g.Expect(rec.SignaledAt).NotTo(BeNil())
			g.Expect(rec.DrainedAt).To(BeNil())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The quiet window requires every finding for the workload to stay
		// advisory and the Instance finding to keep the steadiness reason; a
		// transient observation gap can add a component-wide advisory beside it.
		ginkgo.By("it stays advisory, nothing is dispatched and both members stay put")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			verdicts.observe(g)
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).NotTo(BeEmpty())
			for _, r := range recs {
				g.Expect(r.Outcome).To(Equal(advisoryOutcome), "non-steady gang %s", workloadID(fix))
			}
			r, ok := instanceRec(recs, v1beta1.EngineComponent, 0)
			g.Expect(ok).To(BeTrue(), "no finding for Instance 0: %s", describeRecs(recs))
			expectSteadinessAdvisory(g, r, v1beta1.EngineComponent, src)
			leader, worker := expectGangShape(g, livePodsByRunner(assertNoDispatch(g, fix, src)))
			for _, p := range []corev1.Pod{leader, worker} {
				_, old := oldUIDs[p.UID]
				g.Expect(old).To(BeTrue(), "%s is a replacement, not a parked member", p.Name)
				g.Expect(podIsReady(&p)).To(BeFalse(), "%s became Ready", p.Name)
			}
			expectParked(g, ns.Name, fix.ISVC.Name)
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.State).To(Equal(nodeHealthUnhealthy))
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
			g.Expect(rec.OMEAcceleratorOccupantsPresent).To(BeTrue())
			g.Expect(rec.DrainedAt).To(BeNil(), "%s reported drained with the gang still on it", src)
			g.Expect(nodeEventsSince(g, src, "NodeDrainedForRepair", episodeStart)).To(BeEmpty(), "%s reported drained with the gang still on it", src)
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
