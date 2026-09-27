package alfred_kind

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// The spec sets alfred's nodeSuspicionWindowMinutes itself, to a value other
// than the suite's base one so the write is a real change (an unchanged
// document triggers no reload) and the window it measures is provably the
// configured one: a node whose GpuUnhealthy condition reads False stays out of
// the placement targets until one window past the condition's
// lastTransitionTime. suspicionProbe is how long after that transition the
// spec watches the exclusion; it ends well inside the window so the probe
// never overlaps the admission that follows. suspicionProbeMin is the least
// of it that must remain when the watch starts: two decision ticks, so it
// sees cycles.
const (
	suspicionWindow   = 2 * time.Minute
	suspicionProbe    = 40 * time.Second
	suspicionProbeMin = 10 * time.Second
)

// healthTransitionTime reads back when node's GpuUnhealthy condition last
// changed status, as stored on the Node; alfred measures the suspicion window
// from this instant.
func healthTransitionTime(g Gomega, node string, status corev1.ConditionStatus) time.Time {
	n := &corev1.Node{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Name: node}, n)).To(Succeed())
	for _, c := range n.Status.Conditions {
		if c.Type == healthCondition {
			g.ExpectWithOffset(1, c.Status).To(Equal(status), "%s %s status", node, healthCondition)
			return c.LastTransitionTime.Time
		}
	}
	g.ExpectWithOffset(1, false).To(BeTrue(), "%s carries no %s condition", node, healthCondition)
	return time.Time{}
}

var _ = ginkgo.Describe("alfred suspicion window", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-suspicion-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("excludes a recently recovered node as a target until the window passes", func() {
		src, recovered, full := workers[0], workers[1], workers[2]
		patchAlfredConfig(setConfig("policies.nodeHealth.nodeSuspicionWindowMinutes", int(suspicionWindow/time.Minute)))
		fix := newAlfredFixture(ns.Name, withName("suspicion"))
		readyOn(fix, src, 1)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(1))
		sourceUID := onSrc[0].UID

		// A plain GPU pod holds every GPU of the other healthy worker, so
		// the recovering node is the only place a replacement could land.
		waitNodeGPUsFree(full)
		createFiller(Default, ns.Name, "suspicion-filler", full, gpusPerWorker)

		waitNodeRecordGone(src)
		waitJournalSettled()

		// The flap starts before the source turns unhealthy, so alfred has
		// no target from the first cycle that sees the source: the
		// recovering node is unhealthy, then suspect, and the other is full.
		ginkgo.By("with the recovering node unhealthy and the other target full, the finding has nowhere to go")
		Expect(setHealthCondition(recovered, corev1.ConditionTrue)).To(Succeed())
		markUnhealthy(src)
		headroomAdvisory := SatisfyAll(
			HaveField("Policy", nodeHealthPolicy),
			HaveField("Instance", int32(0)),
			HaveField("FromNode", src),
			HaveField("Outcome", advisoryOutcome),
			HaveField("AdvisoryReason", "NoSurgeHeadroom"),
			HaveField("HintTargets", BeEmpty()),
		)
		Eventually(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(headroomAdvisory), "last cycle: %s", describeRecs(recs))
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
			rec, ok = nodeRecordFor(g, recovered)
			g.Expect(ok).To(BeTrue(), "node record for %s", recovered)
			g.Expect(rec.State).To(Equal(nodeHealthUnhealthy), "%s health state", recovered)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The condition stays on the node with Status False, which is the
		// shape alfred quarantines: the window runs from the transition time
		// the patch stamps, and the node record carries alfred's own deadline.
		ginkgo.By("the condition clears and the node is suspect for one window")
		Expect(setHealthCondition(recovered, corev1.ConditionFalse)).To(Succeed())
		transitioned := healthTransitionTime(Default, recovered, corev1.ConditionFalse)
		var suspectUntil time.Time
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, recovered)
			g.Expect(ok).To(BeTrue(), "node record for %s", recovered)
			g.Expect(rec.State).To(Equal(nodeHealthSuspect), "%s health state", recovered)
			g.Expect(rec.SuspectUntil).NotTo(BeNil(), "%s is suspect without a deadline", recovered)
			suspectUntil = *rec.SuspectUntil
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(suspectUntil.Sub(transitioned)).To(Equal(suspicionWindow), "alfred applies a window other than the configured one")

		// The headroom advisory must be present at every poll and every other
		// finding for the workload advisory too: a transient observation gap
		// can add a component-wide advisory beside it. The probe end is on
		// the spec's clock and the window end on alfred's; alfred runs on a
		// KIND node of the same host, and the part of the window left past
		// the probe covers any skew.
		ginkgo.By("it stays a headroom advisory while the node is suspect: nothing is dispatched, the source pod is untouched")
		probeEnd := transitioned.Add(suspicionProbe)
		Expect(time.Until(probeEnd)).To(BeNumerically(">=", suspicionProbeMin),
			"the condition cleared %s ago; too late to watch the window", time.Since(transitioned).Round(time.Second))
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).NotTo(BeEmpty())
			g.Expect(recs).To(ContainElement(headroomAdvisory), "last cycle: %s", describeRecs(recs))
			for _, r := range recs {
				g.Expect(r.Outcome).To(Equal(advisoryOutcome), "finding for %s: %s", workloadID(fix), describeRecs(recs))
			}
			rec, ok := nodeRecordFor(g, recovered)
			g.Expect(ok).To(BeTrue(), "node record for %s", recovered)
			g.Expect(rec.State).To(Equal(nodeHealthSuspect), "%s left the suspect state inside the window", recovered)
			live := assertNoDispatch(g, fix, src)
			g.Expect(live).To(HaveLen(1))
			g.Expect(live[0].UID).To(Equal(sourceUID))
		}).WithTimeout(time.Until(probeEnd)).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the suspicion probe")

		// alfred plans the surge on a snapshot whose clock is already past
		// suspectUntil and stamps createdAt from that same clock after its
		// preflight snapshot, so the bound is exact and owes nothing to the
		// spec's clock. The only node with room is the recovered one, so it
		// is the sole hint and the predicted placement.
		ginkgo.By("past the window the evacuation is dispatched onto the recovered node")
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		state := func() string { return evacuationState(ns.Name, fix.ISVC.Name) }
		var entry journalEntry
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			entries := journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)
			for _, e := range entries {
				if e.Phase == "failed" || e.Phase == "stalled" {
					StopTrying(fmt.Sprintf("journal %s is %s: %s", e.UUID, e.Phase, e.Reason)).Now()
				}
			}
			g.Expect(entries).To(HaveLen(1), func() string { return "request for the workload; " + state() })
			entry = entries[0]
			g.Expect(entry.Phase).To(BeElementOf("submitted", "acknowledged", "completed"), func() string { return "request phase; " + state() })
		}).WithTimeout(dispatchTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(entry.CreatedAt).To(BeTemporally(">=", suspectUntil),
			"request %s was dispatched %s before the window ended", entry.UUID, suspectUntil.Sub(entry.CreatedAt).Round(time.Second))
		Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(entry.Instance).To(Equal(int32(0)))
		Expect(entry.FromNode).To(Equal(src))
		Expect(entry.Targets).To(ConsistOf(recovered), "predicted placement")
		req := parseMigrationRequest(Default, entry.Payload)
		Expect(req.Reason).To(Equal(nodeUnhealthyReason))
		Expect(req.HintTargetNodes).To(ConsistOf(recovered), "the full worker has no room to be hinted")
		fmt.Fprintf(ginkgo.GinkgoWriter, "suspicion window: dispatched %s after the window ended\n",
			entry.CreatedAt.Sub(suspectUntil).Round(time.Second))

		// Past the window the node is clear while its condition still reads
		// False, so alfred drops its record; the replacement lands there.
		ginkgo.By("the migration completes and the replacement runs on the recovered node")
		waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			live := 0
			for _, p := range omenativePods(g, ns.Name, fix.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				live++
				g.Expect(p.UID).NotTo(Equal(sourceUID), "pod %s is the original", p.Name)
				g.Expect(p.Spec.NodeName).To(Equal(recovered), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
			}
			g.Expect(live).To(Equal(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(healthTransitionTime(g, recovered, corev1.ConditionFalse)).To(BeTemporally("==", transitioned), "%s condition was restamped", recovered)
			_, ok := nodeRecordFor(g, recovered)
			g.Expect(ok).To(BeFalse(), "node record for %s still present after the window", recovered)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
