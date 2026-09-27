package alfred_kind

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// alfred ranks evacuation findings by reason, then executable, then the
// priority value descending, ahead of footprint and workload name; a lower
// value is more protected. These two sit on either side of the 0.5 default.
// Each fixture is named by its value, so the name order (prio-01 before
// prio-09) is the reverse of the ranking: an order that followed names would
// fail this spec.
const (
	exposedPriority   = "0.9"
	protectedPriority = "0.1"
)

var _ = ginkgo.Describe("alfred priority ordering", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-priority-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("evacuates the less protected workload first, the more protected one after it, and never the opted-out one", func() {
		src := workers[0]
		healthy := otherWorkers(src)

		// At a cap of one the arbiter admits the first-ranked finding and
		// names the next as InFlightCap in the same cycle, so the ranking is
		// visible in the cycle record and not only in the dispatch order.
		patchAlfredConfig(setConfig("maxInFlightMigrations", 1))

		// Three single-Instance workloads on the node: the two ranked ones
		// share reason, footprint and executability, so the priority value
		// alone decides their order; the opted-out one is advisory and never
		// ranked.
		release := pinToNode(src)
		exposed := newAlfredFixture(ns.Name, withName("prio-09"), withISVCAnnotation(priorityAnnotation, exposedPriority))
		protected := newAlfredFixture(ns.Name, withName("prio-01"), withISVCAnnotation(priorityAnnotation, protectedPriority))
		out := newAlfredFixture(ns.Name, withName("prio-out"), withISVCAnnotation(movableAnnotation, "false"))
		movers := []*alfredFixture{exposed, protected}
		all := []*alfredFixture{exposed, protected, out}
		for _, f := range all {
			deferFixtureCleanup(f)
			applyFixture(Default, f)
		}
		for _, f := range all {
			waitInstancesReady(ns.Name, f.ISVC.Name, v1beta1.EngineComponent, 1)
		}
		release()
		waitPlacementCooldown()
		sourceUIDs := map[types.UID]struct{}{}
		for _, f := range movers {
			onSrc := livePodsOnNode(omenativePods(Default, ns.Name, f.ISVC.Name), src)
			Expect(onSrc).To(HaveLen(1), "%s on %s", f.ISVC.Name, src)
			sourceUIDs[onSrc[0].UID] = struct{}{}
		}
		outOnSrc := livePodsOnNode(omenativePods(Default, ns.Name, out.ISVC.Name), src)
		Expect(outOnSrc).To(HaveLen(1), "%s on %s", out.ISVC.Name, src)
		outUID := outOnSrc[0].UID

		waitJournalSettled()
		markUnhealthy(src)

		ginkgo.By("the less protected workload is dispatched while the more protected one is named InFlightCap")
		exposedVerdicts := &verdictLog{ns: ns.Name, name: exposed.ISVC.Name}
		protectedVerdicts := &verdictLog{ns: ns.Name, name: protected.ISVC.Name}
		state := func() string {
			return fmt.Sprintf("%s: %s | %s: %s", exposed.ISVC.Name, evacuationState(ns.Name, exposed.ISVC.Name),
				protected.ISVC.Name, evacuationState(ns.Name, protected.ISVC.Name))
		}
		var first journalEntry
		Eventually(func(g Gomega) {
			exposedVerdicts.observe(g)
			protectedVerdicts.observe(g)
			js := journal(g)
			exposedEntries := journalEntriesFor(js, ns.Name, exposed.ISVC.Name)
			protectedEntries := journalEntriesFor(js, ns.Name, protected.ISVC.Name)
			if len(protectedEntries) > 0 && len(exposedEntries) == 0 {
				StopTrying("the more protected workload was dispatched first: " + describeJournal(protectedEntries)).Now()
			}
			// The InFlightCap verdict below is the evidence that the ranking
			// decided the order, and it exists only while the first request
			// is open; a request already closed cannot yield it.
			if len(exposedEntries) == 1 && journalTerminal(exposedEntries[0]) {
				StopTrying(fmt.Sprintf("the first request is already %s and no cycle showed it in flight: %s",
					exposedEntries[0].Phase, describeJournal(exposedEntries))).Now()
			}
			if n := len(inFlight(js.Entries)); n > 1 {
				StopTrying(fmt.Sprintf("%d requests in flight at once", n)).Now()
			}
			g.Expect(exposedEntries).To(HaveLen(1), func() string { return "request for the less protected workload; " + state() })
			g.Expect(inFlight(js.Entries)).To(HaveLen(1), func() string { return "the request in flight; " + state() })
			g.Expect(protectedEntries).To(BeEmpty(), "the more protected workload waits for the budget")
			cycle := recommendations(g)
			recsProtected := recsFor(cycle, workloadID(protected))
			g.Expect(recsProtected).To(ContainElement(rejectedWith("InFlightCap")),
				"more protected workload; last cycle: %s", describeRecs(recsProtected))
			recsOut := recsFor(cycle, workloadID(out))
			g.Expect(recsOut).To(ContainElement(SatisfyAll(HaveField("Outcome", advisoryOutcome), HaveField("AdvisoryReason", ineligibleAdvisory))),
				"opted-out workload; last cycle: %s", describeRecs(recsOut))
			first = exposedEntries[0]
		}).WithTimeout(dispatchTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(first.FromNode).To(Equal(src))
		Expect(parseMigrationRequest(Default, first.Payload).Reason).To(Equal(nodeUnhealthyReason))

		ginkgo.By("the first migration completes")
		waitMigrationCompleted(ns.Name, exposed.ISVC.Name, v1beta1.EngineComponent, first.UUID)
		firstDone := waitJournalPhase(first.UUID, "completed", kindMediumTimeout)
		Expect(firstDone.CompletedAt).NotTo(BeNil())

		// A different workload owes no per-workload cooldown, so the only
		// hold left is the target cooldown: a replacement predicted onto the
		// node the first landed on waits out one perNodeCooldown from the
		// first completion.
		ginkgo.By("the more protected workload is dispatched once the first request has closed")
		var second journalEntry
		Eventually(func(g Gomega) {
			protectedVerdicts.observe(g)
			js := journal(g)
			if n := len(inFlight(js.Entries)); n > 1 {
				StopTrying(fmt.Sprintf("%d requests in flight at once", n)).Now()
			}
			entries := journalEntriesFor(js, ns.Name, protected.ISVC.Name)
			g.Expect(entries).To(HaveLen(1), func() string {
				return "request for the more protected workload; " + evacuationState(ns.Name, protected.ISVC.Name)
			})
			second = entries[0]
		}).WithTimeout(dispatchTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(second.CreatedAt).To(BeTemporally(">", *firstDone.CompletedAt),
			"request %s was dispatched before %s completed", second.UUID, first.UUID)
		Expect(second.FromNode).To(Equal(src))
		Expect(parseMigrationRequest(Default, second.Payload).Reason).To(Equal(nodeUnhealthyReason))
		fmt.Fprintf(ginkgo.GinkgoWriter, "priority ordering: second request %s after the first completion\n",
			second.CreatedAt.Sub(*firstDone.CompletedAt).Round(time.Second))

		ginkgo.By("the second migration completes and both workloads have left the node")
		waitMigrationCompleted(ns.Name, protected.ISVC.Name, v1beta1.EngineComponent, second.UUID)
		waitJournalPhase(second.UUID, "completed", kindMediumTimeout)
		for _, f := range movers {
			waitPodsOffNode(ns.Name, f.ISVC.Name, src)
		}
		Eventually(func(g Gomega) {
			for _, f := range movers {
				live := 0
				for _, p := range omenativePods(g, ns.Name, f.ISVC.Name) {
					if p.DeletionTimestamp != nil {
						continue
					}
					g.Expect(sourceUIDs).NotTo(HaveKey(p.UID), "pod %s is an original", p.Name)
					g.Expect(healthy).To(ContainElement(p.Spec.NodeName), "pod %s", p.Name)
					g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
					live++
				}
				g.Expect(live).To(Equal(1), "%s live pods", f.ISVC.Name)
				g.Expect(migrationRequestAnnotations(g, isvcKey(f))).To(BeEmpty(), "%s mailbox", f.ISVC.Name)
			}
			onSrc := livePodsOnNode(omenativePods(g, ns.Name, out.ISVC.Name), src)
			g.Expect(onSrc).To(HaveLen(1))
			g.Expect(onSrc[0].UID).To(Equal(outUID), "the opted-out pod was replaced")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The quiet window inside asserts one live pod on the node at every
		// poll; a UID unchanged across it means that pod was never replaced.
		ginkgo.By("the opted-out workload stays an ineligible advisory")
		expectIneligibleAdvisory(out, src)
		outOnSrc = livePodsOnNode(omenativePods(Default, ns.Name, out.ISVC.Name), src)
		Expect(outOnSrc).To(HaveLen(1))
		Expect(outOnSrc[0].UID).To(Equal(outUID), "the opted-out pod was replaced")
	})
})
