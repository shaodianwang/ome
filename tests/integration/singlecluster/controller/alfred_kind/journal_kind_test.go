package alfred_kind

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// versionGateWindow is how long the unsupported-version document is watched:
// the dispatcher reloads the journal on every pass, so four ticks hold at
// least two passes over the new document.
const versionGateWindow = 4 * decisionInterval

// seedUnreadableJournal overwrites state.json with raw and returns an
// idempotent restore that writes the empty document back, so the journal the
// spec recovers on holds nothing but what alfred writes afterwards. A spec
// that ends without calling it is restored at cleanup; a failed spec also has
// its fixture's mailbox cleared, since one unresolved request withholds every
// later dispatch. Cleanup is registered before the write so a failed write
// still restores.
func seedUnreadableJournal(fix *alfredFixture, raw string) (restore func()) {
	restored := false
	restore = func() {
		if restored {
			return
		}
		restored = true
		setJournal(emptyJournal)
	}
	ginkgo.DeferCleanup(func() {
		if ginkgo.CurrentSpecReport().Failed() {
			resetDispatchAfterFailure(fix)
			return
		}
		restore()
	})
	setJournal(raw)
	return restore
}

var _ = ginkgo.Describe("alfred dispatch journal", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-journal-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// The journal is the write-ahead record of every request alfred has
	// issued, reloaded from the API on every pass. When it cannot be read the
	// dispatcher cannot know what is already in flight, so it fails closed:
	// the pass still runs and reports, the admitted finding is withheld as
	// JournalUnavailable, and nothing reaches the mailbox. Alfred never
	// repairs or re-initialises the document, so the hold lasts until a
	// readable one is restored, and the pending finding then dispatches on
	// the next pass.
	ginkgo.It("a malformed journal withholds execution and a restored one lets it proceed", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("journal-bad"))
		readyOn(fix, src, 1)
		sourceUID := singleLivePod(Default, fix, src).UID
		waitNodeRecordGone(src)
		waitJournalSettled()

		leader := currentLeader()
		podsBefore := alfredPodStates(Default)
		restore := seedUnreadableJournal(fix, malformedJournal)
		markUnhealthy(src)

		// withheld is the finding as the dispatcher reports it while the
		// journal is unreadable; no request UUID is ever minted for it.
		withheld := SatisfyAll(
			HaveField("Policy", nodeHealthPolicy),
			HaveField("Reason", nodeUnhealthyReason),
			HaveField("Instance", int32(0)),
			HaveField("FromNode", src),
			HaveField("Outcome", "withheld"),
			HaveField("DispatchStatus", "withheld"),
			HaveField("DispatchReason", journalUnavailable),
			HaveField("RequestUUID", ""),
		)
		// heldClosed is every check of the closed state: the finding keeps
		// being reported with the journal reason (a transient observation gap
		// may add a component-wide advisory, nothing else may appear), the
		// document is byte for byte what was written, the mailbox and the IR
		// record nothing, the source pod stands, and alfred neither restarted
		// nor changed leader. A pass reporting instance 0 as anything but
		// withheld fails the hold by design; this spec is stricter than the
		// stall specs on purpose.
		heldClosed := func(g Gomega, document string) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(withheld), "last cycle: %s", describeRecs(recs))
			for _, r := range recs {
				if r.Outcome == advisoryOutcome {
					continue
				}
				g.Expect(r).To(withheld, "finding other than the journal hold: %s", describeRecs(recs))
			}
			g.Expect(journalRaw(g)).To(Equal(document), "alfred rewrote the unreadable journal")
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty(), "a request reached the mailbox")
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(BeEmpty(), "a migration was recorded")
			g.Expect(singleLivePod(g, fix, src).UID).To(Equal(sourceUID), "the source pod was replaced")
			g.Expect(alfredPodStates(g)).To(Equal(podsBefore), "an alfred pod restarted or was replaced")
			g.Expect(leaseHolder(g)).To(Equal(leader.holder), "leadership moved")
		}

		ginkgo.By("the finding is reported withheld as JournalUnavailable and no request is issued")
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		message := fmt.Sprintf("%s migration request uuid= for %s/%s instance 0: withheld (%s)",
			nodeHealthPolicy, workloadID(fix), v1beta1.EngineComponent, journalUnavailable)
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(withheld), "last cycle: %s", describeRecs(recs))
			g.Expect(alfredEventsFor(g, ns.Name, fix.ISVC.Name, "RecommendationWithheld")).To(ContainElement(SatisfyAll(
				HaveField("Type", corev1.EventTypeNormal),
				HaveField("Message", message),
			)), "RecommendationWithheld event")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The pass count proves the leader is still on its regular timer, so
		// the quiet window is a verdict repeated every pass, not a stalled loop.
		ginkgo.By("it stays closed pass after pass, and alfred keeps cycling")
		passesBefore := decisionPassesEventually(leader.pod)
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			verdicts.observe(g)
			heldClosed(g, malformedJournal)
		}).WithTimeout(dispatchQuietWindow).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
		Expect(decisionPassesEventually(leader.pod)-passesBefore).To(BeNumerically(">=", quietWindowMinPasses),
			"the leader completed too few passes in %s for the configured interval", dispatchQuietWindow)

		// A document that decodes but carries an unsupported version is
		// refused by a later check than the first document's shape; the
		// verdict is the same.
		ginkgo.By("a journal of an unsupported version alone is held the same way")
		setJournal(unsupportedVersionJournal)
		passesBefore = decisionPassesEventually(leader.pod)
		cycleBefore = recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			verdicts.observe(g)
			heldClosed(g, unsupportedVersionJournal)
		}).WithTimeout(versionGateWindow).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the version window")
		Expect(decisionPassesEventually(leader.pod)-passesBefore).To(BeNumerically(">=", cadenceMinPasses),
			"the leader completed too few passes in %s for the configured interval", versionGateWindow)

		// The restored document is empty, so the request alfred mints on the
		// next pass is the journal's only entry, created after the restore
		// and accepted by the controller after it. No alfred event is asserted
		// after the restore: the ISVC's Normal-event spam budget is largely
		// consumed by the per-pass withheld events, and the controller's
		// MigrationRequestAccepted comes from a different source bucket.
		ginkgo.By("the restored journal lets the pending finding dispatch and complete")
		restoredAt := time.Now()
		restore()
		entry := waitJournalEntry(ns.Name, fix.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
		Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(entry.Instance).To(Equal(int32(0)))
		Expect(entry.FromNode).To(Equal(src))
		Expect(entry.CreatedAt).To(BeTemporally(">=", restoredAt.Add(-eventSkew)), "the request predates the restore")
		waitControllerEvent(ns.Name, fix.ISVC.Name, "MigrationRequestAccepted", kindMediumTimeout)
		m := waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		Expect(m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
		Expect(m.SourceInstance).To(Equal(int32(0)))
		Expect(m.FromNode).To(Equal(src))
		Expect(m.StartedAt.Time).To(BeTemporally(">=", restoredAt.Add(-eventSkew)), "the row predates the restore")
		closed := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		Expect(closed.AcknowledgedAt).NotTo(BeNil())
		Expect(closed.CompletedAt).NotTo(BeNil())

		ginkgo.By("the pod moved, the mailbox is empty, and the journal holds only that entry")
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			live := 0
			for _, p := range omenativePods(g, ns.Name, fix.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				live++
				g.Expect(p.UID).NotTo(Equal(sourceUID), "pod %s is the original", p.Name)
				g.Expect(otherWorkers(src)).To(ContainElement(p.Spec.NodeName), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
			}
			g.Expect(live).To(Equal(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		entries := journal(Default).Entries
		Expect(entries).To(HaveLen(1), func() string { return "journal after the restore: " + describeJournal(entries) })
		Expect(entries[0].UUID).To(Equal(entry.UUID))
		Expect(migrationEntries(Default, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
		Expect(alfredPodStates(Default)).To(Equal(podsBefore), "an alfred pod restarted or was replaced")
	})
})
