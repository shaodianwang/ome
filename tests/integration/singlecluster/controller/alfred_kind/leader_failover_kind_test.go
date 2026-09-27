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

var _ = ginkgo.Describe("alfred leader failover", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-leader-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// The journal is the only state a request has across leaders: the
	// dispatcher reloads it from the API on every pass and keeps no copy, so a
	// replica taking over resumes exactly the entry it finds there. With the
	// controller away the request cannot complete underneath the handoff,
	// which makes the in-flight state deterministic: the new leader must keep
	// the one entry, let it stall on the deadline the journal carries, and
	// never issue it again. The node's episode record is re-seeded from the
	// recommendations ConfigMap the same way, so the repair signal is not
	// raised twice; that carry-over compares the condition's lastTransitionTime
	// (stamped by the test host) with alfred's observedAt (container clock), so
	// a container clock running behind the host can re-signal the node; the CI
	// lane shares one kernel clock. When the controller returns, the new leader
	// closes the same entry from the controller's row.
	ginkgo.It("a new leader resumes an in-flight request from the journal without re-dispatching", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("leader-failover"))
		readyOn(fix, src, 1)
		sourceUID := singleLivePod(Default, fix, src).UID
		waitNodeRecordGone(src)
		waitJournalSettled()

		restart := stopControllerForSpec(fix)
		markUnhealthy(src)

		// A request consumed with the controller stopped means the stop never
		// took effect, which no amount of waiting corrects.
		ginkgo.By("the leader submits the evacuation and nothing consumes it")
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		state := func() string { return evacuationState(ns.Name, fix.ISVC.Name) }
		var entry journalEntry
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			entries := journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)
			for _, e := range entries {
				switch e.Phase {
				case "failed":
					StopTrying(fmt.Sprintf("journal %s is failed: %s", e.UUID, e.Reason)).Now()
				case "acknowledged", "completed":
					StopTrying(fmt.Sprintf("journal %s is %s: the request was consumed while the controller was stopped", e.UUID, e.Phase)).Now()
				}
			}
			g.Expect(entries).To(HaveLen(1), func() string { return "request for the workload; " + state() })
			entry = entries[0]
			g.Expect(entry.Phase).To(BeElementOf("submitted", "stalled"), func() string { return "request phase; " + state() })
		}).WithTimeout(dispatchTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(entry.Instance).To(Equal(int32(0)))
		Expect(entry.FromNode).To(Equal(src))
		Expect(migrationRequestAnnotations(Default, isvcKey(fix))).To(HaveKeyWithValue(entry.UUID, entry.Payload), "the request is in the mailbox")
		// LastAttempt is stamped on every attempt to patch the request onto
		// the workload, so a value unchanged across the handoff proves the
		// request was not issued again.
		Expect(entry.LastAttempt).NotTo(BeNil(), "a submitted entry carries its attempt time")

		// The pass that dispatched also opened the node's episode: one record
		// with the time it was signaled, and one repair event. Both are what
		// the next leader must inherit rather than recreate.
		var signaledAt time.Time
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.State).To(Equal(nodeHealthUnhealthy), "%s health state", src)
			g.Expect(rec.SignaledAt).NotTo(BeNil(), "%s was not signaled for repair", src)
			signaledAt = *rec.SignaledAt
			g.Expect(nodeEvents(g, src, "NodeRepairNeeded")).To(HaveLen(1))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The Lease is not released on shutdown, so the followers take over
		// only once it expires; the takeover waits the old pod out entirely,
		// so every pass observed from here on is the new leader's.
		ginkgo.By("the leader pod is deleted and another replica takes the lease")
		oldLeader := currentLeader()
		deletedAt := time.Now()
		takeOverLeadership(oldLeader)
		newLeader := currentLeader()
		Expect(newLeader.pod).NotTo(Equal(oldLeader.pod))
		Expect(newLeader.holder).NotTo(Equal(oldLeader.holder))
		fmt.Fprintf(ginkgo.GinkgoWriter, "leadership passed from %s to %s in %s\n",
			oldLeader.pod, newLeader.pod, time.Since(deletedAt).Round(time.Second))
		Eventually(func(g Gomega) {
			g.Expect(leaderCount(g)).To(Equal(1), "pods reporting leadership")
			g.Expect(leaseHolder(g)).To(Equal(newLeader.holder), "lease %s holder", alfredLeaseName)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The new leader runs on the configured interval from its first pass;
		// its cycle record carries the request it found in the journal, not a
		// new one.
		ginkgo.By("the new leader cycles on the inherited journal")
		cycleAtHandoff := recommendations(Default).Timestamp
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			cycle := recommendations(g)
			g.Expect(cycle.Timestamp).To(BeTemporally(">", cycleAtHandoff), "no decision cycle since the handoff")
			g.Expect(recsFor(cycle, workloadID(fix))).To(ContainElement(SatisfyAll(
				HaveField("RequestUUID", entry.UUID),
				HaveField("Policy", nodeHealthPolicy),
				HaveField("Reason", nodeUnhealthyReason),
				HaveField("Instance", int32(0)),
				HaveField("FromNode", src),
				HaveField("DispatchStatus", BeElementOf("submitted", "stalled")),
			)), func() string { return "the inherited request in the cycle record; " + state() })
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The deadline runs from the entry's creation time in the journal,
		// not from the new leader's start: whichever leader observes the
		// transition arms the failure backoff at that moment, so the backoff
		// lands between the deadline and the time the stall is seen here.
		ginkgo.By("the request is stalled once the acknowledgement timeout has elapsed")
		stalled := waitJournalPhase(entry.UUID, "stalled", kindMediumTimeout)
		stalledSeen := time.Now()
		Expect(stalled.Reason).To(Equal("AcknowledgementTimeout"))
		Expect(stalled.AcknowledgedAt).To(BeNil())
		Expect(stalled.CompletedAt).To(BeNil())
		Expect(stalled.CreatedAt).To(BeTemporally("==", entry.CreatedAt))
		Expect(stalled.Payload).To(Equal(entry.Payload))
		Expect(stalledSeen.Sub(entry.CreatedAt)).To(BeNumerically(">=", acknowledgementTimeout-eventSkew), "stalled before the acknowledgement timeout elapsed")
		fmt.Fprintf(ginkgo.GinkgoWriter, "request %s stalled %s after it was created\n", entry.UUID, stalledSeen.Sub(entry.CreatedAt).Round(time.Second))
		Eventually(func(g Gomega) {
			js := journal(g)
			g.Expect(js.BackoffUntil).NotTo(BeNil(), "a stall arms the failure backoff")
			g.Expect(*js.BackoffUntil).To(BeTemporally(">=", entry.CreatedAt.Add(acknowledgementTimeout+failureBackoff-eventSkew)), "the backoff was armed before the deadline")
			g.Expect(*js.BackoffUntil).To(BeTemporally("<=", stalledSeen.Add(failureBackoff+eventSkew)), "the backoff was armed after the stall was seen")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Every check that would betray a second dispatch: a second entry, a
		// new attempt time, a changed mailbox, a controller row, a replaced
		// source pod, a submission event naming another UUID. The node record
		// keeps the time it was first signaled and the repair event is not
		// raised again. The pass count proves the new leader is on the
		// regular timer, so quiet is a verdict, not an idle loop.
		ginkgo.By("the new leader neither re-issues nor withdraws the stalled request")
		passesBefore := decisionPassesEventually(newLeader.pod)
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			verdicts.observe(g)
			entries := journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)
			g.Expect(entries).To(HaveLen(1), func() string { return "a stalled request is never re-issued: " + describeJournal(entries) })
			g.Expect(entries[0].UUID).To(Equal(entry.UUID))
			g.Expect(entries[0].Phase).To(Equal("stalled"))
			g.Expect(entries[0].Reason).To(Equal("AcknowledgementTimeout"))
			g.Expect(entries[0].CreatedAt).To(BeTemporally("==", entry.CreatedAt))
			g.Expect(entries[0].LastAttempt).NotTo(BeNil())
			g.Expect(*entries[0].LastAttempt).To(BeTemporally("==", *entry.LastAttempt), "the request was attempted again")
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(Equal(map[string]string{entry.UUID: entry.Payload}), "the mailbox changed")
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(BeEmpty())
			g.Expect(singleLivePod(g, fix, src).UID).To(Equal(sourceUID), "the source pod was replaced")
			g.Expect(leaseHolder(g)).To(Equal(newLeader.holder), "leadership moved again")
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.SignaledAt).NotTo(BeNil(), "%s lost its repair signal", src)
			g.Expect(*rec.SignaledAt).To(BeTemporally("==", signaledAt), "the repair signal was re-stamped")
			g.Expect(nodeEvents(g, src, "NodeRepairNeeded")).To(HaveLen(1), "repair signaled again")
			// Each leader reports the entry under its own recorder, so the
			// count of MigrationSubmitted events is not a count of submissions;
			// a second submission would carry a UUID other than the entry's.
			for _, e := range alfredEventsFor(g, ns.Name, fix.ISVC.Name, "MigrationSubmitted") {
				g.Expect(e.Message).To(ContainSubstring(entry.UUID), "a submission other than %s was reported", entry.UUID)
			}
		}).WithTimeout(stallQuietWindow).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
		Expect(decisionPassesEventually(newLeader.pod)-passesBefore).To(BeNumerically(">=", quietWindowMinPasses),
			"the new leader completed too few passes in %s for the configured interval", stallQuietWindow)

		// The returning controller finds the request in the mailbox and
		// accepts it; the row is stamped after the restart, and the new leader
		// closes the inherited entry from that row with its original creation
		// time. The acknowledged phase is skipped when the row is first seen
		// terminal, so only the closed entry is waited for.
		ginkgo.By("the controller returns and the new leader closes the entry from its row")
		restartedAt := time.Now()
		restart()
		waitControllerEvent(ns.Name, fix.ISVC.Name, "MigrationRequestAccepted", kindMediumTimeout)
		m := waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		Expect(m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
		Expect(m.SourceInstance).To(Equal(int32(0)))
		Expect(m.FromNode).To(Equal(src))
		Expect(m.StartedAt.Time).To(BeTemporally(">=", restartedAt.Add(-eventSkew)), "the row is stamped by the returning controller")
		Expect(m.CompletedAt).NotTo(BeNil())
		closed := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		Expect(closed.Reason).To(Equal("TerminalStatusObserved"), "the stall reason is replaced")
		Expect(closed.CreatedAt).To(BeTemporally("==", entry.CreatedAt))
		Expect(closed.AcknowledgedAt).NotTo(BeNil())
		Expect(closed.CompletedAt).NotTo(BeNil())
		Expect(*closed.AcknowledgedAt).To(BeTemporally(">=", restartedAt.Add(-eventSkew)), "acknowledgement is taken from the row")
		Expect(*closed.CompletedAt).To(BeTemporally(">=", *closed.AcknowledgedAt))
		Expect(leaseHolder(Default)).To(Equal(newLeader.holder), "leadership moved again")

		// The episode the old leader opened is closed by the new one: the
		// drained time is stamped after the signal time it inherited.
		ginkgo.By("the pod moved, the mailbox is empty, and the journal holds the one entry")
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
			g.Expect(leaderCount(g)).To(Equal(1), "pods reporting leadership")
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.SignaledAt).NotTo(BeNil(), "%s lost its repair signal", src)
			g.Expect(*rec.SignaledAt).To(BeTemporally("==", signaledAt), "the repair signal was re-stamped")
			g.Expect(rec.DrainedAt).NotTo(BeNil(), "%s is not recorded as drained", src)
			g.Expect(*rec.DrainedAt).To(BeTemporally(">", signaledAt))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(journalEntriesFor(journal(Default), ns.Name, fix.ISVC.Name)).To(HaveLen(1), "failover must not duplicate the request")
		Expect(migrationEntries(Default, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
		Expect(nodeEvents(Default, src, "NodeRepairNeeded")).To(HaveLen(1), "repair signaled again")
		waitAlfredReady()
	})
})
