package alfred_kind

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred acknowledgement timeout", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-ack-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// With the controller away, alfred's request sits in the mailbox and no
	// status row ever appears. The dispatcher marks the entry stalled once the
	// acknowledgement timeout has elapsed since the entry was created, arms its
	// failure backoff, and keeps holding the serial gate: a stalled request is
	// neither re-issued nor withdrawn. When the controller returns it consumes
	// the request as usual, and the same entry closes as completed from the
	// status row.
	ginkgo.It("marks an unacknowledged request stalled and resolves it once the controller returns", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("ack-timeout"))
		readyOn(fix, src, 1)
		sourceUID := singleLivePod(Default, fix, src).UID
		waitNodeRecordGone(src)
		waitJournalSettled()

		managerBefore := kindutil.Suite().Manager
		restart := stopControllerForSpec(fix)
		markUnhealthy(src)

		// A request consumed with the controller stopped means the stop never
		// took effect, which no amount of waiting corrects.
		ginkgo.By("alfred submits the evacuation and nothing consumes it")
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
		Expect(migrationRequestAnnotations(Default, isvcKey(fix))).To(HaveKeyWithValue(entry.UUID, entry.Payload), "the request stays in the mailbox")
		Expect(migrationEntries(Default, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(BeEmpty(), "a status row without the controller")

		// The deadline runs from the entry's creation; a stall observed any
		// earlier would be immediate rather than timed. It is never
		// acknowledged, which sets it apart from the stalls of an acknowledged
		// request.
		ginkgo.By("the request is stalled once the acknowledgement timeout has elapsed")
		stalled := waitJournalPhase(entry.UUID, "stalled", kindMediumTimeout)
		stalledSeen := time.Now()
		Expect(stalled.Reason).To(Equal("AcknowledgementTimeout"))
		Expect(stalled.AcknowledgedAt).To(BeNil())
		Expect(stalled.CompletedAt).To(BeNil())
		Expect(stalledSeen.Sub(entry.CreatedAt)).To(BeNumerically(">=", acknowledgementTimeout-eventSkew), "stalled before the acknowledgement timeout elapsed")
		fmt.Fprintf(ginkgo.GinkgoWriter, "request %s stalled %s after it was created\n", entry.UUID, stalledSeen.Sub(entry.CreatedAt).Round(time.Second))

		// The stalled request is reported every pass in place of the policy's
		// own finding for the Instance, and the stall arms the failure backoff.
		ginkgo.By("the stall is reported in the cycle record, as a Warning event, and as a dispatch backoff")
		message := fmt.Sprintf("%s migration request uuid=%s for %s/%s instance 0: stalled (AcknowledgementTimeout)",
			nodeHealthPolicy, entry.UUID, workloadID(fix), v1beta1.EngineComponent)
		Eventually(func(g Gomega) {
			g.Expect(recsFor(recommendations(g), workloadID(fix))).To(ContainElement(SatisfyAll(
				HaveField("RequestUUID", entry.UUID),
				HaveField("Policy", nodeHealthPolicy),
				HaveField("Reason", nodeUnhealthyReason),
				HaveField("Instance", int32(0)),
				HaveField("FromNode", src),
				HaveField("Outcome", "stalled"),
				HaveField("DispatchStatus", "stalled"),
				HaveField("DispatchReason", "AcknowledgementTimeout"),
			)), func() string { return "the stalled request in the cycle record; " + state() })
			g.Expect(alfredEventsFor(g, ns.Name, fix.ISVC.Name, "MigrationStalled")).To(ContainElement(SatisfyAll(
				HaveField("Type", corev1.EventTypeWarning),
				HaveField("Message", message),
			)), "MigrationStalled event")
			js := journal(g)
			g.Expect(js.BackoffUntil).NotTo(BeNil(), "a stall arms the failure backoff")
			g.Expect(*js.BackoffUntil).To(BeTemporally("~", stalledSeen.Add(failureBackoff), failureBackoff))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the stalled request is neither re-issued nor withdrawn")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			entries := journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)
			g.Expect(entries).To(HaveLen(1), func() string { return "a stalled request is never re-issued: " + describeJournal(entries) })
			g.Expect(entries[0].UUID).To(Equal(entry.UUID))
			g.Expect(entries[0].Phase).To(Equal("stalled"))
			g.Expect(entries[0].Reason).To(Equal("AcknowledgementTimeout"))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(HaveKeyWithValue(entry.UUID, entry.Payload), "the request left the mailbox")
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(BeEmpty())
			g.Expect(singleLivePod(g, fix, src).UID).To(Equal(sourceUID), "the source pod was replaced")
		}).WithTimeout(stallQuietWindow).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")

		// The returning controller finds the request in the mailbox and accepts
		// it: the status row is stamped after the restart, long after the entry
		// was created, and alfred closes the entry from that row. The
		// acknowledged phase in between is skipped when the row is first seen
		// terminal, so only the closed entry is waited for.
		ginkgo.By("the controller returns and consumes the request")
		restartedAt := time.Now()
		restart()
		Expect(kindutil.Suite().Manager).NotTo(BeNil())
		Expect(kindutil.Suite().Manager).NotTo(BeIdenticalTo(managerBefore), "the restart must start a new manager instance")
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
		Expect(*closed.AcknowledgedAt).To(BeTemporally(">=", closed.CreatedAt))
		Expect(*closed.CompletedAt).To(BeTemporally(">=", *closed.AcknowledgedAt))

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
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(journalEntriesFor(journal(Default), ns.Name, fix.ISVC.Name)).To(HaveLen(1))
		Expect(migrationEntries(Default, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
	})
})
