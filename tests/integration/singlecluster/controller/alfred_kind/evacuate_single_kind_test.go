package alfred_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred evacuates a single-pod instance off an unhealthy node", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-evac-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("requests one migration through the annotation contract and the controller completes it", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("evac-single"))
		readyOn(fix, src, 1)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(1))
		oldUID := onSrc[0].UID

		waitNodeRecordGone(src)
		markUnhealthy(src)

		// The journal entry is the durable intent; its payload is the exact
		// annotation value the controller consumes.
		ginkgo.By("alfred writes exactly one v1 request for this workload")
		entry := waitJournalEntry(ns.Name, fix.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
		Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(entry.Instance).To(Equal(int32(0)))
		Expect(entry.FromNode).To(Equal(src))
		Expect(entry.Targets).To(HaveLen(1), "one replacement pod, one predicted placement")
		Expect(otherWorkers(src)).To(ContainElement(entry.Targets[0]), "the predicted target must be a healthy worker")
		req := parseMigrationRequest(Default, entry.Payload)
		Expect(req.SchemaVersion).To(Equal("v1"))
		Expect(req.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(req.Instance).To(Equal(int32(0)))
		Expect(req.FromNode).To(Equal(src))
		Expect(req.Reason).To(Equal(nodeUnhealthyReason))
		Expect(req.RequestedBy).To(Equal("alfred"))
		_, err := time.Parse(time.RFC3339Nano, req.RequestedAt)
		Expect(err).NotTo(HaveOccurred(), "requested_at %q is not RFC 3339", req.RequestedAt)
		// Hints are every eligible node of the pool up to alfred's cap of three;
		// this cluster has exactly two healthy workers.
		Expect(req.HintTargetNodes).To(ConsistOf(otherWorkers(src)))
		Expect(journalEntriesFor(journal(Default), ns.Name, fix.ISVC.Name)).To(HaveLen(1))

		// The annotation is consumed into status.migrations as a Manual entry
		// that mirrors the request; the surge is placed under a required
		// exclusion of fromNode and the source is finalized before Completed.
		ginkgo.By("the controller accepts, executes and records it")
		m := waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		Expect(m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
		Expect(m.SourceInstance).To(Equal(int32(0)))
		Expect(m.FromNode).To(Equal(src))
		Expect(m.HintTargetNodes).To(Equal(req.HintTargetNodes))
		Expect(m.Reason).To(Equal(req.Reason))
		Expect(m.SurgeInstance).NotTo(BeNil())
		Expect(*m.SurgeInstance).NotTo(Equal(m.SourceInstance))
		Expect(m.AllocatedAt).NotTo(BeNil())
		Expect(m.CompletedAt).NotTo(BeNil())
		waitControllerEvent(ns.Name, fix.ISVC.Name, "MigrationRequestAccepted", kindMediumTimeout)

		ginkgo.By("the pod moved, the mailbox is empty, the journal closed")
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			live := 0
			for _, p := range omenativePods(g, ns.Name, fix.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				live++
				g.Expect(p.UID).NotTo(Equal(oldUID))
				g.Expect(otherWorkers(src)).To(ContainElement(p.Spec.NodeName))
				g.Expect(podIsReady(&p)).To(BeTrue())
			}
			g.Expect(live).To(Equal(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		closed := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		Expect(closed.AcknowledgedAt).NotTo(BeNil())
		Expect(closed.CompletedAt).NotTo(BeNil())

		// The node is signaled when the episode opens and reported drained
		// once no OME GPU occupant remains on it.
		ginkgo.By("events and the node record tell the story")
		// MigrationAcknowledged fires only when a tick sees the migration in
		// flight; an entry first seen terminal closes straight to completed,
		// so only the journal's acknowledgedAt is guaranteed. alfred's
		// completion event is best effort: the journal's completed phase is
		// authoritative, and a slow migration can spend the ISVC's event burst
		// on alfred's per-pass events before it is recorded.
		waitAlfredEvent(ns.Name, fix.ISVC.Name, "MigrationSubmitted", kindMediumTimeout)
		waitAlfredEventBestEffort(ns.Name, fix.ISVC.Name, "MigrationCompleted", "", kindShortTimeout)
		waitControllerEvent(ns.Name, fix.ISVC.Name, "MigrationCompleted", kindMediumTimeout)
		waitNodeEvent(src, "NodeRepairNeeded", kindMediumTimeout)
		waitNodeEvent(src, "NodeDrainedForRepair", kindMediumTimeout)
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(BeEmpty())
			g.Expect(rec.OMEAcceleratorOccupantsPresent).To(BeFalse())
			g.Expect(rec.SignaledAt).NotTo(BeNil())
			g.Expect(rec.DrainedAt).NotTo(BeNil())
			g.Expect(*rec.DrainedAt).To(BeTemporally(">", *rec.SignaledAt))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The evacuated workload has left the unhealthy node, so the policy
		// has nothing further to move while the node stays unhealthy.
		ginkgo.By("no second request follows")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
			g.Expect(livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)).To(BeEmpty())
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
