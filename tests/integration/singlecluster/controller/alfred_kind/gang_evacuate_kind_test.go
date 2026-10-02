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

var _ = ginkgo.Describe("alfred evacuates a whole gang instance", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-gang-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("moves leader and worker together off the bad node", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("evac-gang"), withGang(1))
		readyOn(fix, src, 1)

		// Leader and worker join one Instance in alfred's snapshot, which is
		// steady only while the row counts both members.
		ginkgo.By("one Ready Instance holds both members on the node")
		leader, worker := expectGangShape(Default, livePodsByRunner(livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)))
		oldUIDs := map[types.UID]struct{}{leader.UID: {}, worker.UID: {}}
		rows := instanceRows(Default, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].Index).To(Equal(int32(0)))
		Expect(rows[0].PodCount).To(Equal(int32(2)))
		Expect(rows[0].ServingPodCount).To(Equal(int32(2)))
		Expect(rows[0].AvailablePodCount).To(Equal(int32(2)))

		waitNodeRecordGone(src)
		waitJournalSettled()
		markUnhealthy(src)

		// The node-health policy emits one finding per Instance, so a gang is
		// one request; its payload is the single-pod contract with the
		// unhealthy member's node as from_node.
		ginkgo.By("alfred writes one request for the whole Instance")
		entry := waitJournalEntry(ns.Name, fix.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
		Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(entry.Instance).To(Equal(int32(0)))
		Expect(entry.FromNode).To(Equal(src))
		// Targets are the distinct nodes the simulation placed the two
		// replacement members on: one when they share a node, else two.
		Expect(entry.Targets).To(SatisfyAll(SatisfyAny(HaveLen(1), HaveLen(2)), HaveEach(BeElementOf(otherWorkers(src)))),
			"predicted targets must be one or two healthy workers")
		req := parseMigrationRequest(Default, entry.Payload)
		Expect(req.SchemaVersion).To(Equal("v1"))
		Expect(req.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(req.Instance).To(Equal(int32(0)))
		Expect(req.FromNode).To(Equal(src))
		Expect(req.Reason).To(Equal(nodeUnhealthyReason))
		Expect(req.RequestedBy).To(Equal("alfred"))
		_, err := time.Parse(time.RFC3339Nano, req.RequestedAt)
		Expect(err).NotTo(HaveOccurred(), "requested_at %q is not RFC 3339", req.RequestedAt)
		Expect(req.HintTargetNodes).To(ConsistOf(otherWorkers(src)), "hints are every eligible node of the pool")

		// A per-member request would surface as a second journal entry or a
		// second mailbox annotation; either ends the wait at once.
		ginkgo.By("one journal entry and at most one mailbox slot until the controller completes it")
		var m v1beta1.MigrationStatus
		Eventually(func(g Gomega) {
			entries := journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)
			if len(entries) != 1 {
				StopTrying("the gang must be one request; journal: " + describeJournal(entries)).Now()
			}
			var pending []string
			for uuid := range migrationRequestAnnotations(g, isvcKey(fix)) {
				pending = append(pending, uuid)
			}
			if len(pending) > 1 || (len(pending) == 1 && pending[0] != entry.UUID) {
				StopTrying(fmt.Sprintf("the mailbox must hold only request %s; has %v", entry.UUID, pending)).Now()
			}
			m = expectMigrationCompleted(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		}).WithTimeout(migrationTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The controller mirrors the request as a Manual migration of the
		// source Instance and moves the whole gang to a fresh surge index.
		ginkgo.By("the controller recorded one Manual migration to a fresh index")
		Expect(m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
		Expect(m.SourceInstance).To(Equal(int32(0)))
		Expect(m.FromNode).To(Equal(src))
		Expect(m.HintTargetNodes).To(Equal(req.HintTargetNodes))
		Expect(m.Reason).To(Equal(req.Reason))
		Expect(m.SurgeInstance).NotTo(BeNil())
		surgeIdx := *m.SurgeInstance
		Expect(surgeIdx).NotTo(Equal(int32(0)), "the surge takes a fresh index")
		Expect(m.AllocatedAt).NotTo(BeNil())
		Expect(m.CompletedAt).NotTo(BeNil())

		// alfred's completion event is best effort: the journal's completed
		// phase is authoritative, and a slow migration can spend the ISVC's
		// event burst on alfred's per-pass events before it is recorded.
		ginkgo.By("both sides name the same request in their events")
		waitEventUUID(ns.Name, fix.ISVC.Name, "MigrationRequestAccepted", controllerEventSource, entry.UUID, kindMediumTimeout)
		waitEventUUID(ns.Name, fix.ISVC.Name, "MigrationCompleted", controllerEventSource, entry.UUID, kindMediumTimeout)
		waitEventUUID(ns.Name, fix.ISVC.Name, "MigrationSubmitted", alfredEventSource, entry.UUID, kindMediumTimeout)
		waitAlfredEventBestEffort(ns.Name, fix.ISVC.Name, "MigrationCompleted", entry.UUID, kindShortTimeout)

		// The surge mirrors the source's runner layout: a fresh leader and a
		// fresh worker, both placed away from the source node; the source row
		// is dropped once the surge is promoted and keeps its own index.
		ginkgo.By("the whole gang moved: fresh leader and worker, one Ready Instance at the surge index")
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			newLeader, newWorker := expectGangShape(g, livePodsByRunner(omenativePods(g, ns.Name, fix.ISVC.Name)))
			for _, p := range []corev1.Pod{newLeader, newWorker} {
				_, old := oldUIDs[p.UID]
				g.Expect(old).To(BeFalse(), "%s is a source pod, not a replacement", p.Name)
				g.Expect(otherWorkers(src)).To(ContainElement(p.Spec.NodeName), "%s landed on %s", p.Name, p.Spec.NodeName)
				g.Expect(podIsReady(&p)).To(BeTrue(), "%s is not Ready", p.Name)
			}
			rows := instanceRows(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].Index).To(Equal(surgeIdx))
			g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(rows[0].Operation).To(BeNil())
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		closed := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		Expect(closed.AcknowledgedAt).NotTo(BeNil())
		Expect(closed.CompletedAt).NotTo(BeNil())
		Expect(journalEntriesFor(journal(Default), ns.Name, fix.ISVC.Name)).To(HaveLen(1))

		ginkgo.By("the node record is drained")
		waitNodeEvent(src, "NodeRepairNeeded", kindMediumTimeout)
		waitNodeEvent(src, "NodeDrainedForRepair", kindMediumTimeout)
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(BeEmpty())
			g.Expect(rec.OMEAcceleratorOccupantsPresent).To(BeFalse())
			g.Expect(rec.SignaledAt).NotTo(BeNil())
			g.Expect(rec.DrainedAt).NotTo(BeNil())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Both members left the unhealthy node in one move, so the policy has
		// nothing left to request per member.
		ginkgo.By("no per-member follow-up")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
			pods := omenativePods(g, ns.Name, fix.ISVC.Name)
			g.Expect(livePodsOnNode(pods, src)).To(BeEmpty())
			leader, worker := expectGangShape(g, livePodsByRunner(pods))
			for _, p := range []corev1.Pod{leader, worker} {
				g.Expect(otherWorkers(src)).To(ContainElement(p.Spec.NodeName), "%s landed on %s", p.Name, p.Spec.NodeName)
				g.Expect(podIsReady(&p)).To(BeTrue(), "%s is not Ready", p.Name)
			}
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
