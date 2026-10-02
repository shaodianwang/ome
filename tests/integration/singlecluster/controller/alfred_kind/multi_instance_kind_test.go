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

// The drained workload: single-pod Instances that all sit on the node that
// goes unhealthy. Alfred issues one request at a time and, between two health
// evacuations of one workload, waits out the health cooldown floor after the
// earlier one completes, so this count sets the spec's wall-clock. The
// deployed hourly budget is far above it.
const drainInstances = 3

// lowestUnused is the smallest non-negative index absent from set: the slot
// the controller gives a migration's surge Instance.
func lowestUnused(set map[int32]struct{}) int32 {
	for i := int32(0); ; i++ {
		if _, taken := set[i]; !taken {
			return i
		}
	}
}

var _ = ginkgo.Describe("alfred drains a multi-instance workload off an unhealthy node", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-multi-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("evacuates each instance in series and reports the node drained only after the last one left", func() {
		src := workers[0]
		healthy := otherWorkers(src)
		fix := newAlfredFixture(ns.Name, withName("drain"), withEngineReplicas(drainInstances))
		readyOn(fix, src, drainInstances)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(drainInstances))
		sourceUIDs := map[types.UID]struct{}{}
		for _, p := range onSrc {
			sourceUIDs[p.UID] = struct{}{}
		}
		srcInstances := instanceIndices(Default, onSrc)
		Expect(srcInstances).To(HaveLen(drainInstances))

		waitNodeRecordGone(src)
		waitJournalSettled()
		markUnhealthy(src)

		// remaining are the Instances still to be requested; rows tracks the
		// Instance index set as each completed migration retires its source
		// and promotes its surge, which the controller seats at the lowest
		// index no row holds, so a freed index is reused by the next move.
		remaining := map[int32]struct{}{}
		rows := map[int32]struct{}{}
		for idx := range srcInstances {
			remaining[idx] = struct{}{}
			rows[idx] = struct{}{}
		}
		seen := map[string]struct{}{}
		var closed []journalEntry
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		for k := 0; k < drainInstances; k++ {
			// A later request waits out the floor after the previous
			// completion; dispatchTimeout covers a one-minute cooldown, a tick
			// and the dispatch itself.
			ginkgo.By(fmt.Sprintf("request %d of %d is issued for one instance still on the node", k+1, drainInstances))
			entry := waitJournalEntryAfter(ns.Name, fix.ISVC.Name, seen, verdicts, dispatchTimeout, "submitted", "acknowledged", "completed")
			seen[entry.UUID] = struct{}{}
			Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
			Expect(entry.FromNode).To(Equal(src))
			Expect(remaining).To(HaveKey(entry.Instance), "request %s names instance %d, which was already moved or never on %s", entry.UUID, entry.Instance, src)
			delete(remaining, entry.Instance)
			req := parseMigrationRequest(Default, entry.Payload)
			Expect(req.Instance).To(Equal(entry.Instance))
			Expect(req.FromNode).To(Equal(src))
			Expect(req.Reason).To(Equal(nodeUnhealthyReason))
			Expect(req.RequestedBy).To(Equal("alfred"))
			if k > 0 {
				prev := closed[k-1]
				Expect(entry.CreatedAt).To(BeTemporally(">", *prev.CompletedAt),
					"request %s for instance %d was issued before request %s for instance %d completed", entry.UUID, entry.Instance, prev.UUID, prev.Instance)
				gap := entry.CreatedAt.Sub(*prev.CompletedAt)
				Expect(gap).To(BeNumerically(">=", deployedHealthFloor),
					"request %s came %s after the previous completion, inside the health cooldown floor", entry.UUID, gap)
				fmt.Fprintf(ginkgo.GinkgoWriter, "request %d (instance %d) issued %s after the previous completion\n", k+1, entry.Instance, gap.Round(time.Second))
			}

			// The other Instances' findings are held by a different verdict
			// in each stretch: the arbiter turns them away as WorkloadBusy in
			// the pass that admits this request; while the migration is in
			// flight the workload fails the eligibility baseline, so they are
			// reported advisory and never reach the arbiter; between its
			// completion and the next dispatch they are rejected Cooldown, the
			// health floor. None is issued, and the node still holds this
			// workload's GPUs, so it cannot count as drained.
			if len(remaining) > 0 {
				ginkgo.By("the other instances are turned away and the node still counts as occupied")
				Eventually(func(g Gomega) {
					verdicts.observe(g)
					recs := recsFor(recommendations(g), workloadID(fix))
					for idx := range remaining {
						g.Expect(recs).To(ContainElement(SatisfyAll(HaveField("Instance", idx), heldBehindRequest())),
							"instance %d; last cycle: %s", idx, describeRecs(recs))
					}
					rec, ok := nodeRecordFor(g, src)
					g.Expect(ok).To(BeTrue(), "node record for %s", src)
					g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
					g.Expect(rec.OMEAcceleratorOccupantsPresent).To(BeTrue())
					g.Expect(rec.DrainedAt).To(BeNil(), "node reported drained with %d instance(s) still on it", len(remaining))
					g.Expect(nodeEvents(g, src, "NodeDrainedForRepair")).To(BeEmpty(), "node reported drained with %d instance(s) still on it", len(remaining))
				}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
			}

			ginkgo.By("the controller completes it as a Manual migration to the lowest free index")
			m := waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
			Expect(m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
			Expect(m.SourceInstance).To(Equal(entry.Instance))
			Expect(m.FromNode).To(Equal(src))
			Expect(m.Reason).To(Equal(req.Reason))
			Expect(m.SurgeInstance).NotTo(BeNil())
			Expect(*m.SurgeInstance).To(Equal(lowestUnused(rows)), "surge index for migration %s with rows %v", entry.UUID, sortedIndices(rows))
			Expect(m.CompletedAt).NotTo(BeNil())
			delete(rows, m.SourceInstance)
			rows[*m.SurgeInstance] = struct{}{}
			done := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
			Expect(done.AcknowledgedAt).NotTo(BeNil())
			Expect(done.CompletedAt).NotTo(BeNil())
			closed = append(closed, done)
		}
		Expect(remaining).To(BeEmpty())

		ginkgo.By("every pod is a fresh one off the node, one Ready row per instance, mailbox empty")
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			var live []corev1.Pod
			for _, p := range omenativePods(g, ns.Name, fix.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				g.Expect(sourceUIDs).NotTo(HaveKey(p.UID), "pod %s is an original", p.Name)
				g.Expect(healthy).To(ContainElement(p.Spec.NodeName), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
				live = append(live, p)
			}
			g.Expect(live).To(HaveLen(drainInstances))
			irRows := instanceRows(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(irRows).To(HaveLen(drainInstances))
			rowSet := map[int32]struct{}{}
			for _, r := range irRows {
				g.Expect(r.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "instance %d", r.Index)
				g.Expect(r.Operation).To(BeNil(), "instance %d", r.Index)
				rowSet[r.Index] = struct{}{}
			}
			g.Expect(sortedIndices(rowSet)).To(Equal(sortedIndices(rows)))
			g.Expect(sortedIndices(rowSet)).To(Equal(sortedIndices(instanceIndices(g, live))))
			g.Expect(freshIR(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent).Status.ReadyReplicas).To(Equal(int32(drainInstances)))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(drainInstances))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the journal holds exactly one completed entry per instance")
		entries := journalEntriesFor(journal(Default), ns.Name, fix.ISVC.Name)
		Expect(entries).To(HaveLen(drainInstances), func() string { return describeJournal(entries) })
		for _, e := range entries {
			Expect(seen).To(HaveKey(e.UUID), "unexpected request %s", e.UUID)
			Expect(e.Phase).To(Equal("completed"), "journal %s (%s)", e.UUID, e.Reason)
		}

		// alfred stamps DrainedAt on its own clock when the last source pod
		// object is gone; the controller's CompletedAt follows the same pod
		// deletion through an independent reconcile tail, so the two are not
		// ordered. The drain is bounded by the signal and by the last request,
		// whose CreatedAt precedes its own source pod's removal outright; the
		// record and the event are written once per episode.
		ginkgo.By("the node is reported drained only once the last instance has left")
		last := closed[len(closed)-1]
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
			g.Expect(*rec.DrainedAt).To(BeTemporally(">", last.CreatedAt), "drained before the last request was issued")
			drained := nodeEvents(g, src, "NodeDrainedForRepair")
			g.Expect(drained).To(HaveLen(1))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Nothing of the workload is left on the node, so the policy
		// has no further Instance to move while the node stays unhealthy.
		ginkgo.By("no further request follows")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(drainInstances))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(drainInstances))
			g.Expect(livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)).To(BeEmpty())
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
