package alfred_kind

import (
	"fmt"
	"sort"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// pdComponents are the two Components of a prefill/decode disaggregated
// workload. Each is its own InferenceReplica and its own migration unit; the
// InferenceService is the one workload alfred budgets them under.
var pdComponents = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}

// otherComponent names the PD Component that is not c.
func otherComponent(c v1beta1.ComponentType) v1beta1.ComponentType {
	if c == v1beta1.EngineComponent {
		return v1beta1.DecoderComponent
	}
	return v1beta1.EngineComponent
}

// expectSinglePodRequest asserts a journal entry is the single-pod v1 request
// for component's Instance 0 off src and returns its parsed payload.
func expectSinglePodRequest(entry journalEntry, component v1beta1.ComponentType, src string) migrationRequest {
	ExpectWithOffset(1, entry.Component).To(Equal(string(component)))
	ExpectWithOffset(1, entry.Instance).To(Equal(int32(0)))
	ExpectWithOffset(1, entry.FromNode).To(Equal(src))
	ExpectWithOffset(1, entry.Targets).To(HaveLen(1), "one replacement pod, one predicted placement")
	ExpectWithOffset(1, otherWorkers(src)).To(ContainElement(entry.Targets[0]), "the predicted target must be a healthy worker")
	req := parseMigrationRequest(Default, entry.Payload)
	ExpectWithOffset(1, req.SchemaVersion).To(Equal("v1"))
	ExpectWithOffset(1, req.Component).To(Equal(string(component)))
	ExpectWithOffset(1, req.Instance).To(Equal(int32(0)))
	ExpectWithOffset(1, req.FromNode).To(Equal(src))
	ExpectWithOffset(1, req.Reason).To(Equal(nodeUnhealthyReason))
	ExpectWithOffset(1, req.RequestedBy).To(Equal("alfred"))
	_, err := time.Parse(time.RFC3339Nano, req.RequestedAt)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "requested_at %q is not RFC 3339", req.RequestedAt)
	// Hints are every eligible node of the pool up to alfred's cap of three;
	// this cluster has exactly two healthy workers, and a single-GPU surge
	// leaves both with headroom for the next.
	ExpectWithOffset(1, req.HintTargetNodes).To(ConsistOf(otherWorkers(src)))
	return req
}

// expectManualMigrationRow asserts the controller's record of req on its
// Component's InferenceReplica: a completed Manual migration of Instance 0
// to a fresh surge index. It returns that index.
func expectManualMigrationRow(m v1beta1.MigrationStatus, req migrationRequest, src string) int32 {
	ExpectWithOffset(1, m.Phase).To(Equal(v1beta1.MigrationPhaseCompleted))
	ExpectWithOffset(1, m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
	ExpectWithOffset(1, m.SourceInstance).To(Equal(int32(0)))
	ExpectWithOffset(1, m.FromNode).To(Equal(src))
	ExpectWithOffset(1, m.HintTargetNodes).To(Equal(req.HintTargetNodes))
	ExpectWithOffset(1, m.Reason).To(Equal(req.Reason))
	ExpectWithOffset(1, m.SurgeInstance).NotTo(BeNil())
	ExpectWithOffset(1, *m.SurgeInstance).NotTo(Equal(int32(0)), "the surge takes a fresh index")
	ExpectWithOffset(1, m.AllocatedAt).NotTo(BeNil())
	ExpectWithOffset(1, m.CompletedAt).NotTo(BeNil())
	return *m.SurgeInstance
}

var _ = ginkgo.Describe("alfred evacuates a prefill/decode disaggregated workload", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-pd-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("moves the engine and the decoder by one request each, in series", func() {
		src := workers[0]
		healthy := otherWorkers(src)

		// Both Components pinned to the source node: one single-pod Instance
		// each, one InferenceReplica each, one workload for alfred.
		release := pinToNode(src)
		fix := newAlfredFixture(ns.Name, withName("pd"), withDecoder())
		deferFixtureCleanup(fix)
		applyFixture(Default, fix)
		for _, c := range pdComponents {
			waitInstancesReady(ns.Name, fix.ISVC.Name, c, 1)
		}
		release()
		waitPlacementCooldown()
		sourceUIDs := map[v1beta1.ComponentType]types.UID{}
		for _, c := range pdComponents {
			onSrc := livePodsOfComponent(livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src), c)
			Expect(onSrc).To(HaveLen(1), "%s pods on %s", c, src)
			sourceUIDs[c] = onSrc[0].UID
		}

		waitNodeRecordGone(src)
		waitJournalSettled()
		markUnhealthy(src)

		// Node health emits one finding per (Component, Instance), so the
		// workload yields two, and the arbiter admits one per workload per
		// pass. They tie on score and footprint and rank by Component name; the
		// order is read from the journal, as a transient advisory can swap it.
		ginkgo.By("alfred writes one request, for one Component")
		first := waitJournalEntry(ns.Name, fix.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
		firstComp := v1beta1.ComponentType(first.Component)
		Expect(pdComponents).To(ContainElement(firstComp))
		secondComp := otherComponent(firstComp)
		firstReq := expectSinglePodRequest(first, firstComp, src)

		// The admission pass rejects the other Component's finding under the
		// per-workload exclusion; from the next pass the workload has a
		// migration in flight and the finding is advisory instead. The event
		// is the durable trace of the rejection, the row holds either form.
		ginkgo.By("the other Component is rejected with WorkloadBusy while the first request is in flight")
		Eventually(func(g Gomega) {
			active := inFlight(journal(g).Entries)
			g.Expect(active).To(HaveLen(1), "one request in flight")
			g.Expect(active[0].UUID).To(Equal(first.UUID))
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(SatisfyAll(
				HaveField("Component", string(secondComp)), HaveField("Instance", int32(0)), heldBehindRequest(),
			)), "last cycle: %s", describeRecs(recs))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			var messages []string
			for _, e := range alfredEventsFor(g, ns.Name, fix.ISVC.Name, "RecommendationRejected") {
				messages = append(messages, e.Message)
			}
			g.Expect(messages).To(ContainElement(HaveSuffix(workloadID(fix) + "/" + string(secondComp) + " rejected: " + rejectWorkloadBusy)))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		// Both Components' rows report against the same ISVC every pass, so
		// alfred's per-object event budget is spent early; only the first
		// dispatch's events are asserted, the records carry the rest.
		waitEventUUID(ns.Name, fix.ISVC.Name, "MigrationSubmitted", alfredEventSource, first.UUID, kindMediumTimeout)

		// The controller consumes only the request addressed to its own
		// Component and records it on that Component's InferenceReplica.
		// While it runs, the other Component's row is never admitted and the
		// journal never holds two unresolved entries.
		ginkgo.By("the first migration completes while the other Component waits")
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		var firstRow v1beta1.MigrationStatus
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			js := journal(g)
			expectSerialJournal(js, ns.Name, fix.ISVC.Name)
			recs := recsFor(recommendations(g), workloadID(fix))
			if row, ok := instanceRec(recs, secondComp, 0); ok && (row.RequestUUID != "" || row.Outcome == "withheld") {
				StopTrying(fmt.Sprintf("%s was admitted while %s's request %s was in flight; last cycle: %s",
					secondComp, firstComp, first.UUID, describeRecs(recs))).Now()
			}
			g.Expect(journalEntriesFor(js, ns.Name, fix.ISVC.Name)).To(HaveLen(1))
			if rows := migrationEntries(g, ns.Name, fix.ISVC.Name, secondComp); len(rows) > 0 {
				StopTrying(fmt.Sprintf("%s records migration %s before its own request", secondComp, rows[0].RequestUUID)).Now()
			}
			firstRow = expectMigrationCompleted(g, ns.Name, fix.ISVC.Name, firstComp, first.UUID)
		}).WithTimeout(migrationTimeout).WithPolling(kindPollInterval).Should(Succeed())
		firstClosed := waitJournalPhase(first.UUID, "completed", kindMediumTimeout)
		Expect(firstClosed.AcknowledgedAt).NotTo(BeNil())
		Expect(firstClosed.CompletedAt).NotTo(BeNil())
		// The arbiter reads this instant from the InferenceReplica row, the
		// dispatcher from the journal; both start the floor here.
		firstDone := *firstClosed.CompletedAt
		surgeIdx := map[v1beta1.ComponentType]int32{}
		surgeIdx[firstComp] = expectManualMigrationRow(firstRow, firstReq, src)

		// The workload's newest terminal migration gates the other Component
		// whichever recorded it: its finding is rejected Cooldown until the
		// floor passes, and the node, still holding an OME GPU occupant, is not
		// drained. The probe ends inside the floor, with two ticks to see cycles.
		ginkgo.By("inside the floor the other Component is rejected with Cooldown and the node stays occupied")
		probeEnd := firstDone.Add(deployedHealthFloor - 3*decisionInterval)
		Expect(time.Until(probeEnd)).To(BeNumerically(">=", 2*decisionInterval),
			"the first completion is already %s old; too late to watch the floor", time.Since(firstDone).Round(time.Second))
		Eventually(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(SatisfyAll(
				HaveField("Component", string(secondComp)), HaveField("Instance", int32(0)), rejectedWith(rejectCooldown),
			)), "last cycle: %s", describeRecs(recs))
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(1), "a second request inside the floor")
			onSrc := livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)
			g.Expect(livePodsOfComponent(onSrc, firstComp)).To(BeEmpty(), "%s still on %s", firstComp, src)
			remaining := livePodsOfComponent(onSrc, secondComp)
			g.Expect(remaining).To(HaveLen(1), "%s pods on %s", secondComp, src)
			g.Expect(remaining[0].UID).To(Equal(sourceUIDs[secondComp]), "the %s pod that stayed is the original", secondComp)
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(ConsistOf(workloadID(fix)))
			g.Expect(rec.OMEAcceleratorOccupantsPresent).To(BeTrue())
			g.Expect(rec.SignaledAt).NotTo(BeNil())
			g.Expect(rec.DrainedAt).To(BeNil(), "drained while %s still occupies %s", secondComp, src)
		}).WithTimeout(time.Until(probeEnd)).WithPolling(kindPollInterval).Should(Succeed())

		// The second request mirrors the first for the other Component under
		// its own UUID. alfred admits it only once its clock is a full floor
		// past the recorded completion and takes CreatedAt after the
		// admission, so the gap is at least the floor.
		ginkgo.By("past the floor the other Component is dispatched")
		var second journalEntry
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			js := journal(g)
			expectSerialJournal(js, ns.Name, fix.ISVC.Name)
			entries := journalEntriesFor(js, ns.Name, fix.ISVC.Name)
			g.Expect(entries).To(HaveLen(2), func() string { return "second request; " + evacuationState(ns.Name, fix.ISVC.Name) })
			for _, e := range entries {
				if e.UUID != first.UUID {
					second = e
				}
			}
		}).WithTimeout(healthCooldownFloor + dispatchTimeout).WithPolling(quietPollInterval).Should(Succeed())
		secondReq := expectSinglePodRequest(second, secondComp, src)
		gap := second.CreatedAt.Sub(firstDone)
		Expect(gap).To(BeNumerically(">=", deployedHealthFloor),
			"request %s came %s after %s's migration completed, inside the floor", second.UUID, gap, firstComp)

		ginkgo.By("the second migration completes on its own InferenceReplica")
		var secondRow v1beta1.MigrationStatus
		Eventually(func(g Gomega) {
			expectSerialJournal(journal(g), ns.Name, fix.ISVC.Name)
			secondRow = expectMigrationCompleted(g, ns.Name, fix.ISVC.Name, secondComp, second.UUID)
		}).WithTimeout(migrationTimeout).WithPolling(kindPollInterval).Should(Succeed())
		secondClosed := waitJournalPhase(second.UUID, "completed", kindMediumTimeout)
		Expect(secondClosed.AcknowledgedAt).NotTo(BeNil())
		Expect(secondClosed.CompletedAt).NotTo(BeNil())
		surgeIdx[secondComp] = expectManualMigrationRow(secondRow, secondReq, src)

		// Each Component ends with one fresh, Ready pod on a healthy worker at
		// its own surge index, one migration row on its own InferenceReplica
		// and nothing left in the mailbox.
		ginkgo.By("every pod of the workload is fresh, Ready and off the node")
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			pods := omenativePods(g, ns.Name, fix.ISVC.Name)
			for _, c := range pdComponents {
				live := livePodsOfComponent(pods, c)
				g.Expect(live).To(HaveLen(1), "%s live pods", c)
				p := live[0]
				g.Expect(p.UID).NotTo(Equal(sourceUIDs[c]), "%s pod %s is the original", c, p.Name)
				g.Expect(healthy).To(ContainElement(p.Spec.NodeName), "%s pod %s landed on %s", c, p.Name, p.Spec.NodeName)
				g.Expect(podIsReady(&p)).To(BeTrue(), "%s pod %s is not Ready", c, p.Name)
				rows := instanceRows(g, ns.Name, fix.ISVC.Name, c)
				g.Expect(rows).To(HaveLen(1), "%s instance rows", c)
				g.Expect(rows[0].Index).To(Equal(surgeIdx[c]), "%s serves from its surge index", c)
				g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(rows[0].Operation).To(BeNil())
				g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, c)).To(HaveLen(1), "%s migration rows", c)
			}
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the journal holds two completed requests, one per Component, dispatched in series")
		closed := journalEntriesFor(journal(Default), ns.Name, fix.ISVC.Name)
		Expect(closed).To(HaveLen(2))
		sort.Slice(closed, func(i, j int) bool { return closed[i].CreatedAt.Before(closed[j].CreatedAt) })
		Expect(closed[0].UUID).To(Equal(first.UUID))
		Expect(closed[1].UUID).To(Equal(second.UUID))
		Expect([]string{closed[0].Component, closed[1].Component}).To(ConsistOf(
			string(v1beta1.EngineComponent), string(v1beta1.DecoderComponent)))
		for _, e := range closed {
			Expect(e.Phase).To(Equal("completed"), "journal %s (%s)", e.UUID, e.Reason)
			Expect(e.FromNode).To(Equal(src))
			Expect(e.AcknowledgedAt).NotTo(BeNil())
			Expect(e.CompletedAt).NotTo(BeNil())
		}
		Expect(closed[1].CreatedAt).To(BeTemporally(">", *closed[0].CompletedAt),
			"the %s request was dispatched before the %s migration completed", closed[1].Component, closed[0].Component)

		// The controller's events are its own budget; both accepts and both
		// completions name their request.
		ginkgo.By("the controller names each request in its events")
		for _, e := range closed {
			waitEventUUID(ns.Name, fix.ISVC.Name, "MigrationRequestAccepted", controllerEventSource, e.UUID, kindMediumTimeout)
			waitEventUUID(ns.Name, fix.ISVC.Name, "MigrationCompleted", controllerEventSource, e.UUID, kindMediumTimeout)
		}

		// The record is drained on the first observation after the signal
		// with no OME GPU occupant left, which is after the second Component's
		// pod went, so after the second request was even dispatched. Both
		// timestamps are alfred's own clock.
		ginkgo.By("the node record drains only once both Components have left")
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
			g.Expect(*rec.DrainedAt).To(BeTemporally(">", second.CreatedAt), "drained before the second request was dispatched")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Neither Component is left on the unhealthy node, so the policy has
		// nothing further to move while the node stays unhealthy.
		ginkgo.By("no third request follows")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(2))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			for _, c := range pdComponents {
				g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, c)).To(HaveLen(1), "%s migration rows", c)
			}
			g.Expect(livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)).To(BeEmpty())
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
