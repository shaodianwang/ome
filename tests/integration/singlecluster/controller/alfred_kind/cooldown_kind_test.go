package alfred_kind

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// The two per-workload cooldowns the spec sets, both counted from the
// workload's newest terminal migration: the standard window a routine move
// waits, three deployed floors long, and the floor a NodeUnhealthy evacuation
// waits instead, set to the deployed value. floorProbe is how long after the
// first completion the spec watches the hold; it ends four decision ticks
// inside the floor so the probe never overlaps the admission that follows.
// floorProbeMin is the least of it that must remain when the watch starts:
// two decision ticks, so it sees cycles.
const (
	standardWindow = 3 * deployedHealthFloor
	floorProbe     = deployedHealthFloor - 4*decisionInterval
	floorProbeMin  = 2 * decisionInterval
)

// cooldownOverridesMetric counts the health evacuations alfred audited as
// admitted while the standard window was still running.
const cooldownOverridesMetric = "alfred_cooldown_overrides_total"

// cooldownOverrides is the leader's audited override count for the
// node-health policy; a series never incremented reads as zero.
func cooldownOverrides() float64 {
	var v float64
	EventuallyWithOffset(1, func(g Gomega) {
		v, _ = scrapeLeader(g).value(cooldownOverridesMetric, map[string]string{"policy": nodeHealthPolicy})
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return v
}

var _ = ginkgo.Describe("alfred cooldowns", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-cooldown-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("a health evacuation waits out the floor and then overrides the standard cooldown", func() {
		src := workers[0]
		healthy := otherWorkers(src)

		// A health evacuation is gated by the floor on both sides of alfred:
		// the arbiter rejects it with Cooldown while the workload's last
		// migration is younger than the floor, and the dispatcher's budget
		// applies the floor, not the standard window, to a NodeUnhealthy
		// request. With the standard window three times the floor, a second
		// request between the two can only be the floor's doing. The in-flight
		// cap plays no part: one workload's other Instance is WorkloadBusy
		// while its first request runs, and dispatch is serial regardless.
		patchAlfredConfig(
			setConfig("perWorkloadCooldownMinutes", int(standardWindow/time.Minute)),
			setConfig("policies.nodeHealth.healthCooldownFloorMinutes", healthCooldownFloorMinutes),
		)
		overridesBefore := cooldownOverrides()

		fix := newAlfredFixture(ns.Name, withName("cooldown"), withEngineReplicas(2))
		readyOn(fix, src, 2)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(2))
		sourceUIDs := map[types.UID]struct{}{}
		for _, p := range onSrc {
			sourceUIDs[p.UID] = struct{}{}
		}

		markUnhealthy(src)

		ginkgo.By("the first Instance is evacuated")
		first := waitJournalEntry(ns.Name, fix.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
		waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, first.UUID)
		firstDone := waitJournalPhase(first.UUID, "completed", kindMediumTimeout)
		Expect(firstDone.CompletedAt).NotTo(BeNil())
		// Both cooldown clocks start here: the arbiter reads the migration's
		// CompletedAt from the InferenceReplica, the dispatcher reads the same
		// instant from the journal.
		completedAt := *firstDone.CompletedAt
		var remaining corev1.Pod
		Eventually(func(g Gomega) {
			live := livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)
			g.Expect(live).To(HaveLen(1))
			remaining = live[0]
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(sourceUIDs).To(HaveKey(remaining.UID), "the pod that stayed is an original")
		remainingIdx, ok := query.InstanceIdxFromLabels(&remaining)
		Expect(ok).To(BeTrue(), "pod %s carries no instance index", remaining.Name)
		Expect(remainingIdx).NotTo(Equal(first.Instance))

		// The record half is authoritative. The event half depends on alfred's
		// per-ISVC event budget (client-go admits a burst of 25 events per
		// object, then one per five minutes) and stays valid while the first
		// migration is short.
		ginkgo.By("inside the floor the remaining Instance is rejected with Cooldown")
		held := SatisfyAll(HaveField("Instance", remainingIdx), rejectedWith(rejectCooldown))
		Eventually(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(held), "last cycle: %s", describeRecs(recs))
			var messages []string
			for _, e := range alfredEventsFor(g, ns.Name, fix.ISVC.Name, "RecommendationRejected") {
				messages = append(messages, e.Message)
			}
			g.Expect(messages).To(ContainElement(HaveSuffix("rejected: " + rejectCooldown)))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The first request's own completion report and a transient
		// observation advisory may share a cycle with the rejection; every
		// other finding for the workload must be that same rejection.
		ginkgo.By("it stays held while the floor runs: no second request, the pod untouched")
		probeEnd := completedAt.Add(floorProbe)
		Expect(time.Until(probeEnd)).To(BeNumerically(">=", floorProbeMin),
			"the first completion is already %s old; too late to watch the floor", time.Since(completedAt).Round(time.Second))
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(held), "last cycle: %s", describeRecs(recs))
			for _, r := range recs {
				if r.Outcome == advisoryOutcome || r.RequestUUID == first.UUID {
					continue
				}
				g.Expect(r.Instance).To(Equal(remainingIdx), "finding for another instance: %s", describeRecs(recs))
				g.Expect(r).To(rejectedWith(rejectCooldown))
			}
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(1), "a second request was dispatched inside the floor")
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
			live := livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)
			g.Expect(live).To(HaveLen(1))
			g.Expect(live[0].UID).To(Equal(remaining.UID))
		}).WithTimeout(time.Until(probeEnd)).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the floor probe")

		// The request must come before the standard window ends, or the run
		// proves nothing about the override; a stall past that point fails
		// here, with the verdict log naming alfred's holds along the way.
		ginkgo.By("past the floor the remaining Instance is dispatched, inside the standard window")
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		second := waitJournalEntryAfter(ns.Name, fix.ISVC.Name, map[string]struct{}{first.UUID: {}}, verdicts,
			time.Until(completedAt.Add(standardWindow)), "submitted", "acknowledged", "completed")
		// alfred admits only once its own clock is a full floor past the
		// recorded completion, and CreatedAt is that clock taken after the
		// admission, so the bound is exact: the second-truncated CompletedAt
		// both alfred and the spec read can only enlarge the measured gap.
		gap := second.CreatedAt.Sub(completedAt)
		Expect(gap).To(BeNumerically(">=", deployedHealthFloor),
			"request %s came %s after the first completion, inside the floor", second.UUID, gap)
		Expect(gap).To(BeNumerically("<", standardWindow),
			"request %s came %s after the first completion, once the standard window had passed", second.UUID, gap)
		Expect(second.Instance).To(Equal(remainingIdx))
		Expect(second.FromNode).To(Equal(src))
		Expect(parseMigrationRequest(Default, second.Payload).Reason).To(Equal(nodeUnhealthyReason))

		ginkgo.By("the second migration completes and the workload has left the node")
		waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, second.UUID)
		waitJournalPhase(second.UUID, "completed", kindMediumTimeout)
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			live := 0
			for _, p := range omenativePods(g, ns.Name, fix.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				g.Expect(sourceUIDs).NotTo(HaveKey(p.UID), "pod %s is an original", p.Name)
				g.Expect(healthy).To(ContainElement(p.Spec.NodeName), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
				live++
			}
			g.Expect(live).To(Equal(2))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// alfred audits an override (the decision's cooldownOverridden flag,
		// the CooldownOverriddenForEvacuation event, the counter) only on a
		// cycle the dispatcher withholds after the arbiter admitted under the
		// floor; a request submitted on the first admitted cycle is reported
		// from the journal, which carries no flag. So the audit is logged, not
		// required: the dispatch time is the proof.
		audits := alfredEventsFor(Default, ns.Name, fix.ISVC.Name, "CooldownOverriddenForEvacuation")
		fmt.Fprintf(ginkgo.GinkgoWriter, "cooldown override: dispatched %s after the first completion; audited by %d event(s), %v counter increment(s)\n",
			gap.Round(time.Second), len(audits), cooldownOverrides()-overridesBefore)
	})
})
