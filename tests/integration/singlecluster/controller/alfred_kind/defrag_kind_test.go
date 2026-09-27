package alfred_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// fragmentationScoreMetric is the executable gate value alfred publishes on
// every observation pass: the largest per-pool score, in [0, 1]. The policy
// enumerates executable moves in a pool only while its score is above
// policies.defragmentation.fragmentationThreshold.
const fragmentationScoreMetric = "alfred_cluster_fragmentation_score"

// fragmentationThreshold reads the executable gate from alfred's live config.
func fragmentationThreshold(g Gomega) float64 {
	v, ok := configValue(loadAlfredConfig(g), "policies.defragmentation.fragmentationThreshold")
	g.ExpectWithOffset(1, ok).To(BeTrue(), "alfred's config sets no fragmentation threshold")
	f, ok := v.(float64)
	g.ExpectWithOffset(1, ok).To(BeTrue(), "fragmentationThreshold is %T, not a number", v)
	return f
}

var _ = ginkgo.Describe("alfred defragmentation and spot policy", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-defrag-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// A routine defragmentation in execute mode with no maintenance window
	// configured. The executable finding is arbitrated in the class below
	// health and maintenance (neither is present), passes the dispatcher's
	// budget (an empty journal owes no node or workload cooldown) and its
	// preflight simulation, and is submitted as a v1 request carrying the
	// Fragmentation reason. The layout is symmetric: the policy ranks the two
	// fragments by score, footprint and then workload name, so the first
	// fragment is dispatched unless its preflight fails in a cycle, in which
	// case the other is tried in the same pass. The spec accepts either and
	// pins that exactly one moves, onto the other worker.
	ginkgo.It("moves one fragment onto the other worker so the whole-worker pod can schedule", func() {
		a, b := workers[0], workers[1]
		// The layout is built while defragmentation is still off, so alfred
		// never sees an intermediate layout as executable, and every pod's
		// placement is older than recentPlacementCooldown before the policy
		// is enabled.
		left, right, pending := fragmentedPair(ns.Name)
		leftUID := singleLivePod(Default, left, a).UID
		rightUID := singleLivePod(Default, right, b).UID
		waitPlacementCooldown()
		waitJournalSettled()
		// The pending pod stays a pressure signal only: with the emergency age
		// far off, the move is a routine one, not boosted and not an
		// emergency, which is the finding a configured window would gate.
		patchAlfredConfig(
			setConfig("policies.defragmentation.enabled", true),
			setConfig("emergencyPendingAgeMinutes", nonEmergencyPendingAgeMinutes),
		)

		ginkgo.By("the fragmentation gauge reads the layout as above the executable gate")
		threshold := fragmentationThreshold(Default)
		Eventually(func(g Gomega) {
			score, ok := scrapeLeader(g).value(fragmentationScoreMetric, nil)
			g.Expect(ok).To(BeTrue(), "%s is not published", fragmentationScoreMetric)
			g.Expect(score).To(BeNumerically(">", threshold))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The cycle record carries the request, attributed to the policy and
		// reason it was dispatched under, from the pass that submits it until
		// the pass that observes it complete. The emergency flag is pinned
		// false; the field is omitted when false, so the pin holds the move
		// routine but cannot tell a renamed field from one.
		ginkgo.By("exactly one fragment is dispatched, as a defragmentation")
		pair := []*alfredFixture{left, right}
		leftVerdicts := &verdictLog{ns: ns.Name, name: left.ISVC.Name}
		rightVerdicts := &verdictLog{ns: ns.Name, name: right.ISVC.Name}
		state := func() string {
			return fmt.Sprintf("%s: %s | %s: %s", left.ISVC.Name, evacuationState(ns.Name, left.ISVC.Name),
				right.ISVC.Name, evacuationState(ns.Name, right.ISVC.Name))
		}
		var entry journalEntry
		Eventually(func(g Gomega) {
			leftVerdicts.observe(g)
			rightVerdicts.observe(g)
			js := journal(g)
			var entries []journalEntry
			for _, f := range pair {
				entries = append(entries, journalEntriesFor(js, ns.Name, f.ISVC.Name)...)
			}
			for _, e := range entries {
				if e.Phase == "failed" || e.Phase == "stalled" {
					StopTrying(fmt.Sprintf("journal %s for %s is %s: %s", e.UUID, e.Workload.Name, e.Phase, e.Reason)).Now()
				}
			}
			if len(entries) > 1 {
				StopTrying("more than one fragment was dispatched: " + describeJournal(entries)).Now()
			}
			g.Expect(entries).To(HaveLen(1), func() string { return "one defragmentation request; " + state() })
			entry = entries[0]
			g.Expect(entry.Phase).To(BeElementOf("submitted", "acknowledged", "completed"), func() string { return "request phase; " + state() })
			recs := recsFor(recommendations(g), entry.Workload.Namespace+"/"+entry.Workload.Name)
			g.Expect(recs).To(ContainElement(SatisfyAll(
				HaveField("Policy", defragPolicy),
				HaveField("Reason", fragmentationReason),
				HaveField("Instance", int32(0)),
				HaveField("FromNode", entry.FromNode),
				HaveField("RequestUUID", entry.UUID),
				HaveField("DispatchStatus", BeElementOf("submitted", "acknowledged", "completed")),
				HaveField("Emergency", BeFalse()),
			)), func() string { return "the request in the cycle record; " + state() })
		}).WithTimeout(dispatchTimeout).WithPolling(kindPollInterval).Should(Succeed())
		moved, stayed, from, to, movedUID, stayedUID := left, right, a, b, leftUID, rightUID
		if entry.Workload.Name == right.ISVC.Name {
			moved, stayed, from, to, movedUID, stayedUID = right, left, b, a, rightUID, leftUID
		}
		fmt.Fprintf(ginkgo.GinkgoWriter, "defragmentation: %s moves off %s\n", moved.ISVC.Name, from)
		Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(entry.Instance).To(Equal(int32(0)))
		Expect(entry.FromNode).To(Equal(from))
		Expect(entry.Targets).To(ConsistOf(to), "predicted placement")
		req := parseMigrationRequest(Default, entry.Payload)
		Expect(req.SchemaVersion).To(Equal("v1"))
		Expect(req.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(req.Instance).To(Equal(int32(0)))
		Expect(req.FromNode).To(Equal(from))
		Expect(req.Reason).To(Equal(fragmentationReason))
		Expect(req.HintTargetNodes).To(ConsistOf(to), "the source is excluded and the cordoned worker is no target")
		Expect(req.RequestedBy).To(Equal("alfred"))

		ginkgo.By("the controller completes it and both fragments share the other worker")
		m := waitMigrationCompleted(ns.Name, moved.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		Expect(m.Reason).To(Equal(fragmentationReason), "the controller records the request's reason")
		Expect(m.FromNode).To(Equal(from))
		Expect(m.HintTargetNodes).To(Equal(req.HintTargetNodes))
		closed := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		Expect(closed.CompletedAt).NotTo(BeNil())
		// The dispatched path's events name the policy and the request.
		for _, reason := range []string{"MigrationSubmitted", "MigrationCompleted"} {
			Eventually(func(g Gomega) {
				var messages []string
				for _, e := range alfredEventsFor(g, ns.Name, moved.ISVC.Name, reason) {
					messages = append(messages, e.Message)
				}
				g.Expect(messages).To(ContainElement(HavePrefix(defragPolicy+" migration request uuid="+entry.UUID)), "%s events", reason)
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}
		waitPodsOffNode(ns.Name, moved.ISVC.Name, from)
		Eventually(func(g Gomega) {
			live := 0
			for _, p := range omenativePods(g, ns.Name, moved.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				live++
				g.Expect(p.UID).NotTo(Equal(movedUID), "pod %s is the original", p.Name)
				g.Expect(p.Spec.NodeName).To(Equal(to), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
			}
			g.Expect(live).To(Equal(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(moved))).To(BeEmpty())
			g.Expect(singleLivePod(g, stayed, to).UID).To(Equal(stayedUID), "the fragment that stayed was replaced")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The emptied worker is the only schedulable one with a whole worker
		// free; the third worker stays cordoned.
		ginkgo.By("the whole-worker pod schedules onto the emptied worker")
		Eventually(func(g Gomega) {
			fresh := freshPod(g, pending)
			g.Expect(fresh.Spec.NodeName).To(Equal(from), "pod %s", pending.Name)
			g.Expect(fresh.Status.Phase).To(Equal(corev1.PodRunning), "pod %s", pending.Name)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// With both fragments on one worker and the other one full, the pool
		// has no reclaimable fragmentation and no pending demand, so nothing
		// executable is left. A transient observation gap can add an
		// advisory, and the completed request is reported once more.
		ginkgo.By("the other fragment is never dispatched")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			cycle := recommendations(g)
			for _, f := range pair {
				recs := recsFor(cycle, workloadID(f))
				for _, r := range recs {
					if r.RequestUUID == entry.UUID {
						continue
					}
					g.Expect(r.Outcome).To(Equal(advisoryOutcome), "finding for %s: %s", workloadID(f), describeRecs(recs))
				}
			}
			g.Expect(journalEntriesFor(journal(g), ns.Name, moved.ISVC.Name)).To(HaveLen(1), "a second request for the moved fragment")
			stayedLive := assertNoDispatch(g, stayed, to)
			g.Expect(stayedLive).To(HaveLen(1))
			g.Expect(stayedLive[0].UID).To(Equal(stayedUID))
			g.Expect(livePodsOnNode(omenativePods(g, ns.Name, moved.ISVC.Name), to)).To(HaveLen(1))
			g.Expect(freshPod(g, pending).Spec.NodeName).To(Equal(from))
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})

	// Both healthy workers are empty and alike, so only the spot policy tells
	// them apart: with spotPolicy.avoidAsTarget on (the config default), a
	// node carrying one of the configured preemptibleLabels with any value
	// but "false" is left out of the surge plan, which makes the other worker
	// the finding's only hint and the only placement the preflight simulation
	// may predict. The controller places the surge under a preferred hint
	// affinity and a required source exclusion, so the replacement landing
	// there is the scheduler honoring that preference between otherwise equal
	// nodes; the journal and the payload are alfred's exact contract.
	ginkgo.It("keeps a preemptible worker out of a health evacuation's targets", func() {
		src, spot, safe := workers[0], workers[1], workers[2]
		cfg := loadAlfredConfig(Default)
		if v, ok := configValue(cfg, "spotPolicy.avoidAsTarget"); ok {
			Expect(v).To(BeTrue(), "the suite's base config turns spotPolicy.avoidAsTarget off")
		}
		if v, ok := configValue(cfg, "spotPolicy.preemptibleLabels"); ok {
			Expect(v).To(ContainElement(preemptibleLabelKey), "the suite's base config does not read %s", preemptibleLabelKey)
		}
		labelNodeTemp(spot, preemptibleLabelKey, "true")
		fix := newAlfredFixture(ns.Name, withName("spot-avoid"))
		readyOn(fix, src, 1)
		sourceUID := singleLivePod(Default, fix, src).UID

		waitNodeRecordGone(src)
		waitJournalSettled()
		markUnhealthy(src)

		ginkgo.By("the evacuation is dispatched with the other worker as its only hint and predicted placement")
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
		Expect(entry.Component).To(Equal(string(v1beta1.EngineComponent)))
		Expect(entry.Instance).To(Equal(int32(0)))
		Expect(entry.FromNode).To(Equal(src))
		Expect(entry.Targets).To(ConsistOf(safe), "predicted placement")
		req := parseMigrationRequest(Default, entry.Payload)
		Expect(req.SchemaVersion).To(Equal("v1"))
		Expect(req.Reason).To(Equal(nodeUnhealthyReason))
		Expect(req.FromNode).To(Equal(src))
		Expect(req.HintTargetNodes).To(ConsistOf(safe), "the preemptible worker is hinted")
		Expect(req.RequestedBy).To(Equal("alfred"))

		ginkgo.By("the controller completes it and the replacement runs on the other worker")
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
				g.Expect(p.Spec.NodeName).To(Equal(safe), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
			}
			g.Expect(live).To(Equal(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), spot)).To(BeEmpty(), "a pod landed on the preemptible worker")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
