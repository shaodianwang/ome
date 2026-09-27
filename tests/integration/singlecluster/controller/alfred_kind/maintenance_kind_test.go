package alfred_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Planned-maintenance vocabulary alfred fixes, pinned like the health
// vocabulary so a rename upstream fails here. Maintenance has its own reason,
// its own pair of node events (Normal, no hardware damage implied) and its own
// counter; the health counter never moves for a maintenance episode.
const (
	maintenanceReason         = "NodeMaintenance"
	maintenanceRequestedEvent = "NodeMaintenanceRequested"
	maintenanceDrainedEvent   = "NodeDrainedForMaintenance"
	maintenanceSignalsMetric  = "alfred_nodemaintenance_signals_total"
	healthSignalsMetric       = "alfred_nodehealth_signals_total"
)

// Names of the triggers the spec configures; the node record lists the ones
// that matched.
const (
	labelTrigger     = "patching-label"
	taintTrigger     = "patching-taint"
	conditionTrigger = "patching-condition"
)

// maintenanceTriggers configures one trigger of each kind under
// policies.nodeHealth.maintenance.triggers. A trigger has a unique name and
// exactly one matcher: a label or taint value is matched exactly when given,
// and a condition matcher names both type and status.
func maintenanceTriggers() configMutator {
	return setConfig("policies.nodeHealth.maintenance.triggers", []any{
		map[string]any{"name": labelTrigger, "label": map[string]any{"key": maintenanceLabelKey, "value": maintenanceLabelValue}},
		map[string]any{"name": taintTrigger, "taint": map[string]any{"key": maintenanceTaintKey, "value": "true", "effect": string(corev1.TaintEffectNoSchedule)}},
		map[string]any{"name": conditionTrigger, "condition": map[string]any{"type": string(maintenanceCondition), "status": string(corev1.ConditionTrue)}},
	})
}

// signalCount is the leader's count of one node-signal counter for node,
// summed over reasons unless one is named; a series never incremented reads
// as zero.
func signalCount(g Gomega, metric, node, reason string) float64 {
	labels := map[string]string{"node": node}
	if reason != "" {
		labels["reason"] = reason
	}
	return scrapeLeader(g).sum(metric, labels)
}

// signalCountEventually reads signalCount with retries, so one pod-proxy
// hiccup does not fail the spec.
func signalCountEventually(metric, node, reason string) float64 {
	var v float64
	EventuallyWithOffset(1, func(g Gomega) {
		v = signalCount(g, metric, node, reason)
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return v
}

func nodeTaints(g Gomega, node string) []corev1.Taint {
	n := &corev1.Node{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Name: node}, n)).To(Succeed())
	return n.Spec.Taints
}

var _ = ginkgo.Describe("alfred planned maintenance", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-maint-")
		Expect(err).NotTo(HaveOccurred())
		patchAlfredConfig(maintenanceTriggers())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// evacuatesOn places one single-pod workload on the source worker, raises
	// one maintenance signal there, and follows the episode through: the
	// finding carries the maintenance reason, the controller completes the
	// migration the finding becomes, and the node is reported requested, then
	// drained, on the maintenance surface alone.
	evacuatesOn := func(name, trigger string, raise func(node string)) {
		src := workers[0]
		healthy := otherWorkers(src)
		fix := newAlfredFixture(ns.Name, withName(name))
		readyOn(fix, src, 1)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(1))
		sourceUID := onSrc[0].UID

		// Both maintenance events fire once per node record, and the record
		// is dropped only once no trigger matches, so an earlier episode's
		// record must be gone before this signal opens a new one. Dispatch is
		// serial: an unresolved request from an earlier spec would hold this
		// one. The counters are read only after both, so nothing an earlier
		// episode still reports is counted against this one.
		waitNodeRecordGone(src)
		waitJournalSettled()
		requestedBefore := signalCountEventually(maintenanceSignalsMetric, src, maintenanceRequestedEvent)
		drainedBefore := signalCountEventually(maintenanceSignalsMetric, src, maintenanceDrainedEvent)
		healthBefore := signalCountEventually(healthSignalsMetric, src, "")
		raise(src)

		// The finding is visible in the cycle record from the first pass
		// that sees the signal until the pod has left the node, whatever the
		// dispatcher makes of it, so it is required alongside the request
		// rather than before it. A maintenance move is a routine one for the
		// dispatcher's budget: a request that left this node, or landed on
		// the predicted target, less than one perNodeCooldown ago withholds
		// it as NodeCooldown or TargetCooldown, which the verdict log shows
		// and dispatchTimeout covers.
		ginkgo.By("the finding carries the maintenance reason and is dispatched")
		finding := SatisfyAll(
			HaveField("Policy", nodeHealthPolicy),
			HaveField("Reason", maintenanceReason),
			HaveField("Instance", int32(0)),
			HaveField("FromNode", src),
		)
		verdicts := &verdictLog{ns: ns.Name, name: fix.ISVC.Name}
		state := func() string { return evacuationState(ns.Name, fix.ISVC.Name) }
		var entry journalEntry
		Eventually(func(g Gomega) {
			verdicts.observe(g)
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(finding), func() string { return "maintenance finding; " + state() })
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
		Expect(entry.Targets).To(HaveLen(1), "one replacement pod, one predicted placement")
		Expect(entry.Targets[0]).To(BeElementOf(healthy), "the predicted target must be a worker not under maintenance")
		req := parseMigrationRequest(Default, entry.Payload)
		Expect(req.SchemaVersion).To(Equal("v1"))
		Expect(req.Reason).To(Equal(maintenanceReason))
		Expect(req.FromNode).To(Equal(src))
		Expect(req.RequestedBy).To(Equal("alfred"))
		Expect(req.HintTargetNodes).NotTo(BeEmpty())
		for _, hint := range req.HintTargetNodes {
			Expect(hint).To(BeElementOf(healthy), "hint %s is the node under maintenance", hint)
		}

		ginkgo.By("the controller completes it and the replacement runs on another worker")
		m := waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, entry.UUID)
		Expect(m.Reason).To(Equal(maintenanceReason), "the controller records the request's reason")
		Expect(m.FromNode).To(Equal(src))
		closed := waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
		Expect(closed.CompletedAt).NotTo(BeNil())
		waitPodsOffNode(ns.Name, fix.ISVC.Name, src)
		Eventually(func(g Gomega) {
			live := 0
			for _, p := range omenativePods(g, ns.Name, fix.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				live++
				g.Expect(p.UID).NotTo(Equal(sourceUID), "pod %s is the original", p.Name)
				g.Expect(healthy).To(ContainElement(p.Spec.NodeName), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
			}
			g.Expect(live).To(Equal(1))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The node is reported requested when the episode opens and drained
		// once no OME GPU occupant remains; the record names the trigger that
		// matched and keeps health clear, with no repair phase, throughout.
		ginkgo.By("the node events, the counter and the node record tell a maintenance story, not a repair")
		waitNodeEvent(src, maintenanceRequestedEvent, kindMediumTimeout)
		waitNodeEvent(src, maintenanceDrainedEvent, kindMediumTimeout)
		Eventually(func(g Gomega) {
			g.Expect(signalCount(g, maintenanceSignalsMetric, src, maintenanceRequestedEvent)).To(BeNumerically(">", requestedBefore))
			g.Expect(signalCount(g, maintenanceSignalsMetric, src, maintenanceDrainedEvent)).To(BeNumerically(">", drainedBefore))
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Maintenance.Requested).To(BeTrue())
			g.Expect(rec.Maintenance.Triggers).To(ConsistOf(trigger))
			g.Expect(rec.State).To(Equal(nodeHealthClear), "%s health state", src)
			g.Expect(rec.SignaledAt).To(BeNil(), "%s was signaled for repair", src)
			g.Expect(rec.DrainedAt).To(BeNil(), "%s was reported drained for repair", src)
			g.Expect(rec.Workloads).To(BeEmpty())
			g.Expect(rec.OMEGPUOccupantsPresent).To(BeFalse())
			g.Expect(rec.MaintenanceRequestedAt).NotTo(BeNil())
			g.Expect(rec.MaintenanceDrainedAt).NotTo(BeNil())
			g.Expect(*rec.MaintenanceDrainedAt).To(BeTemporally(">", *rec.MaintenanceRequestedAt))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(signalCountEventually(healthSignalsMetric, src, "")).To(Equal(healthBefore), "the health counter moved for %s", src)
	}

	ginkgo.It("a configured label evacuates", func() {
		evacuatesOn("maint-label", labelTrigger, func(node string) {
			labelNodeTemp(node, maintenanceLabelKey, maintenanceLabelValue)
		})
	})

	ginkgo.It("a configured taint evacuates", func() {
		evacuatesOn("maint-taint", taintTrigger, func(node string) {
			taintNodeTemp(node, corev1.Taint{Key: maintenanceTaintKey, Value: "true", Effect: corev1.TaintEffectNoSchedule})
		})
	})

	ginkgo.It("a configured condition evacuates", func() {
		evacuatesOn("maint-cond", conditionTrigger, func(node string) {
			setNodeCondition(node, maintenanceCondition, corev1.ConditionTrue)
		})
	})

	// Maintenance is observed only through configured triggers: a taint none
	// of them names requests nothing, so the node gets no record, the workload
	// no finding, and the counters do not move.
	ginkgo.It("an unconfigured taint is inert", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("maint-inert"))
		readyOn(fix, src, 1)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(1))
		sourceUID := onSrc[0].UID

		waitNodeRecordGone(src)
		waitJournalSettled()
		maintenanceBefore := signalCountEventually(maintenanceSignalsMetric, src, "")
		healthBefore := signalCountEventually(healthSignalsMetric, src, "")
		taintNodeTemp(src, corev1.Taint{Key: unconfiguredTaintKey, Value: "1", Effect: corev1.TaintEffectNoSchedule})
		Expect(nodeTaints(Default, src)).To(ContainElement(HaveField("Key", unconfiguredTaintKey)), "the taint is not on %s", src)

		ginkgo.By("alfred keeps deciding and produces no finding, no node record and no request")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(BeEmpty(), "finding for %s: %s", workloadID(fix), describeRecs(recs))
			_, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeFalse(), "node record for %s", src)
			live := assertNoDispatch(g, fix, src)
			g.Expect(live).To(HaveLen(1))
			g.Expect(live[0].UID).To(Equal(sourceUID))
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
		Expect(signalCountEventually(maintenanceSignalsMetric, src, "")).To(Equal(maintenanceBefore), "the maintenance counter moved for %s", src)
		Expect(signalCountEventually(healthSignalsMetric, src, "")).To(Equal(healthBefore), "the health counter moved for %s", src)
		Expect(nodeEvents(Default, src, maintenanceRequestedEvent)).To(BeEmpty(), "%s was reported under maintenance", src)
	})
})
