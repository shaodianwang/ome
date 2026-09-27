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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// waitNoRecentMigrations blocks until no InferenceReplica in the cluster
// records a migration started within the last hour. Both hourly counters read
// that history (the arbiter's per-workload last migration, the dispatcher's
// request set), so an earlier spec's workload still tearing down would spend
// the budget under test.
func waitNoRecentMigrations() {
	ginkgo.By("no earlier migration is left inside the hourly window")
	Eventually(func(g Gomega) {
		irs := &v1beta1.InferenceReplicaList{}
		g.Expect(k8sClient.List(suiteCtx, irs)).To(Succeed())
		cutoff := time.Now().Add(-time.Hour)
		for i := range irs.Items {
			for _, m := range irs.Items[i].Status.Migrations {
				g.Expect(m.StartedAt.Time).To(BeTemporally("<", cutoff),
					"%s/%s still records migration %s from the last hour", irs.Items[i].Namespace, irs.Items[i].Name, m.RequestUUID)
			}
		}
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

var _ = ginkgo.Describe("alfred dispatch budgets", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-budget-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("maxInFlightMigrations=1 serializes three evacuations across two workloads", func() {
		src := workers[0]
		healthy := otherWorkers(src)

		// Dispatch is one request at a time at any cap: the dispatcher writes
		// a new journal entry only while none is unresolved. The cap is the
		// arbiter's gate, and at one it names the workload it turns away while
		// another workload's request is in flight.
		patchAlfredConfig(setConfig("maxInFlightMigrations", 1))

		// Two workloads on the node. Findings rank by workload name at equal
		// score and footprint, so the first workload's Instance is admitted
		// first and the second workload's is the one the cap rejects; the first
		// workload's other Instance is held by its own per-workload exclusion.
		release := pinToNode(src)
		a := newAlfredFixture(ns.Name, withName("budget-a"), withEngineReplicas(2))
		b := newAlfredFixture(ns.Name, withName("budget-b"))
		fixtures := []*alfredFixture{a, b}
		for _, f := range fixtures {
			deferFixtureCleanup(f)
			applyFixture(Default, f)
		}
		waitInstancesReady(ns.Name, a.ISVC.Name, v1beta1.EngineComponent, 2)
		waitInstancesReady(ns.Name, b.ISVC.Name, v1beta1.EngineComponent, 1)
		release()
		waitPlacementCooldown()
		sourceUIDs := map[types.UID]struct{}{}
		for _, f := range fixtures {
			for _, p := range livePodsOnNode(omenativePods(Default, ns.Name, f.ISVC.Name), src) {
				sourceUIDs[p.UID] = struct{}{}
			}
		}
		Expect(sourceUIDs).To(HaveLen(3))

		markUnhealthy(src)

		ginkgo.By("the cap rejects the second workload while the first workload's request is in flight")
		Eventually(func(g Gomega) {
			active := inFlight(journal(g).Entries)
			g.Expect(active).To(HaveLen(1), "one request in flight")
			g.Expect(active[0].Workload.Name).To(Equal(a.ISVC.Name))
			cycle := recommendations(g)
			recsA := recsFor(cycle, workloadID(a))
			g.Expect(recsA).To(ContainElement(heldBehindRequest()), "first workload's other Instance; last cycle: %s", describeRecs(recsA))
			recsB := recsFor(cycle, workloadID(b))
			g.Expect(recsB).To(ContainElement(rejectedWith("InFlightCap")), "second workload; last cycle: %s", describeRecs(recsB))
		}).WithTimeout(dispatchTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			var messages []string
			for _, e := range alfredEventsFor(g, ns.Name, b.ISVC.Name, "RecommendationRejected") {
				messages = append(messages, e.Message)
			}
			g.Expect(messages).To(ContainElement(ContainSubstring("InFlightCap")))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Every Instance leaves by its own request. The second workload's goes
		// once the first request completes; the first workload's other
		// Instance also waits out its health cooldown floor, so the bound is
		// three migrations and one floor, with a second floor as margin.
		ginkgo.By("all three evacuations complete, never more than one in flight")
		state := func() string {
			return fmt.Sprintf("%s: %s | %s: %s", a.ISVC.Name, evacuationState(ns.Name, a.ISVC.Name), b.ISVC.Name, evacuationState(ns.Name, b.ISVC.Name))
		}
		var closed []journalEntry
		Eventually(func(g Gomega) {
			js := journal(g)
			if n := len(inFlight(js.Entries)); n > 1 {
				StopTrying(fmt.Sprintf("%d requests in flight at once", n)).Now()
			}
			closed = nil
			live := 0
			for _, f := range fixtures {
				for _, e := range journalEntriesFor(js, ns.Name, f.ISVC.Name) {
					if e.Phase == "failed" || e.Phase == "stalled" {
						StopTrying(fmt.Sprintf("journal %s for %s instance %d is %s: %s", e.UUID, f.ISVC.Name, e.Instance, e.Phase, e.Reason)).Now()
					}
					g.Expect(e.Phase).To(Equal("completed"), func() string { return fmt.Sprintf("journal %s (%s); %s", e.UUID, e.Reason, state()) })
					closed = append(closed, e)
				}
				for _, m := range migrationEntries(g, ns.Name, f.ISVC.Name, v1beta1.EngineComponent) {
					stopOnFailedMigration(f.ISVC.Name, &m)
					g.Expect(m.Phase).To(Equal(v1beta1.MigrationPhaseCompleted), "migration %s", m.RequestUUID)
				}
				g.Expect(migrationRequestAnnotations(g, isvcKey(f))).To(BeEmpty())
				for _, p := range omenativePods(g, ns.Name, f.ISVC.Name) {
					if p.DeletionTimestamp != nil {
						continue
					}
					g.Expect(sourceUIDs).NotTo(HaveKey(p.UID), "pod %s is an original", p.Name)
					g.Expect(healthy).To(ContainElement(p.Spec.NodeName), "pod %s", p.Name)
					g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
					live++
				}
			}
			g.Expect(closed).To(HaveLen(3), func() string { return "closed requests; " + state() })
			g.Expect(live).To(Equal(3), func() string { return "live pods; " + state() })
		}).WithTimeout(serialEvacuationTimeout(3)).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the journal shows the three requests dispatched in series")
		sort.Slice(closed, func(i, j int) bool { return closed[i].CreatedAt.Before(closed[j].CreatedAt) })
		for i, e := range closed {
			Expect(e.FromNode).To(Equal(src))
			Expect(e.AcknowledgedAt).NotTo(BeNil())
			Expect(e.CompletedAt).NotTo(BeNil())
			if i > 0 {
				Expect(e.CreatedAt).To(BeTemporally(">", *closed[i-1].CompletedAt),
					"request for %s instance %d was dispatched before %s instance %d completed",
					e.Workload.Name, e.Instance, closed[i-1].Workload.Name, closed[i-1].Instance)
			}
		}
		Expect(closed[0].Workload.Name).To(Equal(a.ISVC.Name), "the first workload's Instance is admitted first")
	})

	ginkgo.It("maxMigrationsPerHour=1 holds the second Instance once the hour's budget is spent", func() {
		src := workers[0]

		// The arbiter rebuilds its rolling-hour figure from the snapshot every
		// cycle: in-flight requests plus one per workload with a terminal
		// migration inside the hour. The dispatcher's own budget, checked after
		// admission, also counts the journal's requests, and its verdicts
		// surface as outcome withheld with a dispatchReason, never as a
		// rejection. A cap of one over an emptied journal makes the arbiter the
		// gate that binds, so the rejection and its event name HourlyCap.
		seedJournal(Default, emptyJournal)
		patchAlfredConfig(setConfig("maxMigrationsPerHour", 1))

		fix := newAlfredFixture(ns.Name, withName("budget-hourly"), withEngineReplicas(2))
		readyOn(fix, src, 2)
		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(2))
		sourceUIDs := map[types.UID]struct{}{}
		for _, p := range onSrc {
			sourceUIDs[p.UID] = struct{}{}
		}

		waitNoRecentMigrations()
		Expect(journalEntriesWithin(journal(Default), time.Hour)).To(BeZero(), "the journal holds a request from the hour")
		markUnhealthy(src)

		ginkgo.By("the first Instance is evacuated")
		first := waitJournalEntry(ns.Name, fix.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
		waitMigrationCompleted(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, first.UUID)
		waitJournalPhase(first.UUID, "completed", kindMediumTimeout)
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

		// The workload's health cooldown floor runs first; once it has, the
		// hour's one migration is what turns the Instance away.
		ginkgo.By("the remaining Instance is rejected with HourlyCap")
		Eventually(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(SatisfyAll(HaveField("Instance", remainingIdx), rejectedWith("HourlyCap"))),
				"last cycle: %s", describeRecs(recs))
			var messages []string
			for _, e := range alfredEventsFor(g, ns.Name, fix.ISVC.Name, "RecommendationRejected") {
				messages = append(messages, e.Message)
			}
			g.Expect(messages).To(ContainElement(ContainSubstring("HourlyCap")))
		}).WithTimeout(dispatchTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// A transient observation gap can add a component-wide advisory; any
		// other finding for the workload must be that same rejection.
		ginkgo.By("it stays held: no second request, the pod untouched")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			recs := recsFor(recommendations(g), workloadID(fix))
			g.Expect(recs).To(ContainElement(SatisfyAll(HaveField("Instance", remainingIdx), rejectedWith("HourlyCap"))),
				"last cycle: %s", describeRecs(recs))
			for _, r := range recs {
				if r.Outcome == advisoryOutcome {
					continue
				}
				g.Expect(r.Instance).To(Equal(remainingIdx), "finding for another instance: %s", describeRecs(recs))
				g.Expect(r).To(rejectedWith("HourlyCap"))
			}
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(1), "a second request was dispatched")
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(1))
			live := livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)
			g.Expect(live).To(HaveLen(1))
			g.Expect(live[0].UID).To(Equal(remaining.UID))
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
