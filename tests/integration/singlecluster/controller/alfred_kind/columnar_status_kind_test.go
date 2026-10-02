package alfred_kind

import (
	"fmt"
	"sort"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// The columnar workload: enough Instances for the ColumnarV2 candidate to be
// far smaller than the dense list under the seeded ColumnarV2 target (eight
// steady single-pod rows serialize to about half the dense size), and a fixed
// share of them on the node that goes unhealthy. Evacuations of one workload
// run one at a time with a cooldown floor between them, so that share bounds
// the spec's wall-clock.
const (
	columnarReplicas = 8
	columnarOnSource = 3
)

// instanceIndices is the set of Instance indices the pods' labels carry.
func instanceIndices(g Gomega, pods []corev1.Pod) map[int32]struct{} {
	out := map[int32]struct{}{}
	for i := range pods {
		idx, ok := query.InstanceIdxFromLabels(&pods[i])
		g.ExpectWithOffset(1, ok).To(BeTrue(), "pod %s carries no instance index", pods[i].Name)
		out[idx] = struct{}{}
	}
	return out
}

func sortedIndices(set map[int32]struct{}) []int32 {
	out := make([]int32, 0, len(set))
	for idx := range set {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

var _ = ginkgo.Describe("alfred reads ColumnarV2 InferenceReplica status", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-columnar-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("resolves every instance on the unhealthy node from the columns and evacuates each", func() {
		src := workers[0]
		healthy := otherWorkers(src)

		// The fixture carries no spread constraint, so the scheduler alone
		// guarantees nothing about which worker holds which Instance. Pinning
		// to src while a filler holds all but columnarOnSource of its GPUs
		// places exactly that many Instances there; the rest stay Pending
		// until the pin is released and then land on the healthy workers.
		release := pinToNode(src)
		waitNodeGPUsFree(src)
		createFiller(Default, ns.Name, "columnar-filler", src, gpusPerWorker-columnarOnSource)
		fix := newAlfredFixture(ns.Name, withName("columnar"), withEngineReplicas(columnarReplicas))
		deferFixtureCleanup(fix)
		applyFixture(Default, fix)
		Eventually(func(g Gomega) {
			g.Expect(livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)).To(HaveLen(columnarOnSource))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		release()
		waitInstancesReady(ns.Name, fix.ISVC.Name, v1beta1.EngineComponent, columnarReplicas)
		waitPlacementCooldown()

		// Under the ColumnarV2 target the controller keeps columns only when
		// they serialize strictly smaller than the dense list.
		ginkgo.By("the controller stores the rows as columns")
		Eventually(func(g Gomega) {
			ir := freshIR(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Status.InstanceStatusEncoding).NotTo(BeNil(),
				"dense rows stored; raise columnarReplicas if the dense candidate still wins")
			g.Expect(*ir.Status.InstanceStatusEncoding).To(Equal(v1beta1.InstanceStatusEncodingColumnarV2))
			g.Expect(ir.Status.InstanceStatusColumns).NotTo(BeNil())
			g.Expect(ir.Status.InstanceStatuses).To(BeEmpty(), "a ColumnarV2 object carries no dense rows")
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		onSrc := livePodsOnNode(omenativePods(Default, ns.Name, fix.ISVC.Name), src)
		Expect(onSrc).To(HaveLen(columnarOnSource))
		srcInstances := instanceIndices(Default, onSrc)
		Expect(srcInstances).To(HaveLen(columnarOnSource))

		waitNodeRecordGone(src)
		markUnhealthy(src)

		// Findings come from the decoded rows joined to the pods. Columns that
		// fail to decode leave the component unresolved, which the policy
		// reports as one component-wide advisory (instance -1) and never as a
		// finding that names a real Instance.
		ginkgo.By("alfred lists the workload on the node and names its instances there")
		Eventually(func(g Gomega) {
			rec, ok := nodeRecordFor(g, src)
			g.Expect(ok).To(BeTrue(), "node record for %s", src)
			g.Expect(rec.Workloads).To(ContainElement(workloadID(fix)))
			g.Expect(rec.OMEAcceleratorOccupantsPresent).To(BeTrue())
			recs := recsFor(recommendations(g), workloadID(fix))
			named := 0
			for _, r := range recs {
				if r.Outcome == advisoryOutcome {
					continue
				}
				g.Expect(r.FromNode).To(Equal(src))
				g.Expect(srcInstances).To(HaveKey(r.Instance), "finding for an instance that did not live on %s", src)
				named++
			}
			g.Expect(named).NotTo(BeZero(), "no executable finding for %s; last cycle: %s", workloadID(fix), describeRecs(recs))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Every Instance that lived on src is moved by its own request; the
		// surge takes an unused index and the source row goes, so the row set
		// stays at columnarReplicas and matches the live pods.
		ginkgo.By("every instance that lived on the node is migrated and the status still decodes in full")
		state := func() string { return evacuationState(ns.Name, fix.ISVC.Name) }
		Eventually(func(g Gomega) {
			migrated := map[int32]struct{}{}
			entries := migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			for _, m := range entries {
				stopOnFailedMigration(fix.ISVC.Name, &m)
				g.Expect(m.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
				g.Expect(m.FromNode).To(Equal(src))
				g.Expect(srcInstances).To(HaveKey(m.SourceInstance),
					"migration %s moves instance %d, which did not live on %s", m.RequestUUID, m.SourceInstance, src)
				if m.Phase == v1beta1.MigrationPhaseCompleted {
					migrated[m.SourceInstance] = struct{}{}
				}
			}
			g.Expect(sortedIndices(migrated)).To(Equal(sortedIndices(srcInstances)), func() string {
				return fmt.Sprintf("completed migrations off %s; %s", src, state())
			})
			g.Expect(entries).To(HaveLen(len(srcInstances)), func() string {
				return fmt.Sprintf("one migration per instance that lived on %s; %s", src, state())
			})
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())

			var live []corev1.Pod
			for _, p := range omenativePods(g, ns.Name, fix.ISVC.Name) {
				if p.DeletionTimestamp != nil {
					continue
				}
				g.Expect(healthy).To(ContainElement(p.Spec.NodeName), "pod %s", p.Name)
				g.Expect(podIsReady(&p)).To(BeTrue(), "pod %s", p.Name)
				live = append(live, p)
			}
			g.Expect(live).To(HaveLen(columnarReplicas), func() string { return "live pods; " + state() })

			rows := instanceRows(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(rows).To(HaveLen(columnarReplicas))
			rowSet := map[int32]struct{}{}
			for _, r := range rows {
				g.Expect(r.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "instance %d", r.Index)
				g.Expect(r.Operation).To(BeNil(), "instance %d", r.Index)
				rowSet[r.Index] = struct{}{}
			}
			g.Expect(sortedIndices(rowSet)).To(Equal(sortedIndices(instanceIndices(g, live))))
			ir := freshIR(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Status.InstanceStatusEncoding).NotTo(BeNil())
			g.Expect(*ir.Status.InstanceStatusEncoding).To(Equal(v1beta1.InstanceStatusEncodingColumnarV2))
		}).WithTimeout(serialEvacuationTimeout(len(srcInstances))).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the journal closed one entry per instance, dispatched in series")
		var closed []journalEntry
		Eventually(func(g Gomega) {
			closed = journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)
			for _, e := range closed {
				if e.Phase == "failed" || e.Phase == "stalled" {
					StopTrying(fmt.Sprintf("journal %s for instance %d is %s: %s", e.UUID, e.Instance, e.Phase, e.Reason)).Now()
				}
				g.Expect(e.Phase).To(Equal("completed"), "journal %s (%s)", e.UUID, e.Reason)
			}
			g.Expect(closed).To(HaveLen(len(srcInstances)))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		sort.Slice(closed, func(i, j int) bool { return closed[i].CreatedAt.Before(closed[j].CreatedAt) })
		dispatched := map[int32]struct{}{}
		for i, e := range closed {
			Expect(e.Component).To(Equal(string(v1beta1.EngineComponent)))
			Expect(e.FromNode).To(Equal(src))
			Expect(e.AcknowledgedAt).NotTo(BeNil())
			Expect(e.CompletedAt).NotTo(BeNil())
			req := parseMigrationRequest(Default, e.Payload)
			Expect(req.Instance).To(Equal(e.Instance))
			Expect(req.FromNode).To(Equal(src))
			dispatched[e.Instance] = struct{}{}
			if i > 0 {
				Expect(e.CreatedAt).To(BeTemporally(">", *closed[i-1].CompletedAt),
					"request for instance %d was dispatched before the one for instance %d completed", e.Instance, closed[i-1].Instance)
			}
		}
		Expect(sortedIndices(dispatched)).To(Equal(sortedIndices(srcInstances)))

		ginkgo.By("the node is signaled and then reported drained")
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

		// Nothing of the workload is left on src, so the policy has no
		// further Instance to move while the node stays unhealthy.
		ginkgo.By("no further request follows")
		cycleBefore := recommendations(Default).Timestamp
		Consistently(func(g Gomega) {
			g.Expect(journalEntriesFor(journal(g), ns.Name, fix.ISVC.Name)).To(HaveLen(len(srcInstances)))
			g.Expect(migrationRequestAnnotations(g, isvcKey(fix))).To(BeEmpty())
			g.Expect(migrationEntries(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)).To(HaveLen(len(srcInstances)))
			g.Expect(livePodsOnNode(omenativePods(g, ns.Name, fix.ISVC.Name), src)).To(BeEmpty())
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
		Expect(recommendations(Default).Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
	})
})
