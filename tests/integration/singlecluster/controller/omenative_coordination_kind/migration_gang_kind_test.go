package omenative_coordination_kind

import (
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// migratioGangPods returns the live (non-terminating) engine pods of the
// Instance at idx, keyed by their runner label ("leader" / "worker").
func migratioGangPods(g Gomega, namespace, isvcName string, idx int32) map[string][]corev1.Pod {
	out := map[string][]corev1.Pod{}
	for _, p := range listOMENativePods(g, namespace, isvcName) {
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.Labels[query.LabelInstanceIdx] != strconv.Itoa(int(idx)) {
			continue
		}
		runner := p.Labels[query.LabelRunner]
		out[runner] = append(out[runner], p)
	}
	return out
}

// migratioLivePodCountForInstance counts live pods (any runner) at idx.
func migratioLivePodCountForInstance(g Gomega, namespace, isvcName string, idx int32) int {
	n := 0
	for _, pods := range migratioGangPods(g, namespace, isvcName, idx) {
		n += len(pods)
	}
	return n
}

// Whole-gang (multi-pod Instance) migration. The single-pod migration
// specs pin the state-machine contracts; these pin the gang-specific
// surface on real kubelets:
//
//   - validateFromNode is gang-aware: FromNode names ONE node hosting at
//     least one member (the leader here), and the whole gang relocates.
//   - The surge mirrors the source's full Runner layout (leader +
//     worker), so a gang never migrates into a single-pod shape.
//   - The migration overlay applies to every surge pod: no member of
//     the replacement gang may land back on FromNode.
//   - Deadline expiry with an unschedulable surge gang must tear down
//     the ENTIRE half-created surge gang and restore the untouched
//     source gang — no orphaned surge member, no source pod restart.
var _ = ginkgo.Describe("OMENative gang migration (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "migratio-gang-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("whole-gang migration relocates leader and worker off the source node", func() {
		const isvcName = "migratio-gang"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source gang (1 leader + 1 worker) to be Ready and scheduled")
		var sourceNode string
		sourceUIDs := map[types.UID]struct{}{}
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Index).To(Equal(int32(0)))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].RunningRevision).NotTo(BeEmpty())

			gang := migratioGangPods(g, ns.Name, isvcName, 0)
			g.Expect(gang["leader"]).To(HaveLen(1), "gang must have exactly one leader")
			g.Expect(gang["worker"]).To(HaveLen(1), "gang must have exactly one worker")
			for _, pods := range gang {
				for _, p := range pods {
					g.Expect(p.Spec.NodeName).NotTo(BeEmpty(),
						"every gang member must be scheduled before requesting migration")
					sourceUIDs[p.UID] = struct{}{}
				}
			}
			sourceNode = gang["leader"][0].Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By(fmt.Sprintf("requesting migration of the gang off the leader's node %s", sourceNode))
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting the migration entry to reach Completed with a surge index")
		var surgeIdx int32
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "accept must create the status.migrations entry")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseCompleted),
				"gang migration must complete; got phase=%s message=%q", entry.Phase, entry.Message)
			g.Expect(entry.SurgeInstance).NotTo(BeNil())
			surgeIdx = *entry.SurgeInstance
			g.Expect(surgeIdx).NotTo(Equal(int32(0)), "surge must take a fresh index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting exactly one surviving InstanceStatus: the promoted surge gang")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"after migration exactly one InstanceStatus survives (the surge)")
			g.Expect(insts[0].Index).To(Equal(surgeIdx))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Operation).To(BeNil(),
				"the promoted surge must carry no residual Migrate pin")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the surge to preserve the gang shape with every member OFF the source node")
		Eventually(func(g Gomega) {
			gang := migratioGangPods(g, ns.Name, isvcName, surgeIdx)
			g.Expect(gang["leader"]).To(HaveLen(1),
				"surge must mirror the source Runner layout: one leader")
			g.Expect(gang["worker"]).To(HaveLen(1),
				"surge must mirror the source Runner layout: one worker")
			for runner, pods := range gang {
				for _, p := range pods {
					g.Expect(p.Spec.NodeName).NotTo(BeEmpty(),
						"surge %s pod must be scheduled", runner)
					g.Expect(p.Spec.NodeName).NotTo(Equal(sourceNode),
						"surge %s pod must relocate off %s; landed on %s",
						runner, sourceNode, p.Spec.NodeName)
					_, isSource := sourceUIDs[p.UID]
					g.Expect(isSource).To(BeFalse(),
						"surge pods must be fresh pods, not renamed source pods")
				}
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting every source gang pod to be gone")
		Eventually(func(g Gomega) {
			g.Expect(migratioLivePodCountForInstance(g, ns.Name, isvcName, 0)).To(BeZero(),
				"the drained source gang must be fully deleted")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting a Completed audit row carrying FromNode")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, isvcName)
			matched := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Completed" {
					matched = true
					g.Expect(e.FromNode).To(Equal(sourceNode))
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected a Completed audit row for uuid=%s; got %+v", reqUUID, ledger.Entries)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("gang migration deadline expiry tears down the surge gang and restores the source gang", func() {
		const isvcName = "migratio-gexp"
		// The entry's accept->Deadline window: the Component's effective
		// InstanceReadyTimeout. Short enough to fit the suite budget,
		// long enough to observe the pre-deadline holding pattern.
		const migrationTimeout = 45 * time.Second

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineWorkerSize(1),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			InstanceReadyTimeout: &metav1.Duration{Duration: migrationTimeout},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source gang Ready and recording its node + pod UIDs")
		var sourceNode string
		sourceUIDs := map[string]types.UID{}
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			gang := migratioGangPods(g, ns.Name, isvcName, 0)
			g.Expect(gang["leader"]).To(HaveLen(1))
			g.Expect(gang["worker"]).To(HaveLen(1))
			for _, pods := range gang {
				for _, p := range pods {
					g.Expect(p.Spec.NodeName).NotTo(BeEmpty())
					sourceUIDs[p.Name] = p.UID
				}
			}
			sourceNode = gang["leader"][0].Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("cordoning every off-source worker so the surge gang cannot schedule")
		// The surge gang carries a NotIn[sourceNode] anti-affinity on
		// EVERY member, so with all other workers cordoned no surge pod
		// has anywhere to land.
		var cordoned []string
		for _, w := range schedulableWorkerNodes(Default) {
			if w != sourceNode {
				cordonNode(w)
				cordoned = append(cordoned, w)
			}
		}
		Expect(cordoned).NotTo(BeEmpty(),
			"the suite cluster must have at least one off-source worker to cordon")

		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting SurgePending with a full, unschedulable surge gang")
		var surgeIdx int32
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "accept must create the status.migrations entry")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseSurgePending))
			g.Expect(entry.SurgeInstance).NotTo(BeNil())
			surgeIdx = *entry.SurgeInstance

			gang := migratioGangPods(g, ns.Name, isvcName, surgeIdx)
			g.Expect(gang["leader"]).To(HaveLen(1),
				"the blocked surge must still render its full gang: one leader")
			g.Expect(gang["worker"]).To(HaveLen(1),
				"the blocked surge must still render its full gang: one worker")
			for _, pods := range gang {
				for _, p := range pods {
					g.Expect(p.Spec.NodeName).To(BeEmpty(),
						"surge pod %s must be unschedulable (NotIn[%s] + cordons)", p.Name, sourceNode)
				}
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the source gang keeps serving untouched while the surge waits")
		Consistently(func(g Gomega) {
			gang := migratioGangPods(g, ns.Name, isvcName, 0)
			g.Expect(gang["leader"]).To(HaveLen(1))
			g.Expect(gang["worker"]).To(HaveLen(1))
			for _, pods := range gang {
				for _, p := range pods {
					g.Expect(p.UID).To(Equal(sourceUIDs[p.Name]),
						"no source gang member may be restarted while its migration is blocked")
					g.Expect(podIsReady(&p)).To(BeTrue(),
						"source gang member %s must stay Ready while its migration is blocked", p.Name)
				}
			}
		}, migrationTimeout/3, kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the entry to fail terminally at its Deadline")
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil())
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseFailed))
			g.Expect(entry.CompletedAt).NotTo(BeNil())
			g.Expect(entry.Message).To(ContainSubstring("SurgePending"),
				"the terminal message must name the blocked phase; got %q", entry.Message)
		}).WithTimeout(migrationTimeout + kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting a MigrationExpired warning event")
		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "MigrationExpired", "InferenceService", isvcName)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the ENTIRE surge gang torn down and the source gang restored Ready")
		Eventually(func(g Gomega) {
			g.Expect(migratioLivePodCountForInstance(g, ns.Name, isvcName, surgeIdx)).To(BeZero(),
				"expiry must tear down every member of the half-created surge gang")

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"only the source InstanceStatus survives the expiry")
			g.Expect(insts[0].Index).To(Equal(int32(0)))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"source must be restored Ready from observation")
			g.Expect(insts[0].Operation).To(BeNil(),
				"the Migrate pin must be cleared from the source")

			gang := migratioGangPods(g, ns.Name, isvcName, 0)
			g.Expect(gang["leader"]).To(HaveLen(1))
			g.Expect(gang["worker"]).To(HaveLen(1))
			for _, pods := range gang {
				for _, p := range pods {
					g.Expect(p.UID).To(Equal(sourceUIDs[p.Name]),
						"expiry must restore the source gang without recreating its pods")
				}
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the audit ledger to mirror the terminal Failed row")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, isvcName)
			matched := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Failed" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected a Failed audit row for uuid=%s; got %+v", reqUUID, ledger.Entries)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
