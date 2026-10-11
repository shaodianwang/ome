package omenative_coordination_kind

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A migration mode flipped to Never leaves a standing pair alone.
//
// One engine Instance is asked to move off its node while every other
// worker is cordoned, so the replacement stays Pending and the pair stands
// on the record: the source pinned Migrating, the replacement pinned at a
// fresh index. The operator then sets the migration mode to Never, the
// knob that stops new move requests. Nothing else may change: the source
// is not promoted back to Ready and keeps its pin, no InstanceReady event
// is recorded for it, and no third index appears. The record alone ends
// the pair, at its deadline: the replacement is torn down, the source is
// restored to Ready on the same pod, and only index 0 remains.

// Cells: instance/Migrate/CreateSurge/spec.policyTuning[migrationPolicy], instance/Migrate/SurgeTarget/spec.policyTuning[migrationPolicy]
var _ = ginkgo.Describe("behavior — a migration pair stands under a mode flipped to Never (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "mig-never-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("does not promote the source, creates no extra index, and lets the deadline end the pair", func() {
		const isvcName = "mig-never"
		const podName = isvcName + "-engine-0-default-0"
		// The record's accept-to-deadline window: the Component's effective
		// InstanceReadyTimeout, long enough to flip the mode and watch the
		// pair stand, short enough for the expiry inside the suite budget.
		const migrationTimeout = 60 * time.Second

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			InstanceReadyTimeout: &metav1.Duration{Duration: migrationTimeout},
			MigrationPolicy:      &v1beta1.MigrationPolicy{Mode: v1beta1.MigrationPolicyModeAuto},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source pod Ready and discovering its node and UID")
		var sourceNode string
		var sourceUID types.UID
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: podName}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
			sourceUID = pod.UID
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("cordoning every other worker so the replacement cannot schedule")
		var cordoned []string
		for _, w := range schedulableWorkerNodes(Default) {
			if w != sourceNode {
				cordonNode(w)
				cordoned = append(cordoned, w)
			}
		}
		Expect(cordoned).NotTo(BeEmpty(), "the suite cluster must have at least one off-source worker to cordon")

		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting the pair to stand: the source pinned Migrating, the replacement Pending at a fresh index")
		var surgeIdx int32
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "accept must create the status.migrations entry")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseSurgePending))
			g.Expect(entry.SurgeInstance).NotTo(BeNil())
			surgeIdx = *entry.SurgeInstance
			expectMigrationPairStanding(g, ns.Name, isvcName, surgeIdx, sourceUID, podName)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		surgePodName := fmt.Sprintf("%s-engine-%d-default-0", isvcName, surgeIdx)

		// Every index the Component ever shows, on its rows and on its pods,
		// from the flip to the end: a third one is the defect.
		indices := newIndexLedger()
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				indices.record(row.Index)
			}
			for _, pod := range listOMENativePods(g, ns.Name, isvcName) {
				if idx, ok := query.InstanceIdxFromLabels(&pod); ok {
					indices.record(idx)
				}
			}
			for _, idx := range indices.seen() {
				g.Expect(idx).To(BeElementOf(int32(0), surgeIdx),
					"only the source (0) and its replacement (%d) may ever hold an index; index %d is an Instance the mode flip created", surgeIdx, idx)
			}
		})
		defer stop()

		ginkgo.By("setting the migration mode to Never while the pair stands")
		flipAt := time.Now()
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.Lifecycle.MigrationPolicy = &v1beta1.MigrationPolicy{Mode: v1beta1.MigrationPolicyModeNever}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Strictly inside accept-to-deadline: the pair stands as it was. The
		// source keeps its pin and its phase, the replacement keeps its
		// index and stays Pending, the record stays SurgePending, and no
		// InstanceReady names the source.
		ginkgo.By("the pair stands unchanged under the flipped mode")
		Consistently(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil())
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseSurgePending), "the mode flip must not end or advance the record")
			expectMigrationPairStanding(g, ns.Name, isvcName, surgeIdx, sourceUID, podName)
			g.Expect(instanceEventsSince(g, ns.Name, isvcName, workloadtypes.EventReasonInstanceReady, 0, flipAt)).To(BeEmpty(),
				"the Create pass must not promote a source the migration record still owns")
		}, migrationTimeout/4, kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the record to fail terminally at its deadline")
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

		ginkgo.By("expecting the replacement torn down and the source restored Ready on the same pod")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: surgePodName}, &corev1.Pod{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the expired replacement pod must be deleted (err=%v)", err)

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1), "only the source row survives the expiry")
			g.Expect(insts[0].Index).To(Equal(int32(0)))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady), "the source must be restored Ready from observation")
			g.Expect(insts[0].Operation).To(BeNil(), "the expiry must clear the source's pin")

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: podName}, pod)).To(Succeed())
			g.Expect(pod.UID).To(Equal(sourceUID), "the source must keep the pod it served on throughout")
			g.Expect(podIsReady(pod)).To(BeTrue())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the source was never re-promoted and nothing holds past the expiry")
		Consistently(func(g Gomega) {
			g.Expect(instanceEventsSince(g, ns.Name, isvcName, workloadtypes.EventReasonInstanceReady, 0, flipAt)).To(BeEmpty(),
				"no InstanceReady may be recorded for a source that never left service")
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Operation).To(BeNil())
		}, kindSteadyStateWindow, kindPollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">", 0), "the index ledger must have sampled the pair")
		Expect(indices.seen()).To(ConsistOf(int32(0), surgeIdx),
			"the Component must have shown exactly the source and its replacement, never a third index")
	})
})

// expectMigrationPairStanding asserts the pair the record owns is intact:
// two rows, the source at 0 still Migrating with its Migrate pin and its
// original Ready pod, the replacement at surgeIdx pinned Migrate with a
// pod still Pending and unbound.
func expectMigrationPairStanding(g Gomega, namespace, isvcName string, surgeIdx int32, sourceUID types.UID, sourcePodName string) {
	insts := instanceStatuses(g, namespace, isvcName, v1beta1.EngineComponent)
	g.ExpectWithOffset(1, insts).To(HaveLen(2), "the pair must be exactly the source and its replacement")
	for _, row := range insts {
		g.ExpectWithOffset(1, row.Operation).NotTo(BeNil(), "instance %d must keep its migration pin", row.Index)
		g.ExpectWithOffset(1, row.Operation.Type).To(Equal(v1beta1.InstanceOperationMigrate), "instance %d must be pinned by the migration", row.Index)
		switch row.Index {
		case 0:
			g.ExpectWithOffset(1, row.Phase).To(Equal(v1beta1.OMENativeInstanceMigrating), "the source must not be promoted while the record owns it")
		case surgeIdx:
			g.ExpectWithOffset(1, row.Phase).NotTo(Equal(v1beta1.OMENativeInstanceReady), "a replacement with no node cannot be Ready")
		default:
			g.ExpectWithOffset(1, false).To(BeTrue(), "unexpected instance index %d beside the pair", row.Index)
		}
	}
	source := &corev1.Pod{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: sourcePodName}, source)).To(Succeed())
	g.ExpectWithOffset(1, source.UID).To(Equal(sourceUID), "the source pod must never be replaced while its migration stands")
	g.ExpectWithOffset(1, podIsReady(source)).To(BeTrue(), "the source pod must keep serving while its migration stands")
	surge := &corev1.Pod{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: fmt.Sprintf("%s-engine-%d-default-0", isvcName, surgeIdx)}, surge)).To(Succeed())
	g.ExpectWithOffset(1, surge.Status.Phase).To(Equal(corev1.PodPending))
	g.ExpectWithOffset(1, surge.Spec.NodeName).To(BeEmpty(), "the replacement must stay unschedulable while the other workers are cordoned")
}

// instanceEventsSince lists the messages of the events with reason on the
// InferenceService that name the engine Instance at index and were
// recorded at or after since.
func instanceEventsSince(g Gomega, namespace, isvcName string, reason workloadtypes.EventReason, index int32, since time.Time) []string {
	events := &corev1.EventList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, events, client.InNamespace(namespace))).To(Succeed())
	key := workloadtypes.InstanceKey(workloadtypes.ComponentEngine, index)
	var out []string
	for _, ev := range events.Items {
		if ev.InvolvedObject.Name != isvcName || ev.Reason != string(reason) || !strings.Contains(ev.Message, key) {
			continue
		}
		when := ev.LastTimestamp.Time
		if when.IsZero() {
			when = ev.EventTime.Time
		}
		if when.IsZero() {
			when = ev.CreationTimestamp.Time
		}
		if when.Before(since) {
			continue
		}
		out = append(out, ev.Message)
	}
	return out
}

// indexLedger is the set of Instance indices a sampler has seen.
type indexLedger struct {
	mu   sync.Mutex
	idxs map[int32]struct{}
}

func newIndexLedger() *indexLedger {
	return &indexLedger{idxs: map[int32]struct{}{}}
}

func (l *indexLedger) record(idx int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.idxs[idx] = struct{}{}
}

// seen lists every index recorded so far.
func (l *indexLedger) seen() []int32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]int32, 0, len(l.idxs))
	for idx := range l.idxs {
		out = append(out, idx)
	}
	return out
}
