package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Auto-relocation of a stuck Instance — the deadline disposition's
// relocatable branch.
//
// A single-pod engine parks Running-but-unready on one node (fake-serving
// FAIL_ON_NODE: readiness fails only there — a node-scoped fault with NO
// workload-caused kubelet waiting reason). When the Create attempt
// exceeds InstanceReadyTimeout, the disposition records a TERMINAL
// AutoRecover ledger entry (Phase=Completed, Outcome=relocate-recreate,
// FromNode=<suspect node>) — a relocation directive, not a migration
// work order (the migration detector ignores AutoRecover entries; no
// surge machinery runs) — then clears the Operation and stamps
// Phase=Failed. The normal rebuild recreates the pod with a required
// NodeAffinity hostname NotIn exclusion for the recorded node, it lands
// on a different node, converges Ready, and the directive entries are
// pruned (placement proved; exclusion memory and budget reset).
//
// Initial placement is forced onto the failing node by cordoning the
// other worker; it is uncordoned once the pod is parked so the rebuild
// has an off-suspect home. The relocation budget comes from operator
// config (lifecycle.autoMigrate.maxAttempts); MigrationPolicy.Mode=Auto
// gates the branch.
var _ = ginkgo.Describe("OMENative auto-relocation on stuck Instance (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-auto-mig-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("stuck Instance is relocated off the suspect node via a terminal AutoRecover directive", func() {
		const (
			isvcName = "auto-mig-am1"
			podName  = isvcName + "-engine-0-default-0"
		)

		workers := schedulableWorkerNodes(Default)
		Expect(len(workers)).To(BeNumerically(">=", 2),
			"relocation needs a second schedulable worker; got %v", workers)
		failNode, spareNode := workers[0], workers[1]

		configureFastDisposition()

		// Cordon the spare so the first pod deterministically lands on
		// the failing node. cordonNode's cleanup uncordons on any exit.
		cordonNode(spareNode)

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		withNodeScopedFailure(&fix.ISVC.Spec.Engine.PodSpec.Containers[0], failNode, "")
		fix.ISVC.Spec.Engine.Lifecycle.MigrationPolicy = &v1beta1.MigrationPolicy{
			Mode: v1beta1.MigrationPolicyModeAuto,
		}
		fix.ISVC.Spec.Engine.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 45 * time.Second}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the pod to park Running-but-unready on the failing node")
		var stuckUID types.UID
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: podName}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).To(Equal(failNode),
				"initial placement must hit the failing node (spare is cordoned)")
			g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning),
				"the node-scoped fault must leave the container Running (no kubelet waiting reason)")
			stuckUID = pod.UID

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceCreating))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Give the rebuild somewhere off-suspect to land BEFORE the
		// deadline disposition fires.
		setNodeUnschedulable(spareNode, false)

		ginkgo.By("waiting for the terminal AutoRecover relocation directive in the audit ledger")
		Eventually(func(g Gomega) {
			entries := autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")
			g.Expect(entries).NotTo(BeEmpty(),
				"expected an AutoRecover directive after InstanceReadyTimeout")
			e := entries[0]
			g.Expect(e.Phase).To(Equal("Completed"), "directives are recorded TERMINAL")
			g.Expect(e.Outcome).To(Equal("relocate-recreate"))
			g.Expect(e.FromNode).To(Equal(failNode))
			g.Expect(e.SourceInstance).To(Equal(int32(0)))
			g.Expect(e.SurgeInstance).To(Equal(int32(0)),
				"a directive is not a migration work order; no surge index is ever allocated")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting a born-terminal Auto record in status.migrations")
		// The relocation directive is mirrored as a visibility record on
		// the IR: Trigger=Auto, born terminal (Phase=Relocated), never
		// selected by the migration executor (no surge index is ever
		// allocated). Succeeded stays unset until the relocated Instance
		// next reaches Ready.
		var autoUUID string
		Eventually(func(g Gomega) {
			entries := migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent)
			var auto *v1beta1.MigrationStatus
			for i := range entries {
				if entries[i].Trigger == v1beta1.MigrationTriggerAuto {
					auto = &entries[i]
					break
				}
			}
			g.Expect(auto).NotTo(BeNil(),
				"the relocation directive must mirror as a status.migrations record")
			g.Expect(auto.Phase).To(Equal(v1beta1.MigrationPhaseRelocated), "Auto records are born terminal")
			g.Expect(auto.FromNode).To(Equal(failNode))
			g.Expect(auto.SourceInstance).To(Equal(int32(0)))
			g.Expect(auto.Attempt).To(Equal(int32(1)),
				"first relocation attempt of the configured budget")
			g.Expect(auto.SurgeInstance).To(BeNil(),
				"a directive is not a migration work order; no surge index is ever allocated")
			g.Expect(auto.CompletedAt).NotTo(BeNil(), "born-terminal records carry CompletedAt")
			autoUUID = auto.RequestUUID
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the AutoMigrationTriggered event on the ISVC")
		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "AutoMigrationTriggered", "InferenceService", isvcName)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the rebuild to recreate the pod off the recorded node with the NotIn exclusion")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: podName}, pod)).To(Succeed())
			g.Expect(pod.UID).NotTo(Equal(stuckUID), "relocation must recreate the pod (new UID)")
			g.Expect(podExcludesHostname(pod, failNode)).To(BeTrue(),
				"recreated pod must carry a required kubernetes.io/hostname NotIn[%s] term; affinity=%+v",
				failNode, pod.Spec.Affinity)
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(), "recreated pod must schedule")
			g.Expect(pod.Spec.NodeName).NotTo(Equal(failNode),
				"recreated pod must land off the suspect node")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the Instance to converge Ready off the suspect node")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Index).To(Equal(int32(0)),
				"relocation rebuilds the SAME instance index; no surge index appears")
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the directive entries to be pruned once the Instance is Ready")
		Eventually(func(g Gomega) {
			entries := autoRecoverEntries(loadLedger(g, ns.Name, isvcName), "engine")
			g.Expect(entries).To(BeEmpty(),
				"Ready proves the placement; exclusion memory and budget must reset; got %+v", entries)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the Auto record to persist with Succeeded=true")
		// Ledger pruning resets exclusion memory, but the status record
		// stays as history — with the post-hoc outcome stamped once the
		// relocated Instance reached Ready.
		Eventually(func(g Gomega) {
			auto := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), autoUUID)
			g.Expect(auto).NotTo(BeNil(),
				"the Auto record must survive the ledger prune as visible history")
			g.Expect(auto.Phase).To(Equal(v1beta1.MigrationPhaseRelocated))
			g.Expect(auto.Succeeded).NotTo(BeNil(),
				"Succeeded must be stamped once the relocated Instance is Ready")
			g.Expect(*auto.Succeeded).To(BeTrue())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
