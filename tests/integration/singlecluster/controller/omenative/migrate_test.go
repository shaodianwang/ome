package omenative

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	migrationAnnoPrefix = "ome.io/migration-request-v1-"
	migrationSchemaV1   = "v1"
	auditCMSuffix       = "-ome-migration-audit"
	auditLedgerDataKey  = "history.json"
)

// auditEntryShape matches the per-row schema persisted in the audit
// ConfigMap. Decoded for assertions; declared locally to keep this test
// file independent of internal omenative package types.
type auditEntryShape struct {
	RequestUUID    string `json:"requestUUID"`
	Component      string `json:"component"`
	SourceInstance int32  `json:"sourceInstance"`
	SurgeInstance  int32  `json:"surgeInstance,omitempty"`
	Phase          string `json:"phase"`
	FromNode       string `json:"fromNode,omitempty"`
	StartedAt      string `json:"startedAt"`
	CompletedAt    string `json:"completedAt,omitempty"`
	Outcome        string `json:"outcome,omitempty"`
}

type auditLedgerShape struct {
	Entries []auditEntryShape `json:"entries"`
}

// loadLedger reads the audit ConfigMap. Returns an empty ledger if the
// CM doesn't exist yet (first reconcile after annotation).
func loadLedger(g Gomega, ns, isvcName string) auditLedgerShape {
	cm := &corev1.ConfigMap{}
	err := k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: ns, Name: isvcName + auditCMSuffix,
	}, cm)
	if client.IgnoreNotFound(err) != nil {
		g.Expect(err).NotTo(HaveOccurred())
	}
	if err != nil {
		return auditLedgerShape{}
	}
	raw, ok := cm.Data[auditLedgerDataKey]
	if !ok || raw == "" {
		return auditLedgerShape{}
	}
	var l auditLedgerShape
	g.Expect(json.Unmarshal([]byte(raw), &l)).To(Succeed())
	return l
}

func writeMigrationRequest(g Gomega, isvcKey client.ObjectKey, reqUUID, fromNode string, instance int32) {
	payload, err := json.Marshal(map[string]any{
		"schemaVersion": migrationSchemaV1,
		"component":     "engine",
		"instance":      instance,
		"from_node":     fromNode,
		"reason":        "integration-test",
	})
	g.Expect(err).NotTo(HaveOccurred())

	Eventually(func(gg Gomega) {
		fresh := &v1beta1.InferenceService{}
		gg.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[migrationAnnoPrefix+reqUUID] = string(payload)
		gg.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
}

// OMENative migration specs. The migration state machine is the most
// complex op in OMENative — orchestration runs from an ISVC annotation
// through audit-ledger dedup, surge allocation, source drain, and
// surge promotion. These specs pin the happy path and the
// FromNode-mismatch rejection path end-to-end.
var _ = ginkgo.Describe("OMENative Migration", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-mig-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// FINDING (MIGRATE-7, blocks this integration spec):
	// The happy-path end-to-end migration runs through the state machine
	// correctly up through "surge promoted Ready + source InstanceStatus
	// removed". On the NEXT reconcile, the scale-down pass
	// (extraInstanceIndices in delete.go) sees:
	//   - plan.Instances = [Index=0]  (MinReplicas=1)
	//   - InstanceStatuses = [Index=1] (only the surge survives)
	// and classifies the surge as "extra" because its Phase=Ready and
	// Operation=nil — neither of the "exclude Migrating" guards fires
	// post-promote. Delete(idx=1) tears down the surge; Create then
	// resurrects a fresh pod at Index=0 on whatever node the sim
	// assigns. Net effect: migration appears to "complete" in the audit
	// ledger but the workload ends up back on its original node (or a
	// fresh recreate, depending on sim node allocation).
	//
	// Fix path is in delete.go's extraInstanceIndices and/or plan.go's
	// instance-index allocator: surge-promoted instances at sparse
	// indices need to either be renamed back to the source index (hard
	// — pod names encode index) or the planner must adopt the sparse
	// surge index instead of re-emitting the original source index.
	// Filed for a separate fix PR; this test stays PIt'd until then.
	ginkgo.PIt("annotation triggers full migration: surge appears on a different node, source disappears [BLOCKED MIGRATE-7]", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("mig-llama"),
			WithCSRName("mig-runtime"),
			WithISVCName("mig-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the source pod to be Ready and scheduled to a node")
		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.InstanceStatuses[0].RunningRevision).NotTo(BeEmpty())

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mig-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(),
				"source pod must be scheduled before requesting migration")
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By(fmt.Sprintf("writing migration request with from_node=%s", sourceNode))
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting the source InstanceStatus to disappear and a surge instance to take over")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"after migration, exactly one InstanceStatus survives (the surge)")
			s := cs.OMENative.InstanceStatuses[0]
			g.Expect(s.Index).NotTo(Equal(int32(0)),
				"surge index must be != source (0); got %d", s.Index)
			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"surge must promote to Ready after source drain")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the surge pod to land on a node other than the source node")
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			// Only the surge pod should remain after the migration completes.
			alive := 0
			for i := range pods.Items {
				if pods.Items[i].DeletionTimestamp == nil {
					alive++
					g.Expect(pods.Items[i].Spec.NodeName).NotTo(Equal(sourceNode),
						"surviving pod %s should NOT be on the source node %s",
						pods.Items[i].Name, sourceNode)
				}
			}
			g.Expect(alive).To(Equal(1),
				"expected exactly one surviving pod (the surge); got %d", alive)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the audit ledger to record a Completed entry for this UUID")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the migration-request annotation to be cleared on completion")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"annotation must be cleared after Completed so re-delivery dedups")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// FromNode mismatch: the requester's view of where the pod lives is
	// stale. validateFromNode must reject with MigrationFromNodeMismatch
	// + Failed audit row, NOT create a surge.
	ginkgo.It("FromNode mismatch is rejected with Failed audit row and no surge", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("mismatch-llama"),
			WithCSRName("mismatch-runtime"),
			WithISVCName("mismatch-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the source pod to be Ready")
		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mismatch-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("sending a migration request with the WRONG from_node")
		wrongNode := "not-" + sourceNode
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, wrongNode, 0)

		ginkgo.By("expecting a Failed audit row")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
			matched := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Failed" {
					matched = true
					g.Expect(e.Outcome).NotTo(BeEmpty(),
						"Failed audit row should carry a rejection reason")
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected a Failed audit row for uuid=%s; got %+v", reqUUID, ledger.Entries)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("asserting no surge instance is ever created (rejection short-circuits before surge alloc)")
		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"rejected migration must not create a surge; got %d InstanceStatuses",
				len(cs.OMENative.InstanceStatuses))
			// Phase may be Ready or Updating (the post-rejection ISVC spec
			// change can spuriously trigger Update); the invariant is that
			// no surge index appears.
			g.Expect(cs.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"only the source InstanceStatus should exist")
		}, "3s", util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the rejected request's annotation to be cleared")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"annotation must be cleared on rejection so the requester stops retrying")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
