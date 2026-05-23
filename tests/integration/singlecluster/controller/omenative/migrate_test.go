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
	writeMigrationRequestWithComponent(g, isvcKey, reqUUID, fromNode, instance, "engine")
}

// writeMigrationRequestWithComponent is the generalized form of
// writeMigrationRequest — takes any component string so PD specs can
// write decoder/router-scoped requests on the same ISVC.
func writeMigrationRequestWithComponent(g Gomega, isvcKey client.ObjectKey, reqUUID, fromNode string, instance int32, component string) {
	payload, err := json.Marshal(map[string]any{
		"schemaVersion": migrationSchemaV1,
		"component":     component,
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

	// Root-cause note: this spec was previously PIt'd because the
	// migration appeared to complete in the audit ledger but the
	// workload ended up back on its original node. The actual bug was
	// that Migrate clears the request annotation at accept-time (line
	// ~140 of migrate.go, requester contract is 30s response-or-clear)
	// while the migration itself takes many reconcile passes. With no
	// annotation, the next reconcile's detectMigrationRequest returned
	// false, Migrate was never re-entered, and dispatch fell through
	// to Create — which promoted the still-Migrating source back to
	// Phase=Ready, clearing its Operation. The post-promote scale-down
	// pass then classified the surge (sparse Index=1) as "extra" and
	// tore it down. Fix: detectMigrationRequest now also scans the
	// audit ledger for in-flight Started entries, so Migrate keeps
	// driving the migration until the ledger has a terminal Completed
	// or Failed entry for that UUID.
	ginkgo.It("annotation triggers full migration: surge appears on a different node, source disappears", func() {
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

		ginkgo.By("expecting both MigrationRequestAccepted + MigrationCompleted events")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			gotAccept, gotComplete := false, false
			for _, e := range events.Items {
				if e.InvolvedObject.Name != fx.ISVC.Name {
					continue
				}
				switch e.Reason {
				case "MigrationRequestAccepted":
					gotAccept = true
				case "MigrationCompleted":
					gotComplete = true
				}
			}
			g.Expect(gotAccept).To(BeTrue(), "expected MigrationRequestAccepted event")
			g.Expect(gotComplete).To(BeTrue(), "expected MigrationCompleted event")
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

		ginkgo.By("expecting a MigrationFromNodeMismatch warning event")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "MigrationFromNodeMismatch" && e.InvolvedObject.Name == fx.ISVC.Name {
					matched = true
					g.Expect(e.Message).To(ContainSubstring(wrongNode),
						"event message should reference the wrong node")
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected MigrationFromNodeMismatch warning event for the stale FromNode")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Re-delivery of the same UUID after Completed must dedup via the
	// audit ledger. The requester might retry if its first observation
	// of the annotation-clear didn't land; the controller must NOT
	// re-run the migration.
	ginkgo.It("re-delivery of a Completed UUID dedups via the ledger (no second migration)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("dedup-llama"),
			WithCSRName("dedup-runtime"),
			WithISVCName("dedup-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"migration dispatch requires the source instance to be Ready")
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "dedup-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("waiting for the first migration to complete")
		var surgeIdx int32
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			surgeIdx = cs.OMENative.InstanceStatuses[0].Index
			g.Expect(surgeIdx).NotTo(Equal(int32(0)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("re-delivering the SAME UUID via annotation")
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("asserting no second surge appears (dedup via ledger Completed entry)")
		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"re-delivered Completed UUID must NOT trigger a second migration; got %d InstanceStatuses",
				len(cs.OMENative.InstanceStatuses))
			g.Expect(cs.OMENative.InstanceStatuses[0].Index).To(Equal(surgeIdx),
				"surviving instance index must match the first migration's surge index")
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Two distinct migrations on the same ISVC, in sequence. Tests that
	// the audit ledger keeps both UUIDs straight and that the second
	// migration can pick a fresh surge index after the first lands.
	ginkgo.It("two sequential migrations on the same ISVC each complete", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("seq-llama"),
			WithCSRName("seq-runtime"),
			WithISVCName("seq-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		runMigration := func(label string) (firstIdx int32, completedAt int32) {
			ginkgo.By(fmt.Sprintf("[%s] waiting for the current instance to be Ready on a known node", label))
			var srcIdx int32
			var node string
			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceService{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
				cs := fresh.Status.Components[v1beta1.EngineComponent]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				srcIdx = cs.OMENative.InstanceStatuses[0].Index
				g.Expect(cs.OMENative.InstanceStatuses[0].NodesOccupied).NotTo(BeEmpty())
				node = cs.OMENative.InstanceStatuses[0].NodesOccupied[0]
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

			reqUUID := uuid.NewString()
			ginkgo.By(fmt.Sprintf("[%s] writing migration request uuid=%s from_node=%s instance=%d", label, reqUUID, node, srcIdx))
			writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, node, srcIdx)

			ginkgo.By(fmt.Sprintf("[%s] waiting for migration to complete", label))
			var newIdx int32
			Eventually(func(g Gomega) {
				fresh := &v1beta1.InferenceService{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
				cs := fresh.Status.Components[v1beta1.EngineComponent]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(cs.OMENative.InstanceStatuses[0].Index).NotTo(Equal(srcIdx))
				newIdx = cs.OMENative.InstanceStatuses[0].Index

				ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
				found := false
				for _, e := range ledger.Entries {
					if e.RequestUUID == reqUUID && e.Phase == "Completed" {
						found = true
						break
					}
				}
				g.Expect(found).To(BeTrue(),
					"[%s] expected Completed audit row for uuid=%s", label, reqUUID)
			}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
			return srcIdx, newIdx
		}

		first, second := runMigration("M1")
		Expect(first).NotTo(Equal(second), "first migration must move the instance to a different index")

		third, fourth := runMigration("M2")
		Expect(third).To(Equal(second), "second migration starts where the first ended")
		Expect(fourth).NotTo(Equal(third), "second migration must move the instance again")
	})

	// Audit ConfigMap exists with the deterministic name and carries a
	// Completed entry after a successful migration. upsertEntry replaces
	// the Started entry in-place (last-writer-wins per UUID), so the
	// final shape is one Completed row with the full SourceInstance /
	// SurgeInstance / FromNode / CompletedAt fields populated.
	ginkgo.It("audit ConfigMap exists with a Completed entry after migration", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("audit-llama"),
			WithCSRName("audit-runtime"),
			WithISVCName("audit-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var sourceNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"migration dispatch requires the source instance to be Ready")
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "audit-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("waiting for the audit ConfigMap to carry the Completed entry with full fields")
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: fx.ISVC.Name + auditCMSuffix,
			}, cm)).To(Succeed())
			raw, ok := cm.Data[auditLedgerDataKey]
			g.Expect(ok).To(BeTrue(), "audit ConfigMap missing %q key", auditLedgerDataKey)
			g.Expect(raw).NotTo(BeEmpty())

			var l auditLedgerShape
			g.Expect(json.Unmarshal([]byte(raw), &l)).To(Succeed())
			matched := false
			for _, e := range l.Entries {
				if e.RequestUUID != reqUUID || e.Phase != "Completed" {
					continue
				}
				matched = true
				g.Expect(e.SourceInstance).To(Equal(int32(0)),
					"Completed entry should preserve the original SourceInstance index")
				g.Expect(e.SurgeInstance).NotTo(Equal(int32(0)),
					"Completed entry should record the surge index (sparse, != 0)")
				g.Expect(e.FromNode).To(Equal(sourceNode),
					"Completed entry should carry FromNode for replay")
				g.Expect(e.CompletedAt).NotTo(BeEmpty(),
					"Completed entry must have a CompletedAt timestamp")
				break
			}
			g.Expect(matched).To(BeTrue(),
				"audit ledger must record a Completed entry for uuid=%s", reqUUID)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// MigrationMode=Never disables the migration dispatch entirely —
	// even a valid annotation is ignored (no Migrate called, no audit
	// ledger written, source stays put). Operators set Never when they
	// want migration requests to be handled out-of-band.
	ginkgo.It("MigrationMode=Never ignores migration request annotations", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("nomig-llama"),
			WithCSRName("nomig-runtime"),
			WithISVCName("nomig-isvc"),
			WithMigrationMode(v1beta1.OMENativeMigrationPolicyModeNever),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the source pod Ready")
		var sourceNode string
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "nomig-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing a valid migration request")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("asserting no surge appears, no ledger entry, annotation stays put")
		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"Never policy must NOT create a surge")
			g.Expect(cs.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"only the source index should remain")

			ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
			for _, e := range ledger.Entries {
				g.Expect(e.RequestUUID).NotTo(Equal(reqUUID),
					"Never policy must NOT write an audit ledger entry for the request")
			}

			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeTrue(),
				"annotation must stay (controller did not process it); operator handles out-of-band")
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Unsupported schemaVersion on the migration request is rejected
	// without ever entering the migration state machine. UnsupportedSchemaVersion
	// event fires; the annotation gets cleared so the requester stops
	// retrying; a Failed audit row blocks re-attempts.
	ginkgo.It("unsupported schemaVersion is rejected with UnsupportedSchemaVersion event", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("schemaver-llama"),
			WithCSRName("schemaver-runtime"),
			WithISVCName("schemaver-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing a migration request with schemaVersion=v99 (unsupported)")
		reqUUID := uuid.NewString()
		payload, err := json.Marshal(map[string]any{
			"schemaVersion": "v99",
			"component":     "engine",
			"instance":      0,
			"from_node":     "sim-node-a",
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			if fresh.Annotations == nil {
				fresh.Annotations = map[string]string{}
			}
			fresh.Annotations[migrationAnnoPrefix+reqUUID] = string(payload)
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting UnsupportedSchemaVersion event + Failed audit row + no surge")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "UnsupportedSchemaVersion" && e.InvolvedObject.Name == fx.ISVC.Name {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected UnsupportedSchemaVersion event; got %d events", len(events.Items))

			ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
			ledgerHit := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Failed" {
					ledgerHit = true
					break
				}
			}
			g.Expect(ledgerHit).To(BeTrue(),
				"expected Failed audit row for the unsupported schema request")

			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"unsupported schema must NOT create a surge")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
