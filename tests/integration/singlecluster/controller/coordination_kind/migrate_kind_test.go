package coordination_kind

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
// ConfigMap. Declared locally for test independence from internal types.
type auditEntryShape struct {
	RequestUUID    string `json:"requestUUID"`
	Component      string `json:"component"`
	SourceInstance int32  `json:"sourceInstance"`
	SurgeInstance  int32  `json:"surgeInstance,omitempty"`
	Phase          string `json:"phase"`
	Reason         string `json:"reason,omitempty"`
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

func writeMigrationRequestWithComponent(g Gomega, isvcKey client.ObjectKey, reqUUID, fromNode string, instance int32, component string) {
	payload, err := json.Marshal(map[string]any{
		"schemaVersion": migrationSchemaV1,
		"component":     component,
		"instance":      instance,
		"from_node":     fromNode,
		"reason":        "integration-test",
	})
	g.ExpectWithOffset(1, err).NotTo(HaveOccurred())

	Eventually(func(gg Gomega) {
		fresh := &v1beta1.InferenceService{}
		gg.Expect(k8sClient.Get(suiteCtx, isvcKey, fresh)).To(Succeed())
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[migrationAnnoPrefix+reqUUID] = string(payload)
		gg.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// OMENative Migration specs ported from envtest migrate_test.go. The
// migration state machine orchestrates from an ISVC annotation through
// audit-ledger dedup, surge allocation, source drain, and surge
// promotion.
//
// The suite runs a 2-node cluster (1 control-plane + 1 worker), so the
// off-source surge pod CAN schedule onto the other node — that is why
// these migration specs pass at all (a single-node cluster left the
// surge pod Pending forever). They pin the state-machine contracts
// (audit ledger, surge index allocation, annotation lifecycle, event
// emission, MigrationMode, schema validation); a direct "surge landed
// on a different node than the source" assertion is now feasible on the
// 2-node cluster and is a candidate follow-up.
var _ = ginkgo.Describe("OMENative Migration (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-mig-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Happy-path migration: annotation triggers a full migration, surge
	// takes over (gets a new sparse index), audit ledger records
	// Completed, annotation is cleared, MigrationCompleted event fires.
	ginkgo.It("annotation triggers migration: surge takes over (new index), source disappears", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mig-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source pod to be Ready and scheduled")
		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mig-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].RunningRevision).NotTo(BeEmpty())

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mig-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(),
				"source pod must be scheduled before requesting migration")
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By(fmt.Sprintf("writing migration request with from_node=%s", sourceNode))
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting the source InstanceStatus to disappear and surge to take over")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mig-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
				"after migration, exactly one InstanceStatus survives (the surge)")
			s := cs.Lifecycle.InstanceStatuses[0]
			g.Expect(s.Index).NotTo(Equal(int32(0)),
				"surge index must be != source (0); got %d", s.Index)
			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"surge must promote to Ready after source drain")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the surge pod to land on a DIFFERENT node than the source")
		// This is the actual point of migration: move the Instance off
		// its source node. The controller stamps NodeAffinity excluding
		// sourceNode, so on the 2-node suite cluster the surge must
		// schedule onto the other node. (A single-node cluster left the
		// surge Pending forever — the original reason these specs only
		// pinned the state machine and not the node move.)
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mig-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			surgeIdx := cs.Lifecycle.InstanceStatuses[0].Index
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name,
				Name:      fmt.Sprintf("mig-isvc-engine-%d-default-0", surgeIdx),
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(), "surge pod must be scheduled")
			g.Expect(pod.Spec.NodeName).NotTo(Equal(sourceNode),
				"migration must move the Instance OFF source node %s; surge landed on %s",
				sourceNode, pod.Spec.NodeName)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the audit ledger to record a Completed entry for this UUID")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, "mig-isvc")
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

		ginkgo.By("expecting the migration-request annotation to be cleared on completion")
		Eventually(func(g Gomega) {
			fresh := freshISVC(g, ns.Name, "mig-isvc")
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"annotation must be cleared after Completed so re-delivery dedups")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting both MigrationRequestAccepted + MigrationCompleted events")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			gotAccept, gotComplete := false, false
			for _, e := range events.Items {
				if e.InvolvedObject.Name != "mig-isvc" {
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// FromNode mismatch: requester's view of the source node is stale.
	// validateFromNode must reject with a Failed audit row and a
	// MigrationFromNodeMismatch event; NO surge is created.
	ginkgo.It("FromNode mismatch is rejected with Failed audit row and no surge", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("mismatch-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source pod Ready")
		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mismatch-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "mismatch-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("sending a migration request with the WRONG from_node")
		wrongNode := "not-" + sourceNode
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, wrongNode, 0)

		ginkgo.By("expecting a Failed audit row")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, "mismatch-isvc")
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting no surge instance is ever created")
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mismatch-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
				"rejected migration must not create a surge; got %d InstanceStatuses",
				len(cs.Lifecycle.InstanceStatuses))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"only the source InstanceStatus should exist")
		}, "3s", kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the rejected request's annotation to be cleared")
		Eventually(func(g Gomega) {
			fresh := freshISVC(g, ns.Name, "mismatch-isvc")
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"annotation must be cleared on rejection so the requester stops retrying")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting a MigrationFromNodeMismatch warning event")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "MigrationFromNodeMismatch" && e.InvolvedObject.Name == "mismatch-isvc" {
					matched = true
					g.Expect(e.Message).To(ContainSubstring(wrongNode),
						"event message should reference the wrong node")
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected MigrationFromNodeMismatch warning event for the stale FromNode")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// NodeAffinity conflict: source PodSpec hard-pins
	// kubernetes.io/hostname to the SAME node the request asks to move
	// off. Overlay's NotIn[FromNode] would render the surge permanently
	// unschedulable. Migrate must reject up-front WITHOUT stamping
	// source Phase=Migrating.
	ginkgo.It("NodeAffinity conflict is rejected without wedging source in Phase=Migrating", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("affinity-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source pod Ready and discovering its node")
		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "affinity-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].RunningRevision).NotTo(BeEmpty())
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "affinity-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("editing the ISVC to add an Affinity pinning to the discovered node")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "affinity-isvc")
			cur.Spec.Engine.PodSpec.Affinity = &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key:      "kubernetes.io/hostname",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{sourceNode},
							}},
						}},
					},
				},
			}
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Wait for the affinity edit to be reflected back into a Ready
		// running revision (the controller will Restart the pod with
		// the new affinity).
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "affinity-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).NotTo(BeEmpty())
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("sending a migration request whose FromNode matches the pinned host")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting a Failed audit row referencing the hostname pin")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, "affinity-isvc")
			matched := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Failed" {
					matched = true
					g.Expect(e.Outcome).To(ContainSubstring("hostname"),
						"Failed Outcome should mention the hostname pin")
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected a Failed audit row for uuid=%s; got %+v", reqUUID, ledger.Entries)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting source stays Phase=Ready with no in-flight Operation")
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "affinity-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
				"rejection must not stamp a surge InstanceStatus")
			src := cs.Lifecycle.InstanceStatuses[0]
			g.Expect(src.Index).To(Equal(int32(0)))
			g.Expect(src.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"source must remain Phase=Ready (got %q)", src.Phase)
			g.Expect(src.Operation).To(BeNil(),
				"source must have no in-flight Operation (got %+v)", src.Operation)
		}, "3s", kindPollInterval).Should(Succeed())

		ginkgo.By("expecting a MigrationNodeAffinityConflict warning event")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "MigrationNodeAffinityConflict" && e.InvolvedObject.Name == "affinity-isvc" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected MigrationNodeAffinityConflict warning event")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Re-delivery of the same UUID after Completed must dedup via the
	// audit ledger. The requester might retry if its first observation
	// of the annotation-clear didn't land; the controller must NOT
	// re-run the migration.
	ginkgo.It("re-delivery of a Completed UUID dedups via the ledger (no second migration)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("dedup-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "dedup-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "dedup-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("waiting for the first migration to complete")
		var surgeIdx int32
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "dedup-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			surgeIdx = cs.Lifecycle.InstanceStatuses[0].Index
			g.Expect(surgeIdx).NotTo(Equal(int32(0)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("re-delivering the SAME UUID via annotation")
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("asserting no second surge appears (dedup via ledger Completed entry)")
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "dedup-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
				"re-delivered Completed UUID must NOT trigger a second migration; got %d InstanceStatuses",
				len(cs.Lifecycle.InstanceStatuses))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Index).To(Equal(surgeIdx),
				"surviving instance index must match the first migration's surge index")
		}, "3s", kindPollInterval).Should(Succeed())
	})

	// Two distinct migrations on the same ISVC, in sequence. Audit
	// ledger keeps both UUIDs straight; the second can pick a fresh
	// surge index after the first lands.
	ginkgo.It("two sequential migrations on the same ISVC each complete", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("seq-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		runMigration := func(label string) (firstIdx, completedAt int32) {
			ginkgo.By(fmt.Sprintf("[%s] waiting for the current instance Ready", label))
			var srcIdx int32
			var node string
			Eventually(func(g Gomega) {
				c := freshISVC(g, ns.Name, "seq-isvc")
				cs := c.Status.Components[v1beta1.EngineComponent]
				g.Expect(cs.Lifecycle).NotTo(BeNil())
				g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				srcIdx = cs.Lifecycle.InstanceStatuses[0].Index
				g.Expect(cs.Lifecycle.InstanceStatuses[0].NodesOccupied).NotTo(BeEmpty())
				node = cs.Lifecycle.InstanceStatuses[0].NodesOccupied[0]
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

			reqUUID := uuid.NewString()
			ginkgo.By(fmt.Sprintf("[%s] writing migration request uuid=%s instance=%d", label, reqUUID, srcIdx))
			writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, node, srcIdx)

			ginkgo.By(fmt.Sprintf("[%s] waiting for migration to complete", label))
			var newIdx int32
			Eventually(func(g Gomega) {
				c := freshISVC(g, ns.Name, "seq-isvc")
				cs := c.Status.Components[v1beta1.EngineComponent]
				g.Expect(cs.Lifecycle).NotTo(BeNil())
				g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Index).NotTo(Equal(srcIdx))
				newIdx = cs.Lifecycle.InstanceStatuses[0].Index

				ledger := loadLedger(g, ns.Name, "seq-isvc")
				found := false
				for _, e := range ledger.Entries {
					if e.RequestUUID == reqUUID && e.Phase == "Completed" {
						found = true
						break
					}
				}
				g.Expect(found).To(BeTrue(),
					"[%s] expected Completed audit row for uuid=%s", label, reqUUID)
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
			return srcIdx, newIdx
		}

		first, second := runMigration("M1")
		Expect(first).NotTo(Equal(second), "first migration must move the instance to a different index")

		third, fourth := runMigration("M2")
		Expect(third).To(Equal(second), "second migration starts where the first ended")
		Expect(fourth).NotTo(Equal(third), "second migration must move the instance again")
	})

	// Audit ConfigMap exists with the deterministic name and carries a
	// fully-populated Completed entry. Pins the upsertEntry contract
	// (last-writer-wins per UUID, full field schema).
	ginkgo.It("audit ConfigMap exists with a Completed entry after migration", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("audit-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var sourceNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "audit-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "audit-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("waiting for the audit ConfigMap to carry the Completed entry with full fields")
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "audit-isvc" + auditCMSuffix,
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// MigrationMode=Never disables migration dispatch entirely — even
	// a valid annotation is ignored. No Migrate called, no audit ledger,
	// source stays put, annotation untouched (operator handles
	// out-of-band).
	ginkgo.It("MigrationMode=Never ignores migration request annotations", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("nomig-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		// Apply MigrationMode=Never on the engine component.
		neverMode := v1beta1.MigrationPolicyModeNever
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			MigrationPolicy: &v1beta1.MigrationPolicy{Mode: neverMode},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source pod Ready")
		var sourceNode string
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "nomig-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("writing a valid migration request")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("asserting no surge appears, no ledger entry, annotation stays put")
		Consistently(func(g Gomega) {
			fresh := freshISVC(g, ns.Name, "nomig-isvc")
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
				"Never policy must NOT create a surge")
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"only the source index should remain")

			ledger := loadLedger(g, ns.Name, "nomig-isvc")
			for _, e := range ledger.Entries {
				g.Expect(e.RequestUUID).NotTo(Equal(reqUUID),
					"Never policy must NOT write an audit ledger entry for the request")
			}

			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeTrue(),
				"annotation must stay (controller did not process it); operator handles out-of-band")
		}, "3s", kindPollInterval).Should(Succeed())
	})

	// Unsupported schemaVersion on the migration request is rejected
	// without entering the state machine. UnsupportedSchemaVersion
	// event fires; annotation cleared; Failed audit row blocks
	// re-attempts.
	ginkgo.It("unsupported schemaVersion is rejected with UnsupportedSchemaVersion event", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("schemaver-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "schemaver-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

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
			fresh := freshISVC(g, ns.Name, "schemaver-isvc")
			if fresh.Annotations == nil {
				fresh.Annotations = map[string]string{}
			}
			fresh.Annotations[migrationAnnoPrefix+reqUUID] = string(payload)
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting UnsupportedSchemaVersion event + Failed audit row + no surge")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "UnsupportedSchemaVersion" && e.InvolvedObject.Name == "schemaver-isvc" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected UnsupportedSchemaVersion event; got %d events", len(events.Items))

			ledger := loadLedger(g, ns.Name, "schemaver-isvc")
			ledgerHit := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Failed" {
					ledgerHit = true
					break
				}
			}
			g.Expect(ledgerHit).To(BeTrue(),
				"expected Failed audit row for the unsupported schema request")

			c := freshISVC(g, ns.Name, "schemaver-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1),
				"unsupported schema must NOT create a surge")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
