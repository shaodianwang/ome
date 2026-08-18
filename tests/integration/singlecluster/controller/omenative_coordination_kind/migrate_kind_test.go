package omenative_coordination_kind

import (
	"encoding/json"
	"fmt"
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
// migration state machine orchestrates from an ISVC annotation
// (a transient mailbox: consumed into an IR status.migrations entry at
// accept) through surge allocation, source drain, and surge promotion.
// The status entry is the authoritative record — born Accepted, it
// walks Accepted -> SurgePending -> SurgeReady -> Draining ->
// Completed | Failed and persists as visible history after the
// terminal phase; the audit ConfigMap mirrors it for history only.
//
// The suite cluster runs 1 control-plane + 2 workers, so the
// off-source surge pod CAN schedule onto another worker — that is why
// these migration specs pass at all (a single-node cluster left the
// surge pod Pending forever). They pin the state-machine contracts
// (status-entry lifecycle, surge index allocation, annotation
// lifecycle, event emission, MigrationMode, schema validation) plus
// the node move itself; the deadline-expiry spec inverts the topology
// by cordoning every off-source worker to prove a blocked surge
// cannot wedge the source.
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
			insts := instanceStatuses(g, ns.Name, "mig-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].RunningRevision).NotTo(BeEmpty())

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

		ginkgo.By("expecting the request to be consumed into a status.migrations entry")
		// Consume-on-accept: the entry is born on the IR and the mailbox
		// annotation is deleted in the same accept pass. Phase is not
		// asserted here — the migration may already have advanced past
		// Accepted by the time this observes it.
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, "mig-isvc", v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "accept must create the status.migrations entry")
			g.Expect(entry.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
			g.Expect(entry.SourceInstance).To(Equal(int32(0)))
			g.Expect(entry.FromNode).To(Equal(sourceNode))
			fresh := freshISVC(g, ns.Name, "mig-isvc")
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"the mailbox annotation must be consumed at accept")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the source InstanceStatus to disappear and surge to take over")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mig-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "mig-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"after migration, exactly one InstanceStatus survives (the surge)")
			s := insts[0]
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
		var surgeIdx int32
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mig-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "mig-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			surgeIdx = insts[0].Index
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

		ginkgo.By("expecting the status entry to reach Completed with the promoted surge index")
		// The terminal entry persists as visible history after
		// completion (trimmed only once it ages out of the capacity rate
		// window — trim timing is not asserted here).
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, "mig-isvc", v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "the Completed entry must persist as history")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseCompleted))
			g.Expect(entry.CompletedAt).NotTo(BeNil(), "Completed entry must carry CompletedAt")
			g.Expect(entry.SurgeInstance).NotTo(BeNil(), "Completed entry must record the surge index")
			g.Expect(*entry.SurgeInstance).To(Equal(surgeIdx),
				"entry.SurgeInstance must match the promoted instance index")
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
			insts := instanceStatuses(g, ns.Name, "mismatch-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
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

		ginkgo.By("expecting the status entry to be terminal Failed naming the mismatch")
		// The stale FromNode passes accept-time validation (the adapter
		// cannot observe pod placement), so an entry IS born; the
		// executor's from-node check then fails it terminally before any
		// surge is allocated.
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, "mismatch-isvc", v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "the accepted entry must persist as a Failed record")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseFailed))
			g.Expect(entry.CompletedAt).NotTo(BeNil())
			g.Expect(entry.Message).To(ContainSubstring(wrongNode),
				"the terminal message must name the stale FromNode")
			g.Expect(entry.SurgeInstance).To(BeNil(),
				"a from-node rejection must never allocate a surge index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting no surge instance is ever created")
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "mismatch-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "mismatch-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"rejected migration must not create a surge; got %d InstanceStatuses",
				len(insts))
			g.Expect(insts[0].Index).To(Equal(int32(0)),
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
		var sourceRevision string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "affinity-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "affinity-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].RunningRevision).NotTo(BeEmpty())
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "affinity-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
			sourceRevision = insts[0].RunningRevision
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

		// Wait for the affinity edit to produce a new Ready running revision.
		// Merely waiting for Ready can observe the old revision before reconciliation.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "affinity-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "affinity-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].RunningRevision).NotTo(Equal(sourceRevision))
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

		ginkgo.By("expecting the status entry to be terminal Failed naming the hostname pin")
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, "affinity-isvc", v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "the accepted entry must persist as a Failed record")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseFailed))
			g.Expect(entry.CompletedAt).NotTo(BeNil())
			g.Expect(entry.Message).To(ContainSubstring("hostname"),
				"the terminal message must name the hostname pin")
			g.Expect(entry.SurgeInstance).To(BeNil(),
				"an affinity-conflict rejection must never allocate a surge index")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting source stays Phase=Ready with no in-flight Operation")
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "affinity-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "affinity-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"rejection must not stamp a surge InstanceStatus")
			src := insts[0]
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
			insts := instanceStatuses(g, ns.Name, "dedup-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
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
			_ = freshISVC(g, ns.Name, "dedup-isvc")
			insts := instanceStatuses(g, ns.Name, "dedup-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			surgeIdx = insts[0].Index
			g.Expect(surgeIdx).NotTo(Equal(int32(0)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("re-delivering the SAME UUID via annotation")
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, sourceNode, 0)

		ginkgo.By("expecting the re-delivered annotation to be consumed against the existing entry")
		// Dedup is keyed on the persisted status entry: a re-delivered
		// known UUID only deletes the annotation.
		Eventually(func(g Gomega) {
			fresh := freshISVC(g, ns.Name, "dedup-isvc")
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"re-delivered annotation must be consumed without re-running the migration")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting no second surge appears (dedup via the terminal status entry)")
		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "dedup-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "dedup-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"re-delivered Completed UUID must NOT trigger a second migration; got %d InstanceStatuses",
				len(insts))
			g.Expect(insts[0].Index).To(Equal(surgeIdx),
				"surviving instance index must match the first migration's surge index")

			// Exactly one entry for the UUID, still terminal Completed.
			entries := migrationEntries(g, ns.Name, "dedup-isvc", v1beta1.EngineComponent)
			count := 0
			for i := range entries {
				if entries[i].RequestUUID == reqUUID {
					count++
					g.Expect(entries[i].Phase).To(Equal(v1beta1.MigrationPhaseCompleted),
						"the entry must stay terminal on re-delivery")
				}
			}
			g.Expect(count).To(Equal(1),
				"re-delivery must not duplicate the status entry; got %d for uuid=%s", count, reqUUID)
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
				insts := instanceStatuses(g, ns.Name, "seq-isvc", v1beta1.EngineComponent)
				g.Expect(insts).To(HaveLen(1))
				g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				srcIdx = insts[0].Index
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: ns.Name, Name: fmt.Sprintf("seq-isvc-engine-%d-default-0", srcIdx),
				}, pod)).To(Succeed())
				g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
				node = pod.Spec.NodeName
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
				insts := instanceStatuses(g, ns.Name, "seq-isvc", v1beta1.EngineComponent)
				g.Expect(insts).To(HaveLen(1))
				g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(insts[0].Index).NotTo(Equal(srcIdx))
				newIdx = insts[0].Index

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

				entry := findMigrationEntry(migrationEntries(g, ns.Name, "seq-isvc", v1beta1.EngineComponent), reqUUID)
				g.Expect(entry).NotTo(BeNil(),
					"[%s] the Completed status entry must persist", label)
				g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseCompleted))
				g.Expect(entry.SurgeInstance).NotTo(BeNil())
				g.Expect(*entry.SurgeInstance).To(Equal(newIdx),
					"[%s] entry.SurgeInstance must match the promoted index", label)
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
			insts := instanceStatuses(g, ns.Name, "audit-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
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

	// MigrationMode=Never rejects at ACCEPT: the adapter still consumes
	// a valid annotation (the mailbox is empty at rest under every
	// mode), but the entry is born TERMINAL Failed naming the policy —
	// an immediate, honest answer instead of parking Accepted until the
	// Deadline expires it with a misleading surge message. No surge
	// index is allocated, no surge pod appears, and the source stays
	// put.
	ginkgo.It("MigrationMode=Never rejects the request at accept", func() {
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

		ginkgo.By("expecting the request to be consumed into a born-terminal Failed entry")
		// Also wait for the source Instance to settle Ready so the
		// Consistently below can pin "no Operation" without racing the
		// initial Create.
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, "nomig-isvc", v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "the mailbox is consumed under every mode")
			g.Expect(entry.Trigger).To(Equal(v1beta1.MigrationTriggerManual))
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseFailed),
				"Never must reject at accept — a born-terminal answer, not a parked Accepted entry")
			g.Expect(entry.Message).To(ContainSubstring("MigrationPolicy Mode=Never"),
				"the terminal answer must name the policy that rejected the request")
			g.Expect(entry.CompletedAt).NotTo(BeNil())
			fresh := freshISVC(g, ns.Name, "nomig-isvc")
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"annotation must be consumed at accept even under Never")
			insts := instanceStatuses(g, ns.Name, "nomig-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the rejection is stable: no surge, no source op, entry stays terminal")
		Consistently(func(g Gomega) {
			fresh := freshISVC(g, ns.Name, "nomig-isvc")
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "nomig-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"Never policy must NOT create a surge")
			g.Expect(insts[0].Index).To(Equal(int32(0)),
				"only the source index should remain")
			g.Expect(insts[0].Operation).To(BeNil(),
				"Never policy must NOT stamp a Migrate operation on the source")

			entry := findMigrationEntry(migrationEntries(g, ns.Name, "nomig-isvc", v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil())
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseFailed),
				"the born-terminal rejection must stay terminal")
			g.Expect(entry.SurgeInstance).To(BeNil(),
				"Never policy must NOT allocate a surge index")
		}, "3s", kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the audit ledger carries the terminal Failed row")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, "nomig-isvc")
			found := false
			for _, e := range ledger.Entries {
				if e.RequestUUID == reqUUID && e.Phase == "Failed" {
					found = true
					g.Expect(e.Outcome).To(ContainSubstring("MigrationPolicy Mode=Never"))
					break
				}
			}
			g.Expect(found).To(BeTrue(),
				"audit ledger must record a Failed row for uuid=%s", reqUUID)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Unsupported schemaVersion on the migration request is rejected
	// without entering the state machine. UnsupportedSchemaVersion
	// event fires; annotation cleared; Failed audit row blocks
	// re-attempts.
	//
	// The KIND manager runs WITHOUT a webhook server (see
	// kindutil.StartManager), so this invalid annotation is never
	// rejected at admission — it reaches the adapter's defense-in-depth
	// validation, which is the path under test here: annotation
	// consumed, Warning event, NO status.migrations entry. (A cluster
	// with the ISVC validating webhook installed rejects the same
	// request at kubectl-time instead.)
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
			insts := instanceStatuses(g, ns.Name, "schemaver-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
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

			fresh := freshISVC(g, ns.Name, "schemaver-isvc")
			_, present := fresh.Annotations[migrationAnnoPrefix+reqUUID]
			g.Expect(present).To(BeFalse(),
				"the invalid annotation must be consumed at rejection")
			g.Expect(findMigrationEntry(migrationEntries(g, ns.Name, "schemaver-isvc", v1beta1.EngineComponent), reqUUID)).To(BeNil(),
				"an adapter-rejected request must NOT create a status.migrations entry")
			insts := instanceStatuses(g, ns.Name, "schemaver-isvc", v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"unsupported schema must NOT create a surge")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Deadline expiry — a migration whose surge can never schedule
	// (every off-source worker cordoned) must not wedge anything:
	//
	//   - the source keeps serving throughout on the SAME pod (never
	//     restarted, Ready the whole time);
	//   - at the entry's Deadline (accept + the Component's effective
	//     InstanceReadyTimeout) the entry fails terminally with a
	//     message naming the blocker, the Pending surge is torn down,
	//     and the source is restored Phase=Ready from observation;
	//   - the capacity slot frees structurally, so a second request
	//     filed after uncordoning is accepted and completes.
	ginkgo.It("deadline expiry fails the entry, source keeps serving, and a later request completes", func() {
		const isvcName = "expire-isvc"
		const podName = isvcName + "-engine-0-default-0"
		// The entry's accept->Deadline window: the Component's effective
		// InstanceReadyTimeout. Short enough to fit the suite budget,
		// long enough to observe the pre-deadline holding pattern.
		const migrationTimeout = 45 * time.Second

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			InstanceReadyTimeout: &metav1.Duration{Duration: migrationTimeout},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the source pod Ready and discovering its node + UID")
		var sourceNode string
		var sourceUID types.UID
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: podName,
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			sourceNode = pod.Spec.NodeName
			sourceUID = pod.UID
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("cordoning every other worker so the surge cannot schedule")
		// The surge carries a NotIn[sourceNode] anti-affinity, so with
		// every other worker cordoned it has nowhere to land.
		// cordonNode registers an uncordon cleanup for each node.
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

		ginkgo.By("expecting the entry to reach SurgePending with an unschedulable surge pod")
		var surgeIdx int32
		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), reqUUID)
			g.Expect(entry).NotTo(BeNil(), "accept must create the status.migrations entry")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseSurgePending))
			g.Expect(entry.SurgeInstance).NotTo(BeNil())
			surgeIdx = *entry.SurgeInstance
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name,
				Name:      fmt.Sprintf("%s-engine-%d-default-0", isvcName, surgeIdx),
			}, pod)).To(Succeed())
			g.Expect(pod.Status.Phase).To(Equal(corev1.PodPending))
			g.Expect(pod.Spec.NodeName).To(BeEmpty(),
				"surge must be unschedulable: NotIn[%s] plus all other workers cordoned", sourceNode)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the source keeps serving while the surge waits")
		// Window strictly inside accept->Deadline: the source must stay
		// the SAME pod, Ready the whole time — a blocked migration must
		// never degrade the workload it was asked to move.
		Consistently(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: podName,
			}, pod)).To(Succeed())
			g.Expect(pod.UID).To(Equal(sourceUID),
				"source pod must never be restarted while its migration is blocked")
			g.Expect(podIsReady(pod)).To(BeTrue(),
				"source pod must stay Ready while its migration is blocked")
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

		ginkgo.By("expecting the surge torn down and the source restored Ready on the SAME pod")
		Eventually(func(g Gomega) {
			surgePod := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name,
				Name:      fmt.Sprintf("%s-engine-%d-default-0", isvcName, surgeIdx),
			}, surgePod)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"expired surge pod must be deleted (err=%v)", err)

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1),
				"only the source InstanceStatus survives the expiry")
			g.Expect(insts[0].Index).To(Equal(int32(0)))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"source must be restored Ready from observation")
			g.Expect(insts[0].Operation).To(BeNil(),
				"the Migrate pin must be cleared from the source")

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: podName,
			}, pod)).To(Succeed())
			g.Expect(pod.UID).To(Equal(sourceUID),
				"expiry must restore the source without recreating its pod")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("uncordoning and filing a second request that must complete")
		for _, w := range cordoned {
			setNodeUnschedulable(w, false)
		}
		req2 := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), req2, sourceNode, 0)

		Eventually(func(g Gomega) {
			entry := findMigrationEntry(migrationEntries(g, ns.Name, isvcName, v1beta1.EngineComponent), req2)
			g.Expect(entry).NotTo(BeNil(), "the second request must be accepted (slot freed by the expiry)")
			g.Expect(entry.Phase).To(Equal(v1beta1.MigrationPhaseCompleted),
				"the second migration must complete; got phase=%s message=%q", entry.Phase, entry.Message)
			g.Expect(entry.SurgeInstance).NotTo(BeNil())

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Index).To(Equal(*entry.SurgeInstance))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name,
				Name:      fmt.Sprintf("%s-engine-%d-default-0", isvcName, insts[0].Index),
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			g.Expect(pod.Spec.NodeName).NotTo(Equal(sourceNode),
				"the second migration must move the instance OFF %s", sourceNode)
			g.Expect(pod.UID).NotTo(Equal(sourceUID))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
