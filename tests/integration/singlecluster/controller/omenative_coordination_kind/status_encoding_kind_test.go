package omenative_coordination_kind

import (
	"encoding/json"
	"sort"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/obsmetrics"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Status encoding — a stored ColumnarV2 object under the DenseV1 target.
//
// The suite's manager runs the DenseV1 write target with the seeded decode
// bound. An engine InferenceReplica whose per-Instance status is rewritten as
// ColumnarV2 through the API must be decoded, rewritten as DenseV1 in one
// status write, and otherwise left alone: the same logical rows, the same
// Pods, the same rollout state, one InstanceStatusConverted event, one
// conversion counted, and no representation oscillation afterwards.
var _ = ginkgo.Describe("status encoding — stored ColumnarV2 converts back under the DenseV1 target", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "status-enc-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("converts a directly stored ColumnarV2 engine status back to DenseV1 with no Pod churn", func() {
		const isvcName = "status-enc"
		const replicas = 2
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName), WithoutDecoder(),
			WithEngineReplicas(replicas), WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Ready: every engine Instance Ready, stored as DenseV1, and the ISVC
		// projection settled on a current revision.
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.InstanceStatusEncoding).To(BeNil())
			g.Expect(ir.Status.InstanceStatuses).To(HaveLen(replicas))
			for _, row := range ir.Status.InstanceStatuses {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			g.Expect(ir.Status.ReadyReplicas).To(Equal(int32(replicas)))
			engCS := freshISVC(g, ns.Name, isvcName).Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ReadyReplicas).To(Equal(int32(replicas)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		before := freshInferenceReplica(Default, ns.Name, isvcName, v1beta1.EngineComponent)
		beforeRows := durableRows(before.Status.InstanceStatuses)
		beforeRevision := freshISVC(Default, ns.Name, isvcName).Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		beforePods := podUIDs(listOMENativePods(Default, ns.Name, isvcName))
		Expect(beforePods).To(HaveLen(replicas))
		conversionsBefore := irStatusConversions()

		// Rewrite the stored representation as ColumnarV2 with the same rows,
		// retrying from a fresh read if the manager's own write lands first.
		var columnBytes int
		Eventually(func(g Gomega) {
			live := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(live.Status.InstanceStatusEncoding).To(BeNil(), "the object must still be DenseV1 when it is rewritten")
			rows := make([]v1beta1.OMENativeInstanceStatus, len(live.Status.InstanceStatuses))
			for i := range live.Status.InstanceStatuses {
				rows[i] = *live.Status.InstanceStatuses[i].DeepCopy()
			}
			irstatus.ClearPodDerivedObservations(rows)
			columns, err := irstatus.EncodeColumns(rows, util.SeededMaxDecodedInstances)
			g.Expect(err).NotTo(HaveOccurred())
			encoding := v1beta1.InstanceStatusEncodingColumnarV2
			live.Status.InstanceStatusEncoding = &encoding
			live.Status.InstanceStatusColumns = columns
			live.Status.InstanceStatuses = nil
			g.Expect(k8sClient.Status().Update(suiteCtx, live)).To(Succeed())
			encoded, err := json.Marshal(columns)
			g.Expect(err).NotTo(HaveOccurred())
			columnBytes = len(encoded)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		ginkgo.GinkgoWriter.Printf("representation write: ColumnarV2 (%d bytes of columns) onto %s/%s\n", columnBytes, ns.Name, before.Name)

		// The manager converts the object back within the medium budget and
		// counts exactly one conversion.
		var after *v1beta1.InferenceReplica
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.InstanceStatusEncoding).To(BeNil())
			g.Expect(ir.Status.InstanceStatusColumns).To(BeNil())
			g.Expect(ir.Status.InstanceStatuses).To(HaveLen(replicas))
			g.Expect(irStatusConversions() - conversionsBefore).To(Equal(1.0))
			after = ir
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		ginkgo.GinkgoWriter.Printf("representation read: DenseV1 with %d rows after the manager's conversion\n", len(after.Status.InstanceStatuses))
		Expect(after.UID).To(Equal(before.UID), "conversion must not replace the object")
		Expect(durableRows(after.Status.InstanceStatuses)).To(Equal(beforeRows), "conversion must preserve the logical rows")
		Expect(after.Status.CurrentRevision).To(Equal(before.Status.CurrentRevision))
		Expect(after.Status.UpdateRevision).To(Equal(before.Status.UpdateRevision))

		// One Normal InstanceStatusConverted event on the InferenceReplica.
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			var matched []corev1.Event
			for _, event := range events.Items {
				if event.InvolvedObject.UID == after.UID && event.Reason == inferencereplica.EventReasonInstanceStatusConverted {
					matched = append(matched, event)
				}
			}
			g.Expect(matched).To(HaveLen(1))
			g.Expect(matched[0].Type).To(Equal(corev1.EventTypeNormal))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Afterwards: the same Pods, no second conversion, and the rollout
		// state exactly as before the rewrite.
		Consistently(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(podUIDs(pods)).To(Equal(beforePods), "conversion must cause no Pod churn")
			for _, pod := range pods {
				g.Expect(pod.DeletionTimestamp).To(BeNil())
			}
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.InstanceStatusEncoding).To(BeNil())
			g.Expect(durableRows(ir.Status.InstanceStatuses)).To(Equal(beforeRows))
			g.Expect(irStatusConversions()-conversionsBefore).To(Equal(1.0), "a converged object must not convert again")
			engCS := freshISVC(g, ns.Name, isvcName).Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).To(Equal(beforeRevision))
			g.Expect(engCS.Lifecycle.ReadyReplicas).To(Equal(int32(replicas)))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// durableRow is the per-Instance state a representation-only write must carry
// unchanged. It leaves out readySince and conditions, which a later
// publication may refresh on its own schedule.
type durableRow struct {
	Index                                        int32
	Incarnation                                  int64
	Phase                                        v1beta1.OMENativeInstancePhase
	RunningRevision, TargetRevision              string
	PodCount, ServingPodCount, AvailablePodCount int32
	Admitted                                     bool
	ActiveOrdinal                                int32
	HasOperation, HasFailure                     bool
}

func durableRows(rows []v1beta1.OMENativeInstanceStatus) []durableRow {
	out := make([]durableRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, durableRow{
			Index: row.Index, Incarnation: row.Incarnation, Phase: row.Phase,
			RunningRevision: row.RunningRevision, TargetRevision: row.TargetRevision,
			PodCount: row.PodCount, ServingPodCount: row.ServingPodCount, AvailablePodCount: row.AvailablePodCount,
			Admitted: row.Admitted, ActiveOrdinal: row.ActiveOrdinal,
			HasOperation: row.Operation != nil, HasFailure: row.LastFailure != nil,
		})
	}
	return out
}

func podUIDs(pods []corev1.Pod) []types.UID {
	uids := make([]types.UID, 0, len(pods))
	for _, pod := range pods {
		uids = append(uids, pod.UID)
	}
	sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
	return uids
}

// irStatusConversions reads the in-process manager's ColumnarV2-to-DenseV1
// conversion counter; zero when the series does not exist yet.
func irStatusConversions() float64 {
	families, err := ctrlmetrics.Registry.Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, family := range families {
		if family.GetName() != "ome_omenative_ir_status_conversions_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["from"] == obsmetrics.IRStatusEncodingColumnarV2 && labels["to"] == obsmetrics.IRStatusEncodingDenseV1 && metric.Counter != nil {
				return metric.Counter.GetValue()
			}
		}
	}
	return 0
}
