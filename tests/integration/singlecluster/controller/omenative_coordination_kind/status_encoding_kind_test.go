package omenative_coordination_kind

import (
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

// Status encoding — a representation rewritten through the API under the
// seeded write target.
//
// The suite's manager runs the seeded target with the seeded decode bound. An
// engine InferenceReplica whose per-Instance status is rewritten through the
// API in the other representation, with the same rows, must have no lifecycle
// effect: the same logical rows, the same Pods, the same rollout state, and no
// oscillation afterwards. A rewrite away from the representation the manager
// selected is converted back in one status write, with one
// InstanceStatusConverted event and one conversion counted. Under the
// ColumnarV2 target an object rewritten as ColumnarV2 where the manager had
// selected DenseV1 is already in the target representation and is left as
// stored, with nothing converted.
var _ = ginkgo.Describe("status encoding — a rewritten representation under the seeded target", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "status-enc-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("returns a directly rewritten engine status to the selected representation with no Pod churn", func() {
		const isvcName = "status-enc"
		const replicas = 2
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName), WithoutDecoder(),
			WithEngineReplicas(replicas), WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Ready: every engine Instance Ready and the ISVC projection settled
		// on a current revision.
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			rows, err := util.DecodedInstanceStatuses(ir)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).To(HaveLen(replicas))
			for _, row := range rows {
				g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			g.Expect(ir.Status.ReadyReplicas).To(Equal(int32(replicas)))
			engCS := freshISVC(g, ns.Name, isvcName).Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ReadyReplicas).To(Equal(int32(replicas)))
			g.Expect(engCS.Lifecycle.CurrentRevision).NotTo(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		before := freshInferenceReplica(Default, ns.Name, isvcName, v1beta1.EngineComponent)
		beforeRows := durableRowsOf(Default, before)
		selected, err := irstatus.ObservedEncoding(&before.Status)
		Expect(err).NotTo(HaveOccurred())
		beforeRevision := freshISVC(Default, ns.Name, isvcName).Status.Components[v1beta1.EngineComponent].Lifecycle.CurrentRevision
		beforePods := podUIDs(listOMENativePods(Default, ns.Name, isvcName))
		Expect(beforePods).To(HaveLen(replicas))

		// The rewrite uses the other representation. The manager converts it
		// back unless the rewrite already is the target representation.
		rewritten := irstatus.EncodingColumnarV2
		if selected == irstatus.EncodingColumnarV2 {
			rewritten = irstatus.EncodingDenseV1
		}
		convertsBack := rewritten != util.SeededInstanceStatusEncoding
		conversionsBefore := irStatusConversions(rewritten, selected)
		ginkgo.GinkgoWriter.Printf("target %s: manager selected %s, rewriting as %s, conversion expected: %t\n",
			util.SeededInstanceStatusEncoding, selected, rewritten, convertsBack)

		// Rewrite the stored representation with the same rows, retrying from
		// a fresh read if the manager's own write lands first.
		Eventually(func(g Gomega) {
			live := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			liveEncoding, err := irstatus.ObservedEncoding(&live.Status)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(liveEncoding).To(Equal(selected), "the object must still be in the selected representation when it is rewritten")
			rows, err := util.DecodedInstanceStatuses(live)
			g.Expect(err).NotTo(HaveOccurred())
			irstatus.ClearPodDerivedObservations(rows)
			if rewritten == irstatus.EncodingColumnarV2 {
				columns, err := irstatus.EncodeColumns(rows, util.SeededMaxDecodedInstances)
				g.Expect(err).NotTo(HaveOccurred())
				encoding := v1beta1.InstanceStatusEncodingColumnarV2
				live.Status.InstanceStatusEncoding = &encoding
				live.Status.InstanceStatusColumns = columns
				live.Status.InstanceStatuses = nil
			} else {
				util.InstallDenseInstanceStatuses(&live.Status, rows)
			}
			g.Expect(k8sClient.Status().Update(suiteCtx, live)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		expectedConversions := 0.0
		if convertsBack {
			expectedConversions = 1.0
			// The manager converts the object back within the medium budget
			// and counts exactly one conversion.
			Eventually(func(g Gomega) {
				ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
				encoding, err := irstatus.ObservedEncoding(&ir.Status)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(encoding).To(Equal(selected))
				g.Expect(irStatusConversions(rewritten, selected) - conversionsBefore).To(Equal(1.0))
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

			// One Normal InstanceStatusConverted event on the InferenceReplica.
			Eventually(func(g Gomega) {
				events := conversionEventsFor(g, ns.Name, before.UID)
				g.Expect(events).To(HaveLen(1))
				g.Expect(events[0].Type).To(Equal(corev1.EventTypeNormal))
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}

		after := freshInferenceReplica(Default, ns.Name, isvcName, v1beta1.EngineComponent)
		Expect(after.UID).To(Equal(before.UID), "the rewrite and its handling must not replace the object")
		Expect(after.Status.CurrentRevision).To(Equal(before.Status.CurrentRevision))
		Expect(after.Status.UpdateRevision).To(Equal(before.Status.UpdateRevision))

		// Afterwards: the same Pods, the same rows, no further conversion, and
		// the rollout state exactly as before the rewrite.
		Consistently(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(podUIDs(pods)).To(Equal(beforePods), "a representation rewrite must cause no Pod churn")
			for _, pod := range pods {
				g.Expect(pod.DeletionTimestamp).To(BeNil())
			}
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			if convertsBack {
				encoding, err := irstatus.ObservedEncoding(&ir.Status)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(encoding).To(Equal(selected), "a converged object must not change representation again")
			}
			g.Expect(durableRowsOf(g, ir)).To(Equal(beforeRows), "the logical rows must survive the rewrite unchanged")
			g.Expect(irStatusConversions(rewritten, selected)-conversionsBefore).To(Equal(expectedConversions), "no conversion beyond the expected one")
			g.Expect(conversionEventsFor(g, ns.Name, before.UID)).To(HaveLen(int(expectedConversions)))
			engCS := freshISVC(g, ns.Name, isvcName).Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.CurrentRevision).To(Equal(beforeRevision))
			g.Expect(engCS.Lifecycle.ReadyReplicas).To(Equal(int32(replicas)))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// durableRow is the per-Instance state a representation-only write must carry
// unchanged. It leaves out readySince and conditions, which a later
// publication may refresh on its own schedule, and the pod-derived counts,
// which every persisted representation clears.
type durableRow struct {
	Index                           int32
	Incarnation                     int64
	Phase                           v1beta1.OMENativeInstancePhase
	RunningRevision, TargetRevision string
	Admitted                        bool
	ActiveOrdinal                   int32
	HasOperation, HasFailure        bool
}

func durableRows(rows []v1beta1.OMENativeInstanceStatus) []durableRow {
	out := make([]durableRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, durableRow{
			Index: row.Index, Incarnation: row.Incarnation, Phase: row.Phase,
			RunningRevision: row.RunningRevision, TargetRevision: row.TargetRevision,
			Admitted: row.Admitted, ActiveOrdinal: row.ActiveOrdinal,
			HasOperation: row.Operation != nil, HasFailure: row.LastFailure != nil,
		})
	}
	return out
}

// durableRowsOf decodes ir, whichever representation it is stored in, and
// reduces its rows to the durable fields.
func durableRowsOf(g Gomega, ir *v1beta1.InferenceReplica) []durableRow {
	rows, err := util.DecodedInstanceStatuses(ir)
	g.Expect(err).NotTo(HaveOccurred())
	return durableRows(rows)
}

func podUIDs(pods []corev1.Pod) []types.UID {
	uids := make([]types.UID, 0, len(pods))
	for _, pod := range pods {
		uids = append(uids, pod.UID)
	}
	sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
	return uids
}

// conversionEventsFor lists the InstanceStatusConverted events recorded on
// the InferenceReplica with the given UID.
func conversionEventsFor(g Gomega, namespace string, uid types.UID) []corev1.Event {
	events := &corev1.EventList{}
	g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(namespace))).To(Succeed())
	var matched []corev1.Event
	for _, event := range events.Items {
		if event.InvolvedObject.UID == uid && event.Reason == inferencereplica.EventReasonInstanceStatusConverted {
			matched = append(matched, event)
		}
	}
	return matched
}

// irStatusConversions reads the in-process manager's conversion counter for
// the from-to representation pair; zero when the series does not exist yet.
func irStatusConversions(from, to irstatus.Encoding) float64 {
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
			if labels["from"] == conversionLabel(from) && labels["to"] == conversionLabel(to) && metric.Counter != nil {
				return metric.Counter.GetValue()
			}
		}
	}
	return 0
}

func conversionLabel(encoding irstatus.Encoding) string {
	if encoding == irstatus.EncodingColumnarV2 {
		return obsmetrics.IRStatusEncodingColumnarV2
	}
	return obsmetrics.IRStatusEncodingDenseV1
}
