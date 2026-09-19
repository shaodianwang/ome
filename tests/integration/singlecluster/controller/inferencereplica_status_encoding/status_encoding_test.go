package inferencereplicastatusencoding

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/obsmetrics"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	// decodeLimit is the ColumnarV2 row bound every decoder in this suite is
	// configured with; it sits above the largest fixture.
	decodeLimit uint64 = 8192

	// etcdDefaultRequestLimitBytes is etcd's default --max-request-bytes.
	// envtest starts etcd with default flags, so it is the limit every write
	// in this suite is judged against.
	etcdDefaultRequestLimitBytes = 1572864

	// steadyStateGateBytes is the 256 KiB release gate on the complete
	// selected request and persisted object of the steady fixtures.
	steadyStateGateBytes = 256 * 1024
)

var conversionLabels = map[string]string{
	"from": obsmetrics.IRStatusEncodingColumnarV2,
	"to":   obsmetrics.IRStatusEncodingDenseV1,
}

var _ = ginkgo.Describe("ColumnarV2 representation on the real API server", func() {
	ginkgo.It("accepts a canonical ColumnarV2 status and rejects every union violation", func() {
		rows := irstatustest.FullFeatureRows()
		selection := candidates(rows)
		ir := createInferenceReplica("schema-union", int32(len(rows)))
		key := client.ObjectKeyFromObject(ir)

		request, err := writeStatus(key, selection.ColumnarV2.Status, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "the generated CRD must accept a canonical ColumnarV2 status")
		gomega.Expect(request.statusCode).To(gomega.Equal(http.StatusOK))

		stored := getInferenceReplica(key)
		gomega.Expect(stored.Status.InstanceStatusEncoding).NotTo(gomega.BeNil())
		gomega.Expect(*stored.Status.InstanceStatusEncoding).To(gomega.Equal(v1beta1.InstanceStatusEncodingColumnarV2))
		gomega.Expect(stored.Status.InstanceStatuses).To(gomega.BeEmpty())
		gomega.Expect(equality.Semantic.DeepEqual(stored.Status.InstanceStatusColumns, selection.ColumnarV2.Status.InstanceStatusColumns)).To(gomega.BeTrue(),
			"the API server must return the canonical columns unchanged")
		decoded, encoding := decodeStored(key)
		gomega.Expect(encoding).To(gomega.Equal(irstatus.EncodingColumnarV2))
		expectRowsEqual(decoded.Status.InstanceStatuses, selection.DenseV1.Status.InstanceStatuses)
		acceptedVersion := stored.ResourceVersion

		for _, violation := range unionViolations() {
			err := rawStatusUpdate(key, violation.mutate)
			gomega.Expect(err).To(gomega.HaveOccurred(), "%s must be rejected by the API server", violation.name)
			gomega.Expect(apierrors.IsInvalid(err)).To(gomega.BeTrue(), "%s must be a schema rejection, got %v", violation.name, err)
			gomega.Expect(err.Error()).To(gomega.ContainSubstring(violation.fragment), "%s rejected for an unexpected reason", violation.name)
			ginkgo.GinkgoWriter.Printf("union violation %q rejected: %v\n", violation.name, err)
		}
		after := getInferenceReplica(key)
		gomega.Expect(after.ResourceVersion).To(gomega.Equal(acceptedVersion), "rejected writes must leave the stored object untouched")
	})
})

var _ = ginkgo.Describe("transition gate against the real API server", func() {
	ginkgo.It("rewrites a stored ColumnarV2 object as DenseV1 through the production reconciler", func() {
		rows := irstatustest.RepresentativeRows(500)
		selection := candidates(rows)
		ir := createInferenceReplica("gate-convert", int32(len(rows)))
		key := client.ObjectKeyFromObject(ir)
		seed, err := writeStatus(key, selection.ColumnarV2.Status, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		recorder, shutdown := newEventRecorder()
		defer shutdown()
		reconciler := newReconciler(k8sClient, recorder)
		before := snapshotWriteMetrics()

		statusWrites.begin()
		result, err := reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		requests := statusWrites.finish()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result).To(gomega.Equal(ctrl.Result{Requeue: true}), "the conversion pass must requeue immediately")
		gomega.Expect(requests).To(gomega.HaveLen(1), "the conversion must be exactly one status write")
		gomega.Expect(requests[0].statusCode).To(gomega.Equal(http.StatusOK))
		ginkgo.GinkgoWriter.Printf("gate conversion at %d rows: seeded ColumnarV2 request %d bytes, conversion DenseV1 request %d bytes\n",
			len(rows), len(seed.body), len(requests[0].body))

		stored, encoding := decodeStored(key)
		gomega.Expect(encoding).To(gomega.Equal(irstatus.EncodingDenseV1))
		gomega.Expect(stored.Status.InstanceStatusEncoding).To(gomega.BeNil())
		gomega.Expect(stored.Status.InstanceStatusColumns).To(gomega.BeNil())
		expectRowsEqual(stored.Status.InstanceStatuses, selection.DenseV1.Status.InstanceStatuses)
		expectStatusOutsideRowsEqual(stored.Status, *selection.DenseV1.Status)

		delta := snapshotWriteMetrics().minus(before)
		gomega.Expect(delta).To(gomega.Equal(writeMetrics{attempts: 1, committed: 1, conversions: 1}),
			"one attempt, one commit, one conversion, no conflict")

		gomega.Eventually(func(g gomega.Gomega) {
			events := conversionEvents(g, ir.Name)
			g.Expect(events).To(gomega.HaveLen(1))
			g.Expect(events[0].Type).To(gomega.Equal(corev1.EventTypeNormal))
			g.Expect(events[0].InvolvedObject.UID).To(gomega.Equal(stored.UID))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("retries the conversion from a fresh live read after a 409 between the decoded read and the write", func() {
		rows := irstatustest.RepresentativeRows(300)
		selection := candidates(rows)
		ir := createInferenceReplica("gate-conflict", int32(len(rows)))
		key := client.ObjectKeyFromObject(ir)
		_, err := writeStatus(key, selection.ColumnarV2.Status, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// The first conversion write is preceded by an out-of-band status
		// change on the live object, so the API server answers it with a
		// genuine 409 and the gate must rebuild from a fresh live read.
		base, err := client.NewWithWatch(suiteCfg, client.Options{Scheme: suiteFwk.Scheme()})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		attempts := 0
		var outOfBandGeneration int64
		intercepted := interceptor.NewClient(base, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				attempts++
				if attempts == 1 {
					live := getInferenceReplica(key)
					live.Status.ObservedGeneration++
					outOfBandGeneration = live.Status.ObservedGeneration
					gomega.Expect(k8sClient.Status().Update(ctx, live)).To(gomega.Succeed())
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		})
		recorder, shutdown := newEventRecorder()
		defer shutdown()
		reconciler := newReconciler(intercepted, recorder)
		before := snapshotWriteMetrics()

		statusWrites.begin()
		result, err := reconciler.Reconcile(suiteCtx, ctrl.Request{NamespacedName: key})
		requests := statusWrites.finish()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(result.Requeue).To(gomega.BeTrue())
		gomega.Expect(attempts).To(gomega.Equal(2), "one conflicted attempt and one committed retry")
		codes := make([]int, 0, len(requests))
		for _, request := range requests {
			codes = append(codes, request.statusCode)
		}
		gomega.Expect(codes).To(gomega.Equal([]int{http.StatusOK, http.StatusConflict, http.StatusOK}),
			"out-of-band write, conflicted conversion, committed retry")

		stored, encoding := decodeStored(key)
		gomega.Expect(encoding).To(gomega.Equal(irstatus.EncodingDenseV1))
		gomega.Expect(stored.Status.InstanceStatusEncoding).To(gomega.BeNil())
		gomega.Expect(stored.Status.InstanceStatusColumns).To(gomega.BeNil())
		gomega.Expect(stored.Status.ObservedGeneration).To(gomega.Equal(outOfBandGeneration),
			"the retry must convert the fresh object and keep the concurrent change")
		expectRowsEqual(stored.Status.InstanceStatuses, selection.DenseV1.Status.InstanceStatuses)

		delta := snapshotWriteMetrics().minus(before)
		gomega.Expect(delta).To(gomega.Equal(writeMetrics{attempts: 2, conflicts: 1, committed: 1, conversions: 1}))
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(conversionEvents(g, ir.Name)).To(gomega.HaveLen(1), "one conversion event despite two attempts")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(gomega.Succeed())
	})
})

var _ = ginkgo.Describe("mass-failure fixture on the real API server", func() {
	ginkgo.It("records which complete requests the API server accepts", func() {
		rows := irstatustest.MassFailureRows()
		selection := candidates(rows)
		gomega.Expect(selection.Selected).To(gomega.Equal(irstatus.EncodingColumnarV2))
		ir := createInferenceReplica("mass-failure", int32(len(rows)))
		key := client.ObjectKeyFromObject(ir)
		beforeDense := getInferenceReplica(key)

		denseRequest, denseErr := writeStatus(key, selection.DenseV1.Status, false)
		var denseResult string
		switch {
		case denseErr == nil:
			denseResult = fmt.Sprintf("accepted; served GET %d bytes", len(getRawInferenceReplica(key)))
		case isTooLarge(denseErr):
			denseResult = fmt.Sprintf("rejected as too large (HTTP %d): %v", denseRequest.statusCode, denseErr)
			unchanged := getInferenceReplica(key)
			gomega.Expect(unchanged.ResourceVersion).To(gomega.Equal(beforeDense.ResourceVersion),
				"a rejected write must leave the prior status unchanged")
			gomega.Expect(unchanged.Status.InstanceStatuses).To(gomega.BeEmpty())
		default:
			ginkgo.Fail(fmt.Sprintf("the DenseV1 mass-failure write failed for a reason other than size: %v", denseErr))
		}
		ginkgo.GinkgoWriter.Printf("mass-failure DenseV1 complete request %d bytes (status %d bytes): %s\n",
			len(denseRequest.body), selection.DenseV1.Size, denseResult)

		columnarRequest, err := writeStatus(key, selection.ColumnarV2.Status, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "the ColumnarV2 mass-failure request must be accepted")
		gomega.Expect(columnarRequest.statusCode).To(gomega.Equal(http.StatusOK))
		served := getRawInferenceReplica(key)
		headroom := etcdDefaultRequestLimitBytes - len(columnarRequest.body)
		ginkgo.GinkgoWriter.Printf("mass-failure ColumnarV2 complete request %d bytes (status %d bytes): accepted; served GET %d bytes; headroom under the %d-byte request limit %d bytes (%.1f%%)\n",
			len(columnarRequest.body), selection.ColumnarV2.Size, len(served), etcdDefaultRequestLimitBytes, headroom, 100*float64(headroom)/float64(etcdDefaultRequestLimitBytes))
		gomega.Expect(len(columnarRequest.body)).To(gomega.BeNumerically("<", etcdDefaultRequestLimitBytes))

		stored, encoding := decodeStored(key)
		gomega.Expect(encoding).To(gomega.Equal(irstatus.EncodingColumnarV2))
		expectRowsEqual(stored.Status.InstanceStatuses, selection.DenseV1.Status.InstanceStatuses)
	})
})

var _ = ginkgo.Describe("uniform fixtures as ColumnarV2 on the real API server", func() {
	for _, n := range []int{2000, 5000} {
		ginkgo.It(fmt.Sprintf("round-trips %d rows through the decoder identical to the logical rows", n), func() {
			rows := irstatustest.UniformRows(n)
			selection := candidates(rows)
			ir := createInferenceReplica(fmt.Sprintf("uniform-%d", n), int32(n))
			key := client.ObjectKeyFromObject(ir)

			denseRequest, err := writeStatus(key, selection.DenseV1.Status, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(denseRequest.statusCode).To(gomega.Equal(http.StatusOK))
			denseServed := getRawInferenceReplica(key)
			denseManagedFields := managedFieldsBytes(getInferenceReplica(key))

			columnarRequest, err := writeStatus(key, selection.ColumnarV2.Status, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(columnarRequest.statusCode).To(gomega.Equal(http.StatusOK))
			columnarServed := getRawInferenceReplica(key)
			ginkgo.GinkgoWriter.Printf("uniform %d rows: DenseV1 request %d bytes, served GET %d bytes (managedFields %d bytes); ColumnarV2 request %d bytes, served GET %d bytes (managedFields %d bytes)\n",
				n, len(denseRequest.body), len(denseServed), denseManagedFields,
				len(columnarRequest.body), len(columnarServed), managedFieldsBytes(getInferenceReplica(key)))
			gomega.Expect(len(columnarRequest.body)).To(gomega.BeNumerically("<=", steadyStateGateBytes))
			gomega.Expect(len(columnarServed)).To(gomega.BeNumerically("<=", steadyStateGateBytes))
			gomega.Expect(2*len(columnarRequest.body)).To(gomega.BeNumerically("<=", len(denseRequest.body)),
				"the selected request must be at most half of the accepted DenseV1 request")

			stored, encoding := decodeStored(key)
			gomega.Expect(encoding).To(gomega.Equal(irstatus.EncodingColumnarV2))
			expectRowsEqual(stored.Status.InstanceStatuses, rows)
		})
	}
})

// unionViolation is one spelling of the representation union the schema
// must reject, with a fragment of the server's reason.
type unionViolation struct {
	name     string
	fragment string
	mutate   func(status map[string]any)
}

func unionViolations() []unionViolation {
	columns := func(status map[string]any) map[string]any {
		return status["instanceStatusColumns"].(map[string]any)
	}
	return []unionViolation{
		{name: "columns without marker", fragment: "requires instanceStatusEncoding", mutate: func(s map[string]any) {
			delete(s, "instanceStatusEncoding")
		}},
		{name: "marker without columns", fragment: "requires instanceStatusColumns", mutate: func(s map[string]any) {
			delete(s, "instanceStatusColumns")
		}},
		{name: "marker plus dense rows plus columns", fragment: "forbids instanceStatuses", mutate: func(s map[string]any) {
			s["instanceStatuses"] = []any{map[string]any{"index": int64(0), "phase": "Ready"}}
		}},
		{name: "unknown marker value", fragment: "Unsupported value", mutate: func(s map[string]any) {
			s["instanceStatusEncoding"] = "ColumnarV3"
		}},
		{name: "empty members", fragment: "at least 1 chars", mutate: func(s map[string]any) {
			columns(s)["members"] = ""
		}},
		{name: "index set violating the pattern", fragment: "should match", mutate: func(s map[string]any) {
			columns(s)["members"] = "0-3,"
		}},
		{name: "required group with no items", fragment: "at least 1 items", mutate: func(s map[string]any) {
			columns(s)["phases"] = []any{}
		}},
		{name: "optional group with no items", fragment: "at least 1 items", mutate: func(s map[string]any) {
			columns(s)["incarnations"] = []any{}
		}},
		{name: "incarnation group with value 0", fragment: "must be nonzero", mutate: func(s map[string]any) {
			columns(s)["incarnations"] = []any{map[string]any{"value": int64(0), "indexes": "0-3"}}
		}},
		{name: "entry with no exceptional field", fragment: "must carry at least one of", mutate: func(s map[string]any) {
			columns(s)["entries"] = []any{map[string]any{"index": int64(0)}}
		}},
	}
}

// rawStatusUpdate applies mutate to the stored status document and PUTs it
// through the status subresource as unstructured JSON, so a spec can send
// spellings the typed client cannot express.
func rawStatusUpdate(key client.ObjectKey, mutate func(status map[string]any)) error {
	ginkgo.GinkgoHelper()

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))
	gomega.Expect(k8sClient.Get(suiteCtx, key, u)).To(gomega.Succeed())
	status, found, err := unstructured.NestedMap(u.Object, "status")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(found).To(gomega.BeTrue())
	mutate(status)
	gomega.Expect(unstructured.SetNestedMap(u.Object, status, "status")).To(gomega.Succeed())
	return k8sClient.Status().Update(suiteCtx, u)
}

// candidates builds both complete candidates for rows through the codec.
func candidates(rows []v1beta1.OMENativeInstanceStatus) *irstatus.Selection {
	ginkgo.GinkgoHelper()

	selection, err := irstatus.SelectCandidate(logicalStatus(rows), decodeLimit)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(selection.ColumnarV2).NotTo(gomega.BeNil(), "fixture is not eligible for ColumnarV2: %s", selection.IneligibleReason)
	return selection
}

// logicalStatus wraps rows in the aggregate status a steady IR carries.
func logicalStatus(rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplicaStatus {
	status := irstatustest.LogicalStatus(rows)
	if len(rows) > 0 && rows[0].RunningRevision != "" {
		status.CurrentRevision = rows[0].RunningRevision
	}
	status.UpdateRevision = status.CurrentRevision
	status.ObservedGeneration = 1
	for _, row := range rows {
		if row.Phase == v1beta1.OMENativeInstanceReady {
			status.ReadyReplicas++
			status.AvailableReplicas++
			status.ServingReplicas++
		}
		if row.RunningRevision == status.UpdateRevision {
			status.UpdatedReplicas++
		}
	}
	status.Conditions = []metav1.Condition{{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: 1,
		LastTransitionTime: irstatustest.Time,
		Reason:             "AllInstancesReady",
		Message:            "all instances are ready",
	}}
	return status
}

func newInferenceReplica(name string, replicas int32) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "example",
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			},
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: "example"},
			Component: v1beta1.EngineComponent,
			Replicas:  &replicas,
			Runners: []v1beta1.Runner{{
				Name: v1beta1.RunnerNameDefault,
				Size: 1,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{query.LabelRunner: string(v1beta1.RunnerNameDefault)},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "server", Image: "example.com/serving:v1"}},
					},
				},
			}},
		},
	}
}

// createInferenceReplica creates a fixture object and returns its live copy.
func createInferenceReplica(name string, replicas int32) *v1beta1.InferenceReplica {
	ginkgo.GinkgoHelper()

	ir := newInferenceReplica(name, replicas)
	gomega.Expect(k8sClient.Create(suiteCtx, ir)).To(gomega.Succeed())
	return getInferenceReplica(client.ObjectKeyFromObject(ir))
}

func getInferenceReplica(key client.ObjectKey) *v1beta1.InferenceReplica {
	ginkgo.GinkgoHelper()

	ir := &v1beta1.InferenceReplica{}
	gomega.Expect(k8sClient.Get(suiteCtx, key, ir)).To(gomega.Succeed())
	return ir
}

// writeStatus PUTs status onto the live object and returns the captured
// request together with the write's result. With omitManagedFields the
// request leaves metadata.managedFields out, which tells the API server to
// keep and recompute ownership itself rather than echoing the previous
// representation's entries back in the body.
func writeStatus(key client.ObjectKey, status *v1beta1.InferenceReplicaStatus, omitManagedFields bool) (capturedStatusRequest, error) {
	ginkgo.GinkgoHelper()

	live := getInferenceReplica(key)
	live.Status = *status.DeepCopy()
	if omitManagedFields {
		live.ManagedFields = nil
	}
	statusWrites.begin()
	err := k8sClient.Status().Update(suiteCtx, live)
	requests := statusWrites.finish()
	gomega.Expect(requests).To(gomega.HaveLen(1))
	return requests[0], err
}

// decodeStored fetches the object and decodes it through the codec's reader
// boundary, returning the representation it was stored in.
func decodeStored(key client.ObjectKey) (*v1beta1.InferenceReplica, irstatus.Encoding) {
	ginkgo.GinkgoHelper()

	ir := getInferenceReplica(key)
	encoding, err := irstatus.NewDecoder(decodeLimit).Decode(ir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return ir, encoding
}

func expectRowsEqual(got, want []v1beta1.OMENativeInstanceStatus) {
	ginkgo.GinkgoHelper()

	gomega.Expect(got).To(gomega.HaveLen(len(want)))
	for i := range want {
		gomega.Expect(equality.Semantic.DeepEqual(got[i], want[i])).To(gomega.BeTrue(),
			"row %d differs:\n got  %s\n want %s", i, mustJSON(got[i]), mustJSON(want[i]))
	}
}

// expectStatusOutsideRowsEqual compares every status field except the
// per-Instance representation.
func expectStatusOutsideRowsEqual(got, want v1beta1.InferenceReplicaStatus) {
	ginkgo.GinkgoHelper()

	got.InstanceStatuses, want.InstanceStatuses = nil, nil
	got.InstanceStatusEncoding, want.InstanceStatusEncoding = nil, nil
	got.InstanceStatusColumns, want.InstanceStatusColumns = nil, nil
	gomega.Expect(equality.Semantic.DeepEqual(got, want)).To(gomega.BeTrue(),
		"status outside the per-Instance representation changed:\n got  %s\n want %s", mustJSON(got), mustJSON(want))
}

func mustJSON(value any) []byte {
	ginkgo.GinkgoHelper()

	data, err := json.Marshal(value)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return data
}

func managedFieldsBytes(ir *v1beta1.InferenceReplica) int {
	ginkgo.GinkgoHelper()
	return len(mustJSON(ir.ManagedFields))
}

// isTooLarge reports whether the API server refused a write for its size:
// an explicit 413, or the storage backend's oversized-request failure
// surfaced through an Internal error.
func isTooLarge(err error) bool {
	return apierrors.IsRequestEntityTooLargeError(err) || strings.Contains(err.Error(), "too large")
}

// newReconciler builds the production IR reconciler over the given writer
// client, with an uncached live reader and the suite's decode bound; only
// the fields the transition gate uses are set.
func newReconciler(writer client.Client, recorder record.EventRecorder) *inferencereplica.Reconciler {
	return &inferencereplica.Reconciler{
		Client:                writer,
		APIReader:             k8sClient,
		Log:                   ctrl.Log.WithName("ir-status-encoding"),
		Recorder:              recorder,
		InstanceStatusDecoder: irstatus.NewDecoder(decodeLimit),
		InstanceStatusTarget:  irstatus.EncodingDenseV1,
	}
}

// newEventRecorder records Events into the API server, so a spec can prove
// the gate's event reaches the cluster rather than only a fake recorder.
func newEventRecorder() (record.EventRecorder, func()) {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientset.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(suiteFwk.Scheme(), corev1.EventSource{Component: "ir-status-encoding-suite"})
	return recorder, broadcaster.Shutdown
}

func conversionEvents(g gomega.Gomega, name string) []corev1.Event {
	events, err := clientset.CoreV1().Events(testNamespace).List(suiteCtx, metav1.ListOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	var out []corev1.Event
	for _, event := range events.Items {
		if event.InvolvedObject.Name == name && event.Reason == inferencereplica.EventReasonInstanceStatusConverted {
			out = append(out, event)
		}
	}
	return out
}

// writeMetrics is one reading of the write-boundary counters this suite
// asserts deltas on.
type writeMetrics struct {
	attempts, conflicts, committed, conversions float64
}

func (m writeMetrics) minus(other writeMetrics) writeMetrics {
	return writeMetrics{
		attempts:    m.attempts - other.attempts,
		conflicts:   m.conflicts - other.conflicts,
		committed:   m.committed - other.committed,
		conversions: m.conversions - other.conversions,
	}
}

func snapshotWriteMetrics() writeMetrics {
	ginkgo.GinkgoHelper()

	dense := func(result string) map[string]string {
		return map[string]string{"encoding": obsmetrics.IRStatusEncodingDenseV1, "result": result}
	}
	return writeMetrics{
		attempts:    metricValue("ome_omenative_ir_status_writes_total", dense(obsmetrics.IRStatusWriteAttempt)),
		conflicts:   metricValue("ome_omenative_ir_status_writes_total", dense(obsmetrics.IRStatusWriteConflict)),
		committed:   metricValue("ome_omenative_ir_status_writes_total", dense(obsmetrics.IRStatusWriteCommitted)),
		conversions: metricValue("ome_omenative_ir_status_conversions_total", conversionLabels),
	}
}

// metricValue reads one counter sample from the controller-runtime registry;
// zero when the series does not exist yet.
func metricValue(name string, labels map[string]string) float64 {
	ginkgo.GinkgoHelper()

	families, err := ctrlmetrics.Registry.Gather()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	metrics:
		for _, metric := range family.GetMetric() {
			seen := 0
			for _, pair := range metric.GetLabel() {
				want, ok := labels[pair.GetName()]
				if !ok {
					continue
				}
				if pair.GetValue() != want {
					continue metrics
				}
				seen++
			}
			if seen != len(labels) {
				continue
			}
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}
			if metric.Gauge != nil {
				return metric.Gauge.GetValue()
			}
		}
	}
	return 0
}

// getRawInferenceReplica returns the object exactly as the API server serves
// it, including managedFields, without client-side decoding.
func getRawInferenceReplica(key client.ObjectKey) []byte {
	ginkgo.GinkgoHelper()

	httpClient, err := rest.HTTPClientFor(suiteCfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	base, err := url.Parse(suiteCfg.Host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	base.Path = strings.TrimRight(base.Path, "/") + fmt.Sprintf(
		"/apis/ome.io/v1beta1/namespaces/%s/inferencereplicas/%s",
		url.PathEscape(key.Namespace),
		url.PathEscape(key.Name),
	)

	req, err := http.NewRequestWithContext(suiteCtx, http.MethodGet, base.String(), nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Accept", runtime.ContentTypeJSON)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := httpClient.Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer resp.Body.Close()
	gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
	body, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return body
}
