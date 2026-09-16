package inferencereplicastatussize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

const fixtureReplicas int32 = 2000

var fixtureTime = metav1.NewTime(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))

type sizeMeasurement struct {
	requestBytes                  int
	requestManagedFieldsBytes     int
	requestManagedFieldsEntries   int
	servedObjectBytes             int
	persistedManagedFieldsBytes   int
	persistedManagedFieldsEntries int
}

type persistenceMeasurements struct {
	steadyLegacy  sizeMeasurement
	compactWrite  sizeMeasurement
	steadyCompact sizeMeasurement
}

var _ = ginkgo.Describe("complete InferenceReplica status update size", func() {
	ginkgo.It("measures 2,000-Instance legacy and compact API requests", func() {
		for _, topology := range []struct {
			name                       string
			podsPerInstance            int32
			minimumSteadyReduction     int
			minimumFirstWriteReduction int
			requireManagedFields       bool
		}{
			{name: "singleton", podsPerInstance: 1, minimumSteadyReduction: 25, minimumFirstWriteReduction: 10, requireManagedFields: true},
			{name: "eight-pod-gang", podsPerInstance: 8, minimumSteadyReduction: 50, minimumFirstWriteReduction: 50},
		} {
			measurements := measurePersistence(
				fixtureInferenceReplica("status-"+topology.name, topology.podsPerInstance),
			)
			assertPersistenceAcceptance(
				topology.name,
				measurements,
				topology.minimumSteadyReduction,
				topology.minimumFirstWriteReduction,
				topology.requireManagedFields,
			)
			logPersistenceMeasurements(topology.name, measurements)
		}
	})
})

func measurePersistence(ir *v1beta1.InferenceReplica) persistenceMeasurements {
	ginkgo.GinkgoHelper()

	legacyStatus := ir.Status.DeepCopy()
	gomega.Expect(k8sClient.Create(suiteCtx, ir)).To(gomega.Succeed())
	key := client.ObjectKeyFromObject(ir)

	live := getInferenceReplica(key)
	live.Status = *legacyStatus.DeepCopy()

	// The warm-up produces the status-subresource managed-fields entry that
	// the first measured steady-state update carries in its complete body.
	gomega.Expect(k8sClient.Status().Update(suiteCtx, live)).To(gomega.Succeed())

	steadyLegacy := captureStatusUpdate(getInferenceReplica(key), false)

	compactCandidate := getInferenceReplica(key)
	legacyManagedFieldsBytes := encodedManagedFieldsBytes(compactCandidate)
	applyCompactPersistence(&compactCandidate.Status)
	compactWrite := captureStatusUpdate(compactCandidate, true)
	gomega.Expect(compactWrite.requestManagedFieldsBytes).To(gomega.Equal(legacyManagedFieldsBytes),
		"the compact request must carry any managedFields retained by the legacy object")

	compact := getInferenceReplica(key)
	assertCompactStatus(compact.Status, *legacyStatus)
	steadyCompact := captureStatusUpdate(compact, true)

	return persistenceMeasurements{
		steadyLegacy:  steadyLegacy,
		compactWrite:  compactWrite,
		steadyCompact: steadyCompact,
	}
}

func captureStatusUpdate(ir *v1beta1.InferenceReplica, expectCompact bool) sizeMeasurement {
	ginkgo.GinkgoHelper()

	key := client.ObjectKeyFromObject(ir)
	wantRequestObject := ir.DeepCopy()

	statusWrites.begin()
	updateErr := k8sClient.Status().Update(suiteCtx, ir)
	requests := statusWrites.finish()
	gomega.Expect(updateErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(requests).To(gomega.HaveLen(1))

	request := requests[0]
	gomega.Expect(request.statusCode).To(gomega.Equal(http.StatusOK))
	gomega.Expect(request.contentType).To(gomega.HavePrefix(runtime.ContentTypeJSON))
	gomega.Expect(request.contentLength).To(gomega.Equal(int64(len(request.body))))
	gomega.Expect(request.body).To(gomega.Equal(encodeStatusRequest(wantRequestObject)))
	assertCompleteRequestObject(request.body, wantRequestObject)
	if expectCompact {
		assertPodDerivedFieldsOmitted(request.body)
	}

	persisted := getInferenceReplica(key)
	gomega.Expect(persisted.Spec).To(gomega.Equal(wantRequestObject.Spec))
	expectStatusJSONEqual(persisted.Status, wantRequestObject.Status)

	served := getRawInferenceReplica(suiteCtx, key)
	assertCompleteServedObject(served, persisted)
	if expectCompact {
		assertPodDerivedFieldsOmitted(served)
	}

	return sizeMeasurement{
		requestBytes:                  len(request.body),
		requestManagedFieldsBytes:     encodedManagedFieldsBytes(wantRequestObject),
		requestManagedFieldsEntries:   len(wantRequestObject.ManagedFields),
		servedObjectBytes:             len(served),
		persistedManagedFieldsBytes:   encodedManagedFieldsBytes(persisted),
		persistedManagedFieldsEntries: len(persisted.ManagedFields),
	}
}

func assertPodDerivedFieldsOmitted(body []byte) {
	ginkgo.GinkgoHelper()

	var object struct {
		Status struct {
			InstanceStatuses []map[string]json.RawMessage `json:"instanceStatuses"`
		} `json:"status"`
	}
	gomega.Expect(json.Unmarshal(body, &object)).To(gomega.Succeed())
	for index, status := range object.Status.InstanceStatuses {
		for _, field := range []string{"readyPodCount", "scheduledPodCount", "nodesOccupied"} {
			gomega.Expect(status).NotTo(gomega.HaveKey(field),
				"InstanceStatus[%d] must omit %s from compact JSON", index, field)
		}
	}
}

func getInferenceReplica(key client.ObjectKey) *v1beta1.InferenceReplica {
	ginkgo.GinkgoHelper()

	ir := &v1beta1.InferenceReplica{}
	gomega.Expect(k8sClient.Get(suiteCtx, key, ir)).To(gomega.Succeed())
	return ir
}

// applyCompactPersistence clears the pod-derived row fields the compact write
// omits and keeps the fixture in its dense representation.
func applyCompactPersistence(status *v1beta1.InferenceReplicaStatus) {
	ginkgo.GinkgoHelper()

	rows, err := util.DecodedInstanceStatuses(&v1beta1.InferenceReplica{Status: *status})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for i := range rows {
		rows[i].ReadyPodCount = 0
		rows[i].ScheduledPodCount = 0
		rows[i].NodesOccupied = nil
	}
	util.InstallDenseInstanceStatuses(status, rows)
}

func assertCompactStatus(got, legacy v1beta1.InferenceReplicaStatus) {
	ginkgo.GinkgoHelper()

	want := legacy.DeepCopy()
	applyCompactPersistence(want)
	expectStatusJSONEqual(got, *want)
	encoded, err := json.Marshal(got)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, field := range []string{"readyPodCount", "scheduledPodCount", "nodesOccupied"} {
		gomega.Expect(string(encoded)).NotTo(gomega.ContainSubstring(`"` + field + `"`))
	}
}

func expectStatusJSONEqual(got, want v1beta1.InferenceReplicaStatus) {
	ginkgo.GinkgoHelper()

	gotJSON, err := json.Marshal(got)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	wantJSON, err := json.Marshal(want)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(gotJSON).To(gomega.Equal(wantJSON))
}

func assertPersistenceAcceptance(topology string, measurements persistenceMeasurements, minimumSteadyReduction, minimumFirstWriteReduction int, requireManagedFields bool) {
	ginkgo.GinkgoHelper()

	legacy := measurements.steadyLegacy
	compact := measurements.steadyCompact
	gomega.Expect(compact.requestBytes).To(gomega.BeNumerically("<", legacy.requestBytes),
		"%s compact persistence must reduce the complete status PUT body", topology)
	gomega.Expect(compact.servedObjectBytes).To(gomega.BeNumerically("<", legacy.servedObjectBytes),
		"%s compact persistence must reduce the complete served object", topology)
	gomega.Expect((legacy.requestBytes-compact.requestBytes)*100).To(
		gomega.BeNumerically(">=", legacy.requestBytes*minimumSteadyReduction),
		"%s steady compact status must meet the named reduction gate", topology)
	if requireManagedFields {
		gomega.Expect(legacy.persistedManagedFieldsEntries).To(gomega.BeNumerically(">", 0),
			"%s legacy fixture must retain managedFields so their request cost is exercised", topology)
		gomega.Expect(measurements.compactWrite.requestManagedFieldsEntries).To(gomega.BeNumerically(">", 0),
			"%s compact request must carry the retained legacy managedFields", topology)
	}
	gomega.Expect(measurements.compactWrite.requestBytes).To(gomega.BeNumerically("<", legacy.requestBytes),
		"%s first compact request must be smaller than the steady legacy request", topology)
	gomega.Expect((legacy.requestBytes-measurements.compactWrite.requestBytes)*100).To(
		gomega.BeNumerically(">=", legacy.requestBytes*minimumFirstWriteReduction),
		"%s first compact write must meet the named reduction gate", topology)
}

func logPersistenceMeasurements(topology string, measurements persistenceMeasurements) {
	ginkgo.GinkgoHelper()

	for _, stage := range []struct {
		name        string
		measurement sizeMeasurement
	}{
		{name: "steady-legacy", measurement: measurements.steadyLegacy},
		{name: "compact-write", measurement: measurements.compactWrite},
		{name: "steady-compact", measurement: measurements.steadyCompact},
	} {
		ginkgo.GinkgoWriter.Printf(
			"IR status persistence topology=%s replicas=%d stage=%s request=%d request_managed_fields=%d request_managed_field_entries=%d served=%d persisted_managed_fields=%d persisted_managed_field_entries=%d\n",
			topology,
			fixtureReplicas,
			stage.name,
			stage.measurement.requestBytes,
			stage.measurement.requestManagedFieldsBytes,
			stage.measurement.requestManagedFieldsEntries,
			stage.measurement.servedObjectBytes,
			stage.measurement.persistedManagedFieldsBytes,
			stage.measurement.persistedManagedFieldsEntries,
		)
	}
}

func encodedManagedFieldsBytes(ir *v1beta1.InferenceReplica) int {
	ginkgo.GinkgoHelper()

	withoutManagedFields := ir.DeepCopy()
	withoutManagedFields.ManagedFields = nil
	return len(encodeStatusRequest(ir)) - len(encodeStatusRequest(withoutManagedFields))
}

func encodeStatusRequest(ir *v1beta1.InferenceReplica) []byte {
	ginkgo.GinkgoHelper()

	scheme := runtime.NewScheme()
	gomega.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())
	codecs := serializer.NewCodecFactory(scheme)
	negotiated := serializer.WithoutConversionCodecFactory{CodecFactory: codecs}
	encoder, err := runtime.NewClientNegotiator(negotiated, v1beta1.SchemeGroupVersion).
		Encoder(runtime.ContentTypeJSON, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	body, err := runtime.Encode(encoder, ir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return body
}

func assertCompleteRequestObject(body []byte, want *v1beta1.InferenceReplica) {
	ginkgo.GinkgoHelper()

	var document map[string]json.RawMessage
	gomega.Expect(json.Unmarshal(body, &document)).To(gomega.Succeed())
	for _, field := range []string{"apiVersion", "kind", "metadata", "spec", "status"} {
		gomega.Expect(document).To(gomega.HaveKey(field))
	}

	var got v1beta1.InferenceReplica
	gomega.Expect(json.Unmarshal(body, &got)).To(gomega.Succeed())
	gomega.Expect(got.APIVersion).To(gomega.Equal(v1beta1.SchemeGroupVersion.String()))
	gomega.Expect(got.Kind).To(gomega.Equal("InferenceReplica"))
	gomega.Expect(got.Name).To(gomega.Equal(want.Name))
	gomega.Expect(got.Namespace).To(gomega.Equal(want.Namespace))
	gomega.Expect(got.UID).NotTo(gomega.BeEmpty())
	gomega.Expect(got.ResourceVersion).NotTo(gomega.BeEmpty())
	gomega.Expect(got.CreationTimestamp.IsZero()).To(gomega.BeFalse())
	gomega.Expect(got.ManagedFields).To(gomega.Equal(want.ManagedFields))
	gomega.Expect(got.Spec).To(gomega.Equal(want.Spec))
	expectStatusJSONEqual(got.Status, want.Status)
}

func getRawInferenceReplica(ctx context.Context, key client.ObjectKey) []byte {
	ginkgo.GinkgoHelper()

	httpClient, err := restHTTPClient()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	base, err := url.Parse(suiteCfg.Host)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	base.Path = strings.TrimRight(base.Path, "/") + fmt.Sprintf(
		"/apis/ome.io/v1beta1/namespaces/%s/inferencereplicas/%s",
		url.PathEscape(key.Namespace),
		url.PathEscape(key.Name),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
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

func restHTTPClient() (*http.Client, error) {
	return rest.HTTPClientFor(suiteCfg)
}

func assertCompleteServedObject(body []byte, want *v1beta1.InferenceReplica) {
	ginkgo.GinkgoHelper()

	var got v1beta1.InferenceReplica
	gomega.Expect(json.Unmarshal(body, &got)).To(gomega.Succeed())
	gomega.Expect(got.Name).To(gomega.Equal(want.Name))
	gomega.Expect(got.Namespace).To(gomega.Equal(want.Namespace))
	gomega.Expect(got.UID).To(gomega.Equal(want.UID))
	gomega.Expect(got.ResourceVersion).To(gomega.Equal(want.ResourceVersion))
	gomega.Expect(got.CreationTimestamp).To(gomega.Equal(want.CreationTimestamp))
	gomega.Expect(got.ManagedFields).To(gomega.Equal(want.ManagedFields))
	gomega.Expect(got.Spec).To(gomega.Equal(want.Spec))
	expectStatusJSONEqual(got.Status, want.Status)
}

func fixtureInferenceReplica(name string, podsPerInstance int32) *v1beta1.InferenceReplica {
	replicas := fixtureReplicas
	ir := &v1beta1.InferenceReplica{
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
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       "example",
				UID:        types.UID("example-inferenceservice-uid"),
				Controller: boolPointer(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: "example"},
			Component: v1beta1.EngineComponent,
			Replicas:  &replicas,
			Runners:   fixtureRunners(podsPerInstance),
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   1,
			Replicas:             replicas,
			ReadyReplicas:        replicas,
			ServingReplicas:      replicas,
			AvailableReplicas:    replicas,
			UpdatedReplicas:      replicas,
			UpdatedReadyReplicas: replicas,
			CurrentRevision:      "example-engine-7d9c6f5b4",
			UpdateRevision:       "example-engine-7d9c6f5b4",
			LabelSelector:        "ome.io/inferenceservice=example,ome.io/component=engine",
			Conditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: 1,
				LastTransitionTime: fixtureTime,
				Reason:             "AllInstancesReady",
				Message:            "all instances are ready",
			}},
		},
	}

	rows := make([]v1beta1.OMENativeInstanceStatus, 0, replicas)
	for index := int32(0); index < replicas; index++ {
		rows = append(rows, v1beta1.OMENativeInstanceStatus{
			Index:             index,
			Incarnation:       1,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   ir.Status.CurrentRevision,
			PodCount:          podsPerInstance,
			ReadyPodCount:     podsPerInstance,
			ServingPodCount:   podsPerInstance,
			AvailablePodCount: podsPerInstance,
			ScheduledPodCount: podsPerInstance,
			Admitted:          true,
			NodesOccupied:     fixtureNodes(index, podsPerInstance),
		})
	}
	util.InstallDenseInstanceStatuses(&ir.Status, rows)
	return ir
}

func fixtureRunners(podsPerInstance int32) []v1beta1.Runner {
	if podsPerInstance == 1 {
		return []v1beta1.Runner{fixtureRunner(v1beta1.RunnerNameDefault, 1)}
	}
	return []v1beta1.Runner{
		fixtureRunner(v1beta1.RunnerNameLeader, 1),
		fixtureRunner(v1beta1.RunnerNameWorker, podsPerInstance-1),
	}
}

func fixtureRunner(name v1beta1.RunnerName, size int32) v1beta1.Runner {
	return v1beta1.Runner{
		Name: name,
		Size: size,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{query.LabelRunner: string(name)},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "server",
					Image: "example.com/serving:v1",
				}},
			},
		},
	}
}

func fixtureNodes(index, count int32) []string {
	nodes := make([]string, count)
	for ordinal := int32(0); ordinal < count; ordinal++ {
		nodes[ordinal] = fmt.Sprintf("worker-%04d-%02d.example", index, ordinal)
	}
	return nodes
}

func boolPointer(value bool) *bool {
	return &value
}
