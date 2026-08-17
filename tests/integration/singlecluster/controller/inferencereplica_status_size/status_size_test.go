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

type lifecycleMeasurements struct {
	steadyInline    sizeMeasurement
	projectDerived  sizeMeasurement
	steadyDerived   sizeMeasurement
	rehydrateInline sizeMeasurement
	finalInline     sizeMeasurement
}

var _ = ginkgo.Describe("complete InferenceReplica status update size", func() {
	ginkgo.It("measures a 2,000-Instance representation lifecycle on each object", func() {
		for _, topology := range []struct {
			name                       string
			podsPerInstance            int32
			minimumSteadyReduction     int
			minimumProjectionReduction int
		}{
			{name: "singleton", podsPerInstance: 1, minimumSteadyReduction: 25, minimumProjectionReduction: 10},
			{name: "eight-pod-gang", podsPerInstance: 8, minimumSteadyReduction: 50, minimumProjectionReduction: 50},
		} {
			measurements := measureRepresentationLifecycle(
				fixtureInferenceReplica("status-"+topology.name, topology.podsPerInstance),
			)
			assertLifecycleAcceptance(
				topology.name,
				measurements,
				topology.minimumSteadyReduction,
				topology.minimumProjectionReduction,
			)
			logLifecycleMeasurements(topology.name, measurements)
		}
	})
})

func measureRepresentationLifecycle(ir *v1beta1.InferenceReplica) lifecycleMeasurements {
	ginkgo.GinkgoHelper()

	inlineStatus := ir.Status.DeepCopy()
	gomega.Expect(k8sClient.Create(suiteCtx, ir)).To(gomega.Succeed())
	key := client.ObjectKeyFromObject(ir)

	live := getInferenceReplica(key)
	live.Status = *inlineStatus.DeepCopy()

	// The warm-up produces the status-subresource managed-fields entry that
	// the first measured steady-state update carries in its complete body.
	gomega.Expect(k8sClient.Status().Update(suiteCtx, live)).To(gomega.Succeed())

	steadyInline := captureStatusUpdate(getInferenceReplica(key))

	inlineForProjection := getInferenceReplica(key)
	inlineManagedFieldsBytes := encodedManagedFieldsBytes(inlineForProjection)
	applyTestOnlyDerivedProjection(&inlineForProjection.Status)
	projectDerived := captureStatusUpdate(inlineForProjection)
	gomega.Expect(projectDerived.requestManagedFieldsBytes).To(gomega.Equal(inlineManagedFieldsBytes),
		"the projection request must carry the live Inline managedFields")

	derived := getInferenceReplica(key)
	assertDerivedProjection(derived.Status, *inlineStatus)
	steadyDerived := captureStatusUpdate(derived)

	derivedForRehydrate := getInferenceReplica(key)
	derivedManagedFieldsBytes := encodedManagedFieldsBytes(derivedForRehydrate)
	derivedForRehydrate.Status = *inlineStatus.DeepCopy()
	rehydrateInline := captureStatusUpdate(derivedForRehydrate)
	gomega.Expect(rehydrateInline.requestManagedFieldsBytes).To(gomega.Equal(derivedManagedFieldsBytes),
		"the rehydrate request must carry the live Derived managedFields")

	finalInlineObject := getInferenceReplica(key)
	expectStatusJSONEqual(finalInlineObject.Status, *inlineStatus)
	finalInline := captureStatusUpdate(finalInlineObject)

	return lifecycleMeasurements{
		steadyInline:    steadyInline,
		projectDerived:  projectDerived,
		steadyDerived:   steadyDerived,
		rehydrateInline: rehydrateInline,
		finalInline:     finalInline,
	}
}

func captureStatusUpdate(ir *v1beta1.InferenceReplica) sizeMeasurement {
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

	persisted := getInferenceReplica(key)
	gomega.Expect(persisted.Spec).To(gomega.Equal(wantRequestObject.Spec))
	expectStatusJSONEqual(persisted.Status, wantRequestObject.Status)

	served := getRawInferenceReplica(suiteCtx, key)
	assertCompleteServedObject(served, persisted)

	return sizeMeasurement{
		requestBytes:                  len(request.body),
		requestManagedFieldsBytes:     encodedManagedFieldsBytes(wantRequestObject),
		requestManagedFieldsEntries:   len(wantRequestObject.ManagedFields),
		servedObjectBytes:             len(served),
		persistedManagedFieldsBytes:   encodedManagedFieldsBytes(persisted),
		persistedManagedFieldsEntries: len(persisted.ManagedFields),
	}
}

func getInferenceReplica(key client.ObjectKey) *v1beta1.InferenceReplica {
	ginkgo.GinkgoHelper()

	ir := &v1beta1.InferenceReplica{}
	gomega.Expect(k8sClient.Get(suiteCtx, key, ir)).To(gomega.Succeed())
	return ir
}

// applyTestOnlyDerivedProjection models the proposed representation without
// adding an API marker or changing production serialization behavior.
func applyTestOnlyDerivedProjection(status *v1beta1.InferenceReplicaStatus) {
	for i := range status.InstanceStatuses {
		status.InstanceStatuses[i].ReadyPodCount = 0
		status.InstanceStatuses[i].ScheduledPodCount = 0
		status.InstanceStatuses[i].NodesOccupied = nil
	}
}

func assertDerivedProjection(got, inline v1beta1.InferenceReplicaStatus) {
	ginkgo.GinkgoHelper()

	want := inline.DeepCopy()
	applyTestOnlyDerivedProjection(want)
	expectStatusJSONEqual(got, *want)
}

func expectStatusJSONEqual(got, want v1beta1.InferenceReplicaStatus) {
	ginkgo.GinkgoHelper()

	gotJSON, err := json.Marshal(got)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	wantJSON, err := json.Marshal(want)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(gotJSON).To(gomega.Equal(wantJSON))
}

func assertLifecycleAcceptance(topology string, measurements lifecycleMeasurements, minimumSteadyReduction, minimumProjectionReduction int) {
	ginkgo.GinkgoHelper()

	inline := measurements.steadyInline
	derived := measurements.steadyDerived
	gomega.Expect(derived.requestBytes).To(gomega.BeNumerically("<", inline.requestBytes),
		"%s steady projection must reduce the complete status PUT body", topology)
	gomega.Expect(derived.servedObjectBytes).To(gomega.BeNumerically("<", inline.servedObjectBytes),
		"%s steady projection must reduce the complete served object", topology)
	gomega.Expect((inline.requestBytes-derived.requestBytes)*100).To(
		gomega.BeNumerically(">=", inline.requestBytes*minimumSteadyReduction),
		"%s steady projection must meet the named reduction gate", topology)
	gomega.Expect(measurements.projectDerived.requestBytes).To(gomega.BeNumerically("<", inline.requestBytes),
		"%s projection must reduce the request while carrying prior Inline managedFields", topology)
	gomega.Expect((inline.requestBytes-measurements.projectDerived.requestBytes)*100).To(
		gomega.BeNumerically(">=", inline.requestBytes*minimumProjectionReduction),
		"%s conversion projection must meet the named reduction gate", topology)
	gomega.Expect(measurements.finalInline.requestBytes).To(gomega.BeNumerically(">", derived.requestBytes),
		"%s rehydrated Inline steady state must restore the larger representation", topology)
}

func logLifecycleMeasurements(topology string, measurements lifecycleMeasurements) {
	ginkgo.GinkgoHelper()

	for _, stage := range []struct {
		name        string
		measurement sizeMeasurement
	}{
		{name: "steady-inline", measurement: measurements.steadyInline},
		{name: "project-derived", measurement: measurements.projectDerived},
		{name: "steady-derived", measurement: measurements.steadyDerived},
		{name: "rehydrate-inline", measurement: measurements.rehydrateInline},
		{name: "final-inline", measurement: measurements.finalInline},
	} {
		ginkgo.GinkgoWriter.Printf(
			"IR status lifecycle topology=%s replicas=%d stage=%s request=%d request_managed_fields=%d request_managed_field_entries=%d served=%d persisted_managed_fields=%d persisted_managed_field_entries=%d\n",
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

	ir.Status.InstanceStatuses = make([]v1beta1.OMENativeInstanceStatus, 0, replicas)
	for index := int32(0); index < replicas; index++ {
		row := v1beta1.OMENativeInstanceStatus{
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
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, row)
	}
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
