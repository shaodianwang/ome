package coordination

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

func TestPerRevisionServiceName(t *testing.T) {
	got := PerRevisionServiceName("llama", v1beta1.EngineComponent, "abcd1234")
	if got != "llama-engine-rev-abcd1234" {
		t.Errorf("got %q want llama-engine-rev-abcd1234", got)
	}
}

func TestPerRevisionHeadlessServiceName(t *testing.T) {
	got := PerRevisionHeadlessServiceName("llama", v1beta1.EngineComponent, "abcd1234")
	if got != "llama-engine-rev-abcd1234-headless" {
		t.Errorf("got %q want llama-engine-rev-abcd1234-headless", got)
	}
}

func TestBuildPerRevisionRoutingService_Properties(t *testing.T) {
	isvc := testISVC()
	svc, err := BuildPerRevisionRoutingService(isvc, v1beta1.EngineComponent, "hash1", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Spec.Type: got %q want ClusterIP", svc.Spec.Type)
	}
	if svc.Spec.ClusterIP == corev1.ClusterIPNone {
		t.Errorf("routing service must not be headless")
	}
	if svc.Spec.PublishNotReadyAddresses {
		t.Errorf("routing service should publish only ready addresses")
	}
	checkSelector(t, svc.Spec.Selector, "llama-70b", "engine", "hash1")
	// Single-pod routing Service must NOT carry the rank-0 filter —
	// SurgeThenDrain alternates pod-naming ordinals.
	if _, ok := svc.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("single-pod routing Service selector must NOT include %s; got %v",
			query.LabelPodOrdinal, svc.Spec.Selector)
	}
	checkOwnerRef(t, svc, isvc)
}

func TestBuildPerRevisionRoutingService_MultiPodPinsRank0(t *testing.T) {
	isvc := testISVC()
	svc, err := BuildPerRevisionRoutingService(isvc, v1beta1.EngineComponent, "hash1", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Multi-pod routing Service MUST carry the rank-0 selector so
	// customer traffic lands only on the leader pod.
	got, ok := svc.Spec.Selector[query.LabelPodOrdinal]
	if !ok || got != "0" {
		t.Errorf("multi-pod routing Service must have %s=0; got %q (ok=%v); selector=%v",
			query.LabelPodOrdinal, got, ok, svc.Spec.Selector)
	}
	// The other selector keys still hold.
	checkSelector(t, svc.Spec.Selector, "llama-70b", "engine", "hash1")
	// Service Labels (used for catalog/UI) intentionally exclude the
	// pod-ordinal filter — labels describe the Service itself, not
	// the pod membership filter.
	if _, ok := svc.Labels[query.LabelPodOrdinal]; ok {
		t.Errorf("Service Labels must NOT include %s; got %v",
			query.LabelPodOrdinal, svc.Labels)
	}
}

func TestBuildPerRevisionHeadlessService_Properties(t *testing.T) {
	isvc := testISVC()
	svc, err := BuildPerRevisionHeadlessService(isvc, v1beta1.EngineComponent, "hash1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("headless ClusterIP: got %q want None", svc.Spec.ClusterIP)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Errorf("headless must publish not-ready addresses")
	}
	checkSelector(t, svc.Spec.Selector, "llama-70b", "engine", "hash1")
	// Headless Service NEVER pins the rank-0 selector — workers must
	// stay discoverable for peer DNS during distributed init.
	if _, ok := svc.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("headless Service selector must NEVER include %s; got %v",
			query.LabelPodOrdinal, svc.Spec.Selector)
	}
	checkOwnerRef(t, svc, isvc)
}

func TestBuildPerRevisionRoutingService_RejectsNilISVC(t *testing.T) {
	if _, err := BuildPerRevisionRoutingService(nil, v1beta1.EngineComponent, "hash", false); err == nil {
		t.Errorf("nil ISVC: want error")
	}
}

func TestBuildPerRevisionRoutingService_RejectsEmptyHash(t *testing.T) {
	if _, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "", false); err == nil {
		t.Errorf("empty hash: want error")
	}
}

func TestEnsurePerRevisionServices_Idempotent(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", false)
	if err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	if out.RoutingName != "llama-70b-engine-rev-hash1" || out.HeadlessName != "llama-70b-engine-rev-hash1-headless" {
		t.Errorf("names: got %+v", out)
	}
	// Second call must not error and must not double-create.
	out2, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", false)
	if err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	if out2 != out {
		t.Errorf("idempotent ensure should produce same names: %+v vs %+v", out, out2)
	}
	// Verify both services are present.
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); err != nil {
		t.Errorf("routing service missing: %v", err)
	}
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.HeadlessName}, headless); err != nil {
		t.Errorf("headless service missing: %v", err)
	}
}

func TestEnsurePerRevisionServices_MultiPodTightensRoutingOnly(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", true /* multiPod */)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// Routing Service carries the rank-0 filter on its persisted shape.
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); err != nil {
		t.Fatalf("routing service missing: %v", err)
	}
	if got, ok := routing.Spec.Selector[query.LabelPodOrdinal]; !ok || got != "0" {
		t.Errorf("multi-pod routing Service selector must have %s=0; got %q (ok=%v)",
			query.LabelPodOrdinal, got, ok)
	}
	// runner=leader is required alongside pod-ordinal=0: ordinals are per
	// runner, so the rank-0 worker also carries pod-ordinal=0; without this the
	// routing Service would admit a worker.
	if got, ok := routing.Spec.Selector[query.LabelRunner]; !ok || got != string(v1beta1.RunnerNameLeader) {
		t.Errorf("multi-pod routing Service selector must have %s=leader; got %q (ok=%v)",
			query.LabelRunner, got, ok)
	}
	// Headless Service stays broad — workers must remain discoverable.
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.HeadlessName}, headless); err != nil {
		t.Fatalf("headless service missing: %v", err)
	}
	if _, ok := headless.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("headless Service selector must NEVER include %s; got %v",
			query.LabelPodOrdinal, headless.Spec.Selector)
	}
	if _, ok := headless.Spec.Selector[query.LabelRunner]; ok {
		t.Errorf("headless Service selector must NEVER include %s; got %v",
			query.LabelRunner, headless.Spec.Selector)
	}
}

func TestEnsurePerRevisionServices_NilClientErrors(t *testing.T) {
	_, err := EnsurePerRevisionServices(context.Background(), nil, testISVC(), v1beta1.EngineComponent, "hash", false)
	if err == nil {
		t.Errorf("nil client: want error")
	}
}

func TestGCPerRevisionServices_DeletesBoth(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", false)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := GCPerRevisionServices(context.Background(), c, isvc.Namespace, isvc.Name, v1beta1.EngineComponent, "hash1"); err != nil {
		t.Fatalf("GC: %v", err)
	}
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); !apierrors.IsNotFound(err) {
		t.Errorf("routing should be deleted: %v", err)
	}
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.HeadlessName}, headless); !apierrors.IsNotFound(err) {
		t.Errorf("headless should be deleted: %v", err)
	}
}

func TestGCPerRevisionServices_NotFoundIsNotAnError(t *testing.T) {
	c := fakeClient()
	if err := GCPerRevisionServices(context.Background(), c, "ns", "llama", v1beta1.EngineComponent, "missing"); err != nil {
		t.Errorf("GC of missing service: %v", err)
	}
}

func TestGCPerRevisionServices_NilClientErrors(t *testing.T) {
	if err := GCPerRevisionServices(context.Background(), nil, "ns", "llama", v1beta1.EngineComponent, "hash"); err == nil {
		t.Errorf("nil client: want error")
	}
}

func checkSelector(t *testing.T, sel map[string]string, isvc, component, hash string) {
	t.Helper()
	if sel[constants.InferenceServicePodLabelKey] != isvc {
		t.Errorf("selector inferenceservice: got %q want %q", sel[constants.InferenceServicePodLabelKey], isvc)
	}
	if sel[constants.OMEComponentLabel] != component {
		t.Errorf("selector component: got %q want %q", sel[constants.OMEComponentLabel], component)
	}
	if sel[query.LabelRevisionHash] != hash {
		t.Errorf("selector revision-hash: got %q want %q", sel[query.LabelRevisionHash], hash)
	}
	if sel[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("selector managed-by: got %q want %q", sel[query.LabelManagedBy], query.ManagedByOMENative)
	}
}

func checkOwnerRef(t *testing.T, svc *corev1.Service, isvc *v1beta1.InferenceService) {
	t.Helper()
	if len(svc.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(svc.OwnerReferences))
	}
	ref := svc.OwnerReferences[0]
	if ref.Name != isvc.Name || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef target: got Kind=%q Name=%q", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("Controller flag: want true, got %v", ref.Controller)
	}
}

func testISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			UID:       types.UID("llama-70b-uid"),
		},
	}
}

func fakeClient() client.Client {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}
