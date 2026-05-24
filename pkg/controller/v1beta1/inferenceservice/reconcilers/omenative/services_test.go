package omenative

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
)

// ---------------------------------------------------------------------
// BuildHeadlessService — pure-compute shape assertions.
// ---------------------------------------------------------------------

func TestBuildHeadlessService_NilISVC(t *testing.T) {
	if _, err := BuildHeadlessService(nil, v1beta1.EngineComponent); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
}

func TestBuildHeadlessService_Engine(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			UID:       types.UID("uid-1"),
		},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if svc.Name != "llama-70b-engine-headless" {
		t.Errorf("Name: got %q want llama-70b-engine-headless", svc.Name)
	}
	if svc.Namespace != "prod" {
		t.Errorf("Namespace: got %q want prod", svc.Namespace)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("ClusterIP: got %q want %q (none)", svc.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("PublishNotReadyAddresses: want true")
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Type: got %q want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 0 {
		t.Errorf("Ports: got %d want 0 (headless DNS-only service)", len(svc.Spec.Ports))
	}
}

func TestBuildHeadlessService_SelectorAndLabels(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.DecoderComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "decoder",
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	if diff := cmp.Diff(want, svc.Spec.Selector); diff != "" {
		t.Errorf("Selector (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(want, svc.Labels); diff != "" {
		t.Errorf("Labels (-want +got):\n%s", diff)
	}
}

func TestBuildHeadlessService_OwnerReference(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: types.UID("uid-1")},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(svc.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(svc.OwnerReferences))
	}
	ref := svc.OwnerReferences[0]
	if ref.Name != "llama-70b" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-70b", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
}

func TestBuildHeadlessService_NameVariesWithComponent(t *testing.T) {
	tests := []struct {
		component v1beta1.ComponentType
		wantName  string
	}{
		{v1beta1.EngineComponent, "fleet-engine-headless"},
		{v1beta1.DecoderComponent, "fleet-decoder-headless"},
		{v1beta1.RouterComponent, "fleet-router-headless"},
	}
	for _, tt := range tests {
		t.Run(string(tt.component), func(t *testing.T) {
			svc, err := BuildHeadlessService(&v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "fleet", Namespace: "prod"},
			}, tt.component)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if svc.Name != tt.wantName {
				t.Errorf("got %q want %q", svc.Name, tt.wantName)
			}
		})
	}
}

func TestBuildHeadlessService_SelectorMatchesRenderedPodLabels(t *testing.T) {
	// Pins the cross-file invariant: pod labels stamped by Render() are a
	// superset of the headless Service selector, so the Service publishes
	// every OMENative-managed pod of the same Component.
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"},
	}
	svc, err := BuildHeadlessService(isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The Service selector keys MUST be a subset of the labels Render
	// stamps on each pod. Duplicated inline (rather than calling
	// ops.podLabels) so the top-level test doesn't reach into the
	// ops/ subpackage's unexported helper.
	renderedPodLabels := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
		query.LabelInstanceIdx:                "0",
		query.LabelInstanceIncarnation:        "1",
		query.LabelRunner:                     "default",
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	for k, v := range svc.Spec.Selector {
		if got, ok := renderedPodLabels[k]; !ok || got != v {
			t.Errorf("selector %s=%q not present in pod labels (got %q, ok=%v)", k, v, got, ok)
		}
	}
}

// ---------------------------------------------------------------------
// reconcileHeadlessService — wired-through CreateOrUpdate behavior.
// ---------------------------------------------------------------------

// newServiceReconcilerTestClient builds a fake client wired with the
// scheme reconcileHeadlessService writes to: corev1.Service + the
// ISVC type (needed for owner-ref UID resolution).
func newServiceReconcilerTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func newServiceReconcilerISVC(ns, name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
	}
}

func TestReconcileHeadlessService_FreshCreate(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}

	if err := reconcileHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("first call: %v", err)
	}

	got := &corev1.Service{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get service after reconcile: %v", err)
	}

	if got.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("ClusterIP mismatch: got=%q want=%q", got.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if !got.Spec.PublishNotReadyAddresses {
		t.Errorf("PublishNotReadyAddresses should be true")
	}
	if got.Spec.Selector[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("managed-by selector missing or wrong: %v", got.Spec.Selector)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].UID != isvc.UID {
		t.Errorf("owner reference not stamped against ISVC: %v", got.OwnerReferences)
	}
}

func TestReconcileHeadlessService_Idempotent(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}

	if err := reconcileHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("first call: %v", err)
	}
	first := &corev1.Service{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}
	if err := c.Get(context.Background(), key, first); err != nil {
		t.Fatalf("get after first reconcile: %v", err)
	}
	firstRV := first.ResourceVersion

	// Second call must not bump ResourceVersion — there's nothing to
	// update because the desired shape already matches.
	if err := reconcileHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("second call: %v", err)
	}
	second := &corev1.Service{}
	if err := c.Get(context.Background(), key, second); err != nil {
		t.Fatalf("get after second reconcile: %v", err)
	}
	if second.ResourceVersion != firstRV {
		t.Errorf("Service was updated on second idempotent reconcile (RV %s -> %s)", firstRV, second.ResourceVersion)
	}
}

func TestReconcileHeadlessService_DriftCorrection(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}

	if err := reconcileHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("seed reconcile: %v", err)
	}

	// Simulate an operator flipping PublishNotReadyAddresses to false.
	// The next reconcile must fix it.
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Fatalf("get before mutation: %v", err)
	}
	svc.Spec.PublishNotReadyAddresses = false
	svc.Spec.Selector = map[string]string{"hacked": "true"}
	if err := c.Update(context.Background(), svc); err != nil {
		t.Fatalf("mutate service: %v", err)
	}

	if err := reconcileHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("drift-correcting reconcile: %v", err)
	}

	fixed := &corev1.Service{}
	if err := c.Get(context.Background(), key, fixed); err != nil {
		t.Fatalf("get after drift correction: %v", err)
	}
	if !fixed.Spec.PublishNotReadyAddresses {
		t.Errorf("drift not corrected: PublishNotReadyAddresses still false")
	}
	if fixed.Spec.Selector[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("drift not corrected: selector still %v", fixed.Spec.Selector)
	}
}

func TestReconcileHeadlessService_NilClientRejected(t *testing.T) {
	p := ReconcileParams{ISVC: newServiceReconcilerISVC("ns", "x")}
	if err := reconcileHeadlessService(context.Background(), p); err == nil {
		t.Fatalf("expected error on nil client")
	}
}

func TestReconcileHeadlessService_NilISVCRejected(t *testing.T) {
	c := newServiceReconcilerTestClient(t)
	p := ReconcileParams{Client: c}
	if err := reconcileHeadlessService(context.Background(), p); err == nil {
		t.Fatalf("expected error on nil ISVC")
	}
}
