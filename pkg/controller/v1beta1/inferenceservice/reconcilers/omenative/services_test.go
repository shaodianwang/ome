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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// ---------------------------------------------------------------------
// BuildHeadlessServiceSpec — adapter that builds the typed
// workload.PerComponentServiceSpec from an ISVC + ComponentType.
// The pure-compute BuildHeadlessService + reconcileHeadlessService
// tests live in workload/services_test.go alongside their implementation.
// ---------------------------------------------------------------------

func TestBuildHeadlessServiceSpec_NameAndNamespace(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			UID:       types.UID("uid-1"),
		},
	}
	spec := BuildHeadlessServiceSpec(isvc, v1beta1.EngineComponent)
	if spec.Name != "llama-70b-engine-headless" {
		t.Errorf("Name: got %q want llama-70b-engine-headless", spec.Name)
	}
	if spec.Namespace != "prod" {
		t.Errorf("Namespace: got %q want prod", spec.Namespace)
	}
}

func TestBuildHeadlessServiceSpec_SelectorAndLabels(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"},
	}
	spec := BuildHeadlessServiceSpec(isvc, v1beta1.DecoderComponent)
	want := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "decoder",
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	if diff := cmp.Diff(want, spec.Selector); diff != "" {
		t.Errorf("Selector (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(want, spec.Labels); diff != "" {
		t.Errorf("Labels (-want +got):\n%s", diff)
	}
}

func TestBuildHeadlessServiceSpec_OwnerReference(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: types.UID("uid-1")},
	}
	spec := BuildHeadlessServiceSpec(isvc, v1beta1.EngineComponent)
	if len(spec.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(spec.OwnerReferences))
	}
	ref := spec.OwnerReferences[0]
	if ref.Name != "llama-70b" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-70b", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
}

func TestBuildHeadlessServiceSpec_NameVariesWithComponent(t *testing.T) {
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
			spec := BuildHeadlessServiceSpec(&v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "fleet", Namespace: "prod"},
			}, tt.component)
			if spec.Name != tt.wantName {
				t.Errorf("got %q want %q", spec.Name, tt.wantName)
			}
		})
	}
}

func TestBuildHeadlessServiceSpec_SelectorMatchesRenderedPodLabels(t *testing.T) {
	// Pins the cross-file invariant: pod labels stamped by Render() are a
	// superset of the headless Service selector, so the Service publishes
	// every OMENative-managed pod of the same Component.
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"},
	}
	spec := BuildHeadlessServiceSpec(isvc, v1beta1.EngineComponent)
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
	for k, v := range spec.Selector {
		if got, ok := renderedPodLabels[k]; !ok || got != v {
			t.Errorf("selector %s=%q not present in pod labels (got %q, ok=%v)", k, v, got, ok)
		}
	}
}

// ---------------------------------------------------------------------
// BuildStableService — pure-compute shape assertions.
// ---------------------------------------------------------------------

// newServiceReconcilerTestClient builds a fake client wired with the
// scheme reconcileStableService writes to: corev1.Service + the
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

func TestBuildStableService_NilISVC(t *testing.T) {
	if _, err := BuildStableService(nil, v1beta1.EngineComponent); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
}

func TestBuildStableService_Engine(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: types.UID("uid-1")},
	}
	svc, err := BuildStableService(isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Name must match what RawDeployment / MultiNode use so the ingress
	// strategies — which target <isvc>-<comp> — find OMENative's
	// Service the same way.
	if svc.Name != "llama-70b-engine" {
		t.Errorf("Name: got %q want llama-70b-engine", svc.Name)
	}
	if svc.Spec.ClusterIP == corev1.ClusterIPNone {
		t.Errorf("ClusterIP should not be None (stable Service must be load-balanceable)")
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Type: got %q want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8080 {
		t.Errorf("Ports: got %+v want single 8080/http", svc.Spec.Ports)
	}
}

func TestBuildStableService_SelectorMatchesHeadlessSpec(t *testing.T) {
	// The stable Service must select the same pod set as the headless
	// Service spec — both publish every OMENative pod for the Component
	// regardless of revision. Drift between the two selectors would
	// make ingress routing inconsistent with peer-DNS discovery.
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod"},
	}
	stable, err := BuildStableService(isvc, v1beta1.DecoderComponent)
	if err != nil {
		t.Fatalf("stable: %v", err)
	}
	headlessSpec := BuildHeadlessServiceSpec(isvc, v1beta1.DecoderComponent)
	if diff := cmp.Diff(headlessSpec.Selector, stable.Spec.Selector); diff != "" {
		t.Errorf("selector divergence between stable and headless Services (-headless +stable):\n%s", diff)
	}
}

func TestBuildStableService_NameVariesWithComponent(t *testing.T) {
	tests := []struct {
		component v1beta1.ComponentType
		wantName  string
	}{
		{v1beta1.EngineComponent, "fleet-engine"},
		{v1beta1.DecoderComponent, "fleet-decoder"},
		{v1beta1.RouterComponent, "fleet-router"},
	}
	for _, tt := range tests {
		t.Run(string(tt.component), func(t *testing.T) {
			svc, err := BuildStableService(&v1beta1.InferenceService{
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

// ---------------------------------------------------------------------
// reconcileStableService — wired-through CreateOrUpdate behavior.
// ---------------------------------------------------------------------

func TestReconcileStableService_FreshCreate(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}

	if err := reconcileStableService(context.Background(), p); err != nil {
		t.Fatalf("first call: %v", err)
	}

	got := &corev1.Service{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get service after reconcile: %v", err)
	}

	if got.Spec.ClusterIP == corev1.ClusterIPNone {
		t.Errorf("ClusterIP should not be None")
	}
	if got.Spec.Selector[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("managed-by selector missing or wrong: %v", got.Spec.Selector)
	}
	if len(got.Spec.Ports) != 1 || got.Spec.Ports[0].Port != 8080 {
		t.Errorf("Ports: got %+v want single 8080", got.Spec.Ports)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].UID != isvc.UID {
		t.Errorf("owner reference not stamped against ISVC: %v", got.OwnerReferences)
	}
}

func TestReconcileStableService_Idempotent(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}

	if err := reconcileStableService(context.Background(), p); err != nil {
		t.Fatalf("first call: %v", err)
	}
	first := &corev1.Service{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}
	if err := c.Get(context.Background(), key, first); err != nil {
		t.Fatalf("get after first reconcile: %v", err)
	}
	firstRV := first.ResourceVersion

	if err := reconcileStableService(context.Background(), p); err != nil {
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

func TestReconcileStableService_DriftCorrection(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}

	if err := reconcileStableService(context.Background(), p); err != nil {
		t.Fatalf("seed reconcile: %v", err)
	}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Fatalf("get before mutation: %v", err)
	}
	svc.Spec.Selector = map[string]string{"hacked": "true"}
	if err := c.Update(context.Background(), svc); err != nil {
		t.Fatalf("mutate service: %v", err)
	}

	if err := reconcileStableService(context.Background(), p); err != nil {
		t.Fatalf("drift-correcting reconcile: %v", err)
	}
	fixed := &corev1.Service{}
	if err := c.Get(context.Background(), key, fixed); err != nil {
		t.Fatalf("get after drift correction: %v", err)
	}
	if fixed.Spec.Selector[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("drift not corrected: selector still %v", fixed.Spec.Selector)
	}
}

func TestReconcileStableService_NilClientRejected(t *testing.T) {
	p := ReconcileParams{ISVC: newServiceReconcilerISVC("ns", "x")}
	if err := reconcileStableService(context.Background(), p); err == nil {
		t.Fatalf("expected error on nil client")
	}
}

func TestReconcileStableService_NilISVCRejected(t *testing.T) {
	c := newServiceReconcilerTestClient(t)
	p := ReconcileParams{Client: c}
	if err := reconcileStableService(context.Background(), p); err == nil {
		t.Fatalf("expected error on nil ISVC")
	}
}

// ---------------------------------------------------------------------
// ensureHeadlessService — covers both the workload-owned headless
// Service and the omenative-owned stable Service in one call.
// ---------------------------------------------------------------------

func TestEnsureHeadlessService_CreatesBothServices(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}

	if err := ensureHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("ensureHeadlessService: %v", err)
	}

	for _, name := range []string{"llama-70b-engine-headless", "llama-70b-engine"} {
		got := &corev1.Service{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: name}, got); err != nil {
			t.Errorf("missing Service %q: %v", name, err)
		}
	}
}

func TestEnsureHeadlessService_NilISVCRejected(t *testing.T) {
	c := newServiceReconcilerTestClient(t)
	p := ReconcileParams{Client: c, Component: v1beta1.EngineComponent}
	if err := ensureHeadlessService(context.Background(), p); err == nil {
		t.Fatalf("expected error on nil ISVC")
	}
}
