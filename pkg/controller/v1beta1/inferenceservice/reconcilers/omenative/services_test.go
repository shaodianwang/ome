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
// ensureHeadlessService — exercises the workload-delegated path that
// reconciles ONLY the per-Component headless Service.
//
// The per-Component stable Service + PodMonitor used to be reconciled
// here too, but moved out to the per-Component Reconcile() in
// components/{engine,decoder,router}.go's reconcileOMENativeSubresources
// method so they fire for both OMENative sub-paths (direct and
// IR-managed). The stable Service behavior is asserted in the
// per-Component tests (components/{engine,decoder,router}_test.go) and
// the shape itself in service/service_reconciler_test.go::
// TestServiceReconciler_OMENativeShape.
// ---------------------------------------------------------------------

// newServiceReconcilerTestClient builds a fake client wired with the
// scheme the stable + headless Service writes need: corev1.Service +
// the ISVC type (for owner-ref UID resolution).
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

// newServiceReconcilerPodSpec returns a PodSpec with a single container
// carrying a named HTTP port so the top-level service.ServiceReconciler
// can derive a non-empty ServicePort list from it. The omenative path
// historically hardcoded port 8080 regardless of PodSpec contents; the
// unified path mirrors that via the container's port declaration.
func newServiceReconcilerPodSpec() *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "runner",
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: 8080,
				Protocol:      corev1.ProtocolTCP,
			}},
		}},
	}
}

func TestEnsureHeadlessService_CreatesHeadlessService(t *testing.T) {
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{
		ISVC:      isvc,
		Component: v1beta1.EngineComponent,
		Client:    c,
		PodSpec:   newServiceReconcilerPodSpec(),
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b-engine",
			Namespace: "prod",
		},
	}

	if err := ensureHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("ensureHeadlessService: %v", err)
	}

	got := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}, got); err != nil {
		t.Errorf("missing headless Service: %v", err)
	}
	// Stable Service is no longer reconciled from ensureHeadlessService —
	// it moved out to the per-Component Reconcile() so it covers both
	// OMENative sub-paths uniformly. Verified in
	// components/{engine,decoder,router}_test.go.
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}, got); err == nil {
		t.Errorf("stable Service should NOT be created from ensureHeadlessService anymore — it was moved out")
	}
}

func TestEnsureHeadlessService_NilISVCRejected(t *testing.T) {
	c := newServiceReconcilerTestClient(t)
	p := ReconcileParams{Client: c, Component: v1beta1.EngineComponent}
	if err := ensureHeadlessService(context.Background(), p); err == nil {
		t.Fatalf("expected error on nil ISVC")
	}
}

func TestEnsureHeadlessService_NilPodSpecOK(t *testing.T) {
	// MinReplicas=0 cold-start: PodSpec is nil but the headless Service
	// MUST still be reconciled so peer-DNS discovery wiring is in place
	// for the eventual scale-up. The per-Component
	// reconcileOMENativeSubresources method skips both stable Service +
	// PodMonitor on nil PodSpec — that's covered in
	// components/{engine,decoder,router}_test.go.
	isvc := newServiceReconcilerISVC("prod", "llama-70b")
	c := newServiceReconcilerTestClient(t, isvc)
	p := ReconcileParams{
		ISVC:      isvc,
		Component: v1beta1.EngineComponent,
		Client:    c,
		PodSpec:   nil,
	}

	if err := ensureHeadlessService(context.Background(), p); err != nil {
		t.Fatalf("ensureHeadlessService: %v", err)
	}

	got := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}, got); err != nil {
		t.Errorf("headless Service should be created even with nil PodSpec: %v", err)
	}
}
