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
// ensureHeadlessService — exercises the wired-through path that
// reconciles BOTH the per-Component headless Service (via workload/)
// AND the per-Component stable Service (via the top-level
// service.ServiceReconciler).
//
// The stable Service shape itself (selector, ports, owner refs, type)
// is asserted in the top-level reconciler test
// (pkg/controller/v1beta1/inferenceservice/reconcilers/service/
// service_reconciler_test.go::TestServiceReconciler_OMENativeShape).
// Here we only assert that both Services land in the cluster.
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

func TestEnsureHeadlessService_CreatesBothServices(t *testing.T) {
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

func TestEnsureHeadlessService_NilPodSpecSkipsStableService(t *testing.T) {
	// MinReplicas=0 cold-start: PodSpec is nil, top-level reconciler
	// would emit a portless Service. We skip the stable-Service call
	// in that case (mirrors EnsurePodMonitor's behaviour) but still
	// emit the headless Service so peer-DNS discovery wiring is in
	// place for the eventual scale-up.
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
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}, got); err == nil {
		t.Errorf("stable Service should NOT be created with nil PodSpec")
	}
}
