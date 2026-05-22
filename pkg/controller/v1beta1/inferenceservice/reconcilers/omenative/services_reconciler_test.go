package omenative

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

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
	if got.Spec.Selector[labelManagedBy] != managedByOMENative {
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
	if fixed.Spec.Selector[labelManagedBy] != managedByOMENative {
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
