package omenative

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
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

func newPodMonitorTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := monitoringv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add monitoringv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func newPodMonitorISVC(ns, name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-uid")},
	}
}

func newPodSpec(ports ...corev1.ContainerPort) *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "engine", Ports: ports}},
	}
}

// ---------------------------------------------------------------------
// BuildPodMonitor — pure-compute shape assertions.
// ---------------------------------------------------------------------

func TestBuildPodMonitor_NilISVC(t *testing.T) {
	if _, err := BuildPodMonitor(nil, v1beta1.EngineComponent, newPodSpec()); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
}

func TestBuildPodMonitor_Shape(t *testing.T) {
	isvc := newPodMonitorISVC("prod", "llama-70b")
	pm, err := BuildPodMonitor(isvc, v1beta1.EngineComponent, newPodSpec(corev1.ContainerPort{Name: "http", ContainerPort: 8080}))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if pm.Name != "llama-70b-engine" {
		t.Errorf("Name: got %q want llama-70b-engine", pm.Name)
	}
	if pm.Namespace != "prod" {
		t.Errorf("Namespace: got %q want prod", pm.Namespace)
	}
	wantSelector := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "engine",
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	if diff := cmp.Diff(wantSelector, pm.Spec.Selector.MatchLabels); diff != "" {
		t.Errorf("Selector (-want +got):\n%s", diff)
	}
	if len(pm.Spec.NamespaceSelector.MatchNames) != 1 || pm.Spec.NamespaceSelector.MatchNames[0] != "prod" {
		t.Errorf("NamespaceSelector: got %+v want [prod]", pm.Spec.NamespaceSelector.MatchNames)
	}
	if len(pm.Spec.PodMetricsEndpoints) != 1 || pm.Spec.PodMetricsEndpoints[0].Port == nil ||
		*pm.Spec.PodMetricsEndpoints[0].Port != "http" || pm.Spec.PodMetricsEndpoints[0].Path != "/metrics" {
		t.Errorf("PodMetricsEndpoints: got %+v want single http/metrics", pm.Spec.PodMetricsEndpoints)
	}
}

func TestBuildPodMonitor_PrefersMetricsPort(t *testing.T) {
	isvc := newPodMonitorISVC("prod", "llama-70b")
	pm, err := BuildPodMonitor(isvc, v1beta1.EngineComponent, newPodSpec(
		corev1.ContainerPort{Name: "http", ContainerPort: 8080},
		corev1.ContainerPort{Name: "metrics", ContainerPort: 9090},
	))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got := *pm.Spec.PodMetricsEndpoints[0].Port; got != "metrics" {
		t.Errorf("port: got %q want metrics", got)
	}
}

func TestBuildPodMonitor_DefaultsToHTTP(t *testing.T) {
	isvc := newPodMonitorISVC("prod", "llama-70b")
	pm, err := BuildPodMonitor(isvc, v1beta1.EngineComponent, newPodSpec())
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got := *pm.Spec.PodMetricsEndpoints[0].Port; got != "http" {
		t.Errorf("port: got %q want http", got)
	}
}

func TestBuildPodMonitor_OwnerReference(t *testing.T) {
	isvc := newPodMonitorISVC("prod", "llama-70b")
	pm, err := BuildPodMonitor(isvc, v1beta1.EngineComponent, newPodSpec())
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(pm.OwnerReferences) != 1 || pm.OwnerReferences[0].UID != isvc.UID {
		t.Errorf("OwnerReference not stamped: %+v", pm.OwnerReferences)
	}
}

// ---------------------------------------------------------------------
// reconcilePodMonitor — wired-through CreateOrUpdate behavior.
// ---------------------------------------------------------------------

func TestReconcilePodMonitor_FreshCreate(t *testing.T) {
	isvc := newPodMonitorISVC("prod", "llama-70b")
	c := newPodMonitorTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, PodSpec: newPodSpec(), Client: c}

	if err := reconcilePodMonitor(context.Background(), p); err != nil {
		t.Fatalf("create: %v", err)
	}
	got := &monitoringv1.PodMonitor{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get after reconcile: %v", err)
	}
	if got.Spec.Selector.MatchLabels[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("selector missing managed-by: %+v", got.Spec.Selector)
	}
}

func TestReconcilePodMonitor_Idempotent(t *testing.T) {
	isvc := newPodMonitorISVC("prod", "llama-70b")
	c := newPodMonitorTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, PodSpec: newPodSpec(), Client: c}

	if err := reconcilePodMonitor(context.Background(), p); err != nil {
		t.Fatalf("first: %v", err)
	}
	first := &monitoringv1.PodMonitor{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}
	if err := c.Get(context.Background(), key, first); err != nil {
		t.Fatalf("get first: %v", err)
	}
	firstRV := first.ResourceVersion
	if err := reconcilePodMonitor(context.Background(), p); err != nil {
		t.Fatalf("second: %v", err)
	}
	second := &monitoringv1.PodMonitor{}
	if err := c.Get(context.Background(), key, second); err != nil {
		t.Fatalf("get second: %v", err)
	}
	if second.ResourceVersion != firstRV {
		t.Errorf("PodMonitor updated on idempotent reconcile (RV %s -> %s)", firstRV, second.ResourceVersion)
	}
}

func TestReconcilePodMonitor_DriftCorrection(t *testing.T) {
	isvc := newPodMonitorISVC("prod", "llama-70b")
	c := newPodMonitorTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, PodSpec: newPodSpec(), Client: c}

	if err := reconcilePodMonitor(context.Background(), p); err != nil {
		t.Fatalf("seed: %v", err)
	}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}
	pm := &monitoringv1.PodMonitor{}
	if err := c.Get(context.Background(), key, pm); err != nil {
		t.Fatalf("get: %v", err)
	}
	pm.Spec.Selector = metav1.LabelSelector{MatchLabels: map[string]string{"hacked": "true"}}
	if err := c.Update(context.Background(), pm); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	if err := reconcilePodMonitor(context.Background(), p); err != nil {
		t.Fatalf("correct: %v", err)
	}
	fixed := &monitoringv1.PodMonitor{}
	if err := c.Get(context.Background(), key, fixed); err != nil {
		t.Fatalf("get fixed: %v", err)
	}
	if fixed.Spec.Selector.MatchLabels[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("drift not corrected: %+v", fixed.Spec.Selector)
	}
}

func TestEnsurePodMonitor_NilPodSpecSkips(t *testing.T) {
	// MinReplicas=0 path: no rendered PodSpec → no scrape target → no
	// PodMonitor. Must be a no-op, not an error.
	isvc := newPodMonitorISVC("prod", "llama-70b")
	c := newPodMonitorTestClient(t, isvc)
	p := ReconcileParams{ISVC: isvc, Component: v1beta1.EngineComponent, Client: c}
	if err := EnsurePodMonitor(context.Background(), p); err != nil {
		t.Fatalf("expected no-op, got error: %v", err)
	}
	got := &monitoringv1.PodMonitor{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine"}, got); err == nil {
		t.Errorf("PodMonitor should not have been created when PodSpec is nil")
	}
}
