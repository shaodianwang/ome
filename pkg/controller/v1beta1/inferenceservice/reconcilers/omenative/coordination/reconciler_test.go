package coordination

import (
	"context"
	"testing"
	"time"

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

func TestReconcile_NilISVCErrors(t *testing.T) {
	_, err := Reconcile(context.Background(), ReconcileInputs{Client: testClient()})
	if err == nil {
		t.Errorf("nil ISVC: want error")
	}
}

func TestReconcile_NilClientErrors(t *testing.T) {
	_, err := Reconcile(context.Background(), ReconcileInputs{ISVC: testReconcilerISVC()})
	if err == nil {
		t.Errorf("nil Client: want error")
	}
}

func TestReconcile_NoOpForNonOMENativeISVC(t *testing.T) {
	isvc := testReconcilerISVC()
	// No engine/decoder OMENative annotation.
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:   isvc,
		Client: testClient(),
		Reader: testClient(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured != 0 {
		t.Errorf("should not emit Services for non-OMENative ISVC: got %d", r.PerRevisionServicesEnsured)
	}
}

func TestReconcile_EmitsPerRevisionServiceForOMENativeComponent(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.RolloutCoordination = independentEngineGroup()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 1),
	}
	c := testClient(pods...)
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:   isvc,
		Client: c,
		Reader: c,
		Now:    time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("should ensure per-revision Services: got %+v", r)
	}
	// Verify the Service exists.
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Errorf("per-revision Service missing: %v", err)
	}
}

func TestReconcile_DataPlaneRunsWithoutCoordinationGroup(t *testing.T) {
	// Updated contract: per-revision Services + Traffic[] are the
	// data plane for OEP-0011 HTTPRoute weighted-backendRef consumers.
	// They emit for every OMENative-mode Component, with or without a
	// rolloutCoordination group declared. Only the per-group state
	// machine (Status.RolloutCoordination, RolloutCoordinationReady
	// condition, group events) stays opt-in via the rolloutCoordination
	// block.
	isvc := testOMENativeISVC()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
	}
	c := testClient(pods...)
	r, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.PerRevisionServicesEnsured == 0 {
		t.Errorf("no groups: per-revision Services should still emit for the data plane, got %d", r.PerRevisionServicesEnsured)
	}
	if isvc.Status.RolloutCoordination != nil {
		t.Errorf("no groups: should not write RolloutCoordination status, got %+v", isvc.Status.RolloutCoordination)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok || len(cs.Traffic) == 0 {
		t.Errorf("no groups: Traffic[] should still populate from observed pods, got %+v", cs)
	}
}

func TestReconcile_WritesTrafficStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.RolloutCoordination = independentEngineGroup()
	pods := []runtime.Object{
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 0),
		buildPod(isvc, v1beta1.EngineComponent, "hash1", 1),
	}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok {
		t.Fatalf("Components[engine] not populated")
	}
	if len(cs.Traffic) != 1 {
		t.Fatalf("Traffic: got %d entries want 1", len(cs.Traffic))
	}
	if cs.Traffic[0].RevisionName != "llama-engine-rev-hash1" {
		t.Errorf("Traffic[0].RevisionName: got %q want llama-engine-rev-hash1", cs.Traffic[0].RevisionName)
	}
	if cs.Traffic[0].Percent != 100 {
		t.Errorf("Traffic[0].Percent: got %d want 100", cs.Traffic[0].Percent)
	}
}

// independentEngineGroup returns a minimal RolloutCoordinationSpec
// that opts the engine Component into the coordination layer without
// changing rollout semantics (Independent matches today's defaults).
func independentEngineGroup() *v1beta1.RolloutCoordinationSpec {
	return &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyIndependent,
			},
		},
	}
}

func TestReconcile_WritesGroupStatus(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyIndependent},
		},
	}
	pods := []runtime.Object{buildPod(isvc, v1beta1.EngineComponent, "hash1", 0)}
	c := testClient(pods...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isvc.Status.RolloutCoordination == nil {
		t.Fatalf("RolloutCoordination status nil")
	}
	if len(isvc.Status.RolloutCoordination.Groups) != 1 {
		t.Fatalf("groups: got %d want 1", len(isvc.Status.RolloutCoordination.Groups))
	}
	g := isvc.Status.RolloutCoordination.Groups[0]
	if g.Name != "0" {
		t.Errorf("Name: got %q want 0", g.Name)
	}
	if g.Phase == "" {
		t.Errorf("Phase should be populated")
	}
}

func TestReconcile_InvalidGroupShapeErrors(t *testing.T) {
	isvc := testOMENativeISVC()
	// Sequential without order — Should be caught by webhook in
	// production, but ValidateGroupShape is the runtime safety net.
	isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
		Groups: []v1beta1.RolloutCoordinationGroup{
			{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicySequential,
			},
		},
	}
	c := testClient()
	_, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
	})
	if err == nil {
		t.Errorf("Sequential without order: want error")
	}
}

func testOMENativeISVC() *v1beta1.InferenceService {
	isvc := testReconcilerISVC()
	isvc.Spec.Engine = &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	return isvc
}

func testReconcilerISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama",
			Namespace: "prod",
			UID:       types.UID("llama-uid"),
		},
	}
}

func buildPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int) runtime.Object {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(isvc.Name, component, idx),
			Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               revisionHash,
			},
		},
	}
}

func podName(isvc string, component v1beta1.ComponentType, idx int) string {
	// Test pod names — don't need the full Loop A pattern.
	return isvc + "-" + string(component) + "-pod-" + itoa(idx)
}

func itoa(i int) string {
	switch i {
	case 0:
		return "0"
	case 1:
		return "1"
	case 2:
		return "2"
	default:
		return "n"
	}
}

func testClient(objs ...runtime.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
}
