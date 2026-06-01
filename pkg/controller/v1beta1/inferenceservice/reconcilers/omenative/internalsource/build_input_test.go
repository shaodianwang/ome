package internalsource_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

func newFakeClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("add scheme: %v", err)
		}
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

// TestBuildReconcileInput_ProjectsIdentityAndObservedState exercises
// the read-side of the bridge: the Key + OwnerObject + OwnerGVK +
// EventTarget plumbing and the ObservedState round-trip via the
// workload<->v1beta1 converters.
func TestBuildReconcileInput_ProjectsIdentityAndObservedState(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			Labels:    map[string]string{"app": "llama"},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, Incarnation: 7, Phase: v1beta1.OMENativeInstanceReady, ActiveOrdinal: 1},
							{Index: 2, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating},
						},
					},
				},
			},
		},
	}
	c := newFakeClient(t, isvc)
	input := internalsource.BuildReconcileInput(internalsource.BuildInputDeps{
		ISVC:      isvc,
		Component: v1beta1.EngineComponent,
		Client:    c,
	})

	if input.OwnerObject != isvc || input.EventTarget != isvc {
		t.Errorf("OwnerObject/EventTarget: got %v / %v, want %v", input.OwnerObject, input.EventTarget, isvc)
	}
	if input.OwnerGVK != internalsource.ISVCGVK() {
		t.Errorf("OwnerGVK: got %+v, want %+v", input.OwnerGVK, internalsource.ISVCGVK())
	}
	if input.Key.Namespace != "prod" || input.Key.OwnerName != "llama-70b" || input.Key.Component != workload.ComponentEngine {
		t.Errorf("Key: got %+v", input.Key)
	}
	if got := input.Key.SelectorLabels[constants.InferenceServicePodLabelKey]; got != "llama-70b" {
		t.Errorf("SelectorLabels[%s]: got %q want llama-70b", constants.InferenceServicePodLabelKey, got)
	}
	if got := input.Key.SelectorLabels[constants.OMEComponentLabel]; got != string(v1beta1.EngineComponent) {
		t.Errorf("SelectorLabels[%s]: got %q want engine", constants.OMEComponentLabel, got)
	}
	if got := input.Key.SelectorLabels[query.LabelManagedBy]; got != query.ManagedByOMENative {
		t.Errorf("SelectorLabels[%s]: got %q want %q", query.LabelManagedBy, got, query.ManagedByOMENative)
	}
	if input.Key.OwnerLabels["app"] != "llama" {
		t.Errorf("OwnerLabels[app]: got %q want llama", input.Key.OwnerLabels["app"])
	}

	if got := len(input.ObservedState.InstanceStatuses); got != 2 {
		t.Fatalf("InstanceStatuses len: got %d want 2", got)
	}
	if s := input.ObservedState.InstanceStatuses[0]; s.Index != 0 || s.Incarnation != 7 || s.Phase != workload.InstancePhaseReady || s.ActiveOrdinal != 1 {
		t.Errorf("InstanceStatuses[0]: got %+v", s)
	}
	if s := input.ObservedState.InstanceStatuses[1]; s.Index != 2 || s.Phase != workload.InstancePhaseCreating {
		t.Errorf("InstanceStatuses[1]: got %+v", s)
	}
}

// TestBuildReconcileInput_MutateInstance covers the closure's persist
// path (commit lands on the apiserver) and the no-op short-circuit
// (returning false skips the round-trip and leaves status untouched).
func TestBuildReconcileInput_MutateInstance(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "n", Namespace: "ns"},
	}
	c := newFakeClient(t, isvc)
	input := internalsource.BuildReconcileInput(internalsource.BuildInputDeps{
		ISVC:      isvc,
		Component: v1beta1.EngineComponent,
		Client:    c,
	})

	// No-op mutate: returning false skips the apiserver write.
	if err := input.MutateInstance(context.Background(), 0, func(s *workload.InstanceStatus) bool { return false }); err != nil {
		t.Fatalf("MutateInstance(no-op): %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get: %v", err)
	}
	if cs, ok := fresh.Status.Components[v1beta1.EngineComponent]; ok && cs.Lifecycle != nil && len(cs.Lifecycle.InstanceStatuses) > 0 {
		t.Errorf("expected no status write on no-op; got InstanceStatuses=%+v", cs.Lifecycle.InstanceStatuses)
	}

	// Real mutate: workload-typed callback round-trips through the
	// v1beta1 status writer and the persisted record reflects the
	// mutation.
	if err := input.MutateInstance(context.Background(), 0, func(s *workload.InstanceStatus) bool {
		s.Phase = workload.InstancePhaseReady
		s.Incarnation = 4
		return true
	}); err != nil {
		t.Fatalf("MutateInstance(commit): %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get: %v", err)
	}
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	if cs.Lifecycle == nil || len(cs.Lifecycle.InstanceStatuses) != 1 {
		t.Fatalf("status missing: %+v", cs.Lifecycle)
	}
	got := cs.Lifecycle.InstanceStatuses[0]
	if got.Phase != v1beta1.OMENativeInstanceReady || got.Incarnation != 4 {
		t.Errorf("InstanceStatus[0]: got %+v", got)
	}
}
