package ops

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
)

// Shared test fixtures used by the surviving *_test.go in the
// omenative/ops package (migrate_test.go). The per-Instance
// state-machine helpers that the deleted dispatch / restart / timeouts
// / update tests relied on were removed alongside those tests when the
// legacy OMENative direct path was retired.

// minimalISVC builds an ISVC with the minimum metadata the detectors need.
func minimalISVC(name, ns string, replicas int) *v1beta1.InferenceService {
	mr := replicas
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &mr,
				},
			},
		},
	}
}

// minimalParams builds a core.ReconcileParams in the single-pod / engine shape.
func minimalParams(t *testing.T, isvc *v1beta1.InferenceService, c client.Client) core.ReconcileParams {
	t.Helper()
	return core.ReconcileParams{
		ISVC:         isvc,
		PodSpec:      &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		Client:       c,
	}
}

// newFakeClientWithDiscovery builds a controller-runtime fake client with
// the schemes the omenative/ops tests touch (corev1 + v1beta1 +
// discoveryv1 + appsv1). The status subresource is registered so
// Status().Update mirrors API-server behavior. Detectors read per-instance
// state from the authoritative InferenceReplica, so fixtures seed the IR
// directly — pass it alongside the ISVC.
func newFakeClientWithDiscovery(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}
