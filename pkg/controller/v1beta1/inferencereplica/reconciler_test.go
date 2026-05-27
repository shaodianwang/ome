package inferencereplica

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// testScheme returns a runtime.Scheme with v1beta1 + core/v1 registered.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("v1beta1.AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	return s
}

// baselineIR returns a controller-write-stamped InferenceReplica
// matching the shape the ISVC controller will eventually project.
func baselineIR(name, namespace string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			// Stamp the controller-write annotation so the same fixture
			// would also pass the validating webhook in an end-to-end test.
			// The reconciler itself doesn't check the annotation.
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			Generation: 1,
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{
				Name: "llama",
				UID:  "00000000-0000-0000-0000-000000000001",
			},
			Component: v1beta1.EngineComponent,
			Runners: []v1beta1.Runner{
				{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
						},
					},
				},
			},
		},
	}
}

func newReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		// Status writes use the subresource path; the fake client must
		// be told to honor it for the new type.
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()

	return &Reconciler{
		Client: c,
		Log:    logf.Log.WithName("test"),
		Scheme: testScheme(t),
	}, c
}

func TestReconcile_NotFound_NoError(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"deleted IR between enqueue and reconcile should be a no-op")
	g.Expect(result.Requeue).To(gomega.BeFalse())
	g.Expect(result.RequeueAfter).To(gomega.BeZero())
}

func TestReconcile_StampsObservedGeneration(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod-models")
	r, c := newReconciler(t, ir)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.Requeue).To(gomega.BeFalse())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.ObservedGeneration).To(gomega.Equal(int64(1)),
		"skeleton should mirror metadata.generation into status.observedGeneration")
}

func TestReconcile_AlreadyInSync_ShortCircuit(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod-models")
	ir.Status.ObservedGeneration = ir.Generation
	r, c := newReconciler(t, ir)

	// Snapshot resourceVersion so we can verify the reconciler did not
	// write a no-op update (the cheap shortcut in reconciler.go).
	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		before)).To(gomega.Succeed())

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		after)).To(gomega.Succeed())

	g.Expect(after.ResourceVersion).To(gomega.Equal(before.ResourceVersion),
		"in-sync reconcile must skip the status update; otherwise we'd hot-loop on a no-op")
}

func TestReconcile_GenerationBump_Restamps(t *testing.T) {
	g := gomega.NewWithT(t)
	// Simulate an ISVC controller patch that bumped generation from 1 to
	// 2 but the IR controller hasn't reconciled yet (observedGeneration
	// still 1).
	ir := baselineIR("llama-engine", "prod-models")
	ir.Generation = 2
	ir.Status.ObservedGeneration = 1
	r, c := newReconciler(t, ir)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())

	g.Expect(got.Status.ObservedGeneration).To(gomega.Equal(int64(2)),
		"observedGeneration must follow metadata.generation across bumps")
}
