package placement

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	return s
}

type fakeClusters struct {
	m map[string]workloadcluster.SelectivelyCachingClient
}

func (f fakeClusters) ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool) {
	c, ok := f.m[name]
	return c, ok
}

func readyWC(name string, labels map[string]string) *v1beta1.WorkloadCluster {
	return &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: v1beta1.WorkloadClusterStatus{Conditions: []metav1.Condition{
			{Type: v1beta1.WorkloadClusterReady, Status: metav1.ConditionTrue, Reason: "Reachable"},
		}},
	}
}

func srcISVC(selector string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "uid-1",
			Annotations: map[string]string{ClusterSelectorAnnotation: selector},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}},
		},
	}
}

func newPlacer(s *runtime.Scheme, clusters ClusterClients, objs ...client.Object) (*Reconciler, client.Client) {
	c := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	return &Reconciler{Client: c, Scheme: s, Log: log.Log, Clusters: clusters, Requeue: time.Second}, c
}

func req() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "prod", Name: "svc"}}
}

func TestReconcile_PlacesDerivedOnCandidate(t *testing.T) {
	s := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"cw": workloadcluster.NewNeverCachingClient(worker),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"), readyWC("cw", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	derived := &v1beta1.InferenceService{}
	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, derived))
	assert.NotEmpty(t, derived.Labels[PlacementOriginLabel])

	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	require.NotNil(t, got.Status.Placement)
	assert.Equal(t, "cw", got.Status.Placement.Cluster)
	assert.Equal(t, PhasePlaced, got.Status.Placement.Phase)
}

func TestReconcile_NoCandidate(t *testing.T) {
	s := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(s).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"cw": workloadcluster.NewNeverCachingClient(worker)}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"), readyWC("cw", map[string]string{"gpu": "h100"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	derived := &v1beta1.InferenceService{}
	err = worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, derived)
	assert.True(t, apierrors.IsNotFound(err))

	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	require.NotNil(t, got.Status.Placement)
	assert.Equal(t, PhasePending, got.Status.Placement.Phase)
}

func TestReconcile_DeleteRemovesDerived(t *testing.T) {
	s := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"cw": workloadcluster.NewNeverCachingClient(worker)}}

	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{Cluster: "cw", Phase: PhasePlaced}
	r, cp := newPlacer(s, clusters, isvc)

	require.NoError(t, cp.Delete(context.Background(), isvc)) // finalizer present -> sets DeletionTimestamp

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	derived := &v1beta1.InferenceService{}
	err = worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, derived)
	assert.True(t, apierrors.IsNotFound(err), "derived must be deleted from the worker")

	got := &v1beta1.InferenceService{}
	err = cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got)
	assert.True(t, apierrors.IsNotFound(err), "CP ISVC gone after finalizer cleared")
}
