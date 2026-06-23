package placement

import (
	"context"
	"errors"
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
	"knative.dev/pkg/apis"
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

func (f fakeClusters) Connected() []string {
	names := make([]string, 0, len(f.m))
	for n := range f.m {
		names = append(names, n)
	}
	return names
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

// emptyWorker is a fresh fake worker client (status subresource registered).
func emptyWorker(s *runtime.Scheme) client.WithWatch {
	return fakeclient.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
}

// workerWithAdmittedDerived returns a worker whose derived ISVC already reports
// an admitted engine instance (it won the race).
func workerWithAdmittedDerived(t *testing.T, s *runtime.Scheme) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent, true) // from admission_test.go
	derived.Namespace = "prod"
	derived.Name = "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(derived).Build()
	// Ensure the admitted status is stored (if the builder didn't persist it).
	cur := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if !AnyInstanceAdmitted(cur) {
		cur.Status = derived.Status
		require.NoError(t, w.Status().Update(context.Background(), cur))
	}
	return w
}

func newPlacer(s *runtime.Scheme, clusters ClusterClients, objs ...client.Object) (*Reconciler, client.Client) {
	c := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	return &Reconciler{Client: c, Scheme: s, Log: log.Log, Clusters: clusters, Requeue: time.Second}, c
}

func req() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "prod", Name: "svc"}}
}

func hasDerived(t *testing.T, w client.Client) bool {
	t.Helper()
	o := &v1beta1.InferenceService{}
	err := w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o)
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func cpPlacement(t *testing.T, cp client.Client) *v1beta1.PlacementStatus {
	t.Helper()
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	return o.Status.Placement
}

func workerWithAdmittedURL(t *testing.T, s *runtime.Scheme, host string) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent, true)
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status.URL = &apis.URL{Scheme: "https", Host: host}
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(derived).Build()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if cur.Status.URL == nil || !AnyInstanceAdmitted(cur) {
		cur.Status = derived.Status
		require.NoError(t, w.Status().Update(context.Background(), cur))
	}
	return w
}

func cpStatusURL(t *testing.T, cp client.Client) *apis.URL {
	t.Helper()
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	return o.Status.URL
}

func TestReconcile_NoCandidate(t *testing.T) {
	s := testScheme(t)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"), readyWC("cw", map[string]string{"gpu": "h100"}))
	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
}

func TestReconcile_FansOutToAllCandidates(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.True(t, hasDerived(t, wa), "derived on a")
	assert.True(t, hasDerived(t, wb), "derived on b")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
	assert.Empty(t, p.Cluster)
	assert.Len(t, p.Candidates, 2)
}

func TestReconcile_FirstAdmittedWinsAndDeletesLosers(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s) // b already admitted -> winner
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.False(t, hasDerived(t, wa), "loser a's derived deleted")
	assert.True(t, hasDerived(t, wb), "winner b's derived kept")
}

func TestReconcile_StickyWinner(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.False(t, hasDerived(t, wa), "sticky winner must NOT fan out to a")
	assert.True(t, hasDerived(t, wb))
}

func TestReconcile_ReplaceOnWinnerLost(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s) // b's derived is GONE (winner lost)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Empty(t, p.Cluster, "lost winner cleared")
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
	assert.True(t, hasDerived(t, wa), "re-fanned out to a")
	assert.True(t, hasDerived(t, wb), "re-fanned out to b")
}

func TestReconcile_WinnerFailedSurfacesFailed(t *testing.T) {
	s := testScheme(t)
	// b's derived ISVC reports an engine instance Phase=Failed.
	derivedFailed := isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	derivedFailed.Namespace = "prod"
	derivedFailed.Name = "svc"
	derivedFailed.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	wb := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(derivedFailed).Build()
	// Ensure the failed status is persisted.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, wb.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	cur.Status = derivedFailed.Status
	require.NoError(t, wb.Status().Update(context.Background(), cur))

	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc, readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseFailed, p.Phase, "placement escalated to Failed")
	assert.Equal(t, "b", p.Cluster, "failed cluster recorded")
	assert.True(t, hasDerived(t, wb), "failed derived kept (not deleted)")
}

func TestReconcile_StickyWinnerSweepsStrayLoser(t *testing.T) {
	s := testScheme(t)
	// a is connected and carries a STRAY derived (origin-labeled), not in status.
	wa := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}},
	}).Build()
	wb := workerWithAdmittedDerived(t, s) // winner b, admitted
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}}, // a is NOT listed
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, wa), "stray derived on non-winner cluster a must be swept even though it's not in status.Candidates")
	assert.True(t, hasDerived(t, wb), "winner b kept")
	p := cpPlacement(t, cp)
	assert.Equal(t, "b", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
}

func TestReconcile_DeleteRemovesDerivedEverywhere(t *testing.T) {
	s := testScheme(t)
	wa := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}},
	}).Build()
	wb := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b",
		Candidates: []v1beta1.CandidatePlacement{
			{Cluster: "a", Phase: v1beta1.CandidatePhasePlaced}, {Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted},
		},
	}
	r, cp := newPlacer(s, clusters, isvc)

	require.NoError(t, cp.Delete(context.Background(), isvc)) // finalizer -> DeletionTimestamp

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, wa), "derived removed from a")
	assert.False(t, hasDerived(t, wb), "derived removed from b")
	got := &v1beta1.InferenceService{}
	err = cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got)
	assert.True(t, apierrors.IsNotFound(err), "CP ISVC gone after finalizer cleared")
}

// createFailClient wraps a fake client but fails every Create (simulates a
// cluster that rejects the derived ISVC).
type createFailClient struct {
	client.WithWatch
}

func (c createFailClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return apierrors.NewInternalError(errors.New("create rejected"))
}

func TestReconcile_FanOutPartialFailureStillRaces(t *testing.T) {
	s := testScheme(t)
	// a rejects Create; b accepts.
	wa := createFailClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).Build()}
	wb := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "one bad cluster must not fail the whole reconcile")

	assert.True(t, hasDerived(t, wb), "healthy cluster b still got the derived")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
}

func TestReconcile_PublishesEndpointWhenAdmitted(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedURL(t, s, "svc.prod.b.example")
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Equal(t, "b", p.Cluster)
	assert.Equal(t, "https://svc.prod.b.example", p.Endpoint)
	u := cpStatusURL(t, cp)
	require.NotNil(t, u)
	assert.Equal(t, "svc.prod.b.example", u.Host)
}

func TestReconcile_PlacedButNoEndpointYet(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s) // admitted, but NO status.url
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Empty(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
}

func TestReconcile_ClearsEndpointOnWinnerLost(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s) // b's derived GONE
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: "https://old.example",
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	isvc.Status.URL = &apis.URL{Scheme: "https", Host: "old.example"}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
	assert.Empty(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
}
