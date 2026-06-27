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

// When the winner's derived is gone on a CONNECTED winner cluster, the
// controller holds the existing placement for the grace window (riding out a
// transient gap) before re-racing. With a non-trivial grace, the first pass
// holds: the winner is retained, no re-fan-out, and a requeue is scheduled.
func TestReconcile_WinnerLostWithinGraceHolds(t *testing.T) {
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
	r.WinnerLostGracePeriod = time.Minute // non-trivial: first pass is within grace

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "requeues to re-check after grace")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster, "winner held during grace")
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase, "phase held during grace")
	assert.False(t, hasDerived(t, wa), "must NOT re-fan-out to a within grace")
}

// Once the winner-lost grace window has elapsed (the derived stayed gone on the
// connected winner), the controller gives up on the winner and re-races: it
// clears the winner and re-fans-out to every candidate.
func TestReconcile_ReplaceOnWinnerLostAfterGrace(t *testing.T) {
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
	r.WinnerLostGracePeriod = time.Minute
	// Pre-seed the grace clock so the window is already elapsed on this pass.
	r.winnerLostSince.Store(isvc.UID, time.Now().Add(-2*time.Minute))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Empty(t, p.Cluster, "lost winner cleared after grace")
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
	assert.True(t, hasDerived(t, wa), "re-fanned out to a")
	assert.True(t, hasDerived(t, wb), "re-fanned out to b")
	// Marker cleared once re-raced.
	_, ok := r.winnerLostSince.Load(isvc.UID)
	assert.False(t, ok, "grace marker cleared after re-race")
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
	require.NotNil(t, p.Endpoint)
	assert.Equal(t, "svc.prod.b.example", p.Endpoint.Host)
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
	assert.Nil(t, p.Endpoint)
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
	oldURL := &apis.URL{Scheme: "https", Host: "old.example"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: oldURL,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	isvc.Status.URL = oldURL
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute
	// Grace already elapsed: the endpoint is cleared only once we genuinely
	// re-race (a transient gap within grace keeps the sticky endpoint).
	r.winnerLostSince.Store(isvc.UID, time.Now().Add(-2*time.Minute))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
	assert.Nil(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
}

// Within the grace window the sticky endpoint MUST survive a transient
// winner-derived gap: an external LB watching status.url must not see a spurious
// deroute on a momentary blip.
func TestReconcile_StickyEndpointSurvivesWinnerGapWithinGrace(t *testing.T) {
	s := testScheme(t)
	wb := emptyWorker(s) // b's derived GONE this pass (transient)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	oldURL := &apis.URL{Scheme: "https", Host: "svc.prod.b.example"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: oldURL,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	isvc.Status.URL = oldURL
	r, cp := newPlacer(s, clusters, isvc, readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute // within grace on first pass

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase, "placement held during grace")
	require.NotNil(t, p.Endpoint, "sticky endpoint survives a transient gap within grace")
	assert.Equal(t, "svc.prod.b.example", p.Endpoint.Host)
}

func TestReconcile_StickyEndpointPersistsAcrossTransientGap(t *testing.T) {
	s := testScheme(t)
	wb := workerWithAdmittedDerived(t, s) // admitted, NO status.url this pass
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	prior := &apis.URL{Scheme: "https", Host: "svc.prod.b.example"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: prior,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	isvc.Status.URL = prior
	r, cp := newPlacer(s, clusters, isvc, readyWC("b", map[string]string{"gpu": "gb300"}))

	// The newPlacer fake client should persist the initial status, but ensure it:
	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	if got.Status.URL == nil || got.Status.Placement == nil {
		got.Status = isvc.Status
		require.NoError(t, cp.Status().Update(context.Background(), got))
	}

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	require.NotNil(t, p.Endpoint, "sticky: endpoint must survive a transient worker-URL gap")
	assert.Equal(t, "svc.prod.b.example", p.Endpoint.Host)
	require.NotNil(t, cpStatusURL(t, cp))
}

func TestReconcile_EndpointPreservedOnFailed(t *testing.T) {
	s := testScheme(t)
	// derived: engine instance Failed, but a status.url is present.
	derived := isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status.URL = &apis.URL{Scheme: "https", Host: "svc.prod.b.example"}
	wb := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(derived).Build()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, wb.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if cur.Status.URL == nil || !IsTerminallyFailed(cur) {
		cur.Status = derived.Status
		require.NoError(t, wb.Status().Update(context.Background(), cur))
	}
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
	assert.Equal(t, v1beta1.PlacementPhaseFailed, p.Phase)
	require.NotNil(t, p.Endpoint, "Failed placement keeps routing to its still-addressable URL")
	assert.Equal(t, "svc.prod.b.example", p.Endpoint.Host)
}

func TestEndpointFor_NilSafe(t *testing.T) {
	assert.Nil(t, endpointFor(nil))
	assert.Nil(t, endpointFor(&v1beta1.InferenceService{}))
}

// deleteFailClient fails every Delete (simulates a cluster that errors on
// loser cleanup).
type deleteFailClient struct {
	client.WithWatch
}

func (c deleteFailClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return apierrors.NewInternalError(errors.New("delete rejected"))
}

// getFailClient fails every Get (simulates a transient remote read error).
type getFailClient struct {
	client.WithWatch
}

func (c getFailClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return apierrors.NewInternalError(errors.New("get rejected"))
}

// foreignDerived builds a same-named ISVC NOT created by this control plane (no
// origin label, or an origin label for a different source UID).
func foreignDerived(originUID string) *v1beta1.InferenceService {
	o := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"}}
	if originUID != "" {
		o.Labels = map[string]string{PlacementOriginLabel: originUID}
	}
	return o
}

// Bug #1 (cross-tenant DATA LOSS): the loser sweep must NOT delete a same-named
// ISVC on a connected NON-candidate cluster that this control plane did not
// derive. Pre-fix deleteLosers iterated ALL connected clusters and
// deleteDerivedOn deleted by identity, so a user's same-named ISVC on an
// unrelated connected cluster was reaped.
func TestReconcile_LoserSweepDoesNotTouchForeignISVCOnNonCandidate(t *testing.T) {
	s := testScheme(t)
	// "other" is connected but NOT a candidate (label does not match the
	// selector) and carries a user-created same-named ISVC with no origin label.
	wOther := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(foreignDerived("")).Build()
	wb := workerWithAdmittedDerived(t, s) // winner (candidate)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"other": workloadcluster.NewNeverCachingClient(wOther),
		"b":     workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("other", map[string]string{"gpu": "h100"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.True(t, hasDerived(t, wOther), "foreign same-named ISVC on non-candidate connected cluster must NOT be deleted")
	assert.True(t, hasDerived(t, wb), "winner kept")
	assert.Equal(t, "b", cpPlacement(t, cp).Cluster)
}

// Bug #1 variant: even on a candidate (loser) cluster, an object derived from a
// DIFFERENT source UID must survive the origin-guarded sweep.
func TestReconcile_LoserSweepDoesNotDeleteOtherSourceDerived(t *testing.T) {
	s := testScheme(t)
	// a is a candidate and holds an ISVC derived from a different source.
	wa := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(foreignDerived("some-other-uid")).Build()
	wb := workerWithAdmittedDerived(t, s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	// Make this a sticky-winner pass so a's foreign object is only ever subject
	// to the loser sweep (not adopted by a fan-out CreateOrUpdate).
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, _ := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.True(t, hasDerived(t, wa), "derived owned by a different source UID must NOT be deleted")
}

// Bug: deleteLosers must tolerate a per-cluster delete failure instead of
// aborting the whole reconcile on the first error.
func TestReconcile_LoserCleanupToleratesPerClusterError(t *testing.T) {
	s := testScheme(t)
	// a holds OUR stray derived but errors on Delete; the reconcile must still
	// succeed (place the winner) rather than abort.
	wa := deleteFailClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).
		WithObjects(&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}}}).Build()}
	wb := workerWithAdmittedDerived(t, s) // winner
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "a single loser-cluster delete error must not abort the reconcile")
	assert.Equal(t, v1beta1.PlacementPhasePlaced, cpPlacement(t, cp).Phase)
	assert.Equal(t, "b", cpPlacement(t, cp).Cluster)
}

// Bug: a transient remote GET error in the sticky-winner path must hold the
// existing placement and requeue, not abort and not re-race.
func TestReconcile_StickyWinnerTransientGetHoldsPlacement(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := getFailClient{WithWatch: emptyWorker(s)} // winner's GET fails transiently
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

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "transient remote GET error must not abort the reconcile")
	assert.Positive(t, res.RequeueAfter, "should requeue to retry")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster, "existing placement held")
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.False(t, hasDerived(t, wa), "must NOT re-race / fan out to a on a transient read error")
}

// Bug: placeOn must merge control-plane-owned metadata onto the derived rather
// than clobbering worker-added labels/annotations every poll.
func TestPlaceOn_PreservesWorkerMetadata(t *testing.T) {
	s := testScheme(t)
	// Pre-existing derived carrying a worker-added label + annotation.
	existing := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "svc", Namespace: "prod",
		Labels:      map[string]string{"worker.io/managed": "yes"},
		Annotations: map[string]string{"worker.io/state": "bookkeeping"},
	}}
	w := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, _ := newPlacer(s, clusters, srcISVC(""))

	require.NoError(t, r.placeOn(context.Background(), w, srcISVC("")))

	got := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	assert.Equal(t, "yes", got.Labels["worker.io/managed"], "worker label preserved")
	assert.Equal(t, "bookkeeping", got.Annotations["worker.io/state"], "worker annotation preserved")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel], "control-plane origin label set")
}

// Bug: reconcileDelete must NOT remove the finalizer while a connected cluster
// still holds our derived because its delete errored (would orphan it). It
// requeues instead.
func TestReconcileDelete_HoldsFinalizerWhenConnectedDeleteFails(t *testing.T) {
	s := testScheme(t)
	wa := deleteFailClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).
		WithObjects(&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}}}).Build()}
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	r, cp := newPlacer(s, clusters, isvc, readyWC("a", map[string]string{"gpu": "gb300"}))
	require.NoError(t, cp.Delete(context.Background(), isvc)) // -> DeletionTimestamp (finalizer present)

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "should requeue to retry teardown")

	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	assert.Contains(t, got.Finalizers, PlacementFinalizer, "finalizer held while connected cluster still holds derived")
}

// Winner-lost grace: a DISCONNECTED winner must be treated as transient
// — the controller holds the placement and requeues, and crucially does NOT arm
// the grace clock (which tracks absence on a CONNECTED winner). A transport flap
// must never tear down a healthy placement or even start counting toward a
// re-race.
func TestReconcile_StickyWinnerDisconnectedHoldsWithoutArmingGrace(t *testing.T) {
	s := testScheme(t)
	// Winner "b" is a Ready candidate WorkloadCluster but is NOT in the connected
	// client set (transport flap): ClientFor("b") -> (nil, false).
	wa := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Nanosecond // even a ~zero grace must not re-race a disconnected winner

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "disconnected winner: hold and requeue")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster, "disconnected winner held, not cleared")
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.False(t, hasDerived(t, wa), "must NOT re-race / fan out to a on a disconnected winner")
	_, armed := r.winnerLostSince.Load(isvc.UID)
	assert.False(t, armed, "grace clock must NOT be armed for a disconnected winner")
}

// Winner-lost grace: once the derived reappears on the connected winner, any pending grace
// marker is cleared so a later disappearance starts a fresh window.
func TestReconcile_StickyWinnerPresentClearsGraceMarker(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s) // present + admitted
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
	// Pre-seed a stale grace marker (as if the derived had briefly vanished).
	r.winnerLostSince.Store(isvc.UID, time.Now())

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.Equal(t, v1beta1.PlacementPhasePlaced, cpPlacement(t, cp).Phase)
	_, ok := r.winnerLostSince.Load(isvc.UID)
	assert.False(t, ok, "grace marker cleared once derived is present again")
}

// Fault isolation: a candidate that is NOT in the connected snapshot is
// skipped during fan-out — its absence must not block placing on the connected
// candidates, and the placement still proceeds to Racing.
func TestReconcile_FanOutSkipsDisconnectedCandidate(t *testing.T) {
	s := testScheme(t)
	// Both "a" and "b" are Ready candidates, but only "a" is connected.
	wa := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "a disconnected candidate must not fail the reconcile")

	assert.True(t, hasDerived(t, wa), "connected candidate a got the derived")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
}

// Index: isvcsForClusterChange enqueues an ISVC only when the changed
// cluster is (or could become) one of its candidates — its requirement selector
// matches the cluster's labels — and skips ISVCs the cluster cannot affect.
func TestIsvcsForClusterChange_EnqueuesOnlyMatchingCandidates(t *testing.T) {
	s := testScheme(t)
	// match: selector gpu=gb300 matches the changed cluster.
	match := srcISVCNamed("match", map[string]string{AcceleratorRequirementsAnnotation: "gpu=gb300"})
	// noMatch: requires a different accelerator; the changed cluster is not a candidate.
	noMatch := srcISVCNamed("nomatch", map[string]string{AcceleratorRequirementsAnnotation: "gpu=h100"})
	// noReq: declares no placement requirement; never fanned out, must be excluded
	// (and is not even in the index).
	noReq := srcISVCNamed("noreq", nil)

	c := indexedPlacementClient(t, s, match, noMatch, noReq)
	r := &Reconciler{Client: c, Scheme: s, Log: log.Log}

	changed := readyWC("b", map[string]string{"gpu": "gb300"})
	reqs := r.isvcsForClusterChange(context.Background(), changed)

	got := requestNames(reqs)
	assert.Contains(t, got, "match", "ISVC whose selector matches the changed cluster is enqueued")
	assert.NotContains(t, got, "nomatch", "ISVC whose selector does not match is skipped")
	assert.NotContains(t, got, "noreq", "ISVC with no placement requirement is skipped")
}

// Index: an ISVC whose status references the changed cluster (it is
// placed/racing there) is enqueued even when the post-change labels no longer
// match its selector — this is the "cluster leaving the candidate set" case the
// event's post-change object alone would miss.
func TestIsvcsForClusterChange_EnqueuesStatusReferencedClusterOnLabelLoss(t *testing.T) {
	s := testScheme(t)
	// Selector requires gpu=gb300, but the changed cluster has LOST that label.
	// The ISVC is currently placed on "b", so it must be re-evaluated.
	placed := srcISVCNamed("placed", map[string]string{AcceleratorRequirementsAnnotation: "gpu=gb300"})
	placed.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	c := indexedPlacementClient(t, s, placed)
	r := &Reconciler{Client: c, Scheme: s, Log: log.Log}

	// Changed cluster "b" no longer carries gpu=gb300 (label removed).
	changed := readyWC("b", map[string]string{"gpu": "h100"})
	reqs := r.isvcsForClusterChange(context.Background(), changed)

	assert.Contains(t, requestNames(reqs), "placed",
		"ISVC placed on the cluster is re-enqueued even when the cluster's new labels no longer match its selector")
}

// Index: the field-index extractor marks exactly the requirement-
// declaring ISVCs (and leaves the rest unindexed).
func TestPlacementEligibleIndexExtractor(t *testing.T) {
	withAccel := srcISVCNamed("a", map[string]string{AcceleratorRequirementsAnnotation: "gpu=gb300"})
	withSelector := srcISVCNamed("b", map[string]string{ClusterSelectorAnnotation: "provider=cw"})
	withNone := srcISVCNamed("c", nil)

	assert.Equal(t, []string{placementEligibleIndexValue}, placementEligibleIndexExtractor(withAccel))
	assert.Equal(t, []string{placementEligibleIndexValue}, placementEligibleIndexExtractor(withSelector))
	assert.Nil(t, placementEligibleIndexExtractor(withNone))
	// Wrong type yields no index entry.
	assert.Nil(t, placementEligibleIndexExtractor(&v1beta1.WorkloadCluster{}))
}

// srcISVCNamed builds a placement source ISVC with a given name and annotation
// set (nil annotations => no placement requirement).
func srcISVCNamed(name string, annotations map[string]string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "prod", UID: types.UID("uid-" + name),
			Annotations: annotations,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}},
		},
	}
}

// indexedPlacementClient builds a fake control-plane client with the placement-
// eligible field index registered (mirroring what cmd/manager installs on the
// manager cache), so isvcsForClusterChange's MatchingFields list resolves.
func indexedPlacementClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fakeclient.NewClientBuilder().WithScheme(s).
		WithIndex(&v1beta1.InferenceService{}, placementEligibleIndexField, placementEligibleIndexExtractor).
		WithObjects(objs...).
		Build()
}

func requestNames(reqs []ctrl.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Name)
	}
	return out
}

// hangingClient blocks every Create until the (per-cluster) context deadline
// fires, simulating a wedged remote apiserver that neither succeeds nor returns
// an error promptly.
type hangingClient struct {
	client.WithWatch
}

func (c hangingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	<-ctx.Done()
	return ctx.Err()
}

// Fault isolation: a wedged remote whose apply hangs is bounded by the
// per-cluster PlaceTimeout and skipped, so the healthy peer still gets the
// derived and the reconcile completes (Racing) instead of stalling on the stuck
// cluster.
func TestReconcile_FanOutBoundsStuckClusterAndProceeds(t *testing.T) {
	s := testScheme(t)
	wa := hangingClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).Build()} // wedged
	wb := emptyWorker(s)                                                                // healthy
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.PlaceTimeout = 50 * time.Millisecond // bound the wedged apply tightly for the test

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := r.Reconcile(context.Background(), req())
		assert.NoError(t, err, "a wedged cluster must not fail the whole reconcile")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile blocked on a stuck cluster; per-cluster timeout did not fire")
	}

	assert.True(t, hasDerived(t, wb), "healthy cluster b still got the derived despite the wedged peer")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseRacing, p.Phase)
}
