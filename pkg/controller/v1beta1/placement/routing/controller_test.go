package routing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
)

const (
	testName = "svc"
	testNS   = "prod"
)

func routingScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	return s
}

// splitISVC builds a Placed, Split-mode ISVC with the given per-home admitted
// and ready replica counts. clusters, admitted, and ready are index-aligned.
func splitISVC(clusters []string, admitted, ready []int32) *v1beta1.InferenceService {
	cands := make([]v1beta1.CandidatePlacement, len(clusters))
	for i, c := range clusters {
		cands[i] = v1beta1.CandidatePlacement{
			Cluster:          c,
			Phase:            v1beta1.CandidatePhaseAdmitted,
			Endpoint:         apis.HTTPS(c + ".example"),
			AdmittedReplicas: admitted[i],
			ReadyReplicas:    ready[i],
		}
	}
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS, UID: "uid-1", Generation: 7},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{
				Mode:         v1beta1.PlacementModeSplit,
				Requirements: "gpu=tpu",
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Placement: &v1beta1.PlacementStatus{
				Phase:      v1beta1.PlacementPhasePlaced,
				Candidates: cands,
			},
		},
	}
}

func controllerTestConfig() Config {
	return Config{
		Enabled:  true,
		Observer: validObserverConfig(),
	}
}

func newReconciler(t *testing.T, cfg Config, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fakeclient.NewClientBuilder().WithScheme(routingScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}).
		WithObjects(objs...).Build()
	return &Reconciler{Client: c, APIReader: c, Log: log.Log, Config: cfg}, c
}

func reconcile(t *testing.T, r *Reconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testName, Namespace: testNS},
	})
	require.NoError(t, err)
	return result
}

func ownedTrafficMap(isvc *v1beta1.InferenceService) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      isvc.Name,
			Namespace: isvc.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
	}
}

func testCapacityState() *CapacityPoller {
	return &CapacityPoller{
		Config: CapacityConfig{Path: "/capacity", Samples: 1, Quorum: 1},
		state:  map[capacityKey]*capacityState{},
	}
}

func recordCapacity(
	t *testing.T,
	capacity *CapacityPoller,
	isvc *v1beta1.InferenceService,
	candidate int,
	reported *int32,
	reason string,
) {
	t.Helper()
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[candidate], "")
	require.True(t, capacity.record(target, reported, reason))
}

func failingProbeState(
	t *testing.T,
	isvc *v1beta1.InferenceService,
	policy AllFailedPolicy,
	clusters ...string,
) (*Prober, ResolvedProbePolicy) {
	t.Helper()
	config := testProbeConfig()
	config.AllFailedPolicy = policy
	resolved := testProbePolicy(t, config)
	prober := &Prober{state: map[probeIdentity]*homeProbeState{}}
	wanted := make(map[string]struct{}, len(clusters))
	for _, cluster := range clusters {
		wanted[cluster] = struct{}{}
	}
	for _, candidate := range isvc.Status.Placement.Candidates {
		if _, ok := wanted[candidate.Cluster]; !ok {
			continue
		}
		target := targetForCandidate(isvc, &candidate, resolved.PolicyDigest)
		endpoint, err := CanonicalEndpoint(target.URL)
		require.NoError(t, err)
		identity := identityForTarget(target)
		identity.Endpoint = endpoint
		prober.state[identity] = &homeProbeState{
			identity:            identity,
			gated:               true,
			hasConclusiveResult: true,
			lastResult:          v1beta1.ProbeResultFailing,
			observed:            true,
		}
	}
	return prober, resolved
}

func controllerProbeSpec(config ProbeConfig) *v1beta1.RoutingProbeSpec {
	acceptStatuses := make([]int32, len(config.AcceptStatuses))
	for i, status := range config.AcceptStatuses {
		acceptStatuses[i] = int32(status)
	}
	gateStatuses := make([]int32, len(config.GateStatuses))
	for i, status := range config.GateStatuses {
		gateStatuses[i] = int32(status)
	}
	return &v1beta1.RoutingProbeSpec{
		Path:             config.Path,
		Method:           config.Method,
		AcceptStatuses:   acceptStatuses,
		GateStatuses:     gateStatuses,
		Period:           metav1.Duration{Duration: config.Period},
		Timeout:          metav1.Duration{Duration: config.Timeout},
		FailureThreshold: int32(config.FailureThreshold),
		SuccessThreshold: int32(config.SuccessThreshold),
		AllFailedPolicy:  v1beta1.RoutingAllFailedPolicy(config.AllFailedPolicy),
	}
}

func getTrafficMap(t *testing.T, c client.Client) (*v1beta1.TrafficMap, bool) {
	t.Helper()
	tm := &v1beta1.TrafficMap{}
	err := c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, tm)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	require.NoError(t, err)
	return tm, true
}

func TestReconcile_SplitGeneratesWeightedTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"b", "a"}, []int32{2, 5}, []int32{2, 5})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok, "TrafficMap should be generated")
	assert.Equal(t, testName, tm.Spec.Service)
	assert.Equal(t, v1beta1.PlacementModeSplit, tm.Spec.Mode)
	assert.Equal(t, int64(7), tm.Spec.ObservedISVCGeneration)

	require.Len(t, tm.Spec.Entries, 2)
	// Entries are cluster-sorted: "a" (5 replicas) then "b" (2 replicas) -> 5:2.
	assert.Equal(t, "a", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Weight)
	assert.True(t, tm.Spec.Entries[0].Healthy)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Capacity.Ready)
	require.NotNil(t, tm.Spec.Entries[0].Endpoint)
	assert.Equal(t, "a.example", tm.Spec.Entries[0].Endpoint.Host)

	assert.Equal(t, "b", tm.Spec.Entries[1].Cluster)
	assert.Equal(t, int32(2), tm.Spec.Entries[1].Weight)

	// Owner-ref'd to the ISVC for garbage collection.
	require.True(t, metav1.IsControlledBy(tm, isvc), "TrafficMap should be controlled by its ISVC")
}

func TestReconcile_UnhealthyHomeGatedToZero(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{6, 4}, []int32{6, 0})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight, "healthy home keeps weight")
	assert.True(t, tm.Spec.Entries[0].Healthy)
	assert.Equal(t, int32(0), tm.Spec.Entries[1].Weight, "unhealthy home gated to 0")
	assert.False(t, tm.Spec.Entries[1].Healthy)
}

func TestReconcile_AllModeWeightsFollowReadyReplicas(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{9, 1}, []int32{9, 1})
	isvc.Spec.Placement.Mode = v1beta1.PlacementModeAll
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Capacity.Allocated)
}

func TestReconcile_SingleModeUsesReadyReplicasAndReadinessGate(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{9}, []int32{9})
	isvc.Spec.Placement.Mode = v1beta1.PlacementModeSingle
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 1)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
	assert.True(t, tm.Spec.Entries[0].Healthy)

	isvc.Status.Placement.Candidates[0].ReadyReplicas = 0
	require.NoError(t, c.Status().Update(context.Background(), isvc))
	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.False(t, tm.Spec.Entries[0].Healthy)
}

func TestReconcile_SplitWeightsFollowReadyReplicas(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{9, 9}, []int32{4, 2})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	assert.Equal(t, int32(9), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, int32(9), tm.Spec.Entries[1].Capacity.Allocated)
}

func TestReconcile_NonPlacementISVCDoesNotCreateTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Spec.Placement = nil
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	_, exists := getTrafficMap(t, c)
	assert.False(t, exists)
}

func TestReconcile_EligibilityLossWaitsForEndpointFinalizerBeforeReaping(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Spec.Placement = nil
	isvc.Finalizers = []string{placementendpoint.EndpointFinalizer}
	existing := ownedTrafficMap(isvc)
	existing.Spec.Service = "last-known"
	r, c := newReconciler(t, controllerTestConfig(), isvc, existing)
	r.WaitForPublisherFinalizer = true

	reconcile(t, r)

	tm, exists := getTrafficMap(t, c)
	require.True(t, exists, "the endpoint publisher still owns teardown")
	assert.Equal(t, "last-known", tm.Spec.Service)

	current := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), current))
	current.Finalizers = nil
	require.NoError(t, c.Update(context.Background(), current))
	reconcile(t, r)

	_, exists = getTrafficMap(t, c)
	assert.False(t, exists, "the map is reaped only after endpoint withdrawal completes")
}

func TestReconcile_CompatibilityPublisherDoesNotWaitForEndpointFinalizer(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Spec.Placement = nil
	isvc.Finalizers = []string{placementendpoint.EndpointFinalizer}
	existing := ownedTrafficMap(isvc)
	r, c := newReconciler(t, controllerTestConfig(), isvc, existing)

	reconcile(t, r)

	_, exists := getTrafficMap(t, c)
	assert.False(t, exists, "compatibility publishers do not use the TrafficMap teardown handshake")
}

func TestReconcile_LegacyAnnotationPlacementCreatesSingleModeMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Spec.Placement = nil
	isvc.Annotations = map[string]string{"ome.io/accelerator-requirements": "gpu=tpu"}
	isvc.Status.Placement.Cluster = "legacy-home"
	isvc.Status.Placement.Endpoint = apis.HTTPS("legacy.example")
	isvc.Status.Placement.Candidates = nil
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Equal(t, v1beta1.PlacementModeSingle, tm.Spec.Mode)
	require.Len(t, tm.Spec.Entries, 1)
	assert.Equal(t, "legacy-home", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, "legacy.example", tm.Spec.Entries[0].Endpoint.Host)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
}

func TestReconcile_DisabledReapsExistingTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	// Pre-existing owned TrafficMap that should be reaped when the feature is off.
	existing := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testName,
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
	}
	r, c := newReconciler(t, Config{Enabled: false}, isvc, existing)

	reconcile(t, r)

	_, ok := getTrafficMap(t, c)
	assert.False(t, ok, "disabled controller should reap the TrafficMap it owns")
}

func TestReconcile_UnownedTrafficMapIsNeverMutated(t *testing.T) {
	foreign := func() *v1beta1.TrafficMap {
		return &v1beta1.TrafficMap{
			ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS},
			Spec:       v1beta1.TrafficMapSpec{Service: "foreign-service"},
		}
	}

	t.Run("enabled routing refuses to overwrite", func(t *testing.T) {
		isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
		r, c := newReconciler(t, controllerTestConfig(), isvc, foreign())

		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: client.ObjectKeyFromObject(isvc),
		})
		require.ErrorContains(t, err, "is not controlled by InferenceService")

		tm, exists := getTrafficMap(t, c)
		require.True(t, exists)
		assert.Equal(t, "foreign-service", tm.Spec.Service)
		assert.Empty(t, tm.OwnerReferences)
	})

	t.Run("disabled routing refuses to delete", func(t *testing.T) {
		isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
		cfg := controllerTestConfig()
		cfg.Enabled = false
		r, c := newReconciler(t, cfg, isvc, foreign())

		reconcile(t, r)

		tm, exists := getTrafficMap(t, c)
		require.True(t, exists)
		assert.Equal(t, "foreign-service", tm.Spec.Service)
		assert.Empty(t, tm.OwnerReferences)
	})
}

func TestApply_RechecksUnownedTrafficMapBeforeMutation(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	foreign := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testName,
			Namespace:       testNS,
			UID:             "foreign-map-uid",
			ResourceVersion: "42",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Name:       "foreign-owner",
				UID:        "foreign-owner-uid",
			}},
		},
		Spec: v1beta1.TrafficMapSpec{Service: "foreign-service"},
	}
	original := foreign.DeepCopy()
	r, c := newReconciler(t, controllerTestConfig(), isvc, foreign)

	err := r.apply(context.Background(), isvc, v1beta1.TrafficMapSpec{
		Service: testName,
		Mode:    v1beta1.PlacementModeSplit,
	})
	require.ErrorContains(t, err, "is not controlled by InferenceService")

	got, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Equal(t, original.Spec, got.Spec)
	assert.Equal(t, original.OwnerReferences, got.OwnerReferences)
}

func TestReconcile_ConfirmsInferenceServiceCacheMissWithAPIReader(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	scheme := routingScheme(t)
	cacheClient := fakeclient.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}).
		Build()
	apiReader := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(isvc).Build()
	r := &Reconciler{
		Client:    cacheClient,
		APIReader: apiReader,
		Log:       log.Log,
		Config:    controllerTestConfig(),
	}

	reconcile(t, r)

	tm, exists := getTrafficMap(t, cacheClient)
	require.True(t, exists, "the authoritative read found the live InferenceService")
	assert.Equal(t, testName, tm.Spec.Service)
	assert.True(t, metav1.IsControlledBy(tm, isvc))
}

// routableCond returns the Routable condition, or nil when absent.
func routableCond(tm *v1beta1.TrafficMap) *metav1.Condition {
	return apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapRoutable)
}

// An ISVC that is not Placed keeps its map, emptied, with the reason. Deleting
// it would make "not placed yet" indistinguishable from a wedged controller.
func TestReconcile_NotPlacedKeepsEmptyTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok, "an unroutable ISVC must still have a TrafficMap")
	assert.Empty(t, tm.Spec.Entries)
	assert.Equal(t, testName, tm.Spec.Service, "identity is populated even with no entries")
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, cond.Reason)
}

// Placed, but no candidate publishes an endpoint: empty table, distinct reason.
// The reason is what separates this from NotPlaced, since both tables are empty.
func TestReconcile_NoAddressableHomeKeepsEmptyTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Status.Placement.Candidates[0].Endpoint = nil
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Empty(t, tm.Spec.Entries)
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoAddressableHome, cond.Reason)
}

func TestReconcile_AllHomesUnreadyIsUnroutable(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{3, 3}, []int32{0, 0})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	for _, e := range tm.Spec.Entries {
		assert.False(t, e.Healthy)
		assert.Equal(t, int32(0), e.Weight, "an unready home must not receive traffic")
	}
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesUnready, cond.Reason)
}

func TestReconcile_InlineProbePolicyDrivesObservationAndRequeue(t *testing.T) {
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint

	inline := testProbeConfig()
	inline.Path = "/inline-ready"
	inline.Period = 3 * time.Second
	inline.Timeout = time.Second
	inline.FailureThreshold = 1
	inline.SuccessThreshold = 1
	inline.AllFailedPolicy = AllFailedPolicyDrain
	isvc.Spec.Routing = &v1beta1.RoutingSpec{Probe: controllerProbeSpec(inline)}

	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Equal(t, inline.Period, result.RequeueAfter)
	select {
	case path := <-requests:
		assert.Equal(t, inline.Path, path)
	default:
		t.Fatal("inline probe request was not issued")
	}
	resolved, err := ResolveProbePolicy(cfg, &isvc.Spec)
	require.NoError(t, err)
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	entry := tm.Spec.Entries[0]
	assert.Equal(t, int32(0), entry.Weight)
	assert.False(t, entry.Healthy)
	require.NotNil(t, entry.Probe)
	assert.Equal(t, resolved.PolicyDigest, entry.Probe.PolicyDigest)
	assert.Equal(t, v1beta1.ProbeResultFailing, entry.Probe.Result)
	assert.True(t, entry.Probe.Gated)
	require.NotNil(t, entry.Probe.LastProbeTime)
	assert.True(t, entry.Probe.LastProbeTime.Time.Equal(testProbeNow))
}

func TestReconcile_InvalidProbeEndpointDoesNotBlockValidTarget(t *testing.T) {
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests <- request.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"invalid", "valid"}, []int32{3, 3}, []int32{3, 3})
	isvc.Status.Placement.Candidates[0].Endpoint = &apis.URL{Host: "invalid.example"}
	validEndpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[1].Endpoint = validEndpoint
	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Equal(t, cfg.Probe.Period, result.RequeueAfter)
	select {
	case path := <-requests:
		assert.Equal(t, cfg.Probe.Path, path)
	default:
		t.Fatal("valid endpoint was not probed")
	}
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, "invalid", tm.Spec.Entries[0].Cluster)
	assert.Nil(t, tm.Spec.Entries[0].Probe)
	assert.True(t, tm.Spec.Entries[0].Healthy)
	assert.Positive(t, tm.Spec.Entries[0].Weight)
	assert.Equal(t, "valid", tm.Spec.Entries[1].Cluster)
	require.NotNil(t, tm.Spec.Entries[1].Probe)
	assert.Equal(t, v1beta1.ProbeResultPassing, tm.Spec.Entries[1].Probe.Result)
	assert.False(t, tm.Spec.Entries[1].Probe.Gated)
	assert.True(t, tm.Spec.Entries[1].Healthy)
	assert.Positive(t, tm.Spec.Entries[1].Weight)
	require.NotNil(t, routableCond(tm))
	assert.Equal(t, metav1.ConditionTrue, routableCond(tm).Status)
}

func TestReconcile_InlineProbeDisableSkipsGlobalProbe(t *testing.T) {
	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	isvc.Spec.Routing = &v1beta1.RoutingSpec{
		Probe: &v1beta1.RoutingProbeSpec{Disabled: true},
	}
	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter)
	assert.Empty(t, requests, "an explicit inline disable must suppress the inherited probe")
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	require.Len(t, tm.Spec.Entries, 1)
	assert.Nil(t, tm.Spec.Entries[0].Probe)
	assert.True(t, tm.Spec.Entries[0].Healthy)
}

func TestReconcile_NotPlacedForgetsStaleProbeTargets(t *testing.T) {
	requests := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	endpoint, err := apis.ParseURL(server.URL)
	require.NoError(t, err)
	isvc.Status.Placement.Candidates[0].Endpoint = endpoint
	cfg := controllerTestConfig()
	cfg.Probe = testProbeConfig()
	resolved, err := ResolveProbePolicy(cfg, &isvc.Spec)
	require.NoError(t, err)
	target := targetForCandidate(isvc, &isvc.Status.Placement.Candidates[0], resolved.PolicyDigest)
	clk := clocktesting.NewFakeClock(testProbeNow)
	prober := newTestProber(t, NewObserverClient(), clk, 1, 1, testProbeResponseBytes)
	r, c := newReconciler(t, cfg, isvc)
	r.Prober = prober

	first := reconcile(t, r)
	assert.Equal(t, cfg.Probe.Period, first.RequeueAfter)
	select {
	case <-requests:
	default:
		t.Fatal("initial probe request was not issued")
	}
	require.NotNil(t, prober.Provenance(target))

	current := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), current))
	current.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
	require.NotEmpty(t, current.Status.Placement.Candidates, "the demoted status retains a stale admitted candidate")
	require.NoError(t, c.Status().Update(context.Background(), current))
	clk.Step(cfg.Probe.Period)

	result := reconcile(t, r)

	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter)
	select {
	case <-requests:
		t.Fatal("a non-Placed InferenceService must not be probed")
	default:
	}
	tm, exists := getTrafficMap(t, c)
	require.True(t, exists)
	assert.Empty(t, tm.Spec.Entries)
	require.NotNil(t, routableCond(tm))
	assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, routableCond(tm).Reason)
	assert.Nil(t, prober.Provenance(target), "stale probe state must be forgotten after placement demotion")
}

func TestBuildSpec_NoRoutableCapacityIsReportedPrecisely(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	zero := int32(0)
	capacity := testCapacityState()
	recordCapacity(t, capacity, isvc, 0, &zero, "")
	recordCapacity(t, capacity, isvc, 1, &zero, "")

	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, nil, capacity)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(0), spec.Entries[0].Weight)
	assert.Equal(t, int32(0), spec.Entries[1].Weight)
	assert.True(t, spec.Entries[0].Healthy)
	assert.True(t, spec.Entries[1].Healthy)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
}

func TestBuildSpec_RecordsCapacityFallbackReason(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{7}, []int32{7})
	capacity := testCapacityState()
	recordCapacity(t, capacity, isvc, 0, nil, "capacity request timed out")

	spec, status, reason := buildSpec(isvc, ResolvedProbePolicy{}, nil, capacity)

	require.Len(t, spec.Entries, 1)
	require.NotNil(t, spec.Entries[0].Capacity)
	assert.Equal(t, int32(7), spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, v1beta1.CapacitySourceControlPlane, spec.Entries[0].Capacity.Source)
	assert.Equal(t, "capacity request timed out", spec.Entries[0].Capacity.FallbackReason)
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, reason)
}

func TestBuildSpec_ProbeFailureCannotMaskZeroCapacity(t *testing.T) {
	for _, policy := range []AllFailedPolicy{AllFailedPolicyPreserveTraffic, AllFailedPolicyDrain} {
		t.Run(string(policy), func(t *testing.T) {
			isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
			zero := int32(0)
			capacity := testCapacityState()
			recordCapacity(t, capacity, isvc, 0, &zero, "")
			recordCapacity(t, capacity, isvc, 1, &zero, "")
			probe, resolved := failingProbeState(t, isvc, policy, "a", "b")

			spec, status, reason := buildSpec(isvc, resolved, probe, capacity)
			require.Len(t, spec.Entries, 2)
			assert.Equal(t, int32(0), spec.Entries[0].Weight)
			assert.Equal(t, int32(0), spec.Entries[1].Weight)
			assert.Equal(t, metav1.ConditionFalse, status)
			assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
		})
	}
}

func TestBuildSpec_PreserveTrafficKeepsReadyCapacityOnFleetWideProbeFailure(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	p, resolved := failingProbeState(t, isvc, AllFailedPolicyPreserveTraffic, "a", "b")

	spec, status, reason := buildSpec(isvc, resolved, p, nil)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(7), spec.Entries[0].Weight)
	assert.Equal(t, int32(3), spec.Entries[1].Weight)
	assert.False(t, spec.Entries[0].Healthy)
	assert.False(t, spec.Entries[1].Healthy)
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesProbeFailed, reason)
}

func TestBuildSpec_AllProbeFailuresCanDrainEveryHome(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 3})
	p, resolved := failingProbeState(t, isvc, AllFailedPolicyDrain, "a", "b")

	spec, status, reason := buildSpec(isvc, resolved, p, nil)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(0), spec.Entries[0].Weight)
	assert.Equal(t, int32(0), spec.Entries[1].Weight)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesProbeFailed, reason)
}

func TestBuildSpec_PreserveTrafficRequiresEveryProbeToFail(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{7, 3}, []int32{7, 0})
	p, resolved := failingProbeState(t, isvc, AllFailedPolicyPreserveTraffic, "a")

	spec, status, reason := buildSpec(isvc, resolved, p, nil)
	require.Len(t, spec.Entries, 2)
	assert.Equal(t, int32(0), spec.Entries[0].Weight)
	assert.Equal(t, int32(0), spec.Entries[1].Weight)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoRoutableCapacity, reason)
}

func TestReconcile_ServingHomeReportsRoutable(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, cond.Reason)
}

// The publisher owns Programmed on the same subresource, so writing Routable
// must not drop it.
func TestReconcile_RoutableDoesNotClobberProgrammed(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	r, c := newReconciler(t, controllerTestConfig(), isvc)
	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)

	apimeta.SetStatusCondition(&tm.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapProgrammed, Status: metav1.ConditionTrue,
		Reason: "Published", Message: "publisher realized the map",
	})
	require.NoError(t, c.Status().Update(context.Background(), tm))

	// A later pass that flips the ISVC unroutable must rewrite only our condition.
	isvc.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
	require.NoError(t, c.Status().Update(context.Background(), isvc))
	reconcile(t, r)

	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.NotNil(t, apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapProgrammed),
		"the publisher's condition must survive a Routable write")
	assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, routableCond(tm).Reason)
}

func TestReconcile_UpdatesExistingTrafficMapOnReadyReplicaChange(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	r, c := newReconciler(t, controllerTestConfig(), isvc)
	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)

	// Home "a" becomes ready at 8 vs 2 -> 4:1.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, cur))
	cur.Status.Placement.Candidates[0].AdmittedReplicas = 8
	cur.Status.Placement.Candidates[0].ReadyReplicas = 8
	cur.Status.Placement.Candidates[1].AdmittedReplicas = 2
	cur.Status.Placement.Candidates[1].ReadyReplicas = 2
	require.NoError(t, c.Status().Update(context.Background(), cur))

	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(4), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
}

func TestReconcile_CapacityFactorWeightsHomes(t *testing.T) {
	// Two homes with equal admitted+ready replicas: without factors they would
	// weight 1:1. A per-cluster factor of 2 on "a" doubles its share -> 2:1.
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
	isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{
		"a": resource.MustParse("2"),
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)

	assert.Equal(t, "a", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight, "factor-2 home carries double the share")
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.Factor, "factor recorded as provenance")
	assert.Equal(t, "2", tm.Spec.Entries[0].Capacity.Factor.String())

	assert.Equal(t, "b", tm.Spec.Entries[1].Cluster)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	require.NotNil(t, tm.Spec.Entries[1].Capacity)
	assert.Nil(t, tm.Spec.Entries[1].Capacity.Factor, "home absent from map uses the identity factor")
}

func TestReconcile_RoutingCapacityFactorWeightsHomes(t *testing.T) {
	// The routing-level factor is the canonical field. Even if an object that
	// bypassed admission also carries the deprecated placement alias, the
	// projection deterministically uses routing.
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	isvc.Spec.Routing = &v1beta1.RoutingSpec{
		CapacityFactors: map[string]resource.Quantity{
			"a": resource.MustParse("2"),
		},
	}
	//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
	isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{
		"a": resource.MustParse("3"),
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.Factor)
	assert.Equal(t, "2", tm.Spec.Entries[0].Capacity.Factor.String())
}

func TestReconcile_FractionalCapacityFactor(t *testing.T) {
	// "500m" halves a home's per-replica capacity: a(4 replicas)*0.5 vs b(4)*1
	// -> 2:4 -> reduced 1:2.
	isvc := splitISVC([]string{"a", "b"}, []int32{4, 4}, []int32{4, 4})
	//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
	isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{
		"a": resource.MustParse("500m"),
	}
	r, c := newReconciler(t, controllerTestConfig(), isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(2), tm.Spec.Entries[1].Weight)
}

func TestCapacityTargetsRequireLiveEligibleExactOwner(t *testing.T) {
	newService := func(name string) *v1beta1.InferenceService {
		isvc := splitISVC([]string{"home"}, []int32{3}, []int32{3})
		isvc.Name = name
		isvc.UID = types.UID(name + "-uid")
		return isvc
	}
	newMap := func(isvc *v1beta1.InferenceService) v1beta1.TrafficMap {
		return v1beta1.TrafficMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      isvc.Name,
				Namespace: isvc.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
				},
			},
			Spec: v1beta1.TrafficMapSpec{
				Service: isvc.Name,
				Entries: []v1beta1.TrafficMapEntry{{
					Cluster:  "home",
					Endpoint: apis.HTTPS(isvc.Name + ".example"),
				}},
			},
		}
	}

	live := newService("live")
	optedOut := newService("opted-out")
	disabled := false
	optedOut.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
	ineligible := newService("ineligible")
	ineligible.Spec.Placement.Requirements = ""
	notPlaced := newService("not-placed")
	notPlaced.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
	deleting := newService("deleting")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	uidMismatch := newService("uid-mismatch")
	unowned := newService("unowned")

	maps := []v1beta1.TrafficMap{
		newMap(live),
		newMap(optedOut),
		newMap(ineligible),
		newMap(notPlaced),
		newMap(deleting),
		newMap(uidMismatch),
		newMap(unowned),
	}
	maps[5].OwnerReferences[0].UID = "replaced-isvc-uid"
	maps[6].OwnerReferences = nil
	services := []v1beta1.InferenceService{
		*live, *optedOut, *ineligible, *notPlaced, *deleting, *uidMismatch, *unowned,
	}

	targets := capacityTargets(maps, services, controllerTestConfig())

	require.Len(t, targets, 1)
	assert.Equal(t, live.UID, targets[0].OwnerUID)
	assert.Equal(t, client.ObjectKeyFromObject(live), targets[0].Map)
	assert.Equal(t, "home", targets[0].Cluster)
	assert.Equal(t, "https://live.example", targets[0].URL)
}

func TestRoutingTableChangePredicate(t *testing.T) {
	base := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})

	t.Run("no relevant change is dropped", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Annotations = map[string]string{"unrelated": "x"}
		assert.False(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("capacity change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Status.Placement.Candidates[0].ReadyReplicas = 1
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("routable-ness change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("deletion transition is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		now := metav1.Now()
		nw.DeletionTimestamp = &now
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("capacity factor change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
		nw.Spec.Placement.CapacityFactors = map[string]resource.Quantity{"a": resource.MustParse("2")}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("routing capacity factor change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Spec.Routing = &v1beta1.RoutingSpec{
			CapacityFactors: map[string]resource.Quantity{"a": resource.MustParse("2")},
		}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("routing opt-out is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		disabled := false
		nw.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("inline probe change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Spec.Routing = &v1beta1.RoutingSpec{Probe: controllerProbeSpec(testProbeConfig())}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("endpoint finalizer release is admitted", func(t *testing.T) {
		old := base.DeepCopy()
		old.Finalizers = []string{placementendpoint.EndpointFinalizer}
		nw := old.DeepCopy()
		nw.Finalizers = nil
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: nw}))
	})

	t.Run("placement eligibility removal is admitted", func(t *testing.T) {
		old := base.DeepCopy()
		old.Spec.Placement.Mode = v1beta1.PlacementModeSingle
		nw := old.DeepCopy()
		nw.Spec.Placement.Requirements = ""
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: nw}))
	})
}
