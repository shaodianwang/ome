package endpoint

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

type testTrafficMapPublishPlan struct {
	claims []string
}

func (p testTrafficMapPublishPlan) Claims() []string { return p.claims }

type recordingTrafficMapPublisher struct {
	name      string
	stateful  bool
	claims    []string
	events    []string
	resolve   func(map[string]string, map[string]string) (map[string]string, error)
	plan      func(*v1beta1.InferenceService, *v1beta1.TrafficMap, map[string]string) (TrafficMapPublishPlan, error)
	drain     func(context.Context, types.NamespacedName, []string) error
	apply     func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error)
	unpublish func(context.Context, types.NamespacedName, v1beta1.TrafficMapPublisherStatus) error
}

type preflightingTrafficMapPublisher struct {
	*recordingTrafficMapPublisher
	preflight func(context.Context, TrafficMapPublishPlan) error
}

func (p *preflightingTrafficMapPublisher) Preflight(
	ctx context.Context,
	plan TrafficMapPublishPlan,
) error {
	p.events = append(p.events, "preflight")
	if p.preflight != nil {
		return p.preflight(ctx, plan)
	}
	return nil
}

func (p *recordingTrafficMapPublisher) Name() string { return p.name }
func (p *recordingTrafficMapPublisher) Stateful() bool {
	return p.stateful
}
func (p *recordingTrafficMapPublisher) ResolveOptions(global, inline map[string]string) (map[string]string, error) {
	if p.resolve != nil {
		return p.resolve(global, inline)
	}
	resolved := clonePublisherOptions(global)
	for key, value := range inline {
		resolved[key] = value
	}
	return resolved, nil
}
func (p *recordingTrafficMapPublisher) Plan(
	owner *v1beta1.InferenceService,
	trafficMap *v1beta1.TrafficMap,
	options map[string]string,
) (TrafficMapPublishPlan, error) {
	if p.plan != nil {
		return p.plan(owner, trafficMap, options)
	}
	return testTrafficMapPublishPlan{claims: p.claims}, nil
}
func (p *recordingTrafficMapPublisher) Drain(
	ctx context.Context,
	key types.NamespacedName,
	claims []string,
) error {
	p.events = append(p.events, "drain:"+strings.Join(claims, ","))
	if p.drain != nil {
		return p.drain(ctx, key, claims)
	}
	return nil
}
func (p *recordingTrafficMapPublisher) Apply(
	ctx context.Context,
	plan TrafficMapPublishPlan,
) (TrafficMapPublishResult, error) {
	p.events = append(p.events, "apply")
	if p.apply != nil {
		return p.apply(ctx, plan)
	}
	return TrafficMapPublishResult{}, nil
}
func (p *recordingTrafficMapPublisher) Unpublish(
	ctx context.Context,
	key types.NamespacedName,
	journal v1beta1.TrafficMapPublisherStatus,
) error {
	p.events = append(p.events, "unpublish:"+strings.Join(journal.ClaimedTargets, ","))
	if p.unpublish != nil {
		return p.unpublish(ctx, key, journal)
	}
	return nil
}

func TestTrafficMapPublisherAddsFinalizerBeforeEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Empty(t, publisher.events, "no external effect may precede the persisted finalizer")

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Nil(t, current.Status.Publisher)
}

func TestTrafficMapPublisherWaitsForLegacyEndpointCleanup(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(
		t, publisher, owner, trafficMap, legacyOwner,
	)
	reconciler.RequeueAfter = time.Minute

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.NotContains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)

	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(legacyOwner), legacyOwner))
	legacyOwner.Finalizers = nil
	require.NoError(t, kubeClient.Update(context.Background(), legacyOwner))
	result, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Equal(t, 1, planCalls)
	assert.Empty(t, publisher.events, "publication waits for the TrafficMap finalizer write")
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
}

func TestTrafficMapPublisherForeignJournalDoesNotBypassLegacyCleanup(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: "other-publisher"}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(
		t, publisher, owner, trafficMap, legacyOwner,
	)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)
}

func TestTrafficMapPublisherEstablishedJournalDoesNotUseMigrationFence(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
	}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, _ := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap, legacyOwner)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, publisher.events)
}

func TestTrafficMapPublisherRetriesLegacyStateReadFailure(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	reader := &failInferenceServiceListReader{
		Reader: baseClient,
		err:    errors.New("injected InferenceService list failure"),
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "injected InferenceService list failure")
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)
}

func TestTrafficMapPublisherPreclaimsBeforeDrainAndApply(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "example-publisher",
		ClaimedTargets: []string{"retired"},
	}
	trafficMap.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
		Reason: "Routable", ObservedGeneration: trafficMap.Generation,
	}}
	owner.Spec.Routing = &v1beta1.RoutingSpec{Publisher: &v1beta1.RoutingPublisherSpec{
		Options: map[string]string{"inline": "value"},
	}}

	global := map[string]string{"global": "value"}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		resolve: func(gotGlobal, gotInline map[string]string) (map[string]string, error) {
			assert.Equal(t, global, gotGlobal)
			assert.Equal(t, owner.Spec.Routing.Publisher.Options, gotInline)
			gotGlobal["mutated"] = "resolver"
			gotInline["mutated"] = "resolver"
			return map[string]string{"effective": "blue"}, nil
		},
		plan: func(_ *v1beta1.InferenceService, _ *v1beta1.TrafficMap, options map[string]string) (TrafficMapPublishPlan, error) {
			assert.Equal(t, map[string]string{"effective": "blue"}, options)
			options["effective"] = "plan-mutated"
			return testTrafficMapPublishPlan{claims: []string{"target-z", "target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	reconciler.GlobalOptions = global
	reconciler.RequeueAfter = time.Minute
	assertPreclaimed := func() {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		require.NotNil(t, current.Status.Publisher)
		assert.Equal(t, []string{"retired", "target-a", "target-z"}, current.Status.Publisher.ClaimedTargets)
	}
	publisher.drain = func(_ context.Context, key types.NamespacedName, claims []string) error {
		assertPreclaimed()
		assert.Equal(t, client.ObjectKeyFromObject(trafficMap), key)
		assert.Equal(t, []string{"retired"}, claims)
		return nil
	}
	publisher.apply = func(_ context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		assertPreclaimed()
		assert.Equal(t, []string{"target-z", "target-a"}, plan.Claims())
		return TrafficMapPublishResult{GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "model",
		}}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	assert.Equal(t, []string{"drain:retired", "apply"}, publisher.events)
	assert.Equal(t, map[string]string{"global": "value"}, global,
		"the resolver must receive a copy of global options")
	assert.Equal(t, map[string]string{"inline": "value"}, owner.Spec.Routing.Publisher.Options,
		"the resolver must receive a copy of inline options")

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"retired", "target-a", "target-z"}, current.Status.Publisher.ClaimedTargets,
		"drained targets stay claimed until whole-map finalization")
	wantDigest, err := publisherOptionsDigest("example-publisher", map[string]string{"effective": "blue"})
	require.NoError(t, err)
	assert.Equal(t, wantDigest, current.Status.Publisher.ObservedOptionsDigest)
	assert.True(t, current.Status.Programmed)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.GatewayRef)
	assert.Equal(t, "GlobalRoute", current.Status.GatewayRef.Kind)
	assert.Equal(t, "model", current.Status.GatewayRef.Name)
	programmed := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, programmed)
	assert.Equal(t, metav1.ConditionTrue, programmed.Status)
}

func TestTrafficMapPublisherPreflightRunsAfterPreclaimBeforeEffects(t *testing.T) {
	tests := []struct {
		name      string
		preflight error
		wantError bool
	}{
		{
			name:      "terminal rejection",
			preflight: &terminalPublisherError{err: errors.New("hostname collision")},
		},
		{
			name:      "retryable read failure",
			preflight: errors.New("live route list failed"),
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"retired"},
			}
			base := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
			}
			publisher := &preflightingTrafficMapPublisher{recordingTrafficMapPublisher: base}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			publisher.preflight = func(_ context.Context, plan TrafficMapPublishPlan) error {
				assert.Equal(t, []string{"target-a"}, plan.Claims())
				current := getPublisherTrafficMap(t, kubeClient, trafficMap)
				require.NotNil(t, current.Status.Publisher)
				assert.Equal(t, []string{"retired", "target-a"}, current.Status.Publisher.ClaimedTargets,
					"the durable claim must precede preflight")
				return tt.preflight
			}

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			if tt.wantError {
				require.ErrorContains(t, err, tt.preflight.Error())
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []string{"preflight"}, publisher.events,
				"a failed preflight must precede and prevent Drain and Apply")

			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
		})
	}
}

func TestTrafficMapPublisherWithdrawnResultRecordsConvergence(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Programmed = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{
		Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "old",
	}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			return TrafficMapPublishResult{
				GatewayRef: &v1beta1.TrafficMapGatewayRef{
					Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "ignored",
				},
				Withdrawn: true,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Programmed)
	assert.Nil(t, current.Status.GatewayRef)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets,
		"successful withdrawal retains the durable claim until finalization")
	assert.NotEmpty(t, current.Status.Publisher.ObservedOptionsDigest)
	programmed := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, programmed)
	assert.Equal(t, metav1.ConditionFalse, programmed.Status)
	assert.Equal(t, trafficMapPublisherReasonWithdrawn, programmed.Reason)
	assert.Equal(t, trafficMap.Generation, programmed.ObservedGeneration)
}

func TestTrafficMapPublisherRejectsUnsafeClaimsBeforeEffects(t *testing.T) {
	tests := []struct {
		name       string
		claims     []string
		journal    *v1beta1.TrafficMapPublisherStatus
		other      *v1beta1.TrafficMap
		wantReason string
	}{
		{
			name: "duplicate plan claims", claims: []string{"same", "same"},
			wantReason: trafficMapPublisherReasonInvalidPlan,
		},
		{
			name: "oversized target", claims: []string{strings.Repeat("x", v1beta1.MaxTrafficMapPublisherTargetLength+1)},
			wantReason: trafficMapPublisherReasonInvalidPlan,
		},
		{
			name: "publisher mismatch", claims: []string{"new"},
			journal:    &v1beta1.TrafficMapPublisherStatus{PublisherName: "old-publisher", ClaimedTargets: []string{"old"}},
			wantReason: trafficMapPublisherReasonPublisherChanged,
		},
		{
			name: "claim collision", claims: []string{"shared"},
			other:      publisherClaimedTrafficMap("other", "example-publisher", []string{"shared"}),
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
		{
			name:       "global publisher switch with no desired claim",
			other:      publisherClaimedTrafficMap("other", "different-publisher", []string{"theirs"}),
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
		{
			name: "claim union over bound", claims: []string{"new"},
			journal: &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: numberedClaims(v1beta1.MaxTrafficMapPublisherTargets),
			},
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = tt.journal
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: tt.claims,
			}
			objects := []client.Object{owner, trafficMap}
			if tt.other != nil {
				objects = append(objects, tt.other)
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, objects...)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err, "invalid desired state is reported in status without a retry hot-loop")
			assert.Empty(t, publisher.events, "rejected plans must have no external effect")

			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			if tt.journal == nil {
				assert.Nil(t, current.Status.Publisher)
			} else {
				assert.Equal(t, tt.journal, current.Status.Publisher)
			}
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, tt.wantReason, condition.Reason)
			assert.Equal(t, trafficMapPublisherFailureMessage(tt.wantReason), condition.Message)
		})
	}
}

func TestTrafficMapPublisherRejectsInvalidOptionsWithoutEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		resolve: func(map[string]string, map[string]string) (map[string]string, error) {
			return nil, errors.New("protected option is not allowed")
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.NotContains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Nil(t, current.Status.Publisher)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOptions, condition.Reason)
	assert.NotContains(t, condition.Message, "protected option")
}

func TestTrafficMapPublisherValidatesOwnerIdentityBeforeOptionsOrPlan(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceService, *v1beta1.TrafficMap)
	}{
		{
			name: "map name",
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Name = "different"
			},
		},
		{
			name: "service name",
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Service = "different"
			},
		},
		{
			name: "observed owner generation",
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.Generation++
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			tt.mutate(owner, trafficMap)
			resolveCalls := 0
			planCalls := 0
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true,
				resolve: func(map[string]string, map[string]string) (map[string]string, error) {
					resolveCalls++
					return nil, nil
				},
				plan: func(*v1beta1.InferenceService, *v1beta1.TrafficMap, map[string]string) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Zero(t, resolveCalls)
			assert.Zero(t, planCalls)
			assert.Empty(t, publisher.events)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
			require.NotNil(t, condition)
			assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
		})
	}
}

func TestTrafficMapPublisherRepairsFinalizerBeforeRejectingExistingClaims(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	owner.Generation++
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
	}
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Empty(t, publisher.events)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
}

func TestStatefulTrafficMapPublisherRepairsFinalizerForZeroClaimState(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	owner.Generation++
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher",
	}
	trafficMap.Status.Programmed = true
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Empty(t, publisher.events)
}

func TestTrafficMapStatelessPublisherCannotReturnClaims(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: false, claims: []string{"unexpected"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidPlan, condition.Reason)
	assert.Equal(t, trafficMapPublisherFailureMessage(trafficMapPublisherReasonInvalidPlan), condition.Message)
}

func TestTrafficMapStatelessPublisherHonorsGlobalSwitchFence(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	other := publisherClaimedTrafficMap("other", "different-publisher", []string{"their-target"})
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: false}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap, other)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
}

func TestTrafficMapPublisherSkipsWhenInactiveOrOwnerStops(t *testing.T) {
	falseValue := false
	tests := []struct {
		name          string
		active        bool
		ownerOptedOut bool
		ownerDeleting bool
	}{
		{name: "installation disabled"},
		{name: "owner opted out", active: true, ownerOptedOut: true},
		{name: "owner deleting", active: true, ownerDeleting: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			if tt.ownerOptedOut {
				owner.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &falseValue}
			}
			if tt.ownerDeleting {
				now := metav1.Now()
				owner.DeletionTimestamp = &now
				owner.Finalizers = []string{"example.com/hold"}
			}
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
			}
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target"},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			reconciler.Active = tt.active

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Empty(t, publisher.events, "deletion is requested before publisher effects")
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.True(t, current.DeletionTimestamp.IsZero(), "the routing controller owns map deletion")
			assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
		})
	}
}

func TestTrafficMapPublisherApplyFailureKeepsPreclaimForRetry(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	attempts := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			attempts++
			if attempts == 1 {
				return TrafficMapPublishResult{}, errors.New("temporary backend failure")
			}
			return TrafficMapPublishResult{}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
	assert.Empty(t, current.Status.Publisher.ObservedOptionsDigest)
	assert.False(t, current.Status.Programmed)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonApplyFailed, condition.Reason)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Programmed)
	assert.NotEmpty(t, current.Status.Publisher.ObservedOptionsDigest)
	assert.Equal(t, 2, attempts)
}

func TestTrafficMapPublisherDrainFailureKeepsClaimsForRetry(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"retired"},
	}
	drainAttempts := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"current"},
		drain: func(_ context.Context, key types.NamespacedName, _ []string) error {
			assert.Equal(t, client.ObjectKeyFromObject(trafficMap), key)
			drainAttempts++
			if drainAttempts == 1 {
				return errors.New("temporary drain failure")
			}
			return nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"current", "retired"}, current.Status.Publisher.ClaimedTargets)
	assert.Equal(t, []string{"drain:retired"}, publisher.events)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonDrainFailed, condition.Reason)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:retired", "drain:retired", "apply"}, publisher.events)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Programmed)
}

func TestTrafficMapPublisherDoesNotRecreateLostJournalAfterApply(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		live := getPublisherTrafficMap(t, kubeClient, trafficMap)
		live.Status.Publisher = nil
		require.NoError(t, kubeClient.Status().Update(context.Background(), live))
		return TrafficMapPublishResult{GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Kind: "HTTPRoute", Name: "model",
		}}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher)
	assert.False(t, current.Status.Programmed)
	assert.Nil(t, current.Status.GatewayRef)
}

func TestTrafficMapPublisherRetriesAfterSuccessStatusWriteFails(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	wrapped := &failNthStatusApplyClient{
		Client: baseClient, failAt: 2, err: errors.New("injected status write failure"),
	}
	applyCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			applyCalls++
			return TrafficMapPublishResult{}, nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: wrapped, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "injected status write failure")
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
	assert.False(t, current.Status.Programmed)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, 2, applyCalls, "retry must idempotently reapply after a lost success-status write")
	current = getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.True(t, current.Status.Programmed)
}

func TestTrafficMapPublisherFinalizesWhenInactiveWithoutOwner(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"a", "b"},
	}
	fail := true
	var gotKey types.NamespacedName
	var gotJournal v1beta1.TrafficMapPublisherStatus
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, key types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			gotKey = key
			gotJournal = journal
			if fail {
				return errors.New("temporary cleanup failure")
			}
			return nil
		},
	}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap, legacyOwner)
	reconciler.Active = false

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	assert.Equal(t, client.ObjectKeyFromObject(trafficMap), gotKey)
	assert.Equal(t, []string{"a", "b"}, gotJournal.ClaimedTargets)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Equal(t, []string{"a", "b"}, current.Status.Publisher.ClaimedTargets)

	fail = false
	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	got := &v1beta1.TrafficMap{}
	err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), got)
	assert.True(t, apierrors.IsNotFound(err), "removing the last finalizer completes direct deletion")
}

func TestTrafficMapPublisherBlocksCleanupForMismatchedJournal(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "old-publisher", ClaimedTargets: []string{"target"},
	}
	publisher := &recordingTrafficMapPublisher{name: "new-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, "old-publisher", current.Status.Publisher.PublisherName)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonPublisherChanged, condition.Reason)
}

func TestTrafficMapPublisherFinalizationUsesUncachedJournal(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"live-claim"},
	}
	baseClient := newTrafficMapPublisherClient(t, trafficMap)
	stale := trafficMap.DeepCopy()
	stale.Status.Publisher = nil
	cacheClient := &staleTrafficMapClient{Client: baseClient, stale: stale}
	var gotClaims []string
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, _ types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			gotClaims = append([]string(nil), journal.ClaimedTargets...)
			return nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: cacheClient, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"live-claim"}, gotClaims)
}

func TestTrafficMapPublisherFinalizationRechecksJournalAfterUnpublish(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"a"},
	}

	var kubeClient client.Client
	var unpublished [][]string
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, _ types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			unpublished = append(unpublished, append([]string(nil), journal.ClaimedTargets...))
			journal.ClaimedTargets[0] = "backend-mutated"
			if len(unpublished) == 1 {
				live := getPublisherTrafficMap(t, kubeClient, trafficMap)
				live.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
					PublisherName: "example-publisher", ClaimedTargets: []string{"a", "b"},
				}
				require.NoError(t, kubeClient.Status().Update(context.Background(), live))
			}
			return nil
		},
	}
	reconciler, clientForTest := newTrafficMapPublisherReconciler(t, publisher, trafficMap)
	kubeClient = clientForTest

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"a", "b"}, current.Status.Publisher.ClaimedTargets)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"a"}, {"a", "b"}}, unpublished)
	got := &v1beta1.TrafficMap{}
	err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), got)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestClearPublisherJournalClearsPublisherOwnedStatus(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	desiredStatus := v1beta1.TrafficMapStatus{
		Programmed:                   true,
		ObservedTrafficMapGeneration: trafficMap.Generation,
		GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Group: "networking.example.com", Kind: "GlobalRoute", Name: "model",
		},
		Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
		},
		Conditions: []metav1.Condition{{
			Type: v1beta1.TrafficMapProgrammed, Status: metav1.ConditionTrue, Reason: "Programmed",
		}},
	}
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)
	live := getPublisherTrafficMap(t, kubeClient, trafficMap)
	live.Status = desiredStatus
	require.NoError(t, reconciler.applyPublisherStatus(context.Background(), live))
	live = getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, live.Status.Publisher)

	require.NoError(t, reconciler.clearPublisherJournal(
		context.Background(), client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		*live.Status.Publisher.DeepCopy(),
	))
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Programmed)
	assert.Zero(t, current.Status.ObservedTrafficMapGeneration)
	assert.Nil(t, current.Status.GatewayRef)
	assert.Nil(t, current.Status.Publisher)
	programmed := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, programmed)
	assert.Equal(t, metav1.ConditionFalse, programmed.Status)
	assert.Equal(t, trafficMapPublisherReasonUnpublished, programmed.Reason)
}

func TestTrafficMapPublisherStatusApplyRetriesOnConflict(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	wrapped := &conflictOnceClient{Client: baseClient}
	wrapped.beforeConflict = func() {
		live := getPublisherTrafficMap(t, baseClient, trafficMap)
		apimeta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
			Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
			Reason: "ConcurrentRoutingWrite", ObservedGeneration: live.Generation,
		})
		require.NoError(t, baseClient.Status().Update(context.Background(), live))
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: wrapped, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.True(t, wrapped.conflicted)
	programmed := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapProgrammed)
	require.NotNil(t, programmed)
	assert.Equal(t, metav1.ConditionTrue, programmed.Status)
}

func TestTrafficMapPublisherStatusApplyIsFieldIsolated(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	trafficMap.ResourceVersion = "17"
	trafficMap.Status.Programmed = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
	}
	trafficMap.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{Kind: "HTTPRoute", Name: "model"}
	trafficMap.Status.Conditions = []metav1.Condition{
		{Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue, Reason: "Routable"},
		{Type: v1beta1.TrafficMapProgrammed, Status: metav1.ConditionTrue, Reason: "Programmed"},
	}

	content := publisherApplyContent(t, trafficMapPublisherStatusApply(trafficMap))
	metadata := content["metadata"].(map[string]any)
	assert.Equal(t, "17", metadata["resourceVersion"])
	status := content["status"].(map[string]any)
	conditions := status["conditions"].([]any)
	require.Len(t, conditions, 1)
	assert.Equal(t, v1beta1.TrafficMapProgrammed, conditions[0].(map[string]any)["type"])

	trafficMap.Status.Publisher = nil
	trafficMap.Status.GatewayRef = nil
	trafficMap.Status.Programmed = false
	trafficMap.Status.ObservedTrafficMapGeneration = 0
	content = publisherApplyContent(t, trafficMapPublisherStatusApply(trafficMap))
	status = content["status"].(map[string]any)
	_, found := status["publisher"]
	assert.False(t, found, "owned pointer fields are removed by SSA omission")
	_, found = status["gatewayRef"]
	assert.False(t, found, "owned pointer fields are removed by SSA omission")
	assert.Equal(t, false, status["programmed"])
	assert.Equal(t, int64(0), status["observedTrafficMapGeneration"])
}

func TestTrafficMapPublisherHelpers(t *testing.T) {
	first, err := publisherOptionsDigest("publisher-a", map[string]string{"b": "2", "a": "1"})
	require.NoError(t, err)
	second, err := publisherOptionsDigest("publisher-a", map[string]string{"a": "1", "b": "2"})
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, first)
	otherPublisher, err := publisherOptionsDigest("publisher-b", map[string]string{"a": "1", "b": "2"})
	require.NoError(t, err)
	assert.NotEqual(t, first, otherPublisher)

	options := trafficMapPublisherControllerOptions()
	assert.Equal(t, 1, options.MaxConcurrentReconciles)
	require.NotNil(t, options.NeedLeaderElection)
	assert.True(t, *options.NeedLeaderElection)
	requests := enqueueOwnedTrafficMap(context.Background(), &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"},
	})
	require.Len(t, requests, 1)
	assert.Equal(t, types.NamespacedName{Namespace: "team-a", Name: "model"}, requests[0].NamespacedName)
}

func TestTrafficMapPublisherSetupValidation(t *testing.T) {
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	kubeClient := newTrafficMapPublisherClient(t)
	reconciler := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Publisher: publisher, Active: true,
	}

	err := reconciler.validateSetup()
	require.ErrorContains(t, err, "requires leader election")

	reconciler.LeaderElectionEnabled = true
	err = reconciler.validateSetup()
	require.ErrorContains(t, err, "requires a positive resync period")

	reconciler.Active = false
	reconciler.LeaderElectionEnabled = false
	err = reconciler.validateSetup()
	require.ErrorContains(t, err, "requires leader election")

	reconciler.LeaderElectionEnabled = true
	require.NoError(t, reconciler.validateSetup())
}

func TestTrafficMapPublisherOwnerPredicate(t *testing.T) {
	oldOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "team-a", Generation: 1,
	}}
	unchanged := oldOwner.DeepCopy()
	assert.False(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: unchanged,
	}))

	newGeneration := oldOwner.DeepCopy()
	newGeneration.Generation++
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newGeneration,
	}))

	newAnnotation := oldOwner.DeepCopy()
	newAnnotation.Annotations = map[string]string{"example.com/key": "value"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newAnnotation,
	}))

	newFinalizer := oldOwner.DeepCopy()
	newFinalizer.Finalizers = []string{"example.com/finalizer"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newFinalizer,
	}))
}

func publisherTestObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	controller := true
	owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "team-a", UID: types.UID("owner-uid"), Generation: 2,
	}}
	trafficMap := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "model", Namespace: "team-a", UID: types.UID("map-uid"), Generation: 3,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: owner.Name, UID: owner.UID, Controller: &controller,
			}},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service: "model", ObservedISVCGeneration: owner.Generation,
		},
	}
	return owner, trafficMap
}

func publisherClaimedTrafficMap(name, publisher string, claims []string) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID("uid-" + name)},
		Status: v1beta1.TrafficMapStatus{Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName: publisher, ClaimedTargets: claims,
		}},
	}
}

func numberedClaims(count int) []string {
	claims := make([]string, count)
	for i := range claims {
		claims[i] = fmt.Sprintf("target-%03d", i)
	}
	return claims
}

func newTrafficMapPublisherReconciler(
	t *testing.T,
	publisher TrafficMapPublisher,
	objects ...client.Object,
) (*TrafficMapPublisherReconciler, client.Client) {
	t.Helper()
	kubeClient := newTrafficMapPublisherClient(t, objects...)
	return &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}, kubeClient
}

func newTrafficMapPublisherClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.TrafficMap{}).
		WithObjects(objects...).
		Build()
}

func reconcileTrafficMapPublisher(
	t *testing.T,
	reconciler *TrafficMapPublisherReconciler,
	trafficMap *v1beta1.TrafficMap,
) (ctrl.Result, error) {
	t.Helper()
	return reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(trafficMap),
	})
}

func getPublisherTrafficMap(t *testing.T, kubeClient client.Client, object client.Object) *v1beta1.TrafficMap {
	t.Helper()
	current := &v1beta1.TrafficMap{}
	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(object), current))
	return current
}

func publisherApplyContent(t *testing.T, apply runtime.ApplyConfiguration) map[string]any {
	t.Helper()
	unstructuredApply, ok := apply.(interface{ UnstructuredContent() map[string]any })
	require.True(t, ok)
	return unstructuredApply.UnstructuredContent()
}

type conflictOnceClient struct {
	client.Client
	beforeConflict func()
	conflicted     bool
}

type failNthStatusApplyClient struct {
	client.Client
	failAt int
	calls  int
	err    error
}

// staleTrafficMapClient simulates a cache returning one stale TrafficMap read.
// The reconciler must still use APIReader for the deletion journal.
type staleTrafficMapClient struct {
	client.Client
	stale  *v1beta1.TrafficMap
	served bool
}

type failInferenceServiceListReader struct {
	client.Reader
	err error
}

func (r *failInferenceServiceListReader) List(
	ctx context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	if _, ok := list.(*v1beta1.InferenceServiceList); ok {
		return r.err
	}
	return r.Reader.List(ctx, list, options...)
}

func (c *staleTrafficMapClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	trafficMap, ok := object.(*v1beta1.TrafficMap)
	if !c.served && ok && key == client.ObjectKeyFromObject(c.stale) {
		c.served = true
		c.stale.DeepCopyInto(trafficMap)
		return nil
	}
	return c.Client.Get(ctx, key, object, options...)
}

func (c *conflictOnceClient) Status() client.SubResourceWriter {
	return &conflictOnceStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

func (c *failNthStatusApplyClient) Status() client.SubResourceWriter {
	return &failNthStatusApplyWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type failNthStatusApplyWriter struct {
	client.SubResourceWriter
	client *failNthStatusApplyClient
}

func (w *failNthStatusApplyWriter) Apply(
	ctx context.Context,
	obj runtime.ApplyConfiguration,
	opts ...client.SubResourceApplyOption,
) error {
	w.client.calls++
	if w.client.calls == w.client.failAt {
		return w.client.err
	}
	return w.SubResourceWriter.Apply(ctx, obj, opts...)
}

type conflictOnceStatusWriter struct {
	client.SubResourceWriter
	client *conflictOnceClient
}

func (w *conflictOnceStatusWriter) Apply(
	ctx context.Context,
	obj runtime.ApplyConfiguration,
	opts ...client.SubResourceApplyOption,
) error {
	if !w.client.conflicted {
		w.client.conflicted = true
		w.client.beforeConflict()
		return apierrors.NewConflict(
			schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
			"model", errors.New("injected status conflict"),
		)
	}
	return w.SubResourceWriter.Apply(ctx, obj, opts...)
}

func (w *conflictOnceStatusWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	if !w.client.conflicted {
		w.client.conflicted = true
		w.client.beforeConflict()
		return apierrors.NewConflict(
			schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
			obj.GetName(), errors.New("injected status conflict"),
		)
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}
