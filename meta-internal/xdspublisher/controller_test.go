//go:build meta_internal

package xdspublisher

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
)

type recordingPublisher struct {
	published       []weightedTarget
	cleared         []string
	publishFailures map[string]error
	clearFailures   map[string]error
}

func (p *recordingPublisher) PublishWeight(_ context.Context, target string, weight uint32) error {
	p.published = append(p.published, weightedTarget{name: target, weight: weight})
	if err := p.publishFailures[target]; err != nil {
		delete(p.publishFailures, target)
		return err
	}
	return nil
}

func (p *recordingPublisher) ClearWeight(_ context.Context, target string) error {
	p.cleared = append(p.cleared, target)
	if err := p.clearFailures[target]; err != nil {
		delete(p.clearFailures, target)
		return err
	}
	return nil
}

func testISVC(name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "team-a", UID: types.UID("uid-" + name),
	}}
}

func testTrafficMap() *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: "model-a", Namespace: "team-a"},
		Spec: v1beta1.TrafficMapSpec{
			Service: "model-a",
			Entries: []v1beta1.TrafficMapEntry{
				{
					Cluster:  "home-b",
					Endpoint: &apis.URL{Scheme: "https", Host: "model-a.team-a.home-b.example:8443"},
					Weight:   30,
				},
				{
					Cluster:  "home-a",
					Endpoint: &apis.URL{Scheme: "https", Host: "model-a.team-a.home-a.example"},
					Weight:   70,
				},
			},
		},
	}
}

func newTestPublisher(weights ClusterWeightPublisher) *Publisher {
	return &Publisher{weights: weights}
}

func targetOptions(value string) map[string]string {
	return map[string]string{
		optTargetTemplate:  value,
		optWeightMode:      weightModeNormalized,
		optNormalizedTotal: "100",
	}
}

func targetOptionsWithSelector(value, selector string) map[string]string {
	return map[string]string{
		optTargetTemplate:  value,
		optSourceSelector:  selector,
		optWeightMode:      weightModeNormalized,
		optNormalizedTotal: "100",
	}
}

func TestPublisherIdentityAndState(t *testing.T) {
	p := newTestPublisher(&recordingPublisher{})
	assert.Equal(t, Name, p.Name())
	assert.True(t, p.Stateful())
}

func TestResolveOptionsKeepsGlobalPublicationBehavior(t *testing.T) {
	p := newTestPublisher(&recordingPublisher{})
	global := targetOptionsWithSelector(
		"  {{.EndpointHostname}}  ",
		" publish=true, environment in (prod,canary) ",
	)
	global[optNormalizedTotal] = " 000100 "

	effective, err := p.ResolveOptions(global, nil)
	require.NoError(t, err)
	want := targetOptionsWithSelector(
		"{{.EndpointHostname}}",
		"environment in (canary,prod),publish=true",
	)
	want[optNormalizedTotal] = "100"
	assert.Equal(t, want, effective)

	global[optTargetTemplate] = "changed"
	assert.Equal(t, "{{.EndpointHostname}}", effective[optTargetTemplate])
}

func TestResolveOptionsRejectsEveryInlineOption(t *testing.T) {
	p := newTestPublisher(&recordingPublisher{})
	for _, inline := range []map[string]string{
		{optTargetTemplate: "{{.Cluster}}"},
		{optRequestTimeout: "1s"},
		{"unknown": "value"},
	} {
		_, err := p.ResolveOptions(targetOptions("{{.EndpointHostname}}"), inline)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not accept per-InferenceService options")
	}
}

func TestResolveOptionsValidatesSanitizedGlobals(t *testing.T) {
	p := newTestPublisher(&recordingPublisher{})
	tests := []struct {
		name    string
		options map[string]string
		want    string
	}{
		{name: "missing", options: nil, want: "required"},
		{name: "invalid template", options: targetOptions("{{"), want: optTargetTemplate},
		{name: "invalid source selector", options: targetOptionsWithSelector("{{.Cluster}}", "publish in ("), want: optSourceSelector},
		{name: "missing weight mode", options: map[string]string{optTargetTemplate: "{{.Cluster}}"}, want: optWeightMode},
		{name: "invalid weight mode", options: map[string]string{
			optTargetTemplate: "{{.Cluster}}", optWeightMode: "percent",
		}, want: optWeightMode},
		{name: "missing normalized total", options: map[string]string{
			optTargetTemplate: "{{.Cluster}}", optWeightMode: weightModeNormalized,
		}, want: optNormalizedTotal},
		{name: "invalid normalized total", options: map[string]string{
			optTargetTemplate: "{{.Cluster}}", optWeightMode: weightModeNormalized, optNormalizedTotal: "0",
		}, want: optNormalizedTotal},
		{name: "normalized total with relative mode", options: map[string]string{
			optTargetTemplate: "{{.Cluster}}", optWeightMode: weightModeRelative, optNormalizedTotal: "100",
		}, want: optNormalizedTotal},
		{name: "unexpected", options: map[string]string{optTargetTemplate: "{{.Cluster}}", optOncall: "team"}, want: "unsupported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.ResolveOptions(tt.options, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestPlanUsesSourceSelector(t *testing.T) {
	owner := testISVC("model-a")
	owner.Labels = map[string]string{"publish": "true", "environment": "prod"}
	weights := &recordingPublisher{}
	p := newTestPublisher(weights)

	matching, err := p.Plan(owner, testTrafficMap(), targetOptionsWithSelector(
		"{{.EndpointHostname}}", "publish=true,environment in (prod,canary)",
	))
	require.NoError(t, err)
	assert.Equal(t, []string{
		"model-a.team-a.home-a.example",
		"model-a.team-a.home-b.example",
	}, matching.Claims())

	nonmatching, err := p.Plan(owner, testTrafficMap(), targetOptionsWithSelector(
		"{{.EndpointHostname}}", "publish=false",
	))
	require.NoError(t, err)
	assert.Empty(t, nonmatching.Claims())
	result, err := p.Apply(context.Background(), nonmatching)
	require.NoError(t, err)
	assert.True(t, result.Withdrawn)
	assert.Empty(t, weights.published, "a fresh nonmatch must not call the external publisher")
	assert.Empty(t, weights.cleared)
}

func TestFreshNonmatchingSourceWithdrawsThroughLifecycleWithoutExternalCalls(t *testing.T) {
	owner := testISVC("model-a")
	owner.Generation = 2
	owner.Labels = map[string]string{"publish": "false"}
	owner.Spec.Placement = &v1beta1.PlacementSpec{Requirements: "accelerator=test"}
	trafficMap := testTrafficMap()
	trafficMap.UID = "traffic-map-uid"
	trafficMap.Generation = 3
	trafficMap.Spec.ObservedISVCGeneration = owner.Generation
	trafficMap.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(owner, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
	}
	trafficMap.Status.SourceUID = owner.UID

	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.TrafficMap{}).
		WithObjects(owner, trafficMap).
		Build()
	weights := &recordingPublisher{}
	reconciler := &placementendpoint.TrafficMapPublisherReconciler{
		Client:        kubeClient,
		APIReader:     kubeClient,
		Log:           logr.Discard(),
		Publisher:     newTestPublisher(weights),
		GlobalOptions: targetOptionsWithSelector("{{.EndpointHostname}}", "publish=true"),
		Active:        true,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: trafficMap.Namespace,
		Name:      trafficMap.Name,
	}}

	result, err := reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.True(t, result.Requeue, "the first pass durably installs the lifecycle finalizer")
	_, err = reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.Empty(t, weights.published)
	assert.Empty(t, weights.cleared)

	current := &v1beta1.TrafficMap{}
	require.NoError(t, kubeClient.Get(context.Background(), request.NamespacedName, current))
	require.NotNil(t, current.Status.Publisher)
	assert.Empty(t, current.Status.Publisher.ClaimedTargets)
	assert.False(t, current.Status.Published)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, "Withdrawn", published.Reason)
}

func TestNonmatchingSourceDrainsExistingClaimsThroughLifecycle(t *testing.T) {
	owner := testISVC("model-a")
	owner.Generation = 2
	owner.Labels = map[string]string{"publish": "false"}
	owner.Spec.Placement = &v1beta1.PlacementSpec{Requirements: "accelerator=test"}
	trafficMap := testTrafficMap()
	trafficMap.UID = "traffic-map-uid"
	trafficMap.Generation = 3
	trafficMap.Spec.ObservedISVCGeneration = owner.Generation
	trafficMap.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(owner, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
	}
	trafficMap.Finalizers = []string{placementendpoint.TrafficMapPublisherFinalizer}
	trafficMap.Status = v1beta1.TrafficMapStatus{
		SourceUID: owner.UID,
		Published: true,
		Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName:  Name,
			ClaimedTargets: []string{"old.example"},
		},
	}

	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.TrafficMap{}).
		WithObjects(owner, trafficMap).
		Build()
	weights := &recordingPublisher{}
	reconciler := &placementendpoint.TrafficMapPublisherReconciler{
		Client:        kubeClient,
		APIReader:     kubeClient,
		Log:           logr.Discard(),
		Publisher:     newTestPublisher(weights),
		GlobalOptions: targetOptionsWithSelector("{{.EndpointHostname}}", "publish=true"),
		Active:        true,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: trafficMap.Namespace,
		Name:      trafficMap.Name,
	}})
	require.NoError(t, err)
	assert.Equal(t, []weightedTarget{{name: "old.example", weight: 0}}, weights.published,
		"a previous claim must be drained before withdrawal")
	assert.Empty(t, weights.cleared)

	current := &v1beta1.TrafficMap{}
	require.NoError(t, kubeClient.Get(context.Background(), types.NamespacedName{
		Namespace: trafficMap.Namespace,
		Name:      trafficMap.Name,
	}, current))
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"old.example"}, current.Status.Publisher.ClaimedTargets,
		"a drained claim remains owned until whole-map finalization")
	assert.False(t, current.Status.Published)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, "Withdrawn", published.Reason)
}

func TestPlanIsImmutableAndApplyPublishesZeroBeforePositive(t *testing.T) {
	owner := testISVC("model-a")
	trafficMap := testTrafficMap()
	trafficMap.Spec.Entries[1].Weight = 0
	options := targetOptions("{{.EndpointHostname}}")
	weights := &recordingPublisher{}
	p := newTestPublisher(weights)

	plan, err := p.Plan(owner, trafficMap, options)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"model-a.team-a.home-a.example",
		"model-a.team-a.home-b.example",
	}, plan.Claims())

	claims := plan.Claims()
	claims[0] = "mutated"
	owner.Namespace = "mutated"
	trafficMap.Spec.Entries[0].Endpoint.Host = "mutated.example"
	trafficMap.Spec.Entries[0].Weight = 99
	options[optTargetTemplate] = "mutated"
	assert.Equal(t, "model-a.team-a.home-a.example", plan.Claims()[0])

	result, err := p.Apply(context.Background(), plan)
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Equal(t, []weightedTarget{
		{name: "model-a.team-a.home-a.example", weight: 0},
		{name: "model-a.team-a.home-b.example", weight: 100},
	}, weights.published)
}

func TestPlanWeightModes(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want []uint32
	}{
		{name: "normalized", mode: weightModeNormalized, want: []uint32{60, 40}},
		{name: "relative", mode: weightModeRelative, want: []uint32{3, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trafficMap := testTrafficMap()
			trafficMap.Spec.Entries[0].Weight = 2
			trafficMap.Spec.Entries[1].Weight = 3
			options := targetOptions("{{.EndpointHostname}}")
			options[optWeightMode] = tt.mode
			if tt.mode == weightModeRelative {
				delete(options, optNormalizedTotal)
			}
			plan, err := newTestPublisher(&recordingPublisher{}).Plan(
				testISVC("model-a"), trafficMap, options,
			)
			require.NoError(t, err)
			xdsPlan := plan.(*publishPlan)
			require.Len(t, xdsPlan.targets, 2)
			for i := range tt.want {
				assert.Equal(t, tt.want[i], xdsPlan.targets[i].weight)
			}
		})
	}
}

func TestPlanNormalizedWeightsUseConfiguredTotalOrStayZero(t *testing.T) {
	tests := []struct {
		name    string
		weights []int32
		total   string
		want    []uint32
	}{
		{name: "largest remainder", weights: []int32{1, 2}, total: "100", want: []uint32{67, 33}},
		{name: "equal remainder uses target name", weights: []int32{1, 1}, total: "1", want: []uint32{0, 1}},
		{name: "single positive", weights: []int32{0, 7}, total: "100", want: []uint32{0, 100}},
		{name: "all zero", weights: []int32{0, 0}, total: "100", want: []uint32{0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trafficMap := testTrafficMap()
			for i := range tt.weights {
				trafficMap.Spec.Entries[i].Weight = tt.weights[i]
			}
			options := targetOptions("{{.Cluster}}")
			options[optNormalizedTotal] = tt.total
			plan, err := newTestPublisher(&recordingPublisher{}).Plan(testISVC("model-a"), trafficMap, options)
			require.NoError(t, err)
			xdsPlan := plan.(*publishPlan)
			require.Len(t, xdsPlan.targets, len(tt.want))
			var total uint64
			for i := range tt.want {
				assert.Equal(t, tt.want[i], xdsPlan.targets[i].weight)
				total += uint64(xdsPlan.targets[i].weight)
			}
			if tt.name == "all zero" {
				assert.Zero(t, total)
			} else {
				wantTotal, err := strconv.ParseUint(tt.total, 10, 32)
				require.NoError(t, err)
				assert.Equal(t, wantTotal, total)
			}
		})
	}
}

func TestNormalizeTargetWeightsIsOrderIndependentAndOverflowSafe(t *testing.T) {
	orders := [][]weightedTarget{
		{{name: "c", weight: 1}, {name: "a", weight: 1}, {name: "b", weight: 1}},
		{{name: "b", weight: 1}, {name: "c", weight: 1}, {name: "a", weight: 1}},
	}
	for _, targets := range orders {
		normalizeTargetWeights(targets, 100)
		got := make(map[string]uint32, len(targets))
		for _, target := range targets {
			got[target.name] = target.weight
		}
		assert.Equal(t, map[string]uint32{"a": 34, "b": 33, "c": 33}, got)
	}

	const maxSourceWeight = uint32(1<<31 - 1)
	targets := []weightedTarget{
		{name: "a", weight: maxSourceWeight},
		{name: "b", weight: maxSourceWeight - 1},
	}
	normalizeTargetWeights(targets, uint32(maxNormalizedTotal))
	assert.Equal(t, maxNormalizedTotal, uint64(targets[0].weight)+uint64(targets[1].weight))
}

func TestPlanRejectsLegacyLifecycleStateWithoutPublishing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceService)
		want   string
	}{
		{
			name: "endpoint finalizer",
			mutate: func(owner *v1beta1.InferenceService) {
				owner.Finalizers = []string{placementendpoint.EndpointFinalizer}
			},
			want: placementendpoint.EndpointFinalizer,
		},
		{
			name: "empty annotation",
			mutate: func(owner *v1beta1.InferenceService) {
				owner.Annotations = map[string]string{legacyTargetsAnnotation: ""}
			},
			want: legacyTargetsAnnotation,
		},
		{
			name: "malformed annotation",
			mutate: func(owner *v1beta1.InferenceService) {
				owner.Annotations = map[string]string{legacyTargetsAnnotation: "not-json"}
			},
			want: legacyTargetsAnnotation,
		},
		{
			name: "valid annotation",
			mutate: func(owner *v1beta1.InferenceService) {
				owner.Annotations = map[string]string{legacyTargetsAnnotation: `["old.example"]`}
			},
			want: legacyTargetsAnnotation,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner := testISVC("model-a")
			tt.mutate(owner)
			weights := &recordingPublisher{}
			p := newTestPublisher(weights)

			plan, err := p.Plan(owner, testTrafficMap(), targetOptions("{{.EndpointHostname}}"))
			require.Error(t, err)
			assert.Nil(t, plan)
			assert.Contains(t, err.Error(), tt.want)
			assert.Empty(t, weights.published)
			assert.Empty(t, weights.cleared)
		})
	}
}

func TestNonmatchingSelectorDoesNotBypassLegacyLifecycleState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*v1beta1.InferenceService)
		want   string
	}{
		{
			name: "endpoint finalizer",
			mutate: func(owner *v1beta1.InferenceService) {
				owner.Finalizers = []string{placementendpoint.EndpointFinalizer}
			},
			want: placementendpoint.EndpointFinalizer,
		},
		{
			name: "legacy annotation",
			mutate: func(owner *v1beta1.InferenceService) {
				owner.Annotations = map[string]string{legacyTargetsAnnotation: `["old.example"]`}
			},
			want: legacyTargetsAnnotation,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := testISVC("model-a")
			tc.mutate(owner)
			weights := &recordingPublisher{}
			plan, err := newTestPublisher(weights).Plan(
				owner,
				testTrafficMap(),
				targetOptionsWithSelector("{{.EndpointHostname}}", "publish=true"),
			)
			require.Error(t, err)
			assert.Nil(t, plan)
			assert.Contains(t, err.Error(), tc.want)
			assert.Empty(t, weights.published)
			assert.Empty(t, weights.cleared)
		})
	}
}

func TestPlanValidatesAndCanonicalizesTargets(t *testing.T) {
	owner := testISVC("model-a")
	trafficMap := testTrafficMap()
	p := newTestPublisher(&recordingPublisher{})

	_, err := p.Plan(owner, trafficMap, targetOptions("same-target"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than one entry")

	_, err = p.Plan(owner, trafficMap, targetOptions("scope/{{.Cluster}}"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without a scope prefix")

	plan, err := p.Plan(owner, trafficMap, targetOptions("{{.Cluster}};fallback=other"))
	require.NoError(t, err)
	assert.Equal(t, []string{"home-a", "home-b"}, plan.Claims())
}

func TestPlanRejectsInvalidTrafficMap(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.TrafficMap)
		want   string
	}{
		{name: "empty service", mutate: func(tm *v1beta1.TrafficMap) { tm.Spec.Service = "" }, want: "service"},
		{name: "nil endpoint", mutate: func(tm *v1beta1.TrafficMap) { tm.Spec.Entries[0].Endpoint = nil }, want: "endpoint"},
		{name: "empty host", mutate: func(tm *v1beta1.TrafficMap) {
			tm.Spec.Entries[0].Endpoint = &apis.URL{Scheme: "https"}
		}, want: "endpoint"},
		{name: "negative weight", mutate: func(tm *v1beta1.TrafficMap) { tm.Spec.Entries[0].Weight = -1 }, want: "negative"},
		{name: "too many entries", mutate: func(tm *v1beta1.TrafficMap) {
			tm.Spec.Entries = make([]v1beta1.TrafficMapEntry, v1beta1.MaxTrafficMapPublisherTargets+1)
		}, want: "maximum"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trafficMap := testTrafficMap()
			tt.mutate(trafficMap)
			plan, err := newTestPublisher(&recordingPublisher{}).Plan(
				testISVC("model-a"), trafficMap, targetOptions("{{.EndpointHostname}}"),
			)
			require.Error(t, err)
			assert.Nil(t, plan)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestDrainPublishesZeroForClaims(t *testing.T) {
	weights := &recordingPublisher{}
	p := newTestPublisher(weights)

	require.NoError(t, p.Drain(context.Background(), types.NamespacedName{}, []string{"b.example", "a.example"}))
	assert.Equal(t, []weightedTarget{
		{name: "a.example", weight: 0},
		{name: "b.example", weight: 0},
	}, weights.published)
}

func TestApplyAndDrainStopOnFailure(t *testing.T) {
	weights := &recordingPublisher{publishFailures: map[string]error{"a.example": errors.New("unavailable")}}
	p := newTestPublisher(weights)

	err := p.Drain(context.Background(), types.NamespacedName{}, []string{"a.example", "b.example"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a.example")
	assert.Equal(t, []weightedTarget{{name: "a.example", weight: 0}}, weights.published)

	weights.publishFailures["a.example"] = errors.New("unavailable")
	weights.published = nil
	_, err = p.Apply(context.Background(), &publishPlan{
		targets: []weightedTarget{{name: "a.example", weight: 1}, {name: "b.example", weight: 2}},
		claims:  []string{"a.example", "b.example"},
	})
	require.Error(t, err)
	assert.Equal(t, []weightedTarget{{name: "a.example", weight: 1}}, weights.published)
}

func TestUnpublishClearsOnlyJournaledTargets(t *testing.T) {
	weights := &recordingPublisher{}
	p := newTestPublisher(weights)
	journal := v1beta1.TrafficMapPublisherStatus{
		PublisherName:         Name,
		ClaimedTargets:        []string{"b.example", "a.example"},
		ObservedOptionsDigest: "sha256:not-used-for-cleanup",
	}

	require.NoError(t, p.Unpublish(
		context.Background(),
		types.NamespacedName{Namespace: "deleted", Name: "map"},
		journal,
	))
	assert.Equal(t, []string{"a.example", "b.example"}, weights.cleared)
}

func TestDrainAndUnpublishValidateAllClaimsBeforeMutation(t *testing.T) {
	tests := []struct {
		name   string
		claims []string
	}{
		{name: "scope prefix", claims: []string{"valid.example", "scope/invalid"}},
		{name: "fallback suffix", claims: []string{"valid.example", "other;fallback=backup"}},
		{name: "surrounding whitespace", claims: []string{"valid.example", " other.example "}},
		{name: "duplicate", claims: []string{"valid.example", "valid.example"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			weights := &recordingPublisher{}
			p := newTestPublisher(weights)

			require.Error(t, p.Drain(context.Background(), types.NamespacedName{}, tt.claims))
			assert.Empty(t, weights.published)

			require.Error(t, p.Unpublish(
				context.Background(),
				types.NamespacedName{},
				v1beta1.TrafficMapPublisherStatus{PublisherName: Name, ClaimedTargets: tt.claims},
			))
			assert.Empty(t, weights.cleared)
		})
	}
}

func TestUnpublishFailureAndJournalOwnership(t *testing.T) {
	weights := &recordingPublisher{clearFailures: map[string]error{"a.example": errors.New("unavailable")}}
	p := newTestPublisher(weights)

	err := p.Unpublish(context.Background(), types.NamespacedName{}, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  Name,
		ClaimedTargets: []string{"a.example", "b.example"},
	})
	require.Error(t, err)
	assert.Equal(t, []string{"a.example"}, weights.cleared)

	weights.cleared = nil
	err = p.Unpublish(context.Background(), types.NamespacedName{}, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "another-publisher",
		ClaimedTargets: []string{"a.example"},
	})
	require.Error(t, err)
	assert.Empty(t, weights.cleared)
}

type foreignPlan struct{}

func (foreignPlan) Claims() []string { return nil }

func TestApplyRejectsForeignPlan(t *testing.T) {
	weights := &recordingPublisher{}
	_, err := newTestPublisher(weights).Apply(context.Background(), foreignPlan{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plan of type")
	assert.Empty(t, weights.published)
}
