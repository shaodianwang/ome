//go:build meta_internal

package xdspublisher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"

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
	return map[string]string{optTargetTemplate: value}
}

func TestPublisherIdentityAndState(t *testing.T) {
	p := newTestPublisher(&recordingPublisher{})
	assert.Equal(t, Name, p.Name())
	assert.True(t, p.Stateful())
}

func TestResolveOptionsKeepsOnlyGlobalTargetTemplate(t *testing.T) {
	p := newTestPublisher(&recordingPublisher{})
	global := targetOptions("  {{.EndpointHostname}}  ")

	effective, err := p.ResolveOptions(global, nil)
	require.NoError(t, err)
	assert.Equal(t, targetOptions("{{.EndpointHostname}}"), effective)

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
		{name: "model-a.team-a.home-b.example", weight: 30},
	}, weights.published)
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

	require.NoError(t, p.Drain(context.Background(), []string{"b.example", "a.example"}))
	assert.Equal(t, []weightedTarget{
		{name: "a.example", weight: 0},
		{name: "b.example", weight: 0},
	}, weights.published)
}

func TestApplyAndDrainStopOnFailure(t *testing.T) {
	weights := &recordingPublisher{publishFailures: map[string]error{"a.example": errors.New("unavailable")}}
	p := newTestPublisher(weights)

	err := p.Drain(context.Background(), []string{"a.example", "b.example"})
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

			require.Error(t, p.Drain(context.Background(), tt.claims))
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
