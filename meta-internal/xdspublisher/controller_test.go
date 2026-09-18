//go:build meta_internal

package xdspublisher

import (
	"context"
	"errors"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
)

type recordingPublisher struct {
	published       []weightedTarget
	cleared         []string
	publishFailures map[string]error
	clearFailures   map[string]error
}

func (p *recordingPublisher) PublishWeight(target string, weight uint32) error {
	p.published = append(p.published, weightedTarget{name: target, weight: weight})
	if err := p.publishFailures[target]; err != nil {
		delete(p.publishFailures, target)
		return err
	}
	return nil
}

func (p *recordingPublisher) ClearWeight(target string) error {
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

func testTarget() placementendpoint.Target {
	return placementendpoint.Target{Homes: []placementendpoint.Home{
		{Cluster: "home-b", Endpoint: "https://model-a.team-a.home-b.mks:8443", BackendHost: "model-a.team-a.home-b.mks", Weight: 30},
		{Cluster: "home-a", Endpoint: "https://model-a.team-a.home-a.mks", BackendHost: "model-a.team-a.home-a.mks", Weight: 70},
	}}
}

func newTestPublisher(
	t *testing.T,
	weights ClusterWeightPublisher,
	objects ...client.Object,
) (*Publisher, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return &Publisher{
		Client:    c,
		APIReader: c,
		config: config{
			targetTemplate: template.Must(template.New("target").Parse("{{.EndpointHostname}}")),
		},
		weights: weights,
	}, c
}

func TestPublishWritesWeightsAndJournalsTargets(t *testing.T) {
	isvc := testISVC("model-a")
	weights := &recordingPublisher{}
	p, c := newTestPublisher(t, weights, isvc)

	require.NoError(t, p.Publish(context.Background(), isvc, testTarget()))
	assert.Equal(t, []weightedTarget{
		{name: "model-a.team-a.home-a.mks", weight: 70},
		{name: "model-a.team-a.home-b.mks", weight: 30},
	}, weights.published)

	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
	assert.Equal(t,
		`["model-a.team-a.home-a.mks","model-a.team-a.home-b.mks"]`,
		got.Annotations[targetsAnnotation])
}

func TestPublishZeroesRemovedTargetsBeforePositiveWeights(t *testing.T) {
	isvc := testISVC("model-a")
	isvc.Annotations = map[string]string{
		targetsAnnotation: `["model-a.team-a.home-a.mks","old.mks"]`,
	}
	target := testTarget()
	target.Homes[0].Weight = 0
	weights := &recordingPublisher{}
	p, c := newTestPublisher(t, weights, isvc)

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	assert.Equal(t, []weightedTarget{
		{name: "old.mks", weight: 0},
		{name: "model-a.team-a.home-b.mks", weight: 0},
		{name: "model-a.team-a.home-a.mks", weight: 70},
	}, weights.published)

	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
	assert.Equal(t,
		`["model-a.team-a.home-a.mks","model-a.team-a.home-b.mks","old.mks"]`,
		got.Annotations[targetsAnnotation])
}

func TestPublishPropagatesAllZeroAndRecovery(t *testing.T) {
	isvc := testISVC("model-a")
	weights := &recordingPublisher{}
	p, _ := newTestPublisher(t, weights, isvc)
	target := testTarget()
	for i := range target.Homes {
		target.Homes[i].Weight = 0
	}

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	assert.Equal(t, []weightedTarget{
		{name: "model-a.team-a.home-a.mks", weight: 0},
		{name: "model-a.team-a.home-b.mks", weight: 0},
	}, weights.published)

	weights.published = nil
	target.Homes[0].Weight = 30
	target.Homes[1].Weight = 70
	require.NoError(t, p.Publish(context.Background(), isvc, target))
	assert.Equal(t, []weightedTarget{
		{name: "model-a.team-a.home-a.mks", weight: 70},
		{name: "model-a.team-a.home-b.mks", weight: 30},
	}, weights.published)
}

func TestPublishEmptyTargetHoldsPriorTargetsAtZero(t *testing.T) {
	isvc := testISVC("model-a")
	isvc.Annotations = map[string]string{targetsAnnotation: `["a.mks","b.mks"]`}
	weights := &recordingPublisher{}
	p, c := newTestPublisher(t, weights, isvc)

	require.NoError(t, p.Publish(context.Background(), isvc, placementendpoint.Target{}))
	assert.Equal(t, []weightedTarget{{name: "a.mks", weight: 0}, {name: "b.mks", weight: 0}}, weights.published)

	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
	assert.Equal(t, `["a.mks","b.mks"]`, got.Annotations[targetsAnnotation])
}

func TestPartialPublishLeavesAttemptedTargetsJournaled(t *testing.T) {
	isvc := testISVC("model-a")
	weights := &recordingPublisher{publishFailures: map[string]error{
		"model-a.team-a.home-b.mks": errors.New("native publisher unavailable"),
	}}
	p, c := newTestPublisher(t, weights, isvc)

	err := p.Publish(context.Background(), isvc, testTarget())
	require.Error(t, err)
	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
	assert.Equal(t,
		`["model-a.team-a.home-a.mks","model-a.team-a.home-b.mks"]`,
		got.Annotations[targetsAnnotation])
}

func TestUnpublishClearsJournaledTargets(t *testing.T) {
	isvc := testISVC("model-a")
	isvc.Annotations = map[string]string{targetsAnnotation: `["a.mks","b.mks"]`}
	weights := &recordingPublisher{}
	p, c := newTestPublisher(t, weights, isvc)

	require.NoError(t, p.Unpublish(context.Background(), isvc))
	assert.Equal(t, []string{"a.mks", "b.mks"}, weights.cleared)
	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
	assert.NotContains(t, got.Annotations, targetsAnnotation)
}

func TestUnpublishFailureRetainsJournal(t *testing.T) {
	isvc := testISVC("model-a")
	isvc.Annotations = map[string]string{targetsAnnotation: `["a.mks"]`}
	weights := &recordingPublisher{clearFailures: map[string]error{"a.mks": errors.New("SMC unavailable")}}
	p, c := newTestPublisher(t, weights, isvc)

	require.Error(t, p.Unpublish(context.Background(), isvc))
	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
	assert.Equal(t, `["a.mks"]`, got.Annotations[targetsAnnotation])
}

func TestPublishRejectsTargetClaimedByAnotherService(t *testing.T) {
	owner := testISVC("owner")
	owner.Annotations = map[string]string{targetsAnnotation: `["model-a.team-a.home-a.mks"]`}
	contender := testISVC("model-a")
	target := testTarget()
	target.Homes = target.Homes[1:]
	weights := &recordingPublisher{}
	p, _ := newTestPublisher(t, weights, owner, contender)

	err := p.Publish(context.Background(), contender, target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already claimed")
	assert.Empty(t, weights.published)
}

func TestDesiredTargetsValidateAndCanonicalize(t *testing.T) {
	isvc := testISVC("model-a")
	p, _ := newTestPublisher(t, &recordingPublisher{}, isvc)

	p.config.targetTemplate = template.Must(template.New("same").Parse("same-target"))
	_, err := p.desiredTargets(isvc, testTarget())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than one entry")

	p.config.targetTemplate = template.Must(template.New("scoped").Parse("scope/{{.Cluster}}"))
	_, err = p.desiredTargets(isvc, testTarget())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without a scope prefix")

	p.config.targetTemplate = template.Must(template.New("fallback").Parse("{{.Cluster}};fallback=other"))
	targets, err := p.desiredTargets(isvc, testTarget())
	require.NoError(t, err)
	assert.Equal(t, []weightedTarget{{name: "home-a", weight: 70}, {name: "home-b", weight: 30}}, targets)
}
