package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/pkg/modelcache"
)

func TestClientRecordsCalls(t *testing.T) {
	source := modelcache.Source{URI: "s3://bucket/model"}
	object := modelcache.Object{
		Source:    source,
		Path:      "s3://bucket/model/config.json",
		SizeBytes: 1024,
		Version:   "v1",
	}

	client := &Client{
		CheckSourceFunc: func(ctx context.Context, got modelcache.Source) (*modelcache.SourceStatus, error) {
			return &modelcache.SourceStatus{Reachable: true}, nil
		},
		InspectSourceFunc: func(ctx context.Context, got modelcache.Source) (*modelcache.SourceInspection, error) {
			return &modelcache.SourceInspection{
				Objects: []modelcache.Object{object},
				Metadata: modelcache.ModelMetadataInputs{
					ConfigJSON: []byte("metadata"),
				},
			}, nil
		},
		StartPrewarmFunc: func(ctx context.Context, got modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
			return &modelcache.PrewarmOperation{ID: "op-1"}, nil
		},
		GetPrewarmStatusFunc: func(ctx context.Context, got modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{Operation: got, Phase: modelcache.PrewarmPhaseRunning}, nil
		},
	}

	sourceStatus, err := client.CheckSource(context.Background(), source)
	require.NoError(t, err)
	assert.True(t, sourceStatus.Reachable)

	inspection, err := client.InspectSource(context.Background(), source)
	require.NoError(t, err)
	require.NotNil(t, inspection)
	assert.Equal(t, []modelcache.Object{object}, inspection.Objects)
	assert.Equal(t, []byte("metadata"), inspection.Metadata.ConfigJSON)

	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{Source: source})
	require.NoError(t, err)
	assert.Equal(t, "op-1", operation.ID)

	prewarmStatus, err := client.GetPrewarmStatus(context.Background(), *operation)
	require.NoError(t, err)
	assert.Equal(t, modelcache.PrewarmPhaseRunning, prewarmStatus.Phase)

	assert.Equal(t, []modelcache.Source{source}, client.CheckSourceCalls())
	assert.Equal(t, []modelcache.Source{source}, client.InspectSourceCalls())
	assert.Equal(t, []modelcache.PrewarmRequest{{Source: source}}, client.StartPrewarmCalls())
	assert.Equal(t, []modelcache.PrewarmOperation{{ID: "op-1"}}, client.GetPrewarmStatusCalls())
}

func TestClientReturnsNotImplemented(t *testing.T) {
	client := &Client{}

	_, err := client.CheckSource(context.Background(), modelcache.Source{})
	assert.ErrorIs(t, err, ErrNotImplemented)
}
