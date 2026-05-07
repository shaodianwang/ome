package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sgl-project/ome/pkg/modelcache"
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
		ListSourceFunc: func(ctx context.Context, got modelcache.Source) ([]modelcache.Object, error) {
			return []modelcache.Object{object}, nil
		},
		GetObjectFunc: func(ctx context.Context, got modelcache.ObjectRef) (*modelcache.Object, error) {
			return &object, nil
		},
		ReadMetadataRangeFunc: func(ctx context.Context, got modelcache.MetadataRangeRequest) ([]byte, error) {
			return []byte("metadata"), nil
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

	objects, err := client.ListSource(context.Background(), source)
	require.NoError(t, err)
	assert.Equal(t, []modelcache.Object{object}, objects)

	gotObject, err := client.GetObject(context.Background(), modelcache.ObjectRef{Source: source, Path: object.Path})
	require.NoError(t, err)
	assert.Equal(t, &object, gotObject)

	data, err := client.ReadMetadataRange(context.Background(), modelcache.MetadataRangeRequest{Object: object, Offset: 0, Length: 128})
	require.NoError(t, err)
	assert.Equal(t, []byte("metadata"), data)

	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{Source: source})
	require.NoError(t, err)
	assert.Equal(t, "op-1", operation.ID)

	prewarmStatus, err := client.GetPrewarmStatus(context.Background(), *operation)
	require.NoError(t, err)
	assert.Equal(t, modelcache.PrewarmPhaseRunning, prewarmStatus.Phase)

	assert.Equal(t, []modelcache.Source{source}, client.CheckSourceCalls())
	assert.Equal(t, []modelcache.Source{source}, client.ListSourceCalls())
	assert.Equal(t, []modelcache.ObjectRef{{Source: source, Path: object.Path}}, client.GetObjectCalls())
	assert.Equal(t, []modelcache.MetadataRangeRequest{{Object: object, Offset: 0, Length: 128}}, client.ReadMetadataRangeCalls())
	assert.Equal(t, []modelcache.PrewarmRequest{{Source: source}}, client.StartPrewarmCalls())
	assert.Equal(t, []modelcache.PrewarmOperation{{ID: "op-1"}}, client.GetPrewarmStatusCalls())
}

func TestClientReturnsNotImplemented(t *testing.T) {
	client := &Client{}

	_, err := client.CheckSource(context.Background(), modelcache.Source{})
	assert.ErrorIs(t, err, ErrNotImplemented)
}
