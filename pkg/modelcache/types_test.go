package modelcache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type noopClient struct{}

func (noopClient) CheckSource(context.Context, Source) (*SourceStatus, error) {
	return nil, nil
}

func (noopClient) ListSource(context.Context, Source) ([]Object, error) {
	return nil, nil
}

func (noopClient) GetObject(context.Context, ObjectRef) (*Object, error) {
	return nil, nil
}

func (noopClient) ReadMetadataRange(context.Context, MetadataRangeRequest) ([]byte, error) {
	return nil, nil
}

func (noopClient) StartPrewarm(context.Context, PrewarmRequest) (*PrewarmOperation, error) {
	return nil, nil
}

func (noopClient) GetPrewarmStatus(context.Context, PrewarmOperation) (*PrewarmStatus, error) {
	return nil, nil
}

func TestProviderConfigured(t *testing.T) {
	assert.True(t, NewProvider("test_cache", noopClient{}).Configured())
	assert.False(t, NewProvider("", noopClient{}).Configured())
	assert.False(t, NewProvider("test_cache", nil).Configured())
	assert.False(t, Provider{}.Configured())
}
