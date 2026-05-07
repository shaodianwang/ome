package modelcache

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestRegistryBuild(t *testing.T) {
	registry := NewRegistry()
	err := registry.Register("test_cache", func(config ProviderConfig) (Client, error) {
		assert.Equal(t, "test_cache", config.Provider)
		assert.Equal(t, "http://cache.example", config.Endpoint)
		assert.Equal(t, map[string]string{"timeout": "5s"}, config.Options)
		return noopClient{}, nil
	})
	require.NoError(t, err)

	provider, err := registry.Build(ProviderConfig{
		Provider: " test_cache ",
		Endpoint: "http://cache.example",
		Options:  map[string]string{"timeout": "5s"},
	})
	require.NoError(t, err)

	assert.Equal(t, "test_cache", provider.Name)
	assert.True(t, provider.Configured())
}

func TestRegistryBuildDisabled(t *testing.T) {
	provider, err := NewRegistry().Build(ProviderConfig{})
	require.NoError(t, err)
	assert.False(t, provider.Configured())
}

func TestRegistryBuildUnknownProvider(t *testing.T) {
	provider, err := NewRegistry().Build(ProviderConfig{Provider: "missing"})
	assert.Error(t, err)
	assert.False(t, provider.Configured())
}

func TestRegistryRegisterValidatesProvider(t *testing.T) {
	registry := NewRegistry()

	assert.Error(t, registry.Register("", func(config ProviderConfig) (Client, error) {
		return noopClient{}, nil
	}))
	assert.Error(t, registry.Register("test_cache", nil))
	require.NoError(t, registry.Register("test_cache", func(config ProviderConfig) (Client, error) {
		return noopClient{}, nil
	}))
	assert.Error(t, registry.Register("test_cache", func(config ProviderConfig) (Client, error) {
		return noopClient{}, nil
	}))
}

func TestRegistryBuildPropagatesFactoryErrors(t *testing.T) {
	registry := NewRegistry()
	expectedErr := errors.New("factory failed")
	require.NoError(t, registry.Register("test_cache", func(config ProviderConfig) (Client, error) {
		return nil, expectedErr
	}))

	provider, err := registry.Build(ProviderConfig{Provider: "test_cache"})
	assert.ErrorIs(t, err, expectedErr)
	assert.False(t, provider.Configured())
}

func TestRegistryBuildRejectsNilClient(t *testing.T) {
	registry := NewRegistry()
	require.NoError(t, registry.Register("test_cache", func(config ProviderConfig) (Client, error) {
		return nil, nil
	}))

	provider, err := registry.Build(ProviderConfig{Provider: "test_cache"})
	assert.Error(t, err)
	assert.False(t, provider.Configured())
}
