package clustercache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sgl-project/ome/pkg/modelcache"
)

func TestProviderRegistration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	provider, err := modelcache.BuildProvider(modelcache.ProviderConfig{
		Provider: modelcache.ProviderClusterCache,
		Endpoint: server.URL,
	})

	require.NoError(t, err)
	assert.Equal(t, modelcache.ProviderClusterCache, provider.Name)
	assert.True(t, provider.Configured())
}

func TestNewClientValidatesConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  modelcache.ProviderConfig
		wantErr bool
	}{
		{
			name:    "missing endpoint",
			config:  modelcache.ProviderConfig{},
			wantErr: true,
		},
		{
			name: "bad timeout",
			config: modelcache.ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"timeout": "bad"},
			},
			wantErr: true,
		},
		{
			name: "bad operation retention",
			config: modelcache.ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"operation_retention": "bad"},
			},
			wantErr: true,
		},
		{
			name: "negative operation retention",
			config: modelcache.ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"operation_retention": "-1s"},
			},
			wantErr: true,
		},
		{
			name: "host port endpoint",
			config: modelcache.ProviderConfig{
				Endpoint: "cluster-cache.example:10090",
			},
			wantErr: false,
		},
		{
			name: "http endpoint",
			config: modelcache.ProviderConfig{
				Endpoint: "http://cluster-cache.example:10090",
				Options:  map[string]string{"timeout": "5s"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, client)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, client)
		})
	}
}

func TestNewClientDefaultsAndOptions(t *testing.T) {
	client, err := NewClient(modelcache.ProviderConfig{Endpoint: "http://cluster-cache.example"})
	require.NoError(t, err)

	clusterCacheClient := client.(*Client)
	assert.Equal(t, defaultHTTPTimeout, clusterCacheClient.httpClient.Timeout)
	assert.Equal(t, defaultOperationRetention, clusterCacheClient.operationRetention)

	client, err = NewClient(modelcache.ProviderConfig{
		Endpoint: "http://cluster-cache.example",
		Options: map[string]string{
			"timeout":             "7s",
			"operation_retention": "3m",
		},
	})
	require.NoError(t, err)

	clusterCacheClient = client.(*Client)
	assert.Equal(t, 7*time.Second, clusterCacheClient.httpClient.Timeout)
	assert.Equal(t, 3*time.Minute, clusterCacheClient.operationRetention)
}

func TestCheckSource(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/head", r.URL.Path)
		gotPath = r.URL.Query().Get("path")
		writeJSON(t, w, headResponse{Size: 100, MTime: 123})
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	status, err := client.CheckSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.True(t, status.Reachable)
	assert.Equal(t, "s3://bucket/model", gotPath)
}

func TestCheckSourceNotReachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	status, err := client.CheckSource(context.Background(), modelcache.Source{URI: "s3://bucket/missing"})

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.False(t, status.Reachable)
	assert.Equal(t, reasonSourceNotReachable, status.Reason)
	assert.Contains(t, status.Message, "404")
}

func TestStartPrewarmPostsFetchAndReportsSuccess(t *testing.T) {
	fetchCalled := make(chan fetchRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/fetch", r.URL.Path)

		var req fetchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		fetchCalled <- req

		totalSize := uint64(100)
		totalChunks := 10
		cachedChunks := 4
		warmedChunks := 6
		writeJSON(t, w, fetchResponse{
			Results: []fetchPathResult{
				{
					Path:         "s3://bucket/model",
					Status:       "warmed",
					TotalSize:    &totalSize,
					TotalChunks:  &totalChunks,
					CachedChunks: &cachedChunks,
					WarmedChunks: &warmedChunks,
				},
			},
		})
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source: modelcache.Source{URI: "s3://bucket/model"},
	})

	require.NoError(t, err)
	require.NotNil(t, operation)
	assert.NotEmpty(t, operation.ID)

	require.Eventually(t, func() bool {
		status, err := client.GetPrewarmStatus(context.Background(), *operation)
		require.NoError(t, err)
		return status.Phase == modelcache.PrewarmPhaseSucceeded
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, []string{"s3://bucket/model"}, (<-fetchCalled).Paths)

	status, err := client.GetPrewarmStatus(context.Background(), *operation)
	require.NoError(t, err)
	require.NotNil(t, status.Progress.TotalBytes)
	require.NotNil(t, status.Progress.CachedBytes)
	require.NotNil(t, status.Progress.TotalChunks)
	require.NotNil(t, status.Progress.CachedChunks)
	assert.Equal(t, int64(100), *status.Progress.TotalBytes)
	assert.Equal(t, int64(100), *status.Progress.CachedBytes)
	assert.Equal(t, int32(10), *status.Progress.TotalChunks)
	assert.Equal(t, int32(10), *status.Progress.CachedChunks)
}

func TestStartPrewarmReportsFetchFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, fetchResponse{
			Results: []fetchPathResult{
				{
					Path:   "s3://bucket/model",
					Status: "error",
					Error:  stringPtr("source credentials were rejected"),
				},
			},
		})
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source: modelcache.Source{URI: "s3://bucket/model"},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		status, err := client.GetPrewarmStatus(context.Background(), *operation)
		require.NoError(t, err)
		return status.Phase == modelcache.PrewarmPhaseFailed
	}, time.Second, 10*time.Millisecond)

	status, err := client.GetPrewarmStatus(context.Background(), *operation)
	require.NoError(t, err)
	assert.Equal(t, reasonClusterCacheFetchFailed, status.Reason)
	assert.Contains(t, status.Message, "source credentials were rejected")
}

func TestStartPrewarmReportsEmptyFetchFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, fetchResponse{})
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source: modelcache.Source{URI: "s3://bucket/model"},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		status, err := client.GetPrewarmStatus(context.Background(), *operation)
		require.NoError(t, err)
		return status.Phase == modelcache.PrewarmPhaseFailed
	}, time.Second, 10*time.Millisecond)

	status, err := client.GetPrewarmStatus(context.Background(), *operation)
	require.NoError(t, err)
	assert.Equal(t, reasonClusterCacheFetchFailed, status.Reason)
	assert.Contains(t, status.Message, "no path results")
}

func TestGetPrewarmStatusRestartsDecodedOperation(t *testing.T) {
	fetchCalls := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchCalls <- struct{}{}
		writeJSON(t, w, fetchResponse{
			Results: []fetchPathResult{
				{
					Path:   "s3://bucket/model",
					Status: "cached",
				},
			},
		})
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	operationID, err := encodeOperationID([]string{"s3://bucket/model"})
	require.NoError(t, err)

	status, err := client.GetPrewarmStatus(context.Background(), modelcache.PrewarmOperation{ID: operationID})
	require.NoError(t, err)
	assert.Equal(t, modelcache.PrewarmPhaseRunning, status.Phase)

	require.Eventually(t, func() bool {
		status, err := client.GetPrewarmStatus(context.Background(), modelcache.PrewarmOperation{ID: operationID})
		require.NoError(t, err)
		return status.Phase == modelcache.PrewarmPhaseSucceeded
	}, time.Second, 10*time.Millisecond)
	<-fetchCalls
}

func TestTerminalOperationsAreCleanedUpAfterRetention(t *testing.T) {
	client := newTestClient(t, "http://cluster-cache.example")
	operationID, err := encodeOperationID([]string{"s3://bucket/model"})
	require.NoError(t, err)

	client.mu.Lock()
	client.operationRetention = time.Minute
	client.operations[operationID] = &operationState{
		status: modelcache.PrewarmStatus{
			Operation: modelcache.PrewarmOperation{ID: operationID},
			Phase:     modelcache.PrewarmPhaseSucceeded,
		},
		completedAt: time.Now().Add(-2 * time.Minute),
	}
	client.cleanupOperationsLocked(time.Now())
	_, ok := client.operations[operationID]
	client.mu.Unlock()

	assert.False(t, ok)
}

func TestCloseCancelsRunningPrewarm(t *testing.T) {
	requestStarted := make(chan struct{})
	client := newTestClient(t, "http://cluster-cache.example")
	client.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(requestStarted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})

	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source: modelcache.Source{URI: "s3://bucket/model"},
	})
	require.NoError(t, err)

	<-requestStarted
	require.NoError(t, client.Close())

	require.Eventually(t, func() bool {
		status, err := client.GetPrewarmStatus(context.Background(), *operation)
		require.NoError(t, err)
		return status.Phase == modelcache.PrewarmPhaseFailed
	}, time.Second, 10*time.Millisecond)

	status, err := client.GetPrewarmStatus(context.Background(), *operation)
	require.NoError(t, err)
	assert.Equal(t, reasonClusterCacheFetchFailed, status.Reason)
	assert.Contains(t, status.Message, "context canceled")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestUnsupportedOperations(t *testing.T) {
	client := newTestClient(t, "http://cluster-cache.example")

	_, err := client.ListSource(context.Background(), modelcache.Source{})
	assert.Error(t, err)
	_, err = client.GetObject(context.Background(), modelcache.ObjectRef{})
	assert.Error(t, err)
	_, err = client.ReadMetadataRange(context.Background(), modelcache.MetadataRangeRequest{})
	assert.Error(t, err)
}

func newTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()

	client, err := NewClient(modelcache.ProviderConfig{Endpoint: endpoint})
	require.NoError(t, err)
	return client.(*Client)
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(value))
}

func stringPtr(value string) *string {
	return &value
}
