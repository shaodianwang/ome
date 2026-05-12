package clustercache

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/pkg/modelcache"
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
		require.Equal(t, "/file_metadata", r.URL.Path)
		gotPath = r.URL.Query().Get("path")
		writeJSON(t, w, fileMetadataResponse{
			Root: "s3://bucket/model",
			Files: []fileMetadataEntry{
				{
					Path:         "s3://bucket/model/config.json",
					RelativePath: "config.json",
					Size:         100,
				},
			},
		})
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	status, err := client.CheckSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.True(t, status.Reachable)
	assert.Equal(t, "s3://bucket/model", gotPath)
}

func TestCheckSourceFallsBackToHeadWhenFileMetadataUnsupported(t *testing.T) {
	var gotHeadPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			http.NotFound(w, r)
		case "/head":
			gotHeadPath = r.URL.Query().Get("path")
			writeJSON(t, w, headResponse{Size: 100, MTime: 123})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	status, err := client.CheckSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.True(t, status.Reachable)
	assert.Equal(t, "s3://bucket/model", gotHeadPath)
}

func TestCheckSourceRejectsEmptyFileMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/file_metadata", r.URL.Path)
		writeJSON(t, w, fileMetadataResponse{Root: "s3://bucket/model"})
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	status, err := client.CheckSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.NoError(t, err)
	require.NotNil(t, status)
	assert.False(t, status.Reachable)
	assert.Equal(t, reasonSourceNotReachable, status.Reason)
	assert.Contains(t, status.Message, "no files")
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

func TestCheckSourceReturnsErrorForTransientFileMetadataFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/file_metadata", r.URL.Path)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	status, err := client.CheckSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "file_metadata returned 503")
}

func TestCheckSourceReturnsErrorForTransientHeadFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			http.NotFound(w, r)
		case "/head":
			http.Error(w, "busy", http.StatusTooManyRequests)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	status, err := client.CheckSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "head returned 429")
}

func TestFileMetadataInspectionUsesCanonicalRootAndRelativePaths(t *testing.T) {
	mtime := uint64(1710000000)
	source := modelcache.Source{URI: "manifold://bucket/model"}
	response := &fileMetadataResponse{
		Root: "s3://bucket/model",
		Files: []fileMetadataEntry{
			{
				Path:  "s3://bucket/model/config.json",
				Size:  100,
				MTime: &mtime,
			},
			{
				Path:         "s3://bucket/model/weights/model-00001-of-00001.safetensors",
				RelativePath: "weights/model-00001-of-00001.safetensors",
				Size:         200,
			},
		},
	}

	files := metadataFilesFromResponse(source, response)
	require.Len(t, files, 2)
	assert.Equal(t, "config.json", files[0].relativePath)
	assert.Equal(t, "weights/model-00001-of-00001.safetensors", files[1].relativePath)

	inspection := response.toModelCacheSourceInspection(source)
	require.NotNil(t, inspection)
	assert.Equal(t, "s3://bucket/model", inspection.RootURI)
	require.Len(t, inspection.Objects, 2)
	assert.Equal(t, "config.json", inspection.Objects[0].RelativePath)
	assert.Equal(t, "weights/model-00001-of-00001.safetensors", inspection.Objects[1].RelativePath)
}

func TestInspectSourceUsesFileMetadataRingAndPackedRanges(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	indexPath := sourceURI + "/model.safetensors.index.json"
	weightPath := sourceURI + "/model-00001-of-00001.safetensors"

	configJSON := []byte(`{"model_type": "phi"}`)
	indexJSON := []byte(`{"weight_map":{"weight":"model-00001-of-00001.safetensors"}}`)
	headerJSON := []byte(`{"weight": {"dtype": "F32", "shape": [2, 3], "data_offsets": [0, 24]}}`)
	safetensorsData := safetensorsFileForTest(headerJSON, 128)
	mtimeConfig := uint64(1710000000)
	mtimeIndex := uint64(1710000001)
	mtimeWeight := uint64(1710000002)
	chunkSize := uint64(16)

	var serverAddr string
	var gotFetch fetchRequest
	var packedRangeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			require.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, sourceURI, r.URL.Query().Get("path"))
			writeJSON(t, w, fileMetadataResponse{
				Root: sourceURI,
				Files: []fileMetadataEntry{
					{Path: configPath, RelativePath: "config.json", Size: uint64(len(configJSON)), MTime: &mtimeConfig, ETag: stringPtr("config-etag")},
					{Path: indexPath, RelativePath: "model.safetensors.index.json", Size: uint64(len(indexJSON)), MTime: &mtimeIndex, ETag: stringPtr("index-etag")},
					{Path: weightPath, RelativePath: "model-00001-of-00001.safetensors", Size: uint64(len(safetensorsData)), MTime: &mtimeWeight, ETag: stringPtr("weight-etag")},
				},
			})
		case "/fetch":
			require.Equal(t, http.MethodPost, r.Method)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&gotFetch))
			writeJSON(t, w, fetchResponse{
				Results: []fetchPathResult{
					{
						Path:      configPath,
						Status:    "cached",
						TotalSize: uint64Ptr(uint64(len(configJSON))),
						MTime:     uint64Ptr(mtimeConfig),
						ChunkSize: uint64Ptr(chunkSize),
					},
				},
			})
		case "/ring_info":
			require.Equal(t, http.MethodGet, r.Method)
			writeJSON(t, w, ringInfoResponse{Nodes: []string{serverAddr}})
		case "/packed_byte_ranges_by_key":
			require.Equal(t, http.MethodPost, r.Method)
			packedRangeRequests++
			var req packedByteRangeBatchRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			for _, rangeSpec := range req.Ranges {
				data := map[string][]byte{
					configPath: configJSON,
					indexPath:  indexJSON,
					weightPath: safetensorsData,
				}[rangeSpec.Path]
				start := rangeSpec.ChunkOffset + rangeSpec.RangeOffset
				end := start + rangeSpec.Length
				require.LessOrEqual(t, end, uint64(len(data)))
				_, _ = w.Write(data[start:end])
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverAddr = server.Listener.Addr().String()

	client := newTestClient(t, server.URL)
	inspection, err := client.InspectSource(context.Background(), modelcache.Source{URI: sourceURI})

	require.NoError(t, err)
	require.NotNil(t, inspection)
	assert.Equal(t, fetchRequest{Paths: []string{configPath}}, gotFetch)
	assert.GreaterOrEqual(t, packedRangeRequests, 3)
	assert.Equal(t, sourceURI, inspection.RootURI)
	require.Len(t, inspection.Objects, 3)
	assert.JSONEq(t, string(configJSON), string(inspection.Metadata.ConfigJSON))
	assert.Equal(t, "config.json", inspection.Metadata.ConfigPath)
	assert.JSONEq(t, string(indexJSON), string(inspection.Metadata.SafetensorsIndexJSON))
	assert.Equal(t, "model.safetensors.index.json", inspection.Metadata.SafetensorsIndexPath)
	require.Len(t, inspection.Metadata.SafetensorHeaders, 1)
	assert.Equal(t, weightPath, inspection.Metadata.SafetensorHeaders[0].Path)
	assert.Equal(t, "model-00001-of-00001.safetensors", inspection.Metadata.SafetensorHeaders[0].RelativePath)
	assert.Equal(t, int64(len(safetensorsData)), inspection.Metadata.SafetensorHeaders[0].SizeBytes)
	assert.Equal(t, "weight-etag", inspection.Metadata.SafetensorHeaders[0].Version)
	assert.JSONEq(t, string(headerJSON), string(inspection.Metadata.SafetensorHeaders[0].HeaderJSON))
}

func TestInspectSourceFetchesHFQuantConfigWhenPresent(t *testing.T) {
	// hf_quant_config.json is the NVIDIA ModelOpt / TensorRT-LLM
	// fallback for quantization metadata. Verify the provider fetches
	// it through the same packed-byte-ranges path as config.json and
	// surfaces it in inspection.Metadata.HFQuantConfigJSON. Absence
	// is handled by the existing TestInspectSourceUsesFileMetadataRingAndPackedRanges
	// test (which does NOT include the file).
	const sourceURI = "s3://bucket/quant-model"
	configPath := sourceURI + "/config.json"
	hfQuantPath := sourceURI + "/hf_quant_config.json"

	configJSON := []byte(`{"model_type":"llama4x_text"}`)
	hfQuantJSON := []byte(`{"quant_method":"modelopt","quantization":{"quant_algo":"NVFP4","group_size":16}}`)
	mtimeConfig := uint64(1710000000)
	mtimeQuant := uint64(1710000001)
	chunkSize := uint64(64)

	var serverAddr string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			writeJSON(t, w, fileMetadataResponse{
				Root: sourceURI,
				Files: []fileMetadataEntry{
					{Path: configPath, RelativePath: "config.json", Size: uint64(len(configJSON)), MTime: &mtimeConfig, ETag: stringPtr("config-etag")},
					{Path: hfQuantPath, RelativePath: "hf_quant_config.json", Size: uint64(len(hfQuantJSON)), MTime: &mtimeQuant, ETag: stringPtr("quant-etag")},
				},
			})
		case "/fetch":
			writeJSON(t, w, fetchResponse{
				Results: []fetchPathResult{{
					Path:      configPath,
					Status:    "cached",
					TotalSize: uint64Ptr(uint64(len(configJSON))),
					MTime:     uint64Ptr(mtimeConfig),
					ChunkSize: uint64Ptr(chunkSize),
				}},
			})
		case "/ring_info":
			writeJSON(t, w, ringInfoResponse{Nodes: []string{serverAddr}})
		case "/packed_byte_ranges_by_key":
			var req packedByteRangeBatchRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			for _, rangeSpec := range req.Ranges {
				data := map[string][]byte{
					configPath:  configJSON,
					hfQuantPath: hfQuantJSON,
				}[rangeSpec.Path]
				start := rangeSpec.ChunkOffset + rangeSpec.RangeOffset
				end := start + rangeSpec.Length
				_, _ = w.Write(data[start:end])
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverAddr = server.Listener.Addr().String()

	client := newTestClient(t, server.URL)
	inspection, err := client.InspectSource(context.Background(), modelcache.Source{URI: sourceURI})
	require.NoError(t, err)
	require.NotNil(t, inspection)

	assert.JSONEq(t, string(hfQuantJSON), string(inspection.Metadata.HFQuantConfigJSON),
		"provider must surface hf_quant_config.json bytes in inspection.Metadata.HFQuantConfigJSON")
	assert.Equal(t, "hf_quant_config.json", inspection.Metadata.HFQuantConfigPath)
}

func TestInspectSourceRequiresConfigOrModelIndex(t *testing.T) {
	mtime := uint64(1710000000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			writeJSON(t, w, fileMetadataResponse{
				Root: "s3://bucket/model",
				Files: []fileMetadataEntry{
					{
						Path:         "s3://bucket/model/model-00001-of-00001.safetensors",
						RelativePath: "model-00001-of-00001.safetensors",
						Size:         1024,
						MTime:        &mtime,
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.InspectSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "config.json")
}

func TestInspectSourceReportsEndpointError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.InspectSource(context.Background(), modelcache.Source{URI: "s3://bucket/model"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "file_metadata returned 503")
	assert.Contains(t, err.Error(), "unavailable")
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
	payload, err := decodeOperationID(operation.ID)
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/model", payload.SourceURI)
	assert.Empty(t, payload.Paths)
	assert.NotEmpty(t, payload.PathsHash)

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
		switch r.URL.Path {
		case "/file_metadata":
			writeJSON(t, w, fileMetadataResponse{
				Root: "s3://bucket/model",
				Files: []fileMetadataEntry{
					{Path: "s3://bucket/model/config.json", RelativePath: "config.json", Size: 100},
				},
			})
		case "/fetch":
			fetchCalls <- struct{}{}
			writeJSON(t, w, fetchResponse{
				Results: []fetchPathResult{
					{
						Path:   "s3://bucket/model/config.json",
						Status: "cached",
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	operationID, err := encodeOperationID(modelcache.Source{URI: "s3://bucket/model"}, []string{"s3://bucket/model/config.json"})
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
	operationID, err := encodeOperationID(modelcache.Source{}, []string{"s3://bucket/model"})
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

	_, err := client.InspectSource(context.Background(), modelcache.Source{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source URI is required")
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

func uint64Ptr(value uint64) *uint64 {
	return &value
}

func safetensorsFileForTest(headerJSON []byte, payloadBytes int) []byte {
	data := make([]byte, 8+len(headerJSON)+payloadBytes)
	binary.LittleEndian.PutUint64(data[:8], uint64(len(headerJSON)))
	copy(data[8:], headerJSON)
	return data
}
