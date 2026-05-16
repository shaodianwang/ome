package clustercache

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
	assert.True(t, errors.Is(err, modelcache.ErrSourceUnparseable),
		"missing config.json/model_index.json is a structural source fault and must wrap ErrSourceUnparseable so the controller transitions to state=Failed instead of looping in MetadataPending forever")
}

// chunk_size is daemon-wide configuration, not per-file. When /fetch
// returns chunk_size on a result whose status=error (e.g. the file's
// S3 HEAD returned 404 because it was deleted upstream), we should
// still use the chunk_size — otherwise InspectSource is wedged for the
// entire model just because one seed file went missing at the source.
func TestMetadataChunkSizeAcceptsChunkSizeEvenWhenStatusError(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			writeJSON(t, w, fileMetadataResponse{
				Root:  sourceURI,
				Files: []fileMetadataEntry{{Path: configPath, RelativePath: "config.json", Size: 100}},
			})
		case "/fetch":
			// Mirror cluster_cache's real behavior: when S3 HEAD fails,
			// status="error" + total_size/mtime nil + chunk_size still set.
			writeJSON(t, w, fetchResponse{Results: []fetchPathResult{{
				Path:      configPath,
				Status:    "error",
				ChunkSize: uint64Ptr(96 * 1024 * 1024),
				Error:     stringPtr("HEAD s3://...: service error; status=404"),
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	// Call InspectSource — chunk_size discovery should NOT fail just because
	// /fetch returned status=error for the seed file. Subsequent metadata-
	// byte reads will fail (the file is gone) but that's a separate path.
	_, err := client.InspectSource(context.Background(), modelcache.Source{URI: sourceURI})

	// Expect either success-ish or a downstream error from reading bytes —
	// crucially NOT the "chunk sizing failed" error.
	if err != nil {
		assert.NotContains(t, err.Error(), "chunk sizing", "chunk_size discovery must accept the chunk_size value when the daemon returns it, regardless of per-file status")
	}
}

func TestStartPrewarmRejectsEmptySourceURI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("server should not be hit when source.URI is empty; got %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	op, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		// Source.URI deliberately empty; Objects supplied.
		Objects: []modelcache.ObjectRef{{Path: "s3://bucket/model/config.json"}},
	})

	require.Error(t, err, "owner-routed prewarm calls /file_metadata on Source.URI; rejecting empty up-front is better than silently failing in the goroutine")
	assert.Nil(t, op)
	assert.Contains(t, err.Error(), "source.URI")
}

func TestNewClientHasSeparatePrewarmHTTPClientWithLongerTimeout(t *testing.T) {
	// Default config: short-call client at 5min, prewarm at >= 10min.
	c, err := NewClient(modelcache.ProviderConfig{Endpoint: "http://cluster-cache.example"})
	require.NoError(t, err)
	cc := c.(*Client)
	assert.Equal(t, defaultHTTPTimeout, cc.httpClient.Timeout)
	assert.GreaterOrEqual(t, cc.prewarmHTTPClient.Timeout, defaultPrefetchOwnerTimeout,
		"prewarm client must allow per-request context.WithTimeout(defaultPrefetchOwnerTimeout) to actually elapse — http.Client.Timeout caps the whole request and would silently win if shorter")

	// Operator-tuned short timeout above the prewarm default: prewarm
	// client should be at least the configured value (don't shrink prewarm
	// just because the operator opted into a longer short-call cap).
	c, err = NewClient(modelcache.ProviderConfig{
		Endpoint: "http://cluster-cache.example",
		Options:  map[string]string{"timeout": "20m"},
	})
	require.NoError(t, err)
	cc = c.(*Client)
	assert.Equal(t, 20*time.Minute, cc.httpClient.Timeout)
	assert.GreaterOrEqual(t, cc.prewarmHTTPClient.Timeout, 20*time.Minute)
}

// TestInspectSourceReportsEmptyFileMetadataAsUnparseable pins the wire
// contract: when /file_metadata returns 200 with zero files, the InspectSource
// path must surface that as ErrSourceUnparseable (same as the CheckSource
// SourceNotReachable path). Without this, an empty-prefix source would be
// retried indefinitely as a transient error.
func TestInspectSourceReportsEmptyFileMetadataAsUnparseable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			writeJSON(t, w, fileMetadataResponse{
				Root:  "s3://bucket/empty-model",
				Files: nil,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.InspectSource(context.Background(), modelcache.Source{URI: "s3://bucket/empty-model"})

	require.Error(t, err)
	assert.True(t, errors.Is(err, modelcache.ErrSourceUnparseable),
		"empty file_metadata listing is a structural fault, not a transient error")
	assert.Contains(t, err.Error(), "no files")
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

// prewarmStubConfig scripts the four endpoints the new owner-routed prewarm
// flow hits. Tests fill only the fields they care about; defaults handle
// the rest. The single-server case puts the test server's own host:port in
// the ring so all chunks route back to that server; multi-owner tests
// allocate one httptest server per owner and use multiServerPrewarm.
type prewarmStubConfig struct {
	sourceURI  string
	files      []fileMetadataEntry
	chunkSize  uint64
	headFunc   func(path string) headResponse                                        // optional; nil → 404
	prefetchFn func(req ownedChunkPrefetchRequest) (ownedChunkPrefetchResponse, int) // returns response + HTTP status
}

// newPrewarmStubServer returns an httptest server scripted with the new
// endpoints AND advertises itself in /ring_info, so all chunks route to
// it. Returns (server URL, post counter, captured per-owner requests).
// Use for single-owner tests.
func newPrewarmStubServer(t *testing.T, cfg prewarmStubConfig) (string, *atomic.Int32, *prewarmStubCapture) {
	t.Helper()
	capture := &prewarmStubCapture{}
	postCount := &atomic.Int32{}
	var serverAddr string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			writeJSON(t, w, fileMetadataResponse{Root: cfg.sourceURI, Files: cfg.files})
		case "/fetch":
			// single-path /fetch is used only for chunk_size discovery.
			// echo the requested path back so metadataChunkSize's
			// result.Path match succeeds.
			var req fetchRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			results := make([]fetchPathResult, 0, len(req.Paths))
			for _, p := range req.Paths {
				results = append(results, fetchPathResult{
					Path:      p,
					Status:    "cached",
					ChunkSize: uint64Ptr(cfg.chunkSize),
					TotalSize: uint64Ptr(1),
				})
			}
			writeJSON(t, w, fetchResponse{Results: results})
		case "/ring_info":
			writeJSON(t, w, ringInfoResponse{Nodes: []string{serverAddr}})
		case "/head":
			if cfg.headFunc == nil {
				http.NotFound(w, r)
				return
			}
			writeJSON(t, w, cfg.headFunc(r.URL.Query().Get("path")))
		case "/prefetch_owned_chunks":
			postCount.Add(1)
			var req ownedChunkPrefetchRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			capture.append(req)
			if cfg.prefetchFn == nil {
				writeJSON(t, w, ownedChunkPrefetchResponse{
					TotalChunks:  len(req.Chunks),
					CachedChunks: len(req.Chunks),
				})
				return
			}
			resp, status := cfg.prefetchFn(req)
			if status != 0 && status != http.StatusOK {
				http.Error(w, "owner error", status)
				return
			}
			writeJSON(t, w, resp)
		default:
			http.NotFound(w, r)
		}
	}))
	serverAddr = server.Listener.Addr().String()
	t.Cleanup(server.Close)
	return server.URL, postCount, capture
}

// prewarmStubCapture records every /prefetch_owned_chunks POST body received,
// so tests can assert the expected chunk routing happened.
type prewarmStubCapture struct {
	mu       sync.Mutex
	requests []ownedChunkPrefetchRequest
}

func (c *prewarmStubCapture) append(req ownedChunkPrefetchRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
}

func (c *prewarmStubCapture) snapshot() []ownedChunkPrefetchRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ownedChunkPrefetchRequest, len(c.requests))
	copy(out, c.requests)
	return out
}

// waitForPhase polls GetPrewarmStatus until status.Phase == want or the
// 2-second budget elapses. Bigger than the older 1-second budget because
// the new flow makes more HTTP round trips (file_metadata + fetch +
// ring_info + per-owner POSTs).
func waitForPhase(t *testing.T, client *Client, op modelcache.PrewarmOperation, want modelcache.PrewarmPhase) modelcache.PrewarmStatus {
	t.Helper()
	var got modelcache.PrewarmStatus
	require.Eventually(t, func() bool {
		s, err := client.GetPrewarmStatus(context.Background(), op)
		require.NoError(t, err)
		got = *s
		return s.Phase == want
	}, 2*time.Second, 10*time.Millisecond, "expected phase %q, never reached (last: %+v)", want, got)
	return got
}

func TestStartPrewarmRoutesChunksAndReportsSuccess(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	weightPath := sourceURI + "/model-00001-of-00001.safetensors"
	mtime := uint64(1710000000)
	chunkSize := uint64(16)

	stubURL, postCount, capture := newPrewarmStubServer(t, prewarmStubConfig{
		sourceURI: sourceURI,
		files: []fileMetadataEntry{
			{Path: configPath, RelativePath: "config.json", Size: 8, MTime: &mtime},
			{Path: weightPath, RelativePath: "model-00001-of-00001.safetensors", Size: 40, MTime: &mtime},
		},
		chunkSize: chunkSize,
	})

	client := newTestClient(t, stubURL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: configPath}, {Path: weightPath}},
	})
	require.NoError(t, err)

	status := waitForPhase(t, client, *operation, modelcache.PrewarmPhaseSucceeded)
	assert.Equal(t, int32(1), postCount.Load(), "single-owner test should produce exactly one /prefetch_owned_chunks POST")

	requests := capture.snapshot()
	require.Len(t, requests, 1)
	// config.json (8 bytes): 1 chunk. weight (40 bytes, chunk_size=16): 3 chunks. total 4.
	assert.Len(t, requests[0].Chunks, 4)
	require.NotNil(t, status.Progress.TotalChunks)
	require.NotNil(t, status.Progress.CachedChunks)
	assert.Equal(t, int32(4), *status.Progress.TotalChunks)
	assert.Equal(t, int32(4), *status.Progress.CachedChunks)
}

func TestStartPrewarmReportsPerChunkErrorsAsFailed(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	mtime := uint64(1710000000)

	stubURL, _, _ := newPrewarmStubServer(t, prewarmStubConfig{
		sourceURI: sourceURI,
		files:     []fileMetadataEntry{{Path: configPath, RelativePath: "config.json", Size: 100, MTime: &mtime}},
		chunkSize: 96 * 1024 * 1024,
		prefetchFn: func(req ownedChunkPrefetchRequest) (ownedChunkPrefetchResponse, int) {
			return ownedChunkPrefetchResponse{
				TotalChunks:  len(req.Chunks),
				CachedChunks: 0,
				WarmedChunks: 0,
				Errors:       []string{"cgroup pressure 2GB/2GB"},
			}, http.StatusOK
		},
	})

	client := newTestClient(t, stubURL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: configPath}},
	})
	require.NoError(t, err)

	status := waitForPhase(t, client, *operation, modelcache.PrewarmPhaseFailed)
	assert.Equal(t, reasonClusterCacheFetchFailed, status.Reason)
	assert.Contains(t, status.Message, "cgroup pressure 2GB/2GB", "per-chunk errors from the daemon must propagate into the operation message")
}

func TestStartPrewarmReportsOwnerHTTPErrorAsFailed(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	mtime := uint64(1710000000)

	stubURL, _, _ := newPrewarmStubServer(t, prewarmStubConfig{
		sourceURI: sourceURI,
		files:     []fileMetadataEntry{{Path: configPath, RelativePath: "config.json", Size: 100, MTime: &mtime}},
		chunkSize: 96 * 1024 * 1024,
		prefetchFn: func(req ownedChunkPrefetchRequest) (ownedChunkPrefetchResponse, int) {
			return ownedChunkPrefetchResponse{}, http.StatusInternalServerError
		},
	})

	client := newTestClient(t, stubURL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: configPath}},
	})
	require.NoError(t, err)

	status := waitForPhase(t, client, *operation, modelcache.PrewarmPhaseFailed)
	assert.Equal(t, reasonClusterCacheFetchFailed, status.Reason)
	assert.Contains(t, status.Message, "500")
}

// /file_metadata's `mtime` is Option<u64>. Without a HEAD fallback the
// chunk-key would be (path, offset, 0), which won't match daemon-side
// chunk storage. Pins the HEAD fallback via ensureMetadataFileMTime.
func TestStartPrewarmHeadFallbackWhenMtimeMissing(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	const headMtime uint64 = 1710001234

	var headCalled atomic.Int32
	stubURL, _, capture := newPrewarmStubServer(t, prewarmStubConfig{
		sourceURI: sourceURI,
		// MTime intentionally nil — forces HEAD fallback.
		files:     []fileMetadataEntry{{Path: configPath, RelativePath: "config.json", Size: 100}},
		chunkSize: 96 * 1024 * 1024,
		headFunc: func(path string) headResponse {
			headCalled.Add(1)
			return headResponse{Size: 100, MTime: headMtime}
		},
	})

	client := newTestClient(t, stubURL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: configPath}},
	})
	require.NoError(t, err)

	waitForPhase(t, client, *operation, modelcache.PrewarmPhaseSucceeded)
	assert.GreaterOrEqual(t, headCalled.Load(), int32(1), "HEAD must be called when /file_metadata omitted mtime")
	requests := capture.snapshot()
	require.Len(t, requests, 1)
	require.NotEmpty(t, requests[0].Chunks)
	assert.Equal(t, headMtime, requests[0].Chunks[0].MTime, "chunk-key mtime must come from HEAD fallback, not the zero default")
}

func TestStartPrewarmEmptyFileMetadataFails(t *testing.T) {
	const sourceURI = "s3://bucket/empty"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/file_metadata" {
			writeJSON(t, w, fileMetadataResponse{Root: sourceURI})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: sourceURI + "/config.json"}},
	})
	require.NoError(t, err)

	status := waitForPhase(t, client, *operation, modelcache.PrewarmPhaseFailed)
	assert.Contains(t, status.Message, "no files")
}

// Routes chunks to two owners. Each owner gets only its ring-assigned
// chunks; progress aggregates across both. Uses two raw httptest servers
// for the owner endpoints, plus a single "control" server that serves
// /file_metadata, /fetch (for chunk_size discovery), and /ring_info
// (advertising both owners).
func TestStartPrewarmRoutesChunksAcrossMultipleOwners(t *testing.T) {
	const sourceURI = "s3://bucket/multimodel"
	weightPath := sourceURI + "/weights.bin"
	mtime := uint64(1710000000)
	chunkSize := uint64(16)
	fileSize := uint64(160) // 10 chunks

	type ownerStub struct {
		server  *httptest.Server
		count   atomic.Int32
		capture prewarmStubCapture
	}

	startOwner := func() *ownerStub {
		s := &ownerStub{}
		s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/prefetch_owned_chunks" {
				http.NotFound(w, r)
				return
			}
			s.count.Add(1)
			var req ownedChunkPrefetchRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			s.capture.append(req)
			writeJSON(t, w, ownedChunkPrefetchResponse{
				TotalChunks:  len(req.Chunks),
				CachedChunks: len(req.Chunks),
			})
		}))
		t.Cleanup(s.server.Close)
		return s
	}
	ownerA := startOwner()
	ownerB := startOwner()

	configPath := sourceURI + "/config.json"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			// config.json is required so metadataChunkSize has a seed file.
			writeJSON(t, w, fileMetadataResponse{
				Root: sourceURI,
				Files: []fileMetadataEntry{
					{Path: configPath, RelativePath: "config.json", Size: 8, MTime: &mtime},
					{Path: weightPath, RelativePath: "weights.bin", Size: fileSize, MTime: &mtime},
				},
			})
		case "/fetch":
			var req fetchRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			results := make([]fetchPathResult, 0, len(req.Paths))
			for _, p := range req.Paths {
				results = append(results, fetchPathResult{Path: p, Status: "cached", ChunkSize: uint64Ptr(chunkSize), TotalSize: uint64Ptr(1)})
			}
			writeJSON(t, w, fetchResponse{Results: results})
		case "/ring_info":
			writeJSON(t, w, ringInfoResponse{Nodes: []string{
				ownerA.server.Listener.Addr().String(),
				ownerB.server.Listener.Addr().String(),
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(control.Close)

	client := newTestClient(t, control.URL)
	operation, err := client.StartPrewarm(context.Background(), modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: weightPath}},
	})
	require.NoError(t, err)

	status := waitForPhase(t, client, *operation, modelcache.PrewarmPhaseSucceeded)

	// Both owners must have received exactly one POST — proves client-side
	// routing (not server-side fan-out via /fetch).
	assert.Equal(t, int32(1), ownerA.count.Load(), "owner A must receive exactly one POST")
	assert.Equal(t, int32(1), ownerB.count.Load(), "owner B must receive exactly one POST")

	chunksA := ownerA.capture.snapshot()[0].Chunks
	chunksB := ownerB.capture.snapshot()[0].Chunks
	assert.Equal(t, 10, len(chunksA)+len(chunksB), "chunks must be partitioned (not duplicated) across owners")
	assert.NotEmpty(t, chunksA, "owner A must receive at least one chunk")
	assert.NotEmpty(t, chunksB, "owner B must receive at least one chunk")

	require.NotNil(t, status.Progress.TotalChunks)
	assert.Equal(t, int32(10), *status.Progress.TotalChunks)
}

// After a terminal Failed phase, a re-submit with the same deterministic
// operationID must wipe the dead state and fire fresh owner POSTs.
func TestStartPrewarmReFiresAfterTerminalFailure(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	mtime := uint64(1710000000)

	stubURL, postCount, _ := newPrewarmStubServer(t, prewarmStubConfig{
		sourceURI: sourceURI,
		files:     []fileMetadataEntry{{Path: configPath, RelativePath: "config.json", Size: 100, MTime: &mtime}},
		chunkSize: 96 * 1024 * 1024,
		prefetchFn: func(req ownedChunkPrefetchRequest) (ownedChunkPrefetchResponse, int) {
			return ownedChunkPrefetchResponse{
				TotalChunks: len(req.Chunks),
				Errors:      []string{"transient overload"},
			}, http.StatusOK
		},
	})

	client := newTestClient(t, stubURL)
	req := modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: configPath}},
	}

	op1, err := client.StartPrewarm(context.Background(), req)
	require.NoError(t, err)
	waitForPhase(t, client, *op1, modelcache.PrewarmPhaseFailed)
	postsAfterFirst := postCount.Load()

	op2, err := client.StartPrewarm(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, op1.ID, op2.ID)

	require.Eventually(t, func() bool { return postCount.Load() > postsAfterFirst }, 2*time.Second, 10*time.Millisecond,
		"re-submit after terminal failure must trigger fresh owner POSTs; got %d (after first: %d)", postCount.Load(), postsAfterFirst)
	waitForPhase(t, client, *op2, modelcache.PrewarmPhaseFailed)
}

// In-flight (Pending/Running) re-submits must NOT fan out a duplicate
// owner POST — that would double the cluster_cache load for no benefit.
func TestStartPrewarmIdempotentForInflightOperation(t *testing.T) {
	const sourceURI = "s3://bucket/model"
	configPath := sourceURI + "/config.json"
	mtime := uint64(1710000000)

	releaseOwner := make(chan struct{})
	stubURL, postCount, _ := newPrewarmStubServer(t, prewarmStubConfig{
		sourceURI: sourceURI,
		files:     []fileMetadataEntry{{Path: configPath, RelativePath: "config.json", Size: 100, MTime: &mtime}},
		chunkSize: 96 * 1024 * 1024,
		prefetchFn: func(req ownedChunkPrefetchRequest) (ownedChunkPrefetchResponse, int) {
			<-releaseOwner
			return ownedChunkPrefetchResponse{TotalChunks: len(req.Chunks), CachedChunks: len(req.Chunks)}, http.StatusOK
		},
	})

	client := newTestClient(t, stubURL)
	req := modelcache.PrewarmRequest{
		Source:  modelcache.Source{URI: sourceURI},
		Objects: []modelcache.ObjectRef{{Path: configPath}},
	}

	op1, err := client.StartPrewarm(context.Background(), req)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return postCount.Load() == 1 }, 2*time.Second, 10*time.Millisecond,
		"first submit must hit /prefetch_owned_chunks")

	op2, err := client.StartPrewarm(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, op1.ID, op2.ID)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), postCount.Load(), "in-flight re-submit must not produce a second /prefetch_owned_chunks POST")

	close(releaseOwner)
	waitForPhase(t, client, *op1, modelcache.PrewarmPhaseSucceeded)
}

// Restart-recovery: caller's in-memory operation map was lost (e.g. controller
// restart), but operationID is durable. GetPrewarmStatus must decode the ID,
// re-discover paths via /file_metadata, and re-fire the prewarm.
func TestGetPrewarmStatusRestartsDecodedOperation(t *testing.T) {
	const sourceURI = "s3://bucket/restart-model"
	configPath := sourceURI + "/config.json"
	mtime := uint64(1710000000)

	stubURL, postCount, _ := newPrewarmStubServer(t, prewarmStubConfig{
		sourceURI: sourceURI,
		files:     []fileMetadataEntry{{Path: configPath, RelativePath: "config.json", Size: 100, MTime: &mtime}},
		chunkSize: 96 * 1024 * 1024,
	})

	client := newTestClient(t, stubURL)
	operationID, err := encodeOperationID(modelcache.Source{URI: sourceURI}, []string{configPath})
	require.NoError(t, err)

	// First GetPrewarmStatus on an unknown operation triggers recovery.
	status, err := client.GetPrewarmStatus(context.Background(), modelcache.PrewarmOperation{ID: operationID})
	require.NoError(t, err)
	assert.Equal(t, modelcache.PrewarmPhaseRunning, status.Phase, "restart-recovery returns Running while the recovered goroutine starts")

	waitForPhase(t, client, modelcache.PrewarmOperation{ID: operationID}, modelcache.PrewarmPhaseSucceeded)
	assert.Equal(t, int32(1), postCount.Load(), "recovered operation must fire /prefetch_owned_chunks")
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
