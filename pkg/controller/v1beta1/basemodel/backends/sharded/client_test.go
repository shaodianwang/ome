package sharded

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientValidatesConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ProviderConfig
		wantErr bool
	}{
		{
			name:    "missing endpoint",
			config:  ProviderConfig{},
			wantErr: true,
		},
		{
			name: "bad timeout",
			config: ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"timeout": "bad"},
			},
			wantErr: true,
		},
		{
			name: "bad chunk_size_bytes",
			config: ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"chunk_size_bytes": "not-a-number"},
			},
			wantErr: true,
		},
		{
			name: "zero chunk_size_bytes",
			config: ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"chunk_size_bytes": "0"},
			},
			wantErr: true,
		},
		{
			name: "valid chunk_size_bytes",
			config: ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"chunk_size_bytes": "16777216"},
			},
			wantErr: false,
		},
		{
			name: "bad header_concurrency",
			config: ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"header_concurrency": "abc"},
			},
			wantErr: true,
		},
		{
			name: "zero header_concurrency",
			config: ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"header_concurrency": "0"},
			},
			wantErr: true,
		},
		{
			name: "valid header_concurrency",
			config: ProviderConfig{
				Endpoint: "http://cluster-cache.example",
				Options:  map[string]string{"header_concurrency": "8"},
			},
			wantErr: false,
		},
		{
			name: "host port endpoint",
			config: ProviderConfig{
				Endpoint: "cluster-cache.example:10090",
			},
			wantErr: false,
		},
		{
			name: "http endpoint",
			config: ProviderConfig{
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
	client, err := NewClient(ProviderConfig{Endpoint: "http://cluster-cache.example"})
	require.NoError(t, err)
	cc := client.(*clusterCacheClient)
	assert.Equal(t, defaultHTTPTimeout, cc.httpClient.Timeout)

	client, err = NewClient(ProviderConfig{
		Endpoint: "http://cluster-cache.example",
		Options:  map[string]string{"timeout": "7s"},
	})
	require.NoError(t, err)
	cc = client.(*clusterCacheClient)
	assert.Equal(t, 7*time.Second, cc.httpClient.Timeout)
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
	status, err := client.CheckSource(context.Background(), Source{URI: "s3://bucket/model"})

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
	status, err := client.CheckSource(context.Background(), Source{URI: "s3://bucket/model"})

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
	status, err := client.CheckSource(context.Background(), Source{URI: "s3://bucket/model"})

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
	status, err := client.CheckSource(context.Background(), Source{URI: "s3://bucket/missing"})

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
	status, err := client.CheckSource(context.Background(), Source{URI: "s3://bucket/model"})

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
	status, err := client.CheckSource(context.Background(), Source{URI: "s3://bucket/model"})

	require.Error(t, err)
	assert.Nil(t, status)
	assert.Contains(t, err.Error(), "head returned 429")
}

func TestFileMetadataInspectionUsesCanonicalRootAndRelativePaths(t *testing.T) {
	mtime := uint64(1710000000)
	source := Source{URI: "manifold://bucket/model"}
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

// TestFileMetadataInspectionIgnoresBucketRelativeRelativePath pins the
// derive-from-root-first behavior. cluster_cache emits relative_path as
// the bucket key (e.g. "models/llama-7b/config.json" when the model
// root is "s3://bucket/models/llama-7b"), but the safetensors index
// references shards by *root-relative* path. If OME trusts the bucket
// key, the index-resolution step downstream can't find the shards.
func TestFileMetadataInspectionIgnoresBucketRelativeRelativePath(t *testing.T) {
	source := Source{URI: "s3://bucket/models/llama-7b"}
	response := &fileMetadataResponse{
		Root: "s3://bucket/models/llama-7b",
		Files: []fileMetadataEntry{
			{
				// Cluster_cache emits the bucket key here. Without the
				// fix this would propagate as RelativePath and break
				// safetensors index lookup.
				Path:         "s3://bucket/models/llama-7b/config.json",
				RelativePath: "models/llama-7b/config.json",
				Size:         100,
			},
			{
				Path:         "s3://bucket/models/llama-7b/model-00001-of-00001.safetensors",
				RelativePath: "models/llama-7b/model-00001-of-00001.safetensors",
				Size:         200,
			},
		},
	}

	files := metadataFilesFromResponse(source, response)
	require.Len(t, files, 2)
	assert.Equal(t, "config.json", files[0].relativePath,
		"relative_path must be model-root-relative even when cluster_cache returns the bucket key")
	assert.Equal(t, "model-00001-of-00001.safetensors", files[1].relativePath)

	inspection := response.toModelCacheSourceInspection(source)
	require.NotNil(t, inspection)
	require.Len(t, inspection.Objects, 2)
	assert.Equal(t, "config.json", inspection.Objects[0].RelativePath)
	assert.Equal(t, "model-00001-of-00001.safetensors", inspection.Objects[1].RelativePath)
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

	var packedRangeRequests int
	wholeObjects := map[string][]byte{
		configPath: configJSON,
		indexPath:  indexJSON,
	}
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
		case "/remote_object":
			require.Equal(t, http.MethodGet, r.Method)
			data, ok := wholeObjects[r.URL.Query().Get("path")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		case "/packed_byte_ranges_by_key":
			require.Equal(t, http.MethodPost, r.Method)
			packedRangeRequests++
			var req packedByteRangeBatchRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			for _, rangeSpec := range req.Ranges {
				data := map[string][]byte{
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

	client := newTestClient(t, server.URL)
	inspection, err := client.InspectSource(context.Background(), Source{URI: sourceURI})

	require.NoError(t, err)
	require.NotNil(t, inspection)
	// Only safetensors headers use /packed_byte_ranges_by_key now:
	// 1 call for the 8-byte length prefix, 1 for the header body.
	assert.Equal(t, 2, packedRangeRequests)
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

	wholeObjects := map[string][]byte{
		configPath:  configJSON,
		hfQuantPath: hfQuantJSON,
	}
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
		case "/remote_object":
			data, ok := wholeObjects[r.URL.Query().Get("path")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	inspection, err := client.InspectSource(context.Background(), Source{URI: sourceURI})
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
	_, err := client.InspectSource(context.Background(), Source{URI: "s3://bucket/model"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "config.json")
	assert.True(t, errors.Is(err, ErrSourceUnparseable),
		"missing config.json/model_index.json is a structural source fault and must wrap ErrSourceUnparseable so the controller transitions to state=Failed instead of looping in MetadataPending forever")
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
	_, err := client.InspectSource(context.Background(), Source{URI: "s3://bucket/empty-model"})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrSourceUnparseable),
		"empty file_metadata listing is a structural fault, not a transient error")
	assert.Contains(t, err.Error(), "no files")
}

func TestInspectSourceReportsEndpointError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.InspectSource(context.Background(), Source{URI: "s3://bucket/model"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "file_metadata returned 503")
	assert.Contains(t, err.Error(), "unavailable")
}

func TestUnsupportedOperations(t *testing.T) {
	client := newTestClient(t, "http://cluster-cache.example")

	_, err := client.InspectSource(context.Background(), Source{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source URI is required")
}

// TestReadSafetensorHeadersParallel pins both the concurrency bound
// and the output-order invariant of the parallel header-read path.
// We probe concurrency by blocking each handler on a channel until
// the bound is observed, then releasing.
func TestReadSafetensorHeadersParallel(t *testing.T) {
	const shards = 8
	const concurrency = 4

	headerJSON := []byte(`{"w":{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}`)
	safetensorsData := safetensorsFileForTest(headerJSON, 64)
	mtime := uint64(1710000000)

	var inflightMu sync.Mutex
	var inflight, peak int
	release := make(chan struct{})
	sawConcurrency := make(chan struct{}, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/packed_byte_ranges_by_key", r.URL.Path)

		inflightMu.Lock()
		inflight++
		if inflight > peak {
			peak = inflight
		}
		if inflight == concurrency {
			select {
			case sawConcurrency <- struct{}{}:
			default:
			}
		}
		inflightMu.Unlock()

		<-release

		inflightMu.Lock()
		inflight--
		inflightMu.Unlock()

		var req packedByteRangeBatchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		for _, rangeSpec := range req.Ranges {
			start := rangeSpec.ChunkOffset + rangeSpec.RangeOffset
			end := start + rangeSpec.Length
			_, _ = w.Write(safetensorsData[start:end])
		}
	}))
	defer server.Close()

	cl, err := NewClient(ProviderConfig{
		Endpoint: server.URL,
		Options: map[string]string{
			"header_concurrency": "4",
		},
	})
	require.NoError(t, err)
	cc := cl.(*clusterCacheClient)

	files := make([]clusterCacheMetadataFile, shards)
	for i := range files {
		files[i] = clusterCacheMetadataFile{
			path:         "s3://bucket/model/shard-" + string(rune('a'+i)) + ".safetensors",
			relativePath: "shard-" + string(rune('a'+i)) + ".safetensors",
			size:         uint64(len(safetensorsData)),
			mtime:        &mtime,
		}
	}

	done := make(chan error, 1)
	var headers []SafetensorHeader
	go func() {
		var goErr error
		headers, goErr = cc.readSafetensorHeaders(context.Background(), files)
		done <- goErr
	}()

	select {
	case <-sawConcurrency:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("never saw concurrency == 4 in flight; parallelization is broken")
	}
	close(release)

	require.NoError(t, <-done)

	inflightMu.Lock()
	defer inflightMu.Unlock()
	assert.Equal(t, concurrency, peak, "peak inflight must equal configured concurrency, not more")
	assert.LessOrEqual(t, peak, concurrency, "must not exceed configured concurrency")
	require.Len(t, headers, shards)
	for i := range headers {
		assert.Equal(t, files[i].path, headers[i].Path,
			"output order must match input order regardless of completion order")
	}
}

func newTestClient(t *testing.T, endpoint string) Client {
	t.Helper()

	client, err := NewClient(ProviderConfig{Endpoint: endpoint})
	require.NoError(t, err)
	return client
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(value))
}

func stringPtr(value string) *string {
	return &value
}

func safetensorsFileForTest(headerJSON []byte, payloadBytes int) []byte {
	data := make([]byte, 8+len(headerJSON)+payloadBytes)
	binary.LittleEndian.PutUint64(data[:8], uint64(len(headerJSON)))
	copy(data[8:], headerJSON)
	return data
}
