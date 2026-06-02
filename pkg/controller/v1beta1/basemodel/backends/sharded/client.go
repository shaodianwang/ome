package sharded

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	optionTimeout            = "timeout"
	optionChunkSizeBytes     = "chunk_size_bytes"
	optionHeaderConcurrency  = "header_concurrency"
	defaultHTTPTimeout       = 5 * time.Minute
	defaultHeaderConcurrency = 16

	// defaultChunkSizeBytes mirrors cluster_cache's CACHE_CHUNK_SIZE_MB
	// default (main.rs:587 — 96 MiB). cluster_cache caches chunks under
	// (path, offset, mtime) with no size in the key; writing a smaller
	// chunk shadows the real one. Operators who override the daemon's
	// chunk size must pass options.chunk_size_bytes to keep this in
	// sync — there is no runtime discovery endpoint.
	defaultChunkSizeBytes uint64 = 96 * 1024 * 1024

	reasonSourceReachable    = "SourceReachable"
	reasonSourceNotReachable = "SourceNotReachable"

	maxInt64 = int64(1<<63 - 1)
)

func init() {
	if err := RegisterProvider(ProviderClusterCache, NewClient); err != nil {
		panic(err)
	}
}

// clusterCacheClient is the concrete Client implementation that talks
// to the cluster_cache daemon over HTTP. Unexported because it's
// constructed via NewClient (the factory the registry calls).
type clusterCacheClient struct {
	endpoint          *url.URL
	httpClient        *http.Client
	chunkSize         uint64
	headerConcurrency int
}

type headResponse struct {
	Size  uint64 `json:"size"`
	MTime uint64 `json:"mtime"`
}

type fileMetadataResponse struct {
	Root  string              `json:"root"`
	Files []fileMetadataEntry `json:"files"`
}

type fileMetadataEntry struct {
	Path         string  `json:"path"`
	Bucket       string  `json:"bucket,omitempty"`
	Key          string  `json:"key,omitempty"`
	RelativePath string  `json:"relative_path,omitempty"`
	Size         uint64  `json:"size"`
	MTime        *uint64 `json:"mtime,omitempty"`
	CTime        *uint64 `json:"ctime,omitempty"`
	ETag         *string `json:"etag,omitempty"`
	StorageClass *string `json:"storage_class,omitempty"`
}

type endpointError struct {
	endpoint   string
	status     string
	statusCode int
	body       string
}

func (e *endpointError) Error() string {
	if strings.TrimSpace(e.body) == "" {
		return fmt.Sprintf("cluster_cache %s returned %s", e.endpoint, e.status)
	}
	return fmt.Sprintf("cluster_cache %s returned %s: %s", e.endpoint, e.status, e.body)
}

// NewClient is the ProviderFactory for cluster_cache. Registered with
// the package's default Registry in init(), so cmd/manager picks it up
// via BuildProvider(config).
func NewClient(config ProviderConfig) (Client, error) {
	endpoint, err := normalizeEndpoint(config.Endpoint)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{Timeout: defaultHTTPTimeout}
	if timeout := strings.TrimSpace(config.Options[optionTimeout]); timeout != "" {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("parse cluster_cache %s option: %w", optionTimeout, err)
		}
		httpClient.Timeout = duration
	}

	chunkSize := defaultChunkSizeBytes
	if raw := strings.TrimSpace(config.Options[optionChunkSizeBytes]); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse cluster_cache %s option: %w", optionChunkSizeBytes, err)
		}
		if parsed == 0 {
			return nil, fmt.Errorf("cluster_cache %s option must be > 0", optionChunkSizeBytes)
		}
		chunkSize = parsed
	}

	headerConcurrency := defaultHeaderConcurrency
	if raw := strings.TrimSpace(config.Options[optionHeaderConcurrency]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("parse cluster_cache %s option: %w", optionHeaderConcurrency, err)
		}
		if parsed < 1 {
			return nil, fmt.Errorf("cluster_cache %s option must be >= 1", optionHeaderConcurrency)
		}
		headerConcurrency = parsed
	}

	return &clusterCacheClient{
		endpoint:          endpoint,
		httpClient:        httpClient,
		chunkSize:         chunkSize,
		headerConcurrency: headerConcurrency,
	}, nil
}

func (c *clusterCacheClient) CheckSource(ctx context.Context, source Source) (*SourceStatus, error) {
	if strings.TrimSpace(source.URI) == "" {
		return &SourceStatus{
			Reachable: false,
			Reason:    reasonSourceNotReachable,
			Message:   "model source URI is empty",
		}, nil
	}

	metadata, err := c.fileMetadata(ctx, source.URI)
	if err == nil {
		if len(metadata.Files) == 0 {
			return &SourceStatus{
				Reachable: false,
				Reason:    reasonSourceNotReachable,
				Message:   "cluster_cache file_metadata returned no files for model source",
			}, nil
		}
		return &SourceStatus{
			Reachable: true,
			Reason:    reasonSourceReachable,
			Message:   "model source is reachable through cluster_cache",
		}, nil
	}
	if isTransientEndpointError(err) {
		return nil, err
	}
	if !isEndpointStatus(err, http.StatusNotFound) {
		return &SourceStatus{
			Reachable: false,
			Reason:    reasonSourceNotReachable,
			Message:   err.Error(),
		}, nil
	}

	return c.checkSourceWithHead(ctx, source)
}

func (c *clusterCacheClient) checkSourceWithHead(ctx context.Context, source Source) (*SourceStatus, error) {
	query := url.Values{}
	query.Set("path", source.URI)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL("/head", query), nil)
	if err != nil {
		return nil, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		err := endpointStatusError("head", response)
		if isTransientStatusCode(response.StatusCode) {
			return nil, err
		}
		return &SourceStatus{
			Reachable: false,
			Reason:    reasonSourceNotReachable,
			Message:   err.Error(),
		}, nil
	}

	var head headResponse
	if err := json.NewDecoder(response.Body).Decode(&head); err != nil {
		return nil, fmt.Errorf("decode cluster_cache head response: %w", err)
	}

	return &SourceStatus{
		Reachable: true,
		Reason:    reasonSourceReachable,
		Message:   "model source is reachable through cluster_cache",
	}, nil
}

func (c *clusterCacheClient) InspectSource(ctx context.Context, source Source) (*SourceInspection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(source.URI) == "" {
		return nil, fmt.Errorf("cluster_cache source URI is required for source inspection")
	}

	metadata, err := c.fileMetadata(ctx, source.URI)
	if err != nil {
		return nil, err
	}
	return c.inspectSourceFromFileMetadata(ctx, source, metadata)
}

// remoteObject fetches a whole small object through the cluster_cache
// gateway's /remote_object endpoint. The daemon goes to its own S3
// credentials, caps payload at CACHE_REMOTE_OBJECT_MAX_BYTES (default
// 512 MiB — comfortably above our 64 MiB metadata limit), and returns
// raw bytes. Used for metadata files (config.json, model_index.json,
// safetensors index, hf_quant_config.json) so we don't have to do
// client-side chunk math or hashring routing for files we want in
// full.
func (c *clusterCacheClient) remoteObject(ctx context.Context, sourceURI string) ([]byte, error) {
	query := url.Values{}
	query.Set("path", sourceURI)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL("/remote_object", query), nil)
	if err != nil {
		return nil, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, endpointStatusError("remote_object", response)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read cluster_cache remote_object body: %w", err)
	}
	return data, nil
}

func (c *clusterCacheClient) fileMetadata(ctx context.Context, sourceURI string) (*fileMetadataResponse, error) {
	query := url.Values{}
	query.Set("path", sourceURI)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL("/file_metadata", query), nil)
	if err != nil {
		return nil, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, endpointStatusError("file_metadata", response)
	}

	var parsed fileMetadataResponse
	if err := json.NewDecoder(response.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode cluster_cache file_metadata response: %w", err)
	}
	return &parsed, nil
}

func (r *fileMetadataResponse) toModelCacheSourceInspection(source Source) *SourceInspection {
	if r == nil {
		return &SourceInspection{}
	}

	root := firstNonEmpty(r.Root, source.URI)
	inspection := &SourceInspection{
		RootURI: root,
		Objects: make([]Object, 0, len(r.Files)),
	}
	for _, file := range r.Files {
		objectPath := inspectPath(root, file.Path, file.RelativePath)
		if objectPath == "" {
			continue
		}
		// See metadataFilesFromResponse: cluster_cache's relative_path
		// is bucket-relative, not model-root-relative. Derive from
		// response.Root + file.Path first.
		relativePath := relativePathFromRoot(root, objectPath)
		if relativePath == "" {
			relativePath = cleanMetadataRelativePath(file.RelativePath)
		}
		if relativePath == "" {
			continue
		}
		inspection.Objects = append(inspection.Objects, Object{
			Source:       source,
			Path:         objectPath,
			RelativePath: relativePath,
			SizeBytes:    uint64ToInt64(file.Size),
			Version:      inspectVersion("", stringPtrValue(file.ETag), file.MTime),
		})
	}
	return inspection
}

// endpointURL builds a URL against the configured cluster_cache gateway.
func (c *clusterCacheClient) endpointURL(endpointPath string, query url.Values) string {
	endpoint := *c.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + endpointPath
	endpoint.RawQuery = query.Encode()
	return endpoint.String()
}

func normalizeEndpoint(endpoint string) (*url.URL, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("cluster_cache endpoint is required")
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse cluster_cache endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("cluster_cache endpoint scheme must be http or https")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("cluster_cache endpoint host is required")
	}
	return parsed, nil
}

func uint64ToInt64(value uint64) int64 {
	if value > uint64(maxInt64) {
		return maxInt64
	}
	return int64(value)
}

func inspectVersion(version, etag string, mtime *uint64) string {
	if version != "" {
		return version
	}
	if etag != "" {
		return etag
	}
	if mtime != nil {
		return strconv.FormatUint(*mtime, 10)
	}
	return ""
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func inspectPath(sourceURI, uri, relativePath string) string {
	if value := strings.TrimSpace(uri); value != "" {
		return value
	}
	relativePath = strings.TrimLeft(strings.TrimSpace(relativePath), "/")
	if relativePath == "" {
		return ""
	}
	sourceURI = strings.TrimSpace(sourceURI)
	if sourceURI == "" {
		return relativePath
	}
	if strings.HasSuffix(sourceURI, "/") {
		return sourceURI + relativePath
	}
	return sourceURI + "/" + relativePath
}

func endpointStatusError(endpoint string, response *http.Response) error {
	return &endpointError{
		endpoint:   endpoint,
		status:     response.Status,
		statusCode: response.StatusCode,
		body:       readErrorBody(response.Body),
	}
}

func isEndpointStatus(err error, statusCode int) bool {
	var endpointErr *endpointError
	if !errors.As(err, &endpointErr) {
		return false
	}
	return endpointErr.statusCode == statusCode
}

func isTransientEndpointError(err error) bool {
	var endpointErr *endpointError
	if !errors.As(err, &endpointErr) {
		return true
	}
	return isTransientStatusCode(endpointErr.statusCode)
}

func isTransientStatusCode(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func readErrorBody(reader io.Reader) string {
	body, err := io.ReadAll(io.LimitReader(reader, 4096))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}
