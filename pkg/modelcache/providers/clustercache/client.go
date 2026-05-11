package clustercache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ome-projects/ome/pkg/modelcache"
)

const (
	optionTimeout            = "timeout"
	optionOperationRetention = "operation_retention"
	operationPrefix          = "cluster_cache.fetch.v1."

	defaultHTTPTimeout        = 5 * time.Minute
	defaultOperationRetention = 10 * time.Minute

	reasonClusterCacheFetchFailed = "ClusterCacheFetchFailed"
	reasonOperationNotFound       = "OperationNotFound"
	reasonSourceReachable         = "SourceReachable"
	reasonSourceNotReachable      = "SourceNotReachable"

	maxInt64 = int64(1<<63 - 1)
	maxInt32 = int32(1<<31 - 1)
)

func init() {
	if err := modelcache.RegisterProvider(modelcache.ProviderClusterCache, NewClient); err != nil {
		panic(err)
	}
}

type Client struct {
	endpoint   *url.URL
	httpClient *http.Client

	ctx                context.Context
	cancel             context.CancelFunc
	operationRetention time.Duration
	mu                 sync.Mutex
	operations         map[string]*operationState
}

type operationState struct {
	status      modelcache.PrewarmStatus
	completedAt time.Time
}

type operationIDPayload struct {
	SourceURI string   `json:"source_uri,omitempty"`
	PathsHash string   `json:"paths_hash,omitempty"`
	Paths     []string `json:"paths,omitempty"`
}

type headResponse struct {
	Size  uint64 `json:"size"`
	MTime uint64 `json:"mtime"`
}

type fetchRequest struct {
	Paths []string `json:"paths"`
}

type fetchResponse struct {
	Results     []fetchPathResult `json:"results"`
	ElapsedSecs float64           `json:"elapsed_secs"`
}

type fetchPathResult struct {
	Path         string   `json:"path"`
	Status       string   `json:"status"`
	TotalSize    *uint64  `json:"total_size"`
	MTime        *uint64  `json:"mtime"`
	ChunkSize    *uint64  `json:"chunk_size"`
	TotalChunks  *int     `json:"total_chunks"`
	CachedChunks *int     `json:"cached_chunks"`
	WarmedChunks *int     `json:"warmed_chunks"`
	ElapsedSecs  *float64 `json:"elapsed_secs"`
	Error        *string  `json:"error"`
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

func NewClient(config modelcache.ProviderConfig) (modelcache.Client, error) {
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

	operationRetention := defaultOperationRetention
	if retention := strings.TrimSpace(config.Options[optionOperationRetention]); retention != "" {
		duration, err := time.ParseDuration(retention)
		if err != nil {
			return nil, fmt.Errorf("parse cluster_cache %s option: %w", optionOperationRetention, err)
		}
		if duration <= 0 {
			return nil, fmt.Errorf("cluster_cache %s option must be positive", optionOperationRetention)
		}
		operationRetention = duration
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		endpoint:           endpoint,
		httpClient:         httpClient,
		ctx:                ctx,
		cancel:             cancel,
		operationRetention: operationRetention,
		operations:         map[string]*operationState{},
	}, nil
}

func (c *Client) Close() error {
	c.cancel()
	return nil
}

func (c *Client) CheckSource(ctx context.Context, source modelcache.Source) (*modelcache.SourceStatus, error) {
	if strings.TrimSpace(source.URI) == "" {
		return &modelcache.SourceStatus{
			Reachable: false,
			Reason:    reasonSourceNotReachable,
			Message:   "model source URI is empty",
		}, nil
	}

	metadata, err := c.fileMetadata(ctx, source.URI)
	if err == nil {
		if len(metadata.Files) == 0 {
			return &modelcache.SourceStatus{
				Reachable: false,
				Reason:    reasonSourceNotReachable,
				Message:   "cluster_cache file_metadata returned no files for model source",
			}, nil
		}
		return &modelcache.SourceStatus{
			Reachable: true,
			Reason:    reasonSourceReachable,
			Message:   "model source is reachable through cluster_cache",
		}, nil
	}
	if isTransientEndpointError(err) {
		return nil, err
	}
	if !isEndpointStatus(err, http.StatusNotFound) {
		return &modelcache.SourceStatus{
			Reachable: false,
			Reason:    reasonSourceNotReachable,
			Message:   err.Error(),
		}, nil
	}

	return c.checkSourceWithHead(ctx, source)
}

func (c *Client) checkSourceWithHead(ctx context.Context, source modelcache.Source) (*modelcache.SourceStatus, error) {
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
		return &modelcache.SourceStatus{
			Reachable: false,
			Reason:    reasonSourceNotReachable,
			Message:   err.Error(),
		}, nil
	}

	var head headResponse
	if err := json.NewDecoder(response.Body).Decode(&head); err != nil {
		return nil, fmt.Errorf("decode cluster_cache head response: %w", err)
	}

	return &modelcache.SourceStatus{
		Reachable: true,
		Reason:    reasonSourceReachable,
		Message:   "model source is reachable through cluster_cache",
	}, nil
}

func (c *Client) InspectSource(ctx context.Context, source modelcache.Source) (*modelcache.SourceInspection, error) {
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

func (c *Client) fileMetadata(ctx context.Context, sourceURI string) (*fileMetadataResponse, error) {
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

func (r *fileMetadataResponse) toModelCacheSourceInspection(source modelcache.Source) *modelcache.SourceInspection {
	if r == nil {
		return &modelcache.SourceInspection{}
	}

	root := firstNonEmpty(r.Root, source.URI)
	inspection := &modelcache.SourceInspection{
		RootURI: root,
		Objects: make([]modelcache.Object, 0, len(r.Files)),
	}
	for _, file := range r.Files {
		objectPath := inspectPath(root, file.Path, "", file.RelativePath)
		if objectPath == "" {
			continue
		}
		relativePath := cleanMetadataRelativePath(file.RelativePath)
		if relativePath == "" {
			relativePath = relativePathFromRoot(root, objectPath)
		}
		if relativePath == "" {
			continue
		}
		inspection.Objects = append(inspection.Objects, modelcache.Object{
			Source:       source,
			Path:         objectPath,
			RelativePath: relativePath,
			SizeBytes:    uint64ToInt64(file.Size),
			Version:      inspectVersion("", stringPtrValue(file.ETag), file.MTime),
		})
	}
	return inspection
}

func (c *Client) StartPrewarm(ctx context.Context, req modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}

	paths := prewarmPaths(req)
	if len(paths) == 0 {
		return nil, fmt.Errorf("cluster_cache prewarm requires at least one source path")
	}

	operationID, err := encodeOperationID(req.Source, paths)
	if err != nil {
		return nil, err
	}
	c.startOperation(operationID, paths)
	return &modelcache.PrewarmOperation{ID: operationID}, nil
}

func (c *Client) GetPrewarmStatus(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(operation.ID) == "" {
		return nil, fmt.Errorf("cluster_cache prewarm operation ID is required")
	}

	c.mu.Lock()
	c.cleanupOperationsLocked(time.Now())
	state := c.operations[operation.ID]
	if state != nil {
		status := clonePrewarmStatus(state.status)
		c.mu.Unlock()
		return &status, nil
	}
	c.mu.Unlock()

	payload, err := decodeOperationID(operation.ID)
	if err != nil {
		return &modelcache.PrewarmStatus{
			Operation: operation,
			Phase:     modelcache.PrewarmPhaseFailed,
			Reason:    reasonOperationNotFound,
			Message:   "cluster_cache prewarm operation was not found and could not be reconstructed",
		}, nil
	}
	paths, err := c.reconstructPrewarmPaths(ctx, payload)
	if err != nil {
		return nil, err
	}

	c.startOperation(operation.ID, paths)
	return &modelcache.PrewarmStatus{
		Operation: operation,
		Phase:     modelcache.PrewarmPhaseRunning,
		Message:   "cluster_cache prewarm is running",
	}, nil
}

func (c *Client) startOperation(operationID string, paths []string) {
	c.mu.Lock()
	c.cleanupOperationsLocked(time.Now())
	if _, ok := c.operations[operationID]; ok {
		c.mu.Unlock()
		return
	}
	c.operations[operationID] = &operationState{
		status: modelcache.PrewarmStatus{
			Operation: modelcache.PrewarmOperation{ID: operationID},
			Phase:     modelcache.PrewarmPhasePending,
			Message:   "cluster_cache prewarm has been submitted",
		},
	}
	c.mu.Unlock()

	go c.runPrewarm(operationID, paths)
}

func (c *Client) runPrewarm(operationID string, paths []string) {
	c.updateOperation(operationID, func(status *modelcache.PrewarmStatus) {
		status.Phase = modelcache.PrewarmPhaseRunning
		status.Message = "cluster_cache prewarm is running"
	})

	response, err := c.fetch(c.ctx, paths)
	if err != nil {
		c.updateOperation(operationID, func(status *modelcache.PrewarmStatus) {
			status.Phase = modelcache.PrewarmPhaseFailed
			status.Reason = reasonClusterCacheFetchFailed
			status.Message = err.Error()
		})
		return
	}

	phase, progress, message := prewarmOutcome(response)
	c.updateOperation(operationID, func(status *modelcache.PrewarmStatus) {
		status.Phase = phase
		status.Progress = progress
		status.Message = message
		if phase == modelcache.PrewarmPhaseSucceeded {
			status.Reason = ""
		} else {
			status.Reason = reasonClusterCacheFetchFailed
		}
	})
}

func (c *Client) fetch(ctx context.Context, paths []string) (*fetchResponse, error) {
	body, err := json.Marshal(fetchRequest{Paths: paths})
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpointURL("/fetch", nil), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("cluster_cache fetch returned %s: %s", response.Status, readErrorBody(response.Body))
	}

	var parsed fetchResponse
	if err := json.NewDecoder(response.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode cluster_cache fetch response: %w", err)
	}
	return &parsed, nil
}

func (c *Client) updateOperation(operationID string, update func(*modelcache.PrewarmStatus)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.operations[operationID]
	if state == nil {
		return
	}
	update(&state.status)
	if prewarmPhaseTerminal(state.status.Phase) {
		if state.completedAt.IsZero() {
			state.completedAt = time.Now()
		}
		return
	}
	state.completedAt = time.Time{}
}

func (c *Client) cleanupOperationsLocked(now time.Time) {
	if c.operationRetention <= 0 {
		return
	}

	for operationID, state := range c.operations {
		if state == nil || state.completedAt.IsZero() {
			continue
		}
		if now.Sub(state.completedAt) >= c.operationRetention {
			delete(c.operations, operationID)
		}
	}
}

func prewarmPhaseTerminal(phase modelcache.PrewarmPhase) bool {
	return phase == modelcache.PrewarmPhaseSucceeded ||
		phase == modelcache.PrewarmPhaseFailed ||
		phase == modelcache.PrewarmPhaseCanceled
}

func (c *Client) endpointURL(endpointPath string, query url.Values) string {
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

func prewarmPaths(req modelcache.PrewarmRequest) []string {
	if len(req.Objects) == 0 {
		if strings.TrimSpace(req.Source.URI) == "" {
			return nil
		}
		return []string{req.Source.URI}
	}

	paths := make([]string, 0, len(req.Objects))
	for _, object := range req.Objects {
		if strings.TrimSpace(object.Path) != "" {
			paths = append(paths, object.Path)
		}
	}
	return paths
}

func prewarmOutcome(response *fetchResponse) (modelcache.PrewarmPhase, modelcache.PrewarmProgress, string) {
	if response == nil || len(response.Results) == 0 {
		return modelcache.PrewarmPhaseFailed, modelcache.PrewarmProgress{}, "cluster_cache fetch returned no path results"
	}

	var totalBytes int64
	var totalChunks int32
	var cachedChunks int32
	var hasTotalBytes bool
	var hasTotalChunks bool
	var hasCachedChunks bool
	var errors []string

	for _, result := range response.Results {
		if result.TotalSize != nil {
			totalBytes = addUint64ToInt64(totalBytes, *result.TotalSize)
			hasTotalBytes = true
		}
		if result.TotalChunks != nil {
			totalChunks = addIntToInt32(totalChunks, *result.TotalChunks)
			hasTotalChunks = true
		}

		pathCachedChunks := 0
		if result.CachedChunks != nil {
			pathCachedChunks = addInt(pathCachedChunks, *result.CachedChunks)
			hasCachedChunks = true
		}
		if result.WarmedChunks != nil {
			pathCachedChunks = addInt(pathCachedChunks, *result.WarmedChunks)
			hasCachedChunks = true
		}
		cachedChunks = addIntToInt32(cachedChunks, pathCachedChunks)

		switch result.Status {
		case "cached", "warmed":
		case "partial":
			errors = append(errors, fetchResultError(result, "cluster_cache prewarm partially completed"))
		default:
			errors = append(errors, fetchResultError(result, "cluster_cache prewarm failed"))
		}
	}

	progress := modelcache.PrewarmProgress{}
	if hasTotalBytes {
		progress.TotalBytes = int64Ptr(totalBytes)
	}
	if hasTotalChunks {
		progress.TotalChunks = int32Ptr(totalChunks)
	}
	if hasCachedChunks {
		progress.CachedChunks = int32Ptr(cachedChunks)
	}
	if len(errors) == 0 {
		if hasTotalBytes {
			progress.CachedBytes = int64Ptr(totalBytes)
		}
		return modelcache.PrewarmPhaseSucceeded, progress, "cluster_cache prewarm completed"
	}

	return modelcache.PrewarmPhaseFailed, progress, strings.Join(errors, "; ")
}

func fetchResultError(result fetchPathResult, fallback string) string {
	if result.Error != nil && *result.Error != "" {
		return fmt.Sprintf("%s: %s", result.Path, *result.Error)
	}
	return fmt.Sprintf("%s: %s", result.Path, fallback)
}

func encodeOperationID(source modelcache.Source, paths []string) (string, error) {
	payload := operationIDPayload{Paths: paths}
	if strings.TrimSpace(source.URI) != "" {
		payload = operationIDPayload{
			SourceURI: source.URI,
			PathsHash: pathsHash(paths),
		}
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return operationPrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeOperationID(operationID string) (operationIDPayload, error) {
	encoded, ok := strings.CutPrefix(operationID, operationPrefix)
	if !ok {
		return operationIDPayload{}, fmt.Errorf("operation ID does not use cluster_cache fetch format")
	}

	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return operationIDPayload{}, err
	}

	var payload operationIDPayload
	if err := json.Unmarshal(data, &payload); err == nil {
		if strings.TrimSpace(payload.SourceURI) != "" || len(payload.Paths) > 0 {
			return payload, nil
		}
	}

	var paths []string
	if err := json.Unmarshal(data, &paths); err != nil {
		return operationIDPayload{}, err
	}
	if len(paths) == 0 {
		return operationIDPayload{}, fmt.Errorf("operation ID does not contain source URI or paths")
	}
	return operationIDPayload{Paths: paths}, nil
}

func (c *Client) reconstructPrewarmPaths(ctx context.Context, payload operationIDPayload) ([]string, error) {
	if len(payload.Paths) > 0 {
		return payload.Paths, nil
	}
	if strings.TrimSpace(payload.SourceURI) == "" {
		return nil, fmt.Errorf("operation ID does not contain source URI or paths")
	}
	metadata, err := c.fileMetadata(ctx, payload.SourceURI)
	if err != nil {
		return nil, fmt.Errorf("reconstruct cluster_cache prewarm operation from file_metadata: %w", err)
	}
	inspection := metadata.toModelCacheSourceInspection(modelcache.Source{URI: payload.SourceURI})
	paths := make([]string, 0, len(inspection.Objects))
	for _, object := range inspection.Objects {
		if strings.TrimSpace(object.Path) != "" {
			paths = append(paths, object.Path)
		}
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("reconstructed cluster_cache prewarm operation has no paths")
	}
	return paths, nil
}

func pathsHash(paths []string) string {
	copyPaths := append([]string(nil), paths...)
	sort.Strings(copyPaths)
	data, _ := json.Marshal(copyPaths)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}

func clonePrewarmStatus(status modelcache.PrewarmStatus) modelcache.PrewarmStatus {
	status.Progress.TotalBytes = cloneInt64Ptr(status.Progress.TotalBytes)
	status.Progress.CachedBytes = cloneInt64Ptr(status.Progress.CachedBytes)
	status.Progress.TotalChunks = cloneInt32Ptr(status.Progress.TotalChunks)
	status.Progress.CachedChunks = cloneInt32Ptr(status.Progress.CachedChunks)
	return status
}

func cloneInt64Ptr(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneInt32Ptr(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func int64Ptr(value int64) *int64 {
	return &value
}

func int32Ptr(value int32) *int32 {
	return &value
}

func addUint64ToInt64(total int64, value uint64) int64 {
	if value > uint64(maxInt64-total) {
		return maxInt64
	}
	return total + int64(value)
}

func addIntToInt32(total int32, value int) int32 {
	if value <= 0 {
		return total
	}
	if int64(value) > int64(maxInt32-total) {
		return maxInt32
	}
	return total + int32(value)
}

func addInt(total int, value int) int {
	if value <= 0 {
		return total
	}
	if value > int(^uint(0)>>1)-total {
		return int(^uint(0) >> 1)
	}
	return total + value
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

func inspectPath(sourceURI, uri, path, relativePath string) string {
	if value := firstNonEmpty(uri, path); value != "" {
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
