package clustercache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ome-projects/ome/pkg/modelcache"
)

// Wire shapes mirror prod_inference cluster_cache protocol/src/api.rs:43-63.
type prefetchChunkSpec struct {
	Path   string `json:"path"`
	Offset uint64 `json:"offset"`
	MTime  uint64 `json:"mtime"`
	Length uint64 `json:"length"`
}

type ownedChunkPrefetchRequest struct {
	Chunks []prefetchChunkSpec `json:"chunks"`
}

type ownedChunkPrefetchResponse struct {
	TotalChunks  int      `json:"total_chunks"`
	CachedChunks int      `json:"cached_chunks"`
	WarmedChunks int      `json:"warmed_chunks"`
	Errors       []string `json:"errors"`
}

// defaultPrefetchOwnerTimeout matches the canonical fanout client's
// DEFAULT_PREWARM_REQUEST_TIMEOUT_SECS=600 (fanout/src/lib.rs:135). The
// provider's default HTTP timeout (5min) is too short for an owner that
// has to materialize hundreds of chunks from S3 in one call.
const defaultPrefetchOwnerTimeout = 10 * time.Minute

// ownerPrewarmOutcome is one owner's contribution to the aggregate result.
// Populated by postPrefetchOwnedChunks or by lookup-side bookkeeping.
type ownerPrewarmOutcome struct {
	owner    string
	response *ownedChunkPrefetchResponse
	err      error
}

// runOwnerRoutedPrewarm is the body of runPrewarm. Steps mirror
// FanoutScheduler::prewarm (fanout/src/lib.rs:1301): list files via
// /file_metadata once, discover chunk_size, fetch the ring, compute
// per-owner chunk specs, POST /prefetch_owned_chunks per owner
// concurrently, aggregate.
func (c *Client) runOwnerRoutedPrewarm(ctx context.Context, sourceURI string, paths []string) (modelcache.PrewarmPhase, modelcache.PrewarmProgress, string) {
	metadata, err := c.fileMetadata(ctx, sourceURI)
	if err != nil {
		return modelcache.PrewarmPhaseFailed, modelcache.PrewarmProgress{}, fmt.Sprintf("list source files: %v", err)
	}

	files := metadataFilesFromResponse(modelcache.Source{URI: sourceURI}, metadata)
	if len(files) == 0 {
		return modelcache.PrewarmPhaseFailed, modelcache.PrewarmProgress{}, "cluster_cache file_metadata returned no files"
	}

	chunkSize, err := c.metadataChunkSize(ctx, files)
	if err != nil {
		return modelcache.PrewarmPhaseFailed, modelcache.PrewarmProgress{}, fmt.Sprintf("discover chunk_size: %v", err)
	}

	ring, err := c.ring(ctx)
	if err != nil {
		return modelcache.PrewarmPhaseFailed, modelcache.PrewarmProgress{}, fmt.Sprintf("fetch ring_info: %v", err)
	}

	byOwner, lookupErrors, err := c.groupChunksByOwner(ctx, files, paths, chunkSize, ring)
	if err != nil {
		return modelcache.PrewarmPhaseFailed, modelcache.PrewarmProgress{}, err.Error()
	}
	if len(byOwner) == 0 {
		return modelcache.PrewarmPhaseFailed, modelcache.PrewarmProgress{}, "no chunks routed to any owner"
	}

	outcomes := c.postPerOwnerConcurrent(ctx, byOwner)
	return aggregateOwnerOutcomes(outcomes, lookupErrors)
}

// groupChunksByOwner builds the per-owner chunk lists. HEAD-fallback for
// files whose /file_metadata response carried no mtime (Option<u64> on
// the wire, per protocol/src/api.rs); without a real mtime the chunk-key
// won't match daemon-side chunk storage.
//
// Returned lookupErrors are non-fatal per-path issues (e.g. caller asked
// for a path that /file_metadata didn't return). They contribute to the
// final aggregated message so operators can diagnose, but don't short-
// circuit the per-owner POSTs that DID succeed.
func (c *Client) groupChunksByOwner(ctx context.Context, files []clusterCacheMetadataFile, paths []string, chunkSize uint64, ring *clusterCacheRing) (map[string][]prefetchChunkSpec, []string, error) {
	if chunkSize == 0 {
		return nil, nil, fmt.Errorf("cluster_cache chunk_size is zero")
	}
	if ring == nil {
		return nil, nil, fmt.Errorf("cluster_cache ring is nil")
	}

	// Index files by path for O(1) lookup, supporting both the absolute
	// URI form ("s3://bucket/path") and the relative form ("path") that
	// some callers may submit.
	byPath := make(map[string]*clusterCacheMetadataFile, len(files))
	for i := range files {
		byPath[files[i].path] = &files[i]
	}

	byOwner := map[string][]prefetchChunkSpec{}
	var lookupErrors []string

	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		file := byPath[path]
		if file == nil {
			lookupErrors = append(lookupErrors, fmt.Sprintf("%s: not listed by cluster_cache file_metadata", path))
			continue
		}
		if file.size == 0 {
			continue // empty file: nothing to warm
		}
		if err := c.ensureMetadataFileMTime(ctx, file); err != nil {
			lookupErrors = append(lookupErrors, fmt.Sprintf("%s: head fallback for mtime: %v", path, err))
			continue
		}

		for offset := uint64(0); offset < file.size; offset += chunkSize {
			length := chunkSize
			if remaining := file.size - offset; remaining < length {
				length = remaining
			}
			owner, err := ring.lookup(clusterCacheChunkKey{
				path:   file.path,
				offset: offset,
				mtime:  *file.mtime,
			})
			if err != nil {
				lookupErrors = append(lookupErrors, fmt.Sprintf("%s@%d: ring lookup: %v", path, offset, err))
				continue
			}
			byOwner[owner] = append(byOwner[owner], prefetchChunkSpec{
				Path:   file.path,
				Offset: offset,
				MTime:  *file.mtime,
				Length: length,
			})
		}
	}
	return byOwner, lookupErrors, nil
}

// postPerOwnerConcurrent fans out one POST per owner. Concurrency is
// bounded by len(owners); each owner gets its own request with its own
// timeout. Returns one outcome per owner; caller aggregates.
func (c *Client) postPerOwnerConcurrent(ctx context.Context, byOwner map[string][]prefetchChunkSpec) []ownerPrewarmOutcome {
	owners := make([]string, 0, len(byOwner))
	for owner := range byOwner {
		owners = append(owners, owner)
	}
	sort.Strings(owners) // deterministic order helps test assertions and log readability

	outcomes := make([]ownerPrewarmOutcome, len(owners))
	var wg sync.WaitGroup
	for i, owner := range owners {
		wg.Add(1)
		go func(i int, owner string) {
			defer wg.Done()
			resp, err := c.postPrefetchOwnedChunks(ctx, owner, byOwner[owner])
			outcomes[i] = ownerPrewarmOutcome{owner: owner, response: resp, err: err}
		}(i, owner)
	}
	wg.Wait()
	return outcomes
}

// postPrefetchOwnedChunks sends one owner's chunk list to its daemon.
// Uses defaultPrefetchOwnerTimeout (10m) per request — long enough for an
// owner to materialize hundreds of S3 chunks at PREFETCH_OWNED_CHUNK_CONCURRENCY=8.
func (c *Client) postPrefetchOwnedChunks(ctx context.Context, owner string, chunks []prefetchChunkSpec) (*ownedChunkPrefetchResponse, error) {
	body, err := json.Marshal(ownedChunkPrefetchRequest{Chunks: chunks})
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, defaultPrefetchOwnerTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.nodeEndpointURL(owner, "/prefetch_owned_chunks", url.Values{}), bytes.NewReader(body))
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
		return nil, fmt.Errorf("prefetch_owned_chunks %s returned %s: %s", owner, response.Status, readErrorBody(response.Body))
	}

	var parsed ownedChunkPrefetchResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 16*1024*1024)).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode prefetch_owned_chunks response from %s: %w", owner, err)
	}
	return &parsed, nil
}

// aggregateOwnerOutcomes collapses per-owner results into one PrewarmStatus.
// Policy: any per-owner network error OR any per-chunk error in any
// owner's response OR any lookup error means the operation Failed. Progress
// fields are the sum across all owners (counts the partial work that did
// succeed so operators can see retries are making progress).
func aggregateOwnerOutcomes(outcomes []ownerPrewarmOutcome, lookupErrors []string) (modelcache.PrewarmPhase, modelcache.PrewarmProgress, string) {
	var totalChunks, cachedChunks, warmedChunks int32
	var errMessages []string
	errMessages = append(errMessages, lookupErrors...)

	for _, outcome := range outcomes {
		if outcome.err != nil {
			errMessages = append(errMessages, fmt.Sprintf("%s: %v", outcome.owner, outcome.err))
			continue
		}
		if outcome.response == nil {
			errMessages = append(errMessages, fmt.Sprintf("%s: empty response", outcome.owner))
			continue
		}
		totalChunks += int32(outcome.response.TotalChunks)
		cachedChunks += int32(outcome.response.CachedChunks)
		warmedChunks += int32(outcome.response.WarmedChunks)
		for _, e := range outcome.response.Errors {
			errMessages = append(errMessages, fmt.Sprintf("%s: %s", outcome.owner, e))
		}
	}

	progress := modelcache.PrewarmProgress{}
	if totalChunks > 0 {
		progress.TotalChunks = int32Ptr(totalChunks)
		progress.CachedChunks = int32Ptr(cachedChunks + warmedChunks)
	}

	if len(errMessages) == 0 {
		return modelcache.PrewarmPhaseSucceeded, progress, "cluster_cache prewarm completed"
	}
	return modelcache.PrewarmPhaseFailed, progress, strings.Join(errMessages, "; ")
}
