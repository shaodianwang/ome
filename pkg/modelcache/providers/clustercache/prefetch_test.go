package clustercache

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/pkg/modelcache"
)

// newTestRing constructs a ring from explicit addresses. Reuses the
// production ring code so the unit tests exercise the same lookup logic
// the runtime uses.
func newTestRing(t *testing.T, addrs ...string) *clusterCacheRing {
	t.Helper()
	r, err := newClusterCacheRing(addrs)
	require.NoError(t, err)
	return r
}

// fileWithMtime returns a clusterCacheMetadataFile with mtime already set,
// so groupChunksByOwner doesn't try to HEAD-fallback in unit tests.
func fileWithMtime(path string, size, mtime uint64) clusterCacheMetadataFile {
	return clusterCacheMetadataFile{path: path, size: size, mtime: &mtime}
}

func TestGroupChunksByOwnerSingleOwner(t *testing.T) {
	c := &Client{} // groupChunksByOwner only calls c.ensureMetadataFileMTime,
	// which is a no-op when file.mtime is already set.
	ring := newTestRing(t, "10.0.0.1:10090")
	files := []clusterCacheMetadataFile{
		fileWithMtime("s3://b/config.json", 8, 100),
		fileWithMtime("s3://b/weights.bin", 40, 101),
	}

	byOwner, lookupErrors, err := c.groupChunksByOwner(
		context.Background(), files,
		[]string{"s3://b/config.json", "s3://b/weights.bin"},
		16, ring,
	)

	require.NoError(t, err)
	assert.Empty(t, lookupErrors)
	require.Len(t, byOwner, 1, "single ring node → all chunks land on one owner")

	chunks := byOwner["10.0.0.1:10090"]
	// config.json: 8 bytes / 16 chunk → 1 chunk (partial: 8 bytes)
	// weights.bin: 40 bytes / 16 chunk → 3 chunks (16, 16, 8)
	require.Len(t, chunks, 4)

	// Sort for stable assertions
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Path != chunks[j].Path {
			return chunks[i].Path < chunks[j].Path
		}
		return chunks[i].Offset < chunks[j].Offset
	})
	assert.Equal(t, prefetchChunkSpec{Path: "s3://b/config.json", Offset: 0, MTime: 100, Length: 8}, chunks[0])
	assert.Equal(t, prefetchChunkSpec{Path: "s3://b/weights.bin", Offset: 0, MTime: 101, Length: 16}, chunks[1])
	assert.Equal(t, prefetchChunkSpec{Path: "s3://b/weights.bin", Offset: 16, MTime: 101, Length: 16}, chunks[2])
	assert.Equal(t, prefetchChunkSpec{Path: "s3://b/weights.bin", Offset: 32, MTime: 101, Length: 8}, chunks[3])
}

func TestGroupChunksByOwnerMultipleOwnersPartition(t *testing.T) {
	c := &Client{}
	ring := newTestRing(t, "10.0.0.1:10090", "10.0.0.2:10090", "10.0.0.3:10090")
	files := []clusterCacheMetadataFile{
		fileWithMtime("s3://b/large.bin", 320, 7), // 20 chunks at chunkSize=16
	}

	byOwner, lookupErrors, err := c.groupChunksByOwner(
		context.Background(), files,
		[]string{"s3://b/large.bin"},
		16, ring,
	)
	require.NoError(t, err)
	assert.Empty(t, lookupErrors)

	// All chunks must be partitioned exactly once across owners.
	totalChunks := 0
	for _, chunks := range byOwner {
		totalChunks += len(chunks)
	}
	assert.Equal(t, 20, totalChunks, "every chunk must land on exactly one owner")
	assert.GreaterOrEqual(t, len(byOwner), 2, "with 3 ring nodes and 20 chunks, the ring should hit at least 2 owners")

	// No chunk should appear on more than one owner.
	seen := map[uint64]bool{}
	for _, chunks := range byOwner {
		for _, ch := range chunks {
			require.False(t, seen[ch.Offset], "chunk at offset %d duplicated across owners", ch.Offset)
			seen[ch.Offset] = true
		}
	}
}

func TestGroupChunksByOwnerSkipsEmptyFile(t *testing.T) {
	c := &Client{}
	ring := newTestRing(t, "10.0.0.1:10090")
	files := []clusterCacheMetadataFile{fileWithMtime("s3://b/empty", 0, 1)}

	byOwner, lookupErrors, err := c.groupChunksByOwner(
		context.Background(), files, []string{"s3://b/empty"}, 16, ring,
	)
	require.NoError(t, err)
	assert.Empty(t, lookupErrors)
	assert.Empty(t, byOwner, "zero-size file produces no chunks")
}

func TestGroupChunksByOwnerUnknownPathIsLookupError(t *testing.T) {
	c := &Client{}
	ring := newTestRing(t, "10.0.0.1:10090")
	files := []clusterCacheMetadataFile{fileWithMtime("s3://b/known.bin", 16, 1)}

	byOwner, lookupErrors, err := c.groupChunksByOwner(
		context.Background(), files,
		[]string{"s3://b/known.bin", "s3://b/missing.bin"},
		16, ring,
	)
	require.NoError(t, err)
	require.Len(t, lookupErrors, 1, "missing path must surface as a lookup error, not block known paths")
	assert.Contains(t, lookupErrors[0], "missing.bin")
	assert.Contains(t, lookupErrors[0], "not listed by cluster_cache file_metadata")
	assert.NotEmpty(t, byOwner, "known paths must still be routed")
}

func TestGroupChunksByOwnerRejectsZeroChunkSize(t *testing.T) {
	c := &Client{}
	ring := newTestRing(t, "10.0.0.1:10090")
	files := []clusterCacheMetadataFile{fileWithMtime("s3://b/file", 16, 1)}

	_, _, err := c.groupChunksByOwner(context.Background(), files, []string{"s3://b/file"}, 0, ring)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk_size is zero")
}

func TestGroupChunksByOwnerRejectsNilRing(t *testing.T) {
	c := &Client{}
	files := []clusterCacheMetadataFile{fileWithMtime("s3://b/file", 16, 1)}

	_, _, err := c.groupChunksByOwner(context.Background(), files, []string{"s3://b/file"}, 16, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ring is nil")
}

func TestAggregateOwnerOutcomesAllSucceed(t *testing.T) {
	outcomes := []ownerPrewarmOutcome{
		{owner: "10.0.0.1", response: &ownedChunkPrefetchResponse{TotalChunks: 5, CachedChunks: 3, WarmedChunks: 2}},
		{owner: "10.0.0.2", response: &ownedChunkPrefetchResponse{TotalChunks: 7, CachedChunks: 7, WarmedChunks: 0}},
	}

	phase, progress, failed, message := aggregateOwnerOutcomes(outcomes, nil)
	assert.Equal(t, modelcache.PrewarmPhaseSucceeded, phase)
	require.NotNil(t, progress.TotalChunks)
	require.NotNil(t, progress.CachedChunks)
	assert.Equal(t, int32(12), *progress.TotalChunks)
	assert.Equal(t, int32(12), *progress.CachedChunks, "cached+warmed across all owners → total cached")
	assert.Nil(t, failed, "no failures when all owners succeed")
	assert.Equal(t, "cluster_cache prewarm completed", message)
}

func TestAggregateOwnerOutcomesOwnerHTTPError(t *testing.T) {
	outcomes := []ownerPrewarmOutcome{
		{owner: "10.0.0.1", response: &ownedChunkPrefetchResponse{TotalChunks: 5, CachedChunks: 5}},
		{owner: "10.0.0.2", err: errors.New("POST returned 500: backend gone")},
	}

	phase, progress, _, message := aggregateOwnerOutcomes(outcomes, nil)
	assert.Equal(t, modelcache.PrewarmPhaseFailed, phase)
	require.NotNil(t, progress.TotalChunks)
	assert.Equal(t, int32(5), *progress.TotalChunks, "successful owner's progress still counted so operators see retries chip away")
	assert.Contains(t, message, "10.0.0.2")
	assert.Contains(t, message, "backend gone")
}

func TestAggregateOwnerOutcomesPerChunkErrors(t *testing.T) {
	outcomes := []ownerPrewarmOutcome{
		{owner: "10.0.0.1", response: &ownedChunkPrefetchResponse{
			TotalChunks: 5, CachedChunks: 3, WarmedChunks: 1,
			Errors: []string{"GET range=bytes=0-100 expected_mtime=1 s3://b/chunk-3: service error; status=500; code=InternalError; message=upstream timeout"},
		}},
	}

	phase, progress, failed, message := aggregateOwnerOutcomes(outcomes, nil)
	assert.Equal(t, modelcache.PrewarmPhaseFailed, phase, "any per-chunk error fails the operation — we need ALL weights")
	require.NotNil(t, progress.CachedChunks)
	assert.Equal(t, int32(4), *progress.CachedChunks, "4 of 5 chunks DID cache; surface that progress")
	// failedObjects carries the structured per-object detail; message
	// is now a short summary.
	require.Len(t, failed, 1)
	assert.Equal(t, "chunk-3", failed[0].Path)
	assert.Equal(t, "InternalError", failed[0].Code)
	assert.Equal(t, modelcache.ChunkFailureClassBackendUnreachable, failed[0].Class)
	assert.Contains(t, message, "chunk-3")
}

func TestAggregateOwnerOutcomesLookupErrorsAlone(t *testing.T) {
	phase, progress, failed, message := aggregateOwnerOutcomes(nil, []string{"s3://b/missing: not listed by cluster_cache file_metadata"})
	assert.Equal(t, modelcache.PrewarmPhaseFailed, phase)
	assert.Nil(t, progress.TotalChunks, "no owner outcomes → no chunk counts to report")
	require.Len(t, failed, 1)
	assert.Equal(t, "missing", failed[0].Path)
	assert.Equal(t, modelcache.ChunkFailureClassSourceObjectMissing, failed[0].Class)
	assert.Contains(t, message, "missing")
}

func TestTotalBytesForPathsSumsOnlyRequestedFiles(t *testing.T) {
	files := []clusterCacheMetadataFile{
		{path: "s3://b/a", size: 10},
		{path: "s3://b/b", size: 20},
		{path: "s3://b/c", size: 40},
	}

	// Two of three files requested → only their sizes contribute.
	assert.Equal(t, int64(50), totalBytesForPaths(files, []string{"s3://b/a", "s3://b/c"}))

	// All three requested → full total.
	assert.Equal(t, int64(70), totalBytesForPaths(files, []string{"s3://b/a", "s3://b/b", "s3://b/c"}))

	// A requested path absent from /file_metadata contributes zero —
	// it'll surface as a lookupError elsewhere; total reflects what we
	// can actually route.
	assert.Equal(t, int64(10), totalBytesForPaths(files, []string{"s3://b/a", "s3://b/missing"}))

	// Empty inputs.
	assert.Equal(t, int64(0), totalBytesForPaths(files, nil))
	assert.Equal(t, int64(0), totalBytesForPaths(nil, []string{"s3://b/a"}))
}

func TestAggregateOwnerOutcomesEmptyResponseFails(t *testing.T) {
	// Defensive: an owner outcome with no error AND no response shouldn't
	// silently succeed. Pin that it surfaces as a failure.
	outcomes := []ownerPrewarmOutcome{
		{owner: "10.0.0.1", response: nil, err: nil},
	}

	phase, _, _, message := aggregateOwnerOutcomes(outcomes, nil)
	assert.Equal(t, modelcache.PrewarmPhaseFailed, phase)
	assert.Contains(t, message, "10.0.0.1")
	assert.Contains(t, message, "empty response")
}
