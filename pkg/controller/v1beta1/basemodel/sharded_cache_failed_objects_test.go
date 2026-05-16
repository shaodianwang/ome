package basemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelcache"
)

func TestMergeFailedObjects_FirstRoundSetsFirstAndLastSeen(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	fresh := []modelcache.ChunkFailure{
		{Path: "config.json", Class: modelcache.ChunkFailureClassSourceObjectMissing, Code: "NoSuchKey", Message: "404", Attempts: 1},
	}

	merged, total, truncated := mergeFailedObjects(nil, fresh, now)
	require.Len(t, merged, 1)
	assert.Equal(t, int32(1), total)
	assert.False(t, truncated)
	assert.Equal(t, "config.json", merged[0].Path)
	require.NotNil(t, merged[0].FirstSeenAt)
	require.NotNil(t, merged[0].LastSeenAt)
	assert.True(t, merged[0].FirstSeenAt.Time.Equal(now), "first round seeds FirstSeenAt")
	assert.True(t, merged[0].LastSeenAt.Time.Equal(now))
	require.NotNil(t, merged[0].Attempts)
	assert.Equal(t, int32(1), *merged[0].Attempts)
}

func TestMergeFailedObjects_SecondRoundPreservesFirstAccumulatesAttempts(t *testing.T) {
	earlier := time.Date(2026, 5, 16, 8, 0, 0, 0, time.UTC)
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	prior := []v1beta1.FailedObject{
		{
			Path:        "config.json",
			ErrorClass:  v1beta1.ErrorClassSourceObjectMissing,
			ErrorCode:   "NoSuchKey",
			Message:     "404",
			FirstSeenAt: &metav1.Time{Time: earlier},
			LastSeenAt:  &metav1.Time{Time: earlier},
			Attempts:    ptr.To(int32(8)),
		},
	}
	fresh := []modelcache.ChunkFailure{
		{Path: "config.json", Class: modelcache.ChunkFailureClassSourceObjectMissing, Code: "NoSuchKey", Message: "404 still", Attempts: 8},
	}

	merged, _, _ := mergeFailedObjects(prior, fresh, now)
	require.Len(t, merged, 1)
	assert.True(t, merged[0].FirstSeenAt.Time.Equal(earlier), "FirstSeenAt must be preserved across rounds")
	assert.True(t, merged[0].LastSeenAt.Time.Equal(now), "LastSeenAt always advances to the latest round")
	require.NotNil(t, merged[0].Attempts)
	assert.Equal(t, int32(16), *merged[0].Attempts, "8 prior + 8 fresh = 16 cumulative")
	assert.Equal(t, "404 still", merged[0].Message, "Message takes the freshest report (provider may have refined it)")
}

func TestMergeFailedObjects_ResolvedPathsDropped(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	prior := []v1beta1.FailedObject{
		{Path: "config.json", FirstSeenAt: &metav1.Time{Time: now.Add(-time.Hour)}},
		{Path: "weights.safetensors", FirstSeenAt: &metav1.Time{Time: now.Add(-time.Hour)}},
	}
	fresh := []modelcache.ChunkFailure{
		// only config.json is still failing — weights.safetensors recovered
		{Path: "config.json"},
	}

	merged, total, _ := mergeFailedObjects(prior, fresh, now)
	require.Len(t, merged, 1)
	assert.Equal(t, int32(1), total)
	assert.Equal(t, "config.json", merged[0].Path)
}

func TestMergeFailedObjects_CapsAtMaxAndReportsTruncation(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	// 15 failing objects → should cap at MaxFailedObjectsReported (10).
	fresh := make([]modelcache.ChunkFailure, 15)
	for i := range fresh {
		fresh[i] = modelcache.ChunkFailure{Path: timeKeyPath(i)}
	}

	merged, total, truncated := mergeFailedObjects(nil, fresh, now)
	assert.Len(t, merged, v1beta1.MaxFailedObjectsReported)
	assert.Equal(t, int32(15), total, "total reflects all observed failures, not just the emitted ones")
	assert.True(t, truncated)
}

// timeKeyPath generates predictable, sortable test paths so the cap test
// doesn't depend on map iteration order.
func timeKeyPath(i int) string {
	return string(rune('a'+i)) + ".bin"
}

func TestMergeFailedObjects_SortsByFirstSeenAt(t *testing.T) {
	t0 := time.Date(2026, 5, 16, 8, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 5, 16, 9, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	prior := []v1beta1.FailedObject{
		{Path: "newer", FirstSeenAt: &metav1.Time{Time: t1}},
		{Path: "older", FirstSeenAt: &metav1.Time{Time: t0}},
	}
	fresh := []modelcache.ChunkFailure{
		{Path: "newest"}, // first-seen NOW
		{Path: "newer"},
		{Path: "older"},
	}

	merged, _, _ := mergeFailedObjects(prior, fresh, t2)
	require.Len(t, merged, 3)
	// Ordered oldest → newest by FirstSeenAt.
	assert.Equal(t, "older", merged[0].Path)
	assert.Equal(t, "newer", merged[1].Path)
	assert.Equal(t, "newest", merged[2].Path)
}

func TestApplyFailedObjectsToCache_SuppressesNoOpWrites(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC))
	existing := []v1beta1.FailedObject{
		{
			Path:        "config.json",
			ErrorClass:  v1beta1.ErrorClassSourceObjectMissing,
			ErrorCode:   "NoSuchKey",
			Message:     "404",
			FirstSeenAt: &now,
			LastSeenAt:  &now,
			Attempts:    ptr.To(int32(4)),
		},
	}
	cache := &v1beta1.ModelCacheStatus{
		FailedObjects:      existing,
		FailedObjectsTotal: ptr.To(int32(1)),
	}

	// Same merged set → no write.
	updated := applyFailedObjectsToCache(cache, existing, 1, false)
	assert.False(t, updated, "identical merged set must not flap status")

	// Adding a second object → write.
	merged := append([]v1beta1.FailedObject{}, existing...)
	merged = append(merged, v1beta1.FailedObject{Path: "weights.safetensors"})
	updated = applyFailedObjectsToCache(cache, merged, 2, false)
	assert.True(t, updated)
	assert.Len(t, cache.FailedObjects, 2)
	require.NotNil(t, cache.FailedObjectsTotal)
	assert.Equal(t, int32(2), *cache.FailedObjectsTotal)
}

func TestClearFailedObjectsOnCache_NilsAllThreeFields(t *testing.T) {
	cache := &v1beta1.ModelCacheStatus{
		FailedObjects:          []v1beta1.FailedObject{{Path: "x"}},
		FailedObjectsTotal:     ptr.To(int32(1)),
		FailedObjectsTruncated: ptr.To(true),
	}
	updated := clearFailedObjectsOnCache(cache)
	assert.True(t, updated)
	assert.Nil(t, cache.FailedObjects)
	assert.Nil(t, cache.FailedObjectsTotal)
	assert.Nil(t, cache.FailedObjectsTruncated)

	// Idempotent: clearing an already-clear cache reports no change.
	assert.False(t, clearFailedObjectsOnCache(cache))
}

func TestHasMetadataObjectMissing_RecognizesKnownBasenames(t *testing.T) {
	assert.True(t, hasMetadataObjectMissing([]v1beta1.FailedObject{
		{Path: "config.json", ErrorClass: v1beta1.ErrorClassSourceObjectMissing},
	}))
	assert.True(t, hasMetadataObjectMissing([]v1beta1.FailedObject{
		{Path: "model_index.json", ErrorClass: v1beta1.ErrorClassSourceObjectMissing},
	}))
	assert.True(t, hasMetadataObjectMissing([]v1beta1.FailedObject{
		{Path: "model.safetensors.index.json", ErrorClass: v1beta1.ErrorClassSourceObjectMissing},
	}))
	assert.True(t, hasMetadataObjectMissing([]v1beta1.FailedObject{
		{Path: "hf_quant_config.json", ErrorClass: v1beta1.ErrorClassSourceObjectMissing},
	}))
	// Weights file: not a metadata file → no caveat.
	assert.False(t, hasMetadataObjectMissing([]v1beta1.FailedObject{
		{Path: "model-00001-of-00002.safetensors", ErrorClass: v1beta1.ErrorClassSourceObjectMissing},
	}))
	// Right path but different class (e.g. AuthDenied) → no caveat (the
	// metadata may still be reachable once creds are fixed).
	assert.False(t, hasMetadataObjectMissing([]v1beta1.FailedObject{
		{Path: "config.json", ErrorClass: v1beta1.ErrorClassAuthDenied},
	}))
}

func TestHasSourceObjectMissing(t *testing.T) {
	assert.False(t, hasSourceObjectMissing(nil))
	assert.False(t, hasSourceObjectMissing([]v1beta1.FailedObject{
		{Path: "x", ErrorClass: v1beta1.ErrorClassBackendUnreachable},
	}))
	assert.True(t, hasSourceObjectMissing([]v1beta1.FailedObject{
		{Path: "x", ErrorClass: v1beta1.ErrorClassBackendUnreachable},
		{Path: "y", ErrorClass: v1beta1.ErrorClassSourceObjectMissing},
	}))
}
