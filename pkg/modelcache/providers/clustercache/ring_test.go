package clustercache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterCacheChunkKeyHashMatchesRustGoldenVectors(t *testing.T) {
	tests := []struct {
		name string
		key  clusterCacheChunkKey
		want uint64
	}{
		{
			name: "first chunk",
			key: clusterCacheChunkKey{
				path:   "bucket/path/file_0.bin",
				offset: 0,
				mtime:  1700000000,
			},
			want: 18255516902228543114,
		},
		{
			name: "later chunk",
			key: clusterCacheChunkKey{
				path:   "bucket/path/file_0.bin",
				offset: 96 * 1024 * 1024,
				mtime:  1700000000,
			},
			want: 15933345126218540491,
		},
		{
			name: "different path",
			key: clusterCacheChunkKey{
				path:   "bucket/path/file_1.bin",
				offset: 0,
				mtime:  1700000001,
			},
			want: 10177602664550478931,
		},
		{
			name: "empty",
			key:  clusterCacheChunkKey{},
			want: 12612883901365648434,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.key.hash())
		})
	}
}

func TestNewClusterCacheRingLookupCandidates(t *testing.T) {
	ring, err := newClusterCacheRing([]string{
		"[::1]:10090",
		"[::1]:10091",
		"[::1]:10092",
	})
	require.NoError(t, err)

	candidates := ring.lookupCandidates(clusterCacheChunkKey{
		path:   "s3://bucket/model/model-00001-of-00002.safetensors",
		offset: 0,
		mtime:  1710000000,
	})

	require.Len(t, candidates, 3)
	assert.ElementsMatch(t, []string{"[::1]:10090", "[::1]:10091", "[::1]:10092"}, candidates)
}

func TestClusterCacheRingLookupCandidatesMatchRustGoldenVectors(t *testing.T) {
	ring, err := newClusterCacheRing([]string{
		"[::1]:10092",
		"[::1]:10090",
		"[::1]:10091",
	})
	require.NoError(t, err)

	tests := []struct {
		name string
		key  clusterCacheChunkKey
		want []string
	}{
		{
			name: "first safetensors chunk",
			key: clusterCacheChunkKey{
				path:   "s3://bucket/model/model-00001-of-00002.safetensors",
				offset: 0,
				mtime:  1710000000,
			},
			want: []string{"[::1]:10091", "[::1]:10090", "[::1]:10092"},
		},
		{
			name: "later safetensors chunk",
			key: clusterCacheChunkKey{
				path:   "s3://bucket/model/model-00001-of-00002.safetensors",
				offset: 96 * 1024 * 1024,
				mtime:  1710000000,
			},
			want: []string{"[::1]:10092", "[::1]:10090", "[::1]:10091"},
		},
		{
			name: "second safetensors shard",
			key: clusterCacheChunkKey{
				path:   "s3://bucket/model/model-00002-of-00002.safetensors",
				offset: 0,
				mtime:  1710000001,
			},
			want: []string{"[::1]:10092", "[::1]:10090", "[::1]:10091"},
		},
		{
			name: "non-uri path",
			key: clusterCacheChunkKey{
				path:   "bucket/path/file_0.bin",
				offset: 0,
				mtime:  1700000000,
			},
			want: []string{"[::1]:10092", "[::1]:10091", "[::1]:10090"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ring.lookupCandidates(tt.key))
		})
	}
}

func TestNewClusterCacheRingRejectsEmptyNodes(t *testing.T) {
	_, err := newClusterCacheRing([]string{"", " "})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no nodes")
}
