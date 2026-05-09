package clustercache

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
)

const clusterCacheDefaultVNodes = 4000

type clusterCacheChunkKey struct {
	path   string
	mtime  uint64
	offset uint64
}

func (k clusterCacheChunkKey) hash() uint64 {
	buf := make([]byte, 0, len(k.path)+16)
	buf = append(buf, k.path...)

	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], k.offset)
	buf = append(buf, scratch[:]...)
	binary.LittleEndian.PutUint64(scratch[:], k.mtime)
	buf = append(buf, scratch[:]...)

	return xxh64(buf, 0)
}

type clusterCacheVNode struct {
	position  uint64
	nodeIndex int
}

type clusterCacheRing struct {
	nodes  []string
	vnodes []clusterCacheVNode
}

func newClusterCacheRing(nodes []string) (*clusterCacheRing, error) {
	normalized := make([]string, 0, len(nodes))
	seen := map[string]struct{}{}
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		normalized = append(normalized, node)
	}
	if len(normalized) == 0 {
		return nil, fmt.Errorf("cluster_cache ring has no nodes")
	}

	vnodes := make([]clusterCacheVNode, 0, len(normalized)*clusterCacheDefaultVNodes)
	var scratch [4]byte
	for nodeIndex, node := range normalized {
		nodeSeed := xxh64([]byte(node), 0)
		for vnode := uint32(0); vnode < clusterCacheDefaultVNodes; vnode++ {
			binary.LittleEndian.PutUint32(scratch[:], vnode)
			vnodes = append(vnodes, clusterCacheVNode{
				position:  xxh64(scratch[:], nodeSeed),
				nodeIndex: nodeIndex,
			})
		}
	}

	sort.SliceStable(vnodes, func(i, j int) bool {
		return vnodes[i].position < vnodes[j].position
	})

	return &clusterCacheRing{
		nodes:  normalized,
		vnodes: vnodes,
	}, nil
}

func (r *clusterCacheRing) lookup(key clusterCacheChunkKey) (string, error) {
	candidates := r.lookupCandidates(key)
	if len(candidates) == 0 {
		return "", fmt.Errorf("cluster_cache ring has no nodes")
	}
	return candidates[0], nil
}

func (r *clusterCacheRing) lookupCandidates(key clusterCacheChunkKey) []string {
	if r == nil || len(r.vnodes) == 0 || len(r.nodes) == 0 {
		return nil
	}

	hash := key.hash()
	start := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].position >= hash
	})
	if start >= len(r.vnodes) {
		start = 0
	}

	candidates := make([]string, 0, len(r.nodes))
	seen := make(map[string]struct{}, len(r.nodes))
	for offset := 0; offset < len(r.vnodes); offset++ {
		node := r.nodes[r.vnodes[(start+offset)%len(r.vnodes)].nodeIndex]
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		candidates = append(candidates, node)
		if len(candidates) == len(r.nodes) {
			break
		}
	}
	return candidates
}

func xxh64(data []byte, seed uint64) uint64 {
	digest := xxhash.NewWithSeed(seed)
	_, _ = digest.Write(data)
	return digest.Sum64()
}
