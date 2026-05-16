package clustercache

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// End-to-end pin: a Sharded ClusterBaseModel drives the real
// clustercache.NewClient through the owner-routed prewarm path. Verifies
// the controller posts to /prefetch_owned_chunks (NOT /fetch) and reaches
// Cached=True + Ready=True. Catches the regression where the provider
// falls back to bulk /fetch or accidentally skips per-owner routing.
var _ = ginkgo.Describe("ClusterCache owner-routed prewarm e2e", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(func() {
		setHandler(http.NotFoundHandler())
	})

	ginkgo.It("Sharded CBM drives /prefetch_owned_chunks (not /fetch) and reaches Ready=True", func() {
		sourceURI := "s3://bucket/owner-routed-e2e"
		configPath := sourceURI + "/config.json"
		// Real-looking config.json so the shared parser populates spec
		// fields. Tiny model so the test is fast.
		configJSON := []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"],"hidden_size":64,"num_hidden_layers":2,"num_attention_heads":4,"vocab_size":1000,"max_position_embeddings":128}`)
		// Real safetensors layout so the controller's parse path doesn't
		// bail before reaching prewarm.
		weightPath := sourceURI + "/model-00001-of-00001.safetensors"
		header := []byte(`{"weight":{"dtype":"F32","shape":[2,3],"data_offsets":[0,24]}}`)
		safetensorsData := safetensorsBytesForTest(header, 24)
		mtimeConfig := uint64(1710000001)
		mtimeWeight := uint64(1710000002)
		chunkSize := uint64(96 * 1024 * 1024) // production default

		var (
			fetchPathsMu     sync.Mutex
			fetchPaths       []string
			bulkFetchSeen    atomic.Int32 // /fetch with more than one path → BAD
			prefetchPosts    atomic.Int32 // /prefetch_owned_chunks POST count
			prefetchedChunks atomic.Int32
		)

		var serverAddr string

		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if serverAddr == "" {
				// suiteServer is the httptest server wrapping this handler.
				// We need to advertise its addr in /ring_info, but it's
				// accessible via suiteServer.Listener — populate from
				// the request host instead.
				serverAddr = r.Host
			}
			switch r.URL.Path {
			case "/file_metadata":
				writeJSON(w, map[string]any{
					"root": sourceURI,
					"files": []map[string]any{
						{"path": configPath, "relative_path": "config.json", "size": len(configJSON), "mtime": mtimeConfig},
						{"path": weightPath, "relative_path": "model-00001-of-00001.safetensors", "size": len(safetensorsData), "mtime": mtimeWeight},
					},
				})
			case "/fetch":
				var req struct {
					Paths []string `json:"paths"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				fetchPathsMu.Lock()
				fetchPaths = append(fetchPaths, req.Paths...)
				fetchPathsMu.Unlock()
				if len(req.Paths) > 1 {
					bulkFetchSeen.Add(1)
				}
				results := make([]map[string]any, 0, len(req.Paths))
				for _, p := range req.Paths {
					size := len(configJSON)
					mtime := mtimeConfig
					if p == weightPath {
						size = len(safetensorsData)
						mtime = mtimeWeight
					}
					results = append(results, map[string]any{
						"path":          p,
						"status":        "cached",
						"total_size":    size,
						"mtime":         mtime,
						"chunk_size":    chunkSize,
						"total_chunks":  1,
						"cached_chunks": 1,
						"warmed_chunks": 0,
					})
				}
				writeJSON(w, map[string]any{"results": results})
			case "/ring_info":
				writeJSON(w, map[string]any{"nodes": []string{serverAddr}})
			case "/packed_byte_ranges_by_key":
				// Bounded metadata-byte reads for InspectSource. Stub
				// returns the requested ranges from in-memory data.
				var req struct {
					Ranges []struct {
						Path        string `json:"path"`
						ChunkOffset uint64 `json:"chunk_offset"`
						RangeOffset uint64 `json:"range_offset"`
						Length      uint64 `json:"length"`
					} `json:"ranges"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				for _, rangeSpec := range req.Ranges {
					data := map[string][]byte{configPath: configJSON, weightPath: safetensorsData}[rangeSpec.Path]
					start := rangeSpec.ChunkOffset + rangeSpec.RangeOffset
					end := start + rangeSpec.Length
					if end > uint64(len(data)) {
						http.Error(w, "range out of bounds", http.StatusBadRequest)
						return
					}
					_, _ = w.Write(data[start:end])
				}
			case "/prefetch_owned_chunks":
				var req struct {
					Chunks []struct {
						Path   string `json:"path"`
						Length uint64 `json:"length"`
					} `json:"chunks"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				prefetchPosts.Add(1)
				prefetchedChunks.Add(int32(len(req.Chunks)))
				writeJSON(w, map[string]any{
					"total_chunks":  len(req.Chunks),
					"cached_chunks": len(req.Chunks),
					"warmed_chunks": 0,
					"errors":        []string{},
				})
			default:
				http.NotFound(w, r)
			}
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("owner-routed-e2e").
			StorageURI(sourceURI).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// Truth source is the Ready condition, NOT status.State.
		// applyInitialShardedModelStatus unconditionally resets State=Importing
		// every reconcile (sharded_cache_status.go), so State oscillates after
		// hitting Ready. The Ready condition stays True. See note in
		// tests/integration/singlecluster/controller/basemodel/sharded_test.go.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "owner-routed-e2e"}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"),
				"Sharded CBM must reach Ready=True via owner-routed prewarm; got reason=%q message=%q",
				ready.Reason, ready.Message)
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("True"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Wire-level pins: the controller MUST have hit /prefetch_owned_chunks
		// (not bulk /fetch). The 1-path /fetch for chunk_size discovery is
		// fine — it's a single path well under cluster_cache's fetch_path_limit.
		Expect(prefetchPosts.Load()).To(BeNumerically(">=", 1),
			"controller must POST /prefetch_owned_chunks at least once for the prewarm work")
		Expect(prefetchedChunks.Load()).To(BeNumerically(">=", 2),
			"both files (config + weight) must yield at least one chunk each")
		Expect(bulkFetchSeen.Load()).To(BeZero(),
			"controller must NOT submit bulk /fetch with multiple paths — that's the rate-limited path the rewrite replaces")
	})
})

// safetensorsBytesForTest builds a minimal valid safetensors payload:
// 8-byte little-endian header length + header JSON + arbitrary payload bytes.
func safetensorsBytesForTest(header []byte, payloadBytes int) []byte {
	out := make([]byte, 8+len(header)+payloadBytes)
	// little-endian write
	for i, b := uint64(len(header)), 0; b < 8; b++ {
		out[b] = byte(i & 0xff)
		i >>= 8
	}
	copy(out[8:], header)
	return out
}
