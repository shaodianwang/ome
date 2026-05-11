package basemodel

import (
	"context"
	"sync"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/modelcache"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// Two ClusterBaseModels with the same StorageURI but different names
// must produce two DISTINCT cache identities — the per-BM
// IdempotencyKey (sharded_cache_status.go:620:
// "{kind}/{ns}/{name}/{generation}/{sourceHash}") includes the
// object name, so the cache provider sees two separate prewarm
// requests and can keep separate state per consumer. Without this
// distinction, a delete or status change on one BM could affect the
// other's cache identity.
var _ = ginkgo.Describe("Sharded BaseModel: two CBMs with the same StorageURI", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("two CBMs same URI → distinct IdempotencyKeys, both reach Cached=True independently", func() {
		// Capture IdempotencyKeys per StartPrewarm call so we can
		// assert two distinct keys after both BMs reconcile. Use a
		// mutex to avoid a data race on the shared map.
		var (
			keysMu sync.Mutex
			keys   []string
		)
		suiteFakeMC.StartPrewarmFunc = func(_ context.Context, req modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
			keysMu.Lock()
			keys = append(keys, req.IdempotencyKey)
			keysMu.Unlock()
			return &modelcache.PrewarmOperation{ID: "fake-op-" + req.IdempotencyKey}, nil
		}

		const sharedURI = "oci://n/test/b/models/o/llama-shared"
		cbmA := v1beta1testing.MakeClusterBaseModel("two-cbm-a").
			StorageURI(sharedURI).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		cbmB := v1beta1testing.MakeClusterBaseModel("two-cbm-b").
			StorageURI(sharedURI).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbmA)).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, cbmB)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbmA) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbmB) })

		ginkgo.By("both CBMs reach Cached=True independently")
		for _, name := range []string{"two-cbm-a", "two-cbm-b"} {
			Eventually(func(g Gomega) {
				got := &v1beta1.ClusterBaseModel{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name}, got)).To(Succeed())
				cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).NotTo(BeNil(), "%s: Cached condition must exist", name)
				g.Expect(string(cached.Status)).To(Equal("True"), "%s: Cached must be True", name)
			}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		keysMu.Lock()
		distinctKeys := uniqueStrings(keys)
		keysMu.Unlock()
		// At least 2 distinct keys, one per CBM. May see more if
		// the controller retried (e.g. cache lag), but the SET must
		// always include both BMs' identities.
		Expect(distinctKeys).To(HaveLen(2),
			"each CBM must produce its own IdempotencyKey even though their source URIs match; got %v", distinctKeys)
	})

	ginkgo.It("deleting one CBM does NOT cancel the other's prewarm or affect its state", func() {
		// The Cache.PrewarmID stash is per-BM (Status.Cache is
		// per-CR), so deleting one BM should not touch the other's
		// state. Cache provider sees no signal either way (no
		// CancelPrewarm), so the test asserts ONLY the OME-side
		// invariant: surviving BM's status remains Cached=True.
		const sharedURI = "oci://n/test/b/models/o/llama-isolation"
		cbmKeep := v1beta1testing.MakeClusterBaseModel("two-cbm-keep").
			StorageURI(sharedURI).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		cbmGo := v1beta1testing.MakeClusterBaseModel("two-cbm-go").
			StorageURI(sharedURI).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbmKeep)).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, cbmGo)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbmKeep) })

		// Wait for both to flip Cached=True so we have a clean
		// "before" point.
		for _, name := range []string{"two-cbm-keep", "two-cbm-go"} {
			Eventually(func(g Gomega) {
				got := &v1beta1.ClusterBaseModel{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name}, got)).To(Succeed())
				cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).NotTo(BeNil())
				g.Expect(string(cached.Status)).To(Equal("True"))
			}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		Expect(k8sClient.Delete(suiteCtx, cbmGo)).To(Succeed())

		// Surviving BM must STAY Cached=True — Consistently across a
		// short window because a regression would presumably show as
		// the surviving BM's Cached condition flipping or the BM
		// somehow getting GC'd alongside its sibling.
		Consistently(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "two-cbm-keep"}, got)).To(Succeed())
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("True"),
				"surviving CBM with the same URI must keep its Cached=True state")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// uniqueStrings returns the set of distinct values in s, preserving
// no particular order. Local helper kept private to this file.
func uniqueStrings(s []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
