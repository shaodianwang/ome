package basemodel

import (
	"context"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/modelcache"

	"github.com/sgl-project/ome/tests/integration/util"
)

// Sharded cache-identity sensitivity: shardedCacheIdentityChanged
// (sharded_cache_status.go:488-498) considers ONLY Status.Cache.SourceUri
// and Status.Cache.Backend. It does NOT consider Storage.Parameters
// (region/credentials/etc.) or StorageKey.
//
// Net effect: if an operator updates Storage.Parameters in-place
// (e.g. rotating credentials, switching region), the controller does
// NOT trigger a fresh prewarm — the cache identity hash stays the
// same. The provider keeps using the stale parameters captured in
// the original CheckSource/StartPrewarm calls.
//
// This spec pins that current behavior so a future refactor that
// hashes Parameters into the identity surfaces here.
var _ = ginkgo.Describe("Sharded BaseModel Storage.Parameters change", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("Storage.Parameters change without URI change does NOT trigger a fresh prewarm (current behavior — gap documented)", func() {
		var prewarmCalls atomic.Int32
		suiteFakeMC.StartPrewarmFunc = setSuiteFakeMCDefaultsAndCount(&prewarmCalls)

		params := map[string]string{"region": "us-east-1"}
		cbm := &v1beta1.ClusterBaseModel{}
		cbm.Name = "params-change"
		cbm.Spec = v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{
				StorageUri: ptr.To("oci://n/test/b/models/o/llama-params"),
				Parameters: &params,
			},
			Distribution: ptr.To(v1beta1.DistributionSharded),
			ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
		}
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("wait for the initial prewarm to complete and Status.Cache.SourceUri to be set")
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "params-change"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.SourceUri).To(Equal("oci://n/test/b/models/o/llama-params"))
			g.Expect(got.Status.Cache.PrewarmID).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		callsBeforeChange := prewarmCalls.Load()
		Expect(callsBeforeChange).To(BeNumerically(">=", 1))

		ginkgo.By("mutate Storage.Parameters in place — switch region — leave URI alone")
		got := &v1beta1.ClusterBaseModel{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "params-change"}, got)).To(Succeed())
		newParams := map[string]string{"region": "eu-west-1", "extra": "added"}
		got.Spec.Storage.Parameters = &newParams
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		// shardedCacheIdentityChanged ignores Parameters. The
		// identity hash is unchanged → no resetShardedCacheProgress
		// → no fresh StartPrewarm. Use Consistently to prove the
		// negative across multiple reconciles.
		Consistently(func(g Gomega) {
			g.Expect(prewarmCalls.Load()).To(Equal(callsBeforeChange),
				"current behavior: Parameters change does NOT trigger a fresh prewarm — gap to track")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// --- file-local helpers ----------------------------------------------------

// setSuiteFakeMCDefaultsAndCount returns a StartPrewarmFunc that
// behaves like the suite default but additionally increments the
// supplied counter. Used by specs that need to count invocations
// while keeping the rest of the fake's behavior unchanged.
func setSuiteFakeMCDefaultsAndCount(counter *atomic.Int32) func(context.Context, modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
	return func(_ context.Context, _ modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
		counter.Add(1)
		return &modelcache.PrewarmOperation{ID: "fake-op-counted"}, nil
	}
}
