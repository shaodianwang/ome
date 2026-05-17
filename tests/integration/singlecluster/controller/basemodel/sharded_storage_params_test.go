package basemodel

import (
	"context"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Sharded cache-identity sensitivity: shardedCacheIdentityChanged
// (sharded_cache_status.go) considers ONLY Status.Cache.SourceUri and
// Status.Cache.Backend. Storage.Parameters (region/credentials/etc.)
// is intentionally NOT part of the identity hash — operators rotating
// credentials or switching region in place won't trigger a re-init.
//
// This spec asserts that property by counting CheckSource calls
// across a Parameters mutation: once Ready is established, no
// further CheckSource calls should fire. A future refactor that
// hashes Parameters into the identity would invalidate the cache
// state and re-fire CheckSource — and fail this spec, which is the
// contract we want to preserve.
var _ = ginkgo.Describe("Sharded BaseModel Storage.Parameters change", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("Storage.Parameters change does not re-trigger CheckSource (Parameters intentionally invisible to cache identity)", func() {
		var checkCount atomic.Int32
		suiteFakeMC.CheckSourceFunc = func(_ context.Context, _ sharded.Source) (*sharded.SourceStatus, error) {
			checkCount.Add(1)
			return &sharded.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
		}

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

		ginkgo.By("wait for Ready=True so the CheckSource gate is locked out, then snapshot CheckSource count")
		var checkCountAtReady int32
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "params-change"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.SourceUri).To(Equal("oci://n/test/b/models/o/llama-params"))
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"),
				"wait for Ready=True so the CheckSource gate is locked out and the count is stable")
			checkCountAtReady = checkCount.Load()
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("mutate Storage.Parameters in place — switch region — leave URI alone")
		// Retry on conflict: the controller may write status concurrently.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "params-change"}, got)).To(Succeed())
			newParams := map[string]string{"region": "eu-west-1", "extra": "added"}
			got.Spec.Storage.Parameters = &newParams
			g.Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// If the contract holds (cache identity ignores Parameters),
		// no new CheckSource fires after the Update and the count
		// stays at checkCountAtReady. Use Consistently to verify it
		// stays stable across multiple reconciles in a row.
		Consistently(func(g Gomega) {
			g.Expect(checkCount.Load()).To(Equal(checkCountAtReady),
				"Storage.Parameters change must NOT trigger a fresh CheckSource — Parameters is intentionally invisible to the cache identity; if a refactor adds it, flip this spec or replace it with a positive re-check assertion")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
