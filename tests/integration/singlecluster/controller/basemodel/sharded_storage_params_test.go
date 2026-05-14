package basemodel

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelcache"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Sharded cache-identity sensitivity: shardedCacheIdentityChanged
// (sharded_cache_status.go) considers ONLY Status.Cache.SourceUri and
// Status.Cache.Backend. Storage.Parameters (region/credentials/etc.) is
// intentionally NOT part of the identity hash — operators rotating
// credentials or switching region in place won't trigger a re-prewarm.
//
// This spec asserts that property by checking observable state
// (Status.Cache.PrewarmID is unchanged after a Parameters mutation),
// using a per-call unique fake-op ID so any spurious re-invocation of
// StartPrewarm would surface as a different PrewarmID. A future refactor
// that hashes Parameters into the identity would flip the PrewarmID and
// fail this spec — which is the contract we want to preserve.
var _ = ginkgo.Describe("Sharded BaseModel Storage.Parameters change", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("Storage.Parameters change does not re-trigger StartPrewarm (Parameters intentionally invisible to cache identity)", func() {
		// Per-call unique PrewarmID so a SECOND StartPrewarm — for any
		// reason, including a cache-identity refactor — surfaces as a
		// PrewarmID change in Status. Counting calls would race with
		// reconcile retries and fake-mutation timing; observable status
		// state is single-write per successful StartPrewarm + status
		// update, so it's race-free for the assertion we care about.
		var prewarmCallSeq atomic.Int32
		suiteFakeMC.StartPrewarmFunc = func(_ context.Context, _ modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
			n := prewarmCallSeq.Add(1)
			return &modelcache.PrewarmOperation{ID: fmt.Sprintf("fake-op-%d", n)}, nil
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

		ginkgo.By("wait for the initial prewarm to FULLY COMPLETE (Cached=True), then snapshot PrewarmID")
		// Snapshotting only after Cached=True closes the controller-runtime
		// stale-cache race window: the StartPrewarm branch is gated on
		// `Cached != True`, so once Cached flips True no further
		// StartPrewarm can fire for any reason except a cache-identity
		// change (which is precisely what this spec asserts does NOT
		// happen on a Parameters mutation). Without this gate, two
		// concurrent reconciles can both observe PrewarmID="" briefly,
		// both call StartPrewarm, and PrewarmID transitions through
		// multiple values before stabilizing — making any earlier
		// snapshot unreliable.
		var initialPrewarmID string
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "params-change"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.SourceUri).To(Equal("oci://n/test/b/models/o/llama-params"))
			g.Expect(got.Status.Cache.PrewarmID).NotTo(BeEmpty())
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(cached.Status).To(Equal(metav1.ConditionTrue),
				"wait for Cached=True so the prewarm branch is locked out and PrewarmID is stable")
			initialPrewarmID = got.Status.Cache.PrewarmID
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

		// PrewarmID changes only on a successful StartPrewarm + status
		// update via applyPrewarmStarted. If the contract holds (cache
		// identity ignores Parameters), no new StartPrewarm fires after
		// the Update, and PrewarmID stays at initialPrewarmID. Use
		// Consistently to verify it stays stable across multiple
		// reconciles in a row.
		Consistently(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "params-change"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.PrewarmID).To(Equal(initialPrewarmID),
				"Storage.Parameters change must NOT alter Status.Cache.PrewarmID — Parameters is intentionally invisible to the cache identity hash; if a refactor adds it, flip this spec or replace it with a positive re-prewarm assertion")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
