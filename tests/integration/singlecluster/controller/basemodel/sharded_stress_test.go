package basemodel

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Sharded operational stress. concurrent_reconcile_test.go covers
// the per-node (PVC-less / agent-driven) reconciler under N parallel
// BMs; this file covers the SHARDED reconcile path under analogous
// load. The two paths have entirely separate hot loops
// (sharded_cache_status.go vs the per-node CM watch), so concurrent
// safety on one doesn't transfer to the other.
const shardedStressN = 10

var _ = ginkgo.Describe("Sharded BaseModel operational stress", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("creating N sharded ClusterBaseModels simultaneously: all reach Ready=True", func() {
		// Catches a regression where one slow / blocked CR's reconcile
		// starves the others (e.g., an unbounded Eventually inside the
		// reconciler, or a global lock around modelcache calls). All
		// N share the suite's single fakeMC; per-CR isolation comes
		// from the URI-based dedup key.
		names := make([]string, 0, shardedStressN)
		for i := 0; i < shardedStressN; i++ {
			name := fmt.Sprintf("sharded-stress-%02d", i)
			names = append(names, name)
			cbm := v1beta1testing.MakeClusterBaseModel(name).
				StorageURI(fmt.Sprintf("oci://n/test/b/models/o/llama-%02d", i)).
				Distribution(v1beta1.DistributionSharded).
				ModelFormat("safetensors").Obj()
			Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		}
		// Cleanup loops over `names` (captured cleanly) instead of
		// the per-iteration cbm pointer, which would otherwise
		// resolve to the LAST loop value at deferred run time.
		ginkgo.DeferCleanup(func() {
			for _, name := range names {
				toDelete := &v1beta1.ClusterBaseModel{}
				if err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: name}, toDelete); err == nil {
					_ = k8sClient.Delete(suiteCtx, toDelete)
				}
			}
		})

		// Single Eventually across the whole batch — a single laggard
		// fails with a precise pointer to which CR is stuck and why.
		Eventually(func(g Gomega) {
			for _, name := range names {
				got := &v1beta1.ClusterBaseModel{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name}, got)).To(Succeed())
				ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).NotTo(BeNil(), "CBM %q has no Ready condition yet", name)
				g.Expect(string(ready.Status)).To(Equal("True"),
					"CBM %q stuck (reason=%q msg=%q) — concurrent reconciles must not starve any single CR",
					name, ready.Reason, ready.Message)
				g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("rapid Storage.StorageUri churn: controller observes final URI and reaches Ready", func() {
		// Storage URI changes trigger shardedCacheIdentityChanged
		// (sharded_cache_status.go:658) which resets cache progress
		// and fires a fresh prewarm. Under rapid churn the watch
		// coalesces intermediate versions; the controller must
		// converge to the LATEST URI without getting stuck on an
		// intermediate one. Catches a regression where rapid
		// updates cause the SourceUri tracking to lag Spec forever.
		const urisInChurn = 5
		uriFor := func(i int) string {
			return fmt.Sprintf("oci://n/test/b/models/o/churn-llama-v%d", i)
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-churn").
			StorageURI(uriFor(0)).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// Wait for v0 to settle so the churn doesn't race against
		// the initial Create's first reconcile — otherwise a
		// genuine churn-handling bug would look like a trivial
		// single-update test.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-churn"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.SourceUri).To(Equal(uriFor(0)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Each Update lands in etcd; the controller's watch may
		// coalesce intermediate versions, but the LAST must drive
		// the final reconcile.
		for i := 1; i < urisInChurn; i++ {
			Eventually(func() error {
				latest := &v1beta1.ClusterBaseModel{}
				if err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-churn"}, latest); err != nil {
					return err
				}
				newURI := uriFor(i)
				latest.Spec.Storage.StorageUri = &newURI
				return k8sClient.Update(suiteCtx, latest)
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		// Status.Cache.SourceUri tracks Spec via applyInitialShardedModelStatus
		// on every reconcile; convergence here proves the controller
		// observed every URI change at least up to the final value.
		// Ready=True proves the post-churn reconcile sequence
		// (reset → CheckSource → InspectSource → StartPrewarm →
		// GetPrewarmStatus(Succeeded) → metadata-parse) all completed
		// without getting stuck on an intermediate URI version.
		finalURI := uriFor(urisInChurn - 1)
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-churn"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.SourceUri).To(Equal(finalURI),
				"Status.Cache.SourceUri must converge to the final spec URI after churn (drift = controller missed an update)")

			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"),
				"CBM must reach Ready=True after churn settles — no stuck reconcile from intermediate URI version")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
