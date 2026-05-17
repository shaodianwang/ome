package basemodel

import (
	"context"
	"sync"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Two ClusterBaseModels with the same StorageURI but different names
// must reconcile independently. The cache provider should see two
// separate InspectSource calls (one per CBM), and a delete on one
// must not affect the other's Ready state.
var _ = ginkgo.Describe("Sharded BaseModel: two CBMs with the same StorageURI", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("two CBMs same URI → both reach Ready=True independently", func() {
		// Capture the source URIs InspectSource is called with so we
		// can sanity-check both CBMs drove the provider through the
		// same path. Use a mutex to avoid a data race on the slice.
		var (
			mu      sync.Mutex
			sources []string
		)
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			mu.Lock()
			sources = append(sources, src.URI)
			mu.Unlock()
			return &sharded.SourceInspection{
				Objects: []sharded.Object{{Source: src, Path: src.URI + "/config.json"}},
				Metadata: sharded.ModelMetadataInputs{
					ConfigJSON: []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`),
				},
			}, nil
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

		ginkgo.By("both CBMs reach Ready=True independently")
		for _, name := range []string{"two-cbm-a", "two-cbm-b"} {
			Eventually(func(g Gomega) {
				got := &v1beta1.ClusterBaseModel{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name}, got)).To(Succeed())
				ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).NotTo(BeNil(), "%s: Ready condition must exist", name)
				g.Expect(string(ready.Status)).To(Equal("True"), "%s: Ready must be True", name)
			}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		mu.Lock()
		gotSources := append([]string(nil), sources...)
		mu.Unlock()
		// Both CBMs must have driven InspectSource for the shared URI
		// — sources slice contains at least 2 entries, both pointing
		// at sharedURI (controller may have retried; the SET matters).
		Expect(len(gotSources)).To(BeNumerically(">=", 2),
			"each CBM must drive its own InspectSource call; got %v", gotSources)
		for _, s := range gotSources {
			Expect(s).To(Equal(sharedURI))
		}
	})

	ginkgo.It("deleting one CBM does NOT affect the other's Ready state", func() {
		// Status.Cache is per-CR, so deleting one BM should not touch
		// the other's state. There's no cache-provider-side signal
		// either way (lazy-fetch, no operation handle), so the test
		// asserts ONLY the OME-side invariant: surviving BM's status
		// remains Ready=True.
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

		// Wait for both to flip Ready=True so we have a clean
		// "before" point.
		for _, name := range []string{"two-cbm-keep", "two-cbm-go"} {
			Eventually(func(g Gomega) {
				got := &v1beta1.ClusterBaseModel{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name}, got)).To(Succeed())
				ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).NotTo(BeNil())
				g.Expect(string(ready.Status)).To(Equal("True"))
			}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		Expect(k8sClient.Delete(suiteCtx, cbmGo)).To(Succeed())

		// Surviving BM must STAY Ready=True — Consistently across a
		// short window because a regression would presumably show as
		// the surviving BM's Ready condition flipping or the BM
		// somehow getting GC'd alongside its sibling.
		Consistently(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "two-cbm-keep"}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"),
				"surviving CBM with the same URI must keep its Ready=True state")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
