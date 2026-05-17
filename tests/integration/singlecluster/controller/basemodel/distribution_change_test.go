package basemodel

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelagent"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Distribution change at runtime. The reconciler dispatches on
// Distribution at the top of every reconcile (controller.go:100):
//
//	if isShardedDistribution(...) { sharded path } else if pvc { ... } else { per-node }
//
// Operators occasionally need to flip Distribution after the fact —
// for example, migrating a model from per-node to a newly-deployed
// sharded cache. These specs verify the controller actually swaps
// branches on the next reconcile and surfaces a coherent status,
// rather than getting stuck driving the old branch's conditions.
var _ = ginkgo.Describe("BaseModel Distribution change at runtime", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("PerNode → Sharded: controller switches branches and Sharded Ready appears", func() {
		// Start as PerNode (the default — no Distribution set), drive
		// it to a per-node Ready state via a fake agent CM, then
		// flip Distribution=Sharded. The next reconcile should run
		// the sharded path, fire CheckSource/InspectSource, and
		// flip the Ready condition via metadata parse.
		nodeName := "dist-pn2sharded-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		// CBM (cluster-scoped to keep the helper paths simple).
		cbm := v1beta1testing.MakeClusterBaseModel("dist-pn2sharded").
			StorageURI("oci://n/test/b/models/o/llama-pn2sharded").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("agent reports Ready in PerNode mode → state=Ready, NodesReady populated")
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "dist-pn2sharded", "", true,
			modelagent.ModelStatusReady, nil)).To(Succeed())
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "dist-pn2sharded"}, got)).To(Succeed())
			g.Expect(got.Status.NodesReady).To(ConsistOf(nodeName))
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flip Distribution=Sharded → controller must take the sharded branch on next reconcile")
		got := &v1beta1.ClusterBaseModel{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "dist-pn2sharded"}, got)).To(Succeed())
		dist := v1beta1.DistributionSharded
		got.Spec.Distribution = &dist
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		// Sharded path drives Ready=True via inline metadata parse.
		// NodesReady belongs to the per-node path; the controller
		// does NOT proactively clear stale per-node fields when
		// switching branches, so we deliberately don't assert
		// NodesReady is empty here. We only assert the new branch's
		// Ready signal appeared.
		Eventually(func(g Gomega) {
			latest := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "dist-pn2sharded"}, latest)).To(Succeed())
			ready := meta.FindStatusCondition(latest.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Sharded → PerNode: controller switches branches once distribution flips", func() {
		// Mirror of the above. Start Sharded (gets to Ready via
		// metadata parse), then flip back to PerNode. The
		// reconciler should stop sharded reconciles and start
		// consuming per-node CMs.
		nodeName := "dist-sharded2pn-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		cbm := v1beta1testing.MakeClusterBaseModel("dist-sharded2pn").
			StorageURI("oci://n/test/b/models/o/llama-sharded2pn").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("wait for sharded path to write Status.Cache (Backend + SourceUri populated)")
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "dist-sharded2pn"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.Backend).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flip Distribution back to PerNode by clearing the field")
		got := &v1beta1.ClusterBaseModel{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "dist-sharded2pn"}, got)).To(Succeed())
		got.Spec.Distribution = nil
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		ginkgo.By("agent reports Ready on the per-node path — proves controller is now driving per-node, not sharded")
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "dist-sharded2pn", "", true,
			modelagent.ModelStatusReady, nil)).To(Succeed())
		Eventually(func(g Gomega) {
			latest := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "dist-sharded2pn"}, latest)).To(Succeed())
			g.Expect(latest.Status.NodesReady).To(ConsistOf(nodeName),
				"per-node branch must populate NodesReady after the Distribution flip")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// silence unused-import on context; kept available for any spec that
// later needs to construct overrides for the fake's funcs without
// pulling context as an unused import in the file's existing block.
var _ = context.Background
