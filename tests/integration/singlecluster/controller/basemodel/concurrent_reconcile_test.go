package basemodel

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/modelagent"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// Concurrent reconcile stress: creating many BaseModels at once
// must not drop events or leave any of them stuck. The per-node
// reconciler uses the same client + work-queue for every BM in the
// suite, so a regression like a queue starvation, a watch coalescing
// bug, or a per-key serialization mistake would show up as some
// subset of BMs never reaching Ready while their siblings do.
//
// 10 is the intentional N — large enough to expose ordering bugs,
// small enough to run in seconds against envtest. CI runners with
// slow etcd will take longer but should still complete within the
// LongTimeout window.
const concurrentBMCount = 10

var _ = ginkgo.Describe("BaseModel reconciler under concurrent load", func() {

	ginkgo.It("creating N BaseModels + N agent ConfigMap entries simultaneously: all reach Ready", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "concurrent-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})

		// Single shared Node — proves the per-node CM key isolation
		// (one CM, N entries) doesn't break the rollup. If we used
		// N nodes the test would also work but wouldn't exercise the
		// "many keys in one CM" code path that's the realistic shape
		// (one model-agent pod per node, downloading multiple models).
		nodeName := "concurrent-node"
		makeNode(suiteCtx, k8sClient, nodeName)
		ginkgo.DeferCleanup(deleteNode(k8sClient, nodeName))
		ginkgo.DeferCleanup(deleteNodeConfigMap(k8sClient, nodeName))

		ginkgo.By(fmt.Sprintf("creating %d BaseModels in %s", concurrentBMCount, ns.Name))
		names := make([]string, 0, concurrentBMCount)
		for i := 0; i < concurrentBMCount; i++ {
			name := fmt.Sprintf("concurrent-llama-%02d", i)
			names = append(names, name)
			bm := v1beta1testing.MakeBaseModel(name, ns.Name).
				StorageURI(fmt.Sprintf("oci://n/test/b/models/o/llama-%02d", i)).Obj()
			Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		}

		ginkgo.By(fmt.Sprintf("agent reports Ready for all %d BMs (sequential writes; controller queues + reconciles)", concurrentBMCount))
		for _, name := range names {
			Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
				nodeName, name, ns.Name, false,
				modelagent.ModelStatusReady, nil)).To(Succeed())
		}

		ginkgo.By("every BM must reach state=Ready with NodesReady=[node] — no event dropped")
		Eventually(func(g Gomega) {
			for _, name := range names {
				got := &v1beta1.BaseModel{}
				g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: name, Namespace: ns.Name}, got)).To(Succeed())
				g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady),
					"BM %q must reach Ready", name)
				g.Expect(got.Status.NodesReady).To(ConsistOf(nodeName),
					"BM %q must have the shared node in NodesReady", name)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
