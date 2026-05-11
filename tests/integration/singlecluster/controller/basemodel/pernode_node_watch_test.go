package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/modelagent"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// Per-node Node-watch auto-cleanup. handleNodeDeletion in
// controller.go is the BaseModel reconciler's Node-deletion event
// handler: when a Node disappears, the matching per-node ConfigMap
// in the OME namespace becomes orphaned and the handler must garbage-
// collect it. The companion behavior (entry being pruned from
// Status.NodesReady) is covered by pernode_basics_test.go; this file
// covers the COMPLEMENTARY behavior — the ConfigMap itself going
// away — plus the safety check that prevents the handler from
// deleting unrelated ConfigMaps that happen to share a name with a
// deleted Node.
var _ = ginkgo.Describe("BaseModel per-node Node-deletion auto-cleanup", func() {

	ginkgo.It("Node deletion → labeled per-node ConfigMap in OME namespace is auto-deleted", func() {
		// Pre-create a Node + a per-node CM the way a real
		// model-agent would. handleNodeDeletion fires on the Node
		// deletion event, identifies the orphaned CM by name match
		// and label, and deletes it.
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "node-watch-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})
		nodeName := "node-watch-cleanup"
		makeNode(suiteCtx, k8sClient, nodeName)

		// Create a BaseModel that consumes this CM. The reconciler
		// then HAS to load the CM into its informer cache to compute
		// NodesReady. Waiting for NodesReady to populate is our
		// proof-of-cache-sync — without this gate the subsequent
		// Node Delete event can race the CM informer's initial
		// sync, and handleNodeDeletion's cached Get returns
		// NotFound before getting a chance to GC the CM.
		bm := v1beta1testing.MakeBaseModel("node-watch-bm", ns.Name).
			StorageURI("oci://n/test/b/models/o/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())
		Expect(writeNodeAgentEntry(suiteCtx, k8sClient,
			nodeName, "node-watch-bm", ns.Name, false,
			modelagent.ModelStatusReady, nil)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "node-watch-bm", Namespace: ns.Name}, got)).To(Succeed())
			g.Expect(got.Status.NodesReady).To(ConsistOf(nodeName),
				"controller must have observed the per-node CM before we delete the Node")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Delete the Node — handleNodeDeletion must garbage-collect the orphaned ConfigMap")
		Expect(k8sClient.Delete(suiteCtx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})).To(Succeed())

		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: nodeName, Namespace: constants.OMENamespace}, &corev1.ConfigMap{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"per-node ConfigMap must be deleted by handleNodeDeletion")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Node deletion does NOT touch a same-named ConfigMap that lacks the model-status label (safety guard)", func() {
		// isModelStatusConfigMap (controller.go) gates the deletion
		// on the ModelStatusConfigMapLabel. Without this guard the
		// reconciler would happily delete any ConfigMap in the OME
		// namespace whose name happens to collide with a deleted
		// node — operator-managed configmaps, helm chart artifacts,
		// etc. Spec injects an unrelated CM and verifies the handler
		// leaves it alone.
		nodeName := "node-watch-safety"
		makeNode(suiteCtx, k8sClient, nodeName)

		// Create an UNLABELED CM with the same name as the node.
		// handleNodeDeletion's predicate must skip this.
		unrelated := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nodeName,
				Namespace: constants.OMENamespace,
				// No ModelStatusConfigMapLabel — pretend this is some
				// operator's own configmap that happens to name itself
				// after a node.
			},
			Data: map[string]string{"unrelated-key": "unrelated-value"},
		}
		Expect(k8sClient.Create(suiteCtx, unrelated)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, unrelated) })

		Expect(k8sClient.Delete(suiteCtx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})).To(Succeed())

		// Use Consistently — the handler must NEVER delete this CM,
		// not just "delete it slowly". A passing point-check would
		// be vacuous since the CM wasn't going to be deleted at t=0
		// either.
		Consistently(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: nodeName, Namespace: constants.OMENamespace}, cm)
			g.Expect(err).NotTo(HaveOccurred(),
				"unlabeled CM with the same name as a deleted Node must be left alone")
			g.Expect(cm.Data).To(HaveKey("unrelated-key"))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Node deletion when no matching ConfigMap exists is a silent no-op", func() {
		// Defensive coverage — Node deletion must not error or
		// requeue forever if there's no CM to clean up. Most nodes
		// will be in this state if model-agent never ran on them.
		nodeName := "node-watch-no-cm"
		makeNode(suiteCtx, k8sClient, nodeName)

		Expect(k8sClient.Delete(suiteCtx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})).To(Succeed())

		// Just confirm the node went away cleanly (the handler
		// returning an error or panicking would surface as the
		// reconciler logs filling with retries).
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: nodeName}, &corev1.Node{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
