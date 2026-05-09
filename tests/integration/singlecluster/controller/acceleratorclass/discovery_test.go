package acceleratorclass

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// AcceleratorClass discovery: nodes whose labels match the AC's
// Discovery.NodeSelector are listed in Status.Nodes; the count is
// reflected in Status.AvailableNodes. Reconciles fire on Node
// create/update/delete (not just AC change), so labeling a Node after
// the AC exists must trigger a re-list.
var _ = ginkgo.Describe("AcceleratorClass Discovery", ginkgo.Ordered, func() {

	ginkgo.It("populates Status.Nodes when a node matches the Discovery.NodeSelector", func() {
		ginkgo.By("creating an AcceleratorClass with a single-key NodeSelector")
		ac := v1beta1testing.MakeAcceleratorClass("ac-discovery-1").
			Vendor("nvidia").
			DiscoveryNodeSelector("nvidia.com/gpu.present", "true").Obj()
		Expect(k8sClient.Create(suiteCtx, ac)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, ac) })

		ginkgo.By("creating two nodes — one matches the selector, one doesn't")
		matched := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "ac-discovery-node-match",
				Labels: map[string]string{"nvidia.com/gpu.present": "true"},
			},
		}
		Expect(k8sClient.Create(suiteCtx, matched)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, matched) })

		other := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "ac-discovery-node-other",
				Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""},
			},
		}
		Expect(k8sClient.Create(suiteCtx, other)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, other) })

		ginkgo.By("expecting Status.Nodes to contain only the matching node")
		Eventually(func(g Gomega) {
			got := &v1beta1.AcceleratorClass{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "ac-discovery-1"}, got)).To(Succeed())
			g.Expect(got.Status.Nodes).To(ConsistOf("ac-discovery-node-match"))
			g.Expect(got.Status.AvailableNodes).To(BeNumerically("==", 1))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("re-list happens when a node is labeled AFTER the AC exists", func() {
		ginkgo.By("creating an AC with no matching nodes yet")
		ac := v1beta1testing.MakeAcceleratorClass("ac-discovery-2").
			Vendor("nvidia").
			DiscoveryNodeSelector("ac-discovery-2/gpu", "true").Obj()
		Expect(k8sClient.Create(suiteCtx, ac)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, ac) })

		ginkgo.By("waiting for the initial reconcile to converge on AvailableNodes=0")
		Eventually(func(g Gomega) {
			got := &v1beta1.AcceleratorClass{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "ac-discovery-2"}, got)).To(Succeed())
			g.Expect(got.Status.AvailableNodes).To(BeNumerically("==", 0))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("now creating a node with the matching label — Node-watch must re-enqueue the AC")
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "ac-discovery-2-node",
				Labels: map[string]string{"ac-discovery-2/gpu": "true"},
			},
		}
		Expect(k8sClient.Create(suiteCtx, node)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, node) })

		Eventually(func(g Gomega) {
			got := &v1beta1.AcceleratorClass{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "ac-discovery-2"}, got)).To(Succeed())
			g.Expect(got.Status.Nodes).To(ConsistOf("ac-discovery-2-node"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
