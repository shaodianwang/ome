package acceleratorquota_capacity

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = ginkgo.Describe("Derived root capacity", func() {
	ginkgo.BeforeEach(func() {
		resetRoot()
	})

	ginkgo.It("records the fleet total on the reserved root", func() {
		mustCreateNode(uniqueName("node"), "4")
		mustCreateNode(uniqueName("node"), "4")

		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"8", "8"}))
	})

	// The watch is the point. With the resync set to ten minutes, a spec that
	// observes the change within seconds can only have been woken by the Node
	// event — a poll would still be waiting.
	ginkgo.It("notices a new node without waiting for a resync", func() {
		mustCreateNode(uniqueName("node"), "4")
		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"4", "4"}))

		mustCreateNode(uniqueName("node"), "4")
		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"8", "8"}))
	})

	// Draining is the routine operation the mark exists to survive, and the
	// amount drained is not the point: cordoning half a fleet is far past any
	// band, but the hardware has not gone anywhere. Allocatable drops, the mark
	// does not, and budgets stay out of Degraded through the maintenance.
	ginkgo.It("holds the mark however much of the fleet is cordoned", func() {
		mustCreateNode(uniqueName("node"), "4")
		half := mustCreateNode(uniqueName("node"), "4")

		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"8", "8"}))

		cordon(half)

		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"4", "8"}))
	})

	// Hardware physically leaving is what the band judges, and half the fleet is
	// past it: a cluster that genuinely shrank must stop claiming capacity it no
	// longer has.
	ginkgo.It("follows the mark down when a node leaves the fleet", func() {
		mustCreateNode(uniqueName("node"), "4")
		gone := mustCreateNode(uniqueName("node"), "4")

		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"8", "8"}))

		gomega.Expect(k8sClient.Delete(suiteCtx, gone)).To(gomega.Succeed())

		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"4", "4"}))
	})

	// A node no ResourceFlavor claims is not capacity anyone can budget
	// against, so it is left out of the total rather than inflating it.
	ginkgo.It("ignores a node no flavor claims", func() {
		mustCreateNode(uniqueName("node"), "4")
		mustCreateNode(uniqueName("stray"), "100", func(n *corev1.Node) {
			n.Labels = map[string]string{flavorLabel: "some-other-flavor"}
		})

		// Reach the claimed total first, then hold it: Consistently alone would
		// pass against the empty status that exists before the first write.
		gomega.Eventually(rootCapacity(), timeout, interval).
			Should(gomega.Equal([2]string{"4", "4"}))
		gomega.Consistently(rootCapacity(), "3s", interval).
			Should(gomega.Equal([2]string{"4", "4"}))
	})
})

func cordon(n *corev1.Node) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		var live corev1.Node
		if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: n.Name}, &live); err != nil {
			return err
		}
		live.Spec.Unschedulable = true
		return k8sClient.Update(suiteCtx, &live)
	}, timeout, interval).Should(gomega.Succeed())
}
