package alfred_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred observes the cluster", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-observe-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("runs three replicas with exactly one leader", func() {
		Eventually(func(g Gomega) {
			pods := alfredPods(g)
			g.Expect(pods).To(HaveLen(3))
			leaders := 0
			for _, p := range pods {
				if v, ok := scrapePod(g, p.Name).value("alfred_leader_status", nil); ok && v == 1 {
					leaders++
				}
			}
			g.Expect(leaders).To(Equal(1), "exactly one alfred replica may lead")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("publishes the faked GPU inventory and pending pressure", func() {
		ginkgo.By("total capacity per worker")
		Eventually(func(g Gomega) {
			m := scrapeLeader(g)
			for _, w := range workers {
				v, ok := m.value("alfred_gpu_capacity", map[string]string{"node": w, "status": "total"})
				g.Expect(ok).To(BeTrue(), "no total gauge for %s", w)
				g.Expect(v).To(Equal(float64(gpusPerWorker)))
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("allocation follows a running GPU pod")
		createFiller(Default, ns.Name, "observe-filler", workers[1], 2)
		Eventually(func(g Gomega) {
			v, _ := scrapeLeader(g).value("alfred_gpu_capacity", map[string]string{"node": workers[1], "status": "allocated"})
			g.Expect(v).To(BeNumerically(">=", 2))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("an unschedulable GPU pod raises pending pressure")
		pendingGPUPod(Default, ns.Name, "observe-pending", gpusPerWorker+1)
		Eventually(func(g Gomega) {
			v, _ := scrapeLeader(g).value("alfred_pending_pod_count", nil)
			g.Expect(v).To(BeNumerically(">=", 1))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("exposes the metric families the design documents", func() {
		Eventually(func(g Gomega) {
			m := scrapeLeader(g)
			for _, name := range []string{
				"alfred_gpu_capacity", "alfred_pending_pod_count", "alfred_leader_status",
				"alfred_cluster_fragmentation_score", "alfred_observation_loop_duration_seconds",
			} {
				g.Expect(m).To(HaveKey(name), "metric family %s", name)
			}
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
