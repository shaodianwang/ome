package alfred_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred config hot reload", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-reload-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("rejects an invalid document and keeps the last-known-good", func() {
		successesBefore := reloadCountEventually("success")
		failuresBefore := reloadCountEventually("failure")

		// Well-formed YAML with only known keys, so the strict parse accepts
		// it and the rejection comes from schema validation (the mode enum).
		ginkgo.By("the rejection is counted and recorded against the config")
		writeInvalidAlfredConfig("schemaVersion: 1\nmode: bogus\n")
		Eventually(func(g Gomega) {
			g.Expect(reloadFailures(g)).To(BeNumerically(">", failuresBefore))
			g.Expect(eventsFor(g, alfredNamespace, alfredConfigMapName, "PolicyReloadFailed")).NotTo(BeEmpty())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(reloadCountEventually("success")).To(Equal(successesBefore), "a rejected document never counts as a reload")

		ginkgo.By("decision cycles keep running on the last-known-good config")
		cycleBefore := recommendations(Default).Timestamp
		Eventually(func(g Gomega) {
			cycle := recommendations(g)
			g.Expect(cycle.Timestamp).To(BeTemporally(">", cycleBefore), "no decision cycle since the rejection")
			g.Expect(cycle.Mode).To(Equal("execute"))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("applies a valid change: disabling node health stops evacuation recommendations", func() {
		src := workers[0]
		// Registered first so its restore runs last: the policy comes back
		// only after the node condition is cleared and the fixture is gone.
		patchAlfredConfig(setConfig("policies.nodeHealth.enabled", false))
		fix := newAlfredFixture(ns.Name, withName("reload-off"))
		readyOn(fix, src, 1)
		markUnhealthy(src)

		// With the policy off alfred neither signals for repair nor produces
		// a candidate, so nothing reaches the arbiter or the dispatcher.
		ginkgo.By("the unhealthy node yields no recommendation, signal or dispatch")
		Consistently(func(g Gomega) {
			g.Expect(recsFor(recommendations(g), workloadID(fix))).To(BeEmpty())
			g.Expect(nodeEvents(g, src, "NodeRepairNeeded")).To(BeEmpty())
			g.Expect(assertNoDispatch(g, fix, src)).To(HaveLen(1), "%s pod left %s", workloadID(fix), src)
		}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(Succeed())
	})
})
