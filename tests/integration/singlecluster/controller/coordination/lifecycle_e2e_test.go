package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Lifecycle end-to-end specs for OEP-0007.1.
//
// Every E2E-* requires the full reconciler invoking coordination
// (G1) and most also require peer-discovery env injection (G2) or
// RatioBalanced wiring (G3+G6). These are the eventual acceptance
// suite once the gaps land — the PIts here pin the contracts so
// flipping them to It later is mechanical.

var _ = ginkgo.Describe("Coordination lifecycle end-to-end (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-e2e-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// All G1-blocked PIts
	// ----------------------------------------------------------------

	ginkgo.PIt("G1+G2: E2E-1 2-Component Atomic model bump → per-revision Services + env vars + Traffic + condition + events converge", func() {
		// The most important end-to-end test: a full rollout exercises
		// every piece of the producer/consumer contract.
		fix := newCoordFixture(ns.Name, WithISVCName("e2e1"),
			WithAtomicGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Bump engine image; observe convergence on every signal.
		cur := freshISVC(Default, ns.Name, "e2e1")
		cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "e2e1")
			g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhaseIdle))
			svcs := listPerRevisionServices(g, ns.Name)
			g.Expect(svcs).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("G1+G2: E2E-2 3-Component Atomic (router+engine+decoder) full convergence", func() {})
	ginkgo.PIt("G1: E2E-3 Sequential decoder→engine end-to-end", func() {})
	ginkgo.PIt("G1: E2E-4 PairedMixable 2-Component end-to-end", func() {})
	ginkgo.PIt("G1: E2E-5 Independent (default) preserves baseline behavior", func() {})
	ginkgo.PIt("G1: E2E-6 coordination + ISVC delete → per-revision Services cascade-deleted", func() {})
	ginkgo.PIt("G1+G4: E2E-7 multi-revision in flight (operator double-bumps spec)", func() {})
	ginkgo.PIt("G1: E2E-8 successful then immediate revert (v1 → v2 → v1)", func() {})
	ginkgo.PIt("G1: E2E-9 cancel a rollout via spec revert mid-Surging", func() {})
	ginkgo.PIt("G3+G6: E2E-10 RatioBalanced pacing on 40:20 PD topology", func() {})
})
