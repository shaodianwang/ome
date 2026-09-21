package irmanaged_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// bad-image failure on the IR-managed path: a bumped engine image
// to a non-existent tag must escalate the per-Instance Phase to
// Failed within the stuck-pod grace window (default 60s + kubelet
// pull-retry headroom) so the parent ISVC's coordination layer can
// see Component.Failed and flip the group's Phase to Failed.
//
// The coordination-suite bad-image scenario is covered by
// omenative_coordination_kind/bad_image_kind_test.go. This spec pins
// the IR-controller escalation path — without it, a wedged bad-image
// rollout would sit at Phase=Updating until InstanceReadyTimeout.
//
// Single-replica engine with the default SurgeThenDrain strategy.
// InstanceReadyTimeout is intentionally left to the suite's seeded
// lifecycle.instanceReadyTimeout of 30 minutes (operator-facing
// window; we never want a fast-path escalator that depends on it).
// The expected behavior under bad
// image is:
//
//  1. The engine bump produces a surge pod stuck in ImagePullBackOff
//     (the original v1 pod stays serving — capacity preserved).
//  2. Within ~the config-driven stuck-pod grace (lifecycle
//     stuckPodGracePeriod, 60s in this suite) of the surge pod
//     reaching the terminal kubelet state, the IR controller's
//     aggregator escalates the per-Instance Phase to Failed.
//  3. The IR's Ready condition flips to False/InstanceFailed.
//  4. The parent ISVC observes Component.Failed via the irprojector
//     rollup and the ISVC top-level Ready stays NOT True.
//
// Budget: 3 minutes is comfortable for kubelet's pull-retry
// (~30-60s) + the 60s grace + reconcile + status flush.
var _ = ginkgo.Describe("IR-managed OMENative bad-image failure", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "irm-badimg-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("IRM-BADIMG-1 bad-image bump escalates Instance to Failed (v1 capacity preserved)", func() {
		name := "irm-badimg-isvc"
		fix := newIRFixture(ns.Name, withISVCName(name), withEngineReplicas(1),
			withEngineImage("fake-serving:v1"))
		applyIRFixture(Default, fix)
		deferIRFixtureCleanup(fix)

		// Wait for steady-state Ready before bumping. waitSteadyReady
		// asserts readyReplicas=1 and CurrentRevision==UpdateRevision.
		waitSteadyReady(ns.Name, name)

		ginkgo.By("bumping engine image to a non-existent tag")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, name)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting the IR aggregator to escalate the Instance to Phase=Failed within the stuck-pod grace + kubelet pull-retry budget")
		// Budget = stuckPodGracePeriod (60s) + kubelet pull-retry
		// backoff (~60s worst case) + reconcile/status-flush headroom.
		// 3 minutes is comfortable. Asserts on IR.Status.InstanceStatuses
		// directly: that's the surface the omenative status_aggregate
		// fast-failure escalator writes; the IR aggregator must apply
		// the same contract.
		Eventually(func(g Gomega) {
			ir, ok := engineIR(ns.Name, name)
			g.Expect(ok).To(BeTrue(), "engine IR must exist after the bump")
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].Phase).
				To(Equal(v1beta1.OMENativeInstanceFailed),
					"IR aggregator must escalate Instance to Phase=Failed when surge pod stuck in ImagePullBackOff past grace")
		}).WithTimeout(3 * time.Minute).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("verifying v1 capacity preserved — operator-reported 'no auto-rollback' principle")
		// PodCount>=1 on the InstanceStatus confirms the v1 pod stayed
		// up alongside the (Failed) surge pod — the rollout did not
		// take v1 down to roll a doomed v2.
		Eventually(func(g Gomega) {
			ir, ok := engineIR(ns.Name, name)
			g.Expect(ok).To(BeTrue())
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].PodCount).To(BeNumerically(">=", int32(1)),
				"v1 pod (and likely the wedged surge pod) must still be present — no auto-rollback")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
