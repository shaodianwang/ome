package omenative_coordination_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A replacement gang that loses a member after it took over serving is a
// failed attempt: today's rule, pinned.
//
// The same two-gang SurgeThenDrain roll as gang_surge_drain_member_lost,
// but the replacement's leader is removed from the API at once (a
// grace-0 delete, the shape of an eviction or a force delete), so OME
// never sees the member in a terminal phase and finds its name free. The
// engine reads that as the replacement losing a member past the hand-over:
// it closes the attempt, counts it once on the new revision's retry ladder
// with the reason that names the loss, records the loss on the old gang's
// row and warns. The ladder then paces the next attempt, which rebuilds
// the member under a fresh deadline, and the roll converges with both
// gangs whole on the new revision.
//
// The charge is the engine's documented reading, distinct from the retired
// source's; whether a member a hand removed should cost the revision an
// attempt is a product decision this spec records as it stands.

// Cells: instance/Update/SurgeDrain/pod.deleted[target]/boundary@T-surgedrain-gang-replacement-lost, instance/Update/SurgeDrain/pod.deleted[target]/after@T-surgedrain-gang-replacement-lost
var _ = ginkgo.Describe("behavior - a replacement gang that loses a member after the hand-over is a failed attempt (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-lost-rule-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("charges the new revision's ladder once with the loss as its reason, then rebuilds and converges", func() {
		const (
			isvcName   = "gang-lost-rule"
			replicas   = 2
			workerSize = 1
			gangSize   = workerSize + 1
		)
		configureFastUpdateRetry()
		fix := gangSurgeDrainFixture(ns.Name, isvcName, replicas, workerSize)
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		runningRev, runningHash := awaitGangsServing(ns.Name, isvcName, replicas, gangSize)
		holdRunningPods(ns.Name, isvcName)
		newRev, newHash := pushGangRevision(ns.Name, isvcName, runningRev)
		sourceIdx, surgeIdx, sourcePods := awaitFirstGangDraining(ns.Name, isvcName, runningHash, newHash, gangSize)

		ginkgo.By(fmt.Sprintf("removing the replacement's leader (instance %d) from the API at once while the old gang drains", surgeIdx))
		var killedName string
		Eventually(func(g Gomega) {
			leader := gangLeaderPod(g, ns.Name, isvcName, surgeIdx, newHash)
			g.Expect(k8sClient.Delete(suiteCtx, leader, client.GracePeriodSeconds(0))).To(Succeed())
			killedName = leader.Name
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// The documented reading of the loss, as the ladder and the row record
		// it: one attempt counted against the new revision with the loss as
		// its reason, the old gang's row carrying the loss, and the warning.
		lossReason := fmt.Sprintf("replacement gang (surge-index=%d) lost 1 of %d member(s) after it took over serving", surgeIdx, gangSize)
		ginkgo.By("the engine counts the loss once on the new revision's ladder with its reason")
		Eventually(func(g Gomega) {
			block := findRetryBlock(retryBlocks(g, ns.Name, isvcName, v1beta1.EngineComponent), newRev)
			g.Expect(block).NotTo(BeNil(), "a RetryBlock must be recorded for the new revision %s", newRev)
			g.Expect(block.Reason).To(Equal(lossReason), "the block must name the loss")
			g.Expect(block.AttemptsStarted).To(Equal(int32(1)), "the loss counts as one attempt")
			g.Expect(block.State).To(BeElementOf(v1beta1.RetryBlockBackoff, v1beta1.RetryBlockRetryInProgress),
				"the ladder paces the next attempt after the loss")
			g.Expect(eventsNaming(g, ns.Name, workloadtypes.EventReasonInstanceFailed, lossReason)).NotTo(BeEmpty(),
				"the loss must be warned with its reason")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			recorded := false
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				if row.Index == sourceIdx && row.LastFailure != nil && row.LastFailure.Reason == ops.ReplacementLostReason {
					recorded = true
				}
			}
			g.Expect(recorded).To(BeTrue(), "the old gang's row must record the loss as %s", ops.ReplacementLostReason)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Expect(killedName).NotTo(BeEmpty())

		ginkgo.By("releasing the old gang's pods; the ladder's next attempt rebuilds the member and the roll converges")
		for _, name := range sourcePods {
			setDrainHold(name, ns.Name, false)
		}
		awaitGangsConvergedOn(ns.Name, isvcName, newRev, newHash, runningHash, replicas, gangSize)
		Consistently(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.CurrentRevision).To(Equal(newRev))
			g.Expect(ir.Status.RolloutHold).To(BeNil())
			g.Expect(decodedInstanceStatuses(g, ir)).To(HaveLen(replicas))
		}, kindSteadyStateWindow, kindPollInterval).Should(Succeed())
	})
})
