package omenative_upgrade_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Pins the cross-version half of the operator-upgrade identity contract
// that manager_restart_identity_kind_test.go (in the in-process coordination
// suite) cannot: a manager built from DIFFERENT code — OME_UPGRADE_FROM_IMAGE
// vs. the current commit — must not disturb a single live pod, node
// assignment, or revision record for workloads whose spec never changed.
// This is exactly the incident class where revision-hash inputs drift
// across manager versions and roll an entire fleet on an otherwise no-op
// upgrade.
//
// Coverage is two-shaped, matching the in-process sibling spec: a
// single-pod engine Instance and a multi-pod Leader+Worker gang Instance.
var _ = ginkgo.Describe("OMENative operator-upgrade identity stability (deployed KIND)", func() {
	const (
		singleName = "upgrade-single"
		gangName   = "upgrade-gang"
	)

	ginkgo.BeforeEach(func() {
		deleteISVC(singleName)
		deleteISVC(gangName)
		ginkgo.DeferCleanup(deleteISVC, singleName)
		ginkgo.DeferCleanup(deleteISVC, gangName)
	})

	ginkgo.It("holds pod identity, node assignment, and revision lineage steady across a real manager Pod replacement", func() {
		Expect(k8sClient.Create(suiteCtx, singlePodISVC(upgradeNamespace, singleName))).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, gangISVC(upgradeNamespace, gangName))).To(Succeed())

		ginkgo.By("waiting for both fixtures to become Ready under the PREVIOUS manager version")
		Eventually(func(g Gomega) {
			single := freshIR(g, singleName, v1beta1.EngineComponent).Status.InstanceStatuses
			g.Expect(single).To(HaveLen(1))
			g.Expect(single[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(snapshotPods(g, singleName)).To(HaveLen(1))

			gang := freshIR(g, gangName, v1beta1.EngineComponent).Status.InstanceStatuses
			g.Expect(gang).To(HaveLen(1))
			g.Expect(gang[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(snapshotPods(g, gangName)).To(HaveLen(2), "the leader+worker gang must materialize two pods")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("recording pre-upgrade pod placement, revision lineage, ControllerRevisions, and manager Pod identity")
		var (
			singlePodsBefore, gangPodsBefore map[string]podPlacement
			singleRevBefore, gangRevBefore   revisionPair
			crsBefore                        map[string]types.UID
			managerBefore                    map[string]types.UID
		)
		Eventually(func(g Gomega) {
			singlePodsBefore = snapshotPods(g, singleName)
			g.Expect(singlePodsBefore).To(HaveLen(1))
			gangPodsBefore = snapshotPods(g, gangName)
			g.Expect(gangPodsBefore).To(HaveLen(2))

			singleIR := freshIR(g, singleName, v1beta1.EngineComponent)
			singleRevBefore = revisionOf(singleIR)
			g.Expect(singleRevBefore.current).NotTo(BeEmpty())
			g.Expect(singleRevBefore.current).To(Equal(singleRevBefore.update),
				"the single-pod engine must be settled, not mid-rollout, before the upgrade")

			gangIR := freshIR(g, gangName, v1beta1.EngineComponent)
			gangRevBefore = revisionOf(gangIR)
			g.Expect(gangRevBefore.current).NotTo(BeEmpty())
			g.Expect(gangRevBefore.current).To(Equal(gangRevBefore.update),
				"the gang engine must be settled, not mid-rollout, before the upgrade")

			crsBefore = mergeCRs(snapshotControllerRevisions(g, singleName), snapshotControllerRevisions(g, gangName))
			g.Expect(len(crsBefore)).To(BeNumerically(">=", 2),
				"each ISVC must have already minted its own ControllerRevision")

			managerBefore = snapshotManagerPods(g)
			g.Expect(managerBefore).NotTo(BeEmpty(), "the deployed manager must have at least one live Pod")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("upgrading the operator: re-running deploy-ome-kind.sh to land the current commit's image")
		Expect(upgradeToCurrentBuild()).To(Succeed())

		ginkgo.By("asserting the manager Pod was actually replaced")
		Eventually(func(g Gomega) {
			managerAfter := snapshotManagerPods(g)
			g.Expect(managerAfter).NotTo(BeEmpty())
			g.Expect(managerAfter).NotTo(Equal(managerBefore),
				"the post-upgrade manager Pod set must differ from the pre-upgrade one; "+
					"an identical set would mean the upgrade never happened and this run would be a no-op")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting zero churn on the pre-existing workloads across a sustained post-upgrade window")
		Consistently(func(g Gomega) {
			g.Expect(snapshotPods(g, singleName)).To(Equal(singlePodsBefore),
				"the single-pod engine's pod identity and placement must stay byte-identical after the upgrade")
			g.Expect(snapshotPods(g, gangName)).To(Equal(gangPodsBefore),
				"the gang's leader+worker pod identity and placement must stay byte-identical after the upgrade")

			singleIR := freshIR(g, singleName, v1beta1.EngineComponent)
			g.Expect(revisionOf(singleIR)).To(Equal(singleRevBefore),
				"the single-pod engine's revision lineage must not move on an upgrade with no spec change")
			g.Expect(singleIR.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(singleIR.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			gangIR := freshIR(g, gangName, v1beta1.EngineComponent)
			g.Expect(revisionOf(gangIR)).To(Equal(gangRevBefore),
				"the gang engine's revision lineage must not move on an upgrade with no spec change")
			g.Expect(gangIR.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(gangIR.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			g.Expect(mergeCRs(snapshotControllerRevisions(g, singleName), snapshotControllerRevisions(g, gangName))).To(Equal(crsBefore),
				"the upgrade must mint zero new ControllerRevisions and must not delete/recreate an existing one")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
