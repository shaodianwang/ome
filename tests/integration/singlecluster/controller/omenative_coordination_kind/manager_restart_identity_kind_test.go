package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// podPlacement pins one pod's identity and scheduling decision — the two
// facts a controller-manager restart must never change for a live Instance.
type podPlacement struct {
	uid  types.UID
	node string
}

// snapshotPodPlacements maps every live OMENative pod for isvcName by name to
// its podPlacement. A pod recreated under the same name still surfaces as a
// UID mismatch; a pod rescheduled elsewhere surfaces as a node mismatch.
func snapshotPodPlacements(g Gomega, namespace, isvcName string) map[string]podPlacement {
	pods := listOMENativePods(g, namespace, isvcName)
	out := make(map[string]podPlacement, len(pods))
	for i := range pods {
		p := &pods[i]
		g.Expect(p.DeletionTimestamp).To(BeNil(), "pod %s must be alive, not terminating", p.Name)
		g.Expect(p.Spec.NodeName).NotTo(BeEmpty(), "pod %s must already be scheduled", p.Name)
		out[p.Name] = podPlacement{uid: p.UID, node: p.Spec.NodeName}
	}
	return out
}

// revisionPair is one Component's IR revision lineage. Current == Update at
// steady state; a real rollout diverges them before re-converging.
type revisionPair struct {
	current string
	update  string
}

func revisionOf(ir *v1beta1.InferenceReplica) revisionPair {
	return revisionPair{current: ir.Status.CurrentRevision, update: ir.Status.UpdateRevision}
}

// snapshotControllerRevisions maps every OMENative ControllerRevision in
// namespace by name to its UID, so a delete-and-recreate under an identical
// name still surfaces as a mismatch.
func snapshotControllerRevisions(g Gomega, namespace string) map[string]types.UID {
	crs := listOMENativeCRs(g, namespace)
	out := make(map[string]types.UID, len(crs))
	for i := range crs {
		out[crs[i].Name] = crs[i].UID
	}
	return out
}

// Pins a manager-restart identity-stability contract: tearing down the
// running controller-manager instance and starting a fresh one against the
// same cluster must not touch a single live pod, node assignment, or
// revision record for workloads that did not change — no other spec in this
// suite pins that as a dedicated, standalone contract. This is a
// same-binary restart property (cache rebuild, re-adoption, re-hashing on
// resync); it does not exercise a manager built from different code, which
// this suite's in-process manager (see suite_test.go) has no mechanism to
// swap in.
//
// Coverage is deliberately two-shaped: a single-pod engine Instance and a
// multi-pod Leader+Worker gang Instance, since gang membership (PodGroup,
// per-pod ordinals) is exactly the kind of state a naive restart-time
// re-adoption could scramble even when a singleton would survive untouched.
var _ = ginkgo.Describe("OMENative manager-restart identity stability (KIND)", ginkgo.Ordered, func() {
	var ns, postNS *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "restart-identity-")
		Expect(err).NotTo(HaveOccurred())
		// A separate namespace for the post-restart liveness check: it mints
		// its own ControllerRevision on purpose, and listOMENativeCRs is
		// namespace-scoped, so sharing ns would make that expected new
		// revision look like unwanted churn on the pre-existing workloads.
		postNS, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "restart-postcheck-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		if ns != nil {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		}
		if postNS != nil {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, postNS.Name)).To(Succeed())
		}
	})

	ginkgo.It("holds pod identity, node assignment, and revision lineage steady across a manager restart", func() {
		const (
			singleName = "restart-single"
			gangName   = "restart-gang"
		)

		singleFix := newCoordFixture(ns.Name, WithISVCName(singleName), WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, singleFix)
		deferFixtureCleanup(singleFix)

		gangFix := newCoordFixture(ns.Name, WithISVCName(gangName), WithoutDecoder(),
			WithEngineWorkerSize(1), WithEngineImage("fake-serving:v1"))
		applyFixture(Default, gangFix)
		deferFixtureCleanup(gangFix)

		ginkgo.By("waiting for the single-pod engine and the leader+worker gang to become Ready")
		Eventually(func(g Gomega) {
			singleStatuses := instanceStatuses(g, ns.Name, singleName, v1beta1.EngineComponent)
			g.Expect(singleStatuses).To(HaveLen(1))
			g.Expect(singleStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(listOMENativePods(g, ns.Name, singleName)).To(HaveLen(1))

			gangStatuses := instanceStatuses(g, ns.Name, gangName, v1beta1.EngineComponent)
			g.Expect(gangStatuses).To(HaveLen(1))
			g.Expect(gangStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(listOMENativePods(g, ns.Name, gangName)).To(HaveLen(2), "the leader+worker gang must materialize two pods")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("recording pre-restart pod placement, revision lineage, and ControllerRevisions")
		var (
			singlePodsBefore, gangPodsBefore map[string]podPlacement
			singleRevBefore, gangRevBefore   revisionPair
			crsBefore                        map[string]types.UID
		)
		Eventually(func(g Gomega) {
			singlePodsBefore = snapshotPodPlacements(g, ns.Name, singleName)
			g.Expect(singlePodsBefore).To(HaveLen(1))
			gangPodsBefore = snapshotPodPlacements(g, ns.Name, gangName)
			g.Expect(gangPodsBefore).To(HaveLen(2))

			singleIR := freshInferenceReplica(g, ns.Name, singleName, v1beta1.EngineComponent)
			singleRevBefore = revisionOf(singleIR)
			g.Expect(singleRevBefore.current).NotTo(BeEmpty())
			g.Expect(singleRevBefore.current).To(Equal(singleRevBefore.update),
				"the single-pod engine must be settled, not mid-rollout, before the restart")

			gangIR := freshInferenceReplica(g, ns.Name, gangName, v1beta1.EngineComponent)
			gangRevBefore = revisionOf(gangIR)
			g.Expect(gangRevBefore.current).NotTo(BeEmpty())
			g.Expect(gangRevBefore.current).To(Equal(gangRevBefore.update),
				"the gang engine must be settled, not mid-rollout, before the restart")

			crsBefore = snapshotControllerRevisions(g, ns.Name)
			g.Expect(len(crsBefore)).To(BeNumerically(">=", 2),
				"each ISVC must have already minted its own ControllerRevision")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("restarting the controller manager through the suite's own mechanism")
		mgrBefore := kindutil.Suite().Manager
		Expect(mgrBefore).NotTo(BeNil())
		Expect(kindutil.Suite().RestartManager()).To(Succeed())
		mgrAfter := kindutil.Suite().Manager
		Expect(mgrAfter).NotTo(BeNil())
		Expect(mgrAfter == mgrBefore).To(BeFalse(),
			"the post-restart manager must be a new instance; an identical one would mean the restart never happened and this run would be a no-op")

		ginkgo.By("proving the post-restart manager instance actually reconciles, not just that it swapped in")
		const postRestartName = "restart-postcheck"
		postFix := newCoordFixture(postNS.Name, WithISVCName(postRestartName), WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, postFix)
		deferFixtureCleanup(postFix)
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, postNS.Name, postRestartName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(1))
			g.Expect(statuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed(),
			"the post-restart manager must reconcile a brand-new ISVC to Ready")

		ginkgo.By("asserting zero churn on the pre-existing workloads across a sustained post-restart window")
		Consistently(func(g Gomega) {
			g.Expect(snapshotPodPlacements(g, ns.Name, singleName)).To(Equal(singlePodsBefore),
				"the single-pod engine's pod identity and placement must stay byte-identical after the restart")
			g.Expect(snapshotPodPlacements(g, ns.Name, gangName)).To(Equal(gangPodsBefore),
				"the gang's leader+worker pod identity and placement must stay byte-identical after the restart")

			singleIR := freshInferenceReplica(g, ns.Name, singleName, v1beta1.EngineComponent)
			g.Expect(revisionOf(singleIR)).To(Equal(singleRevBefore),
				"the single-pod engine's revision lineage must not move on a restart with no spec change")
			g.Expect(singleIR.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(singleIR.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			gangIR := freshInferenceReplica(g, ns.Name, gangName, v1beta1.EngineComponent)
			g.Expect(revisionOf(gangIR)).To(Equal(gangRevBefore),
				"the gang engine's revision lineage must not move on a restart with no spec change")
			g.Expect(gangIR.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(gangIR.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			g.Expect(snapshotControllerRevisions(g, ns.Name)).To(Equal(crsBefore),
				"the restart must mint zero new ControllerRevisions and must not delete/recreate an existing one")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
