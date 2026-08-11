package omenative_coordination_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Corrective-edit recovery specs for a SHARED
// ServingRuntime after a failed rollout (the runtime edit is the
// rollout lever end to end — the ISVC omits the image and autoSync
// re-renders from the live runtime):
//
//   - roll-forward: bad runtime edit → Held on all attempts → second
//     runtime edit to a NEW fixed image → rollout converges, dead surge
//     wreckage cleaned;
//   - roll-back: bad runtime edit → Held → runtime REVERTED to the
//     exact original image (the target returns to the revision the
//     instance already runs) → wreckage cleaned and the instance leaves
//     Failed even though "no update" is needed;
//   - gang roll-back at the dirty timing: revert
//     lands while the source is Failed-with-Operation and the crashed
//     replacement gang is still live.
//
// Each direction also asserts the coordination group Phase exits Failed
// — the terminal-state exit.
//
//	GINKGO_FOCUS='runtime corrective' make test-kind

// engineOnlyBlueGreenGroup gives the single-Component fixtures an
// explicit coordination group so ISVC.Status.RolloutCoordination is
// populated and the Failed-phase exit is assertable.
func engineOnlyBlueGreenGroup() FixtureOption {
	return WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	})
}

// updateEngineRunnerImage edits the shared runtime's engine runner
// image — the single-pod shape (spec.engineConfig.runner).
func updateEngineRunnerImage(g Gomega, csrName, image string) {
	csr := &v1beta1.ClusterServingRuntime{}
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: csrName}, csr)).To(Succeed())
	csr.Spec.EngineConfig.Runner.Container.Image = image
	g.Expect(k8sClient.Update(suiteCtx, csr)).To(Succeed())
}

// updateEngineGangRunnerImage edits the shared runtime's leader AND
// worker runner images — the gang shape (spec.engineConfig.leader/worker).
func updateEngineGangRunnerImage(g Gomega, csrName, image string) {
	csr := &v1beta1.ClusterServingRuntime{}
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: csrName}, csr)).To(Succeed())
	csr.Spec.EngineConfig.Leader.Runner.Container.Image = image
	csr.Spec.EngineConfig.Worker.Runner.Container.Image = image
	g.Expect(k8sClient.Update(suiteCtx, csr)).To(Succeed())
}

// enginePodsWithImageIn returns the non-deleting engine pods whose ome
// container runs image.
func enginePodsWithImageIn(g Gomega, namespace, isvcName, image string) []*corev1.Pod {
	snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
	g.Expect(err).NotTo(HaveOccurred())
	var matched []*corev1.Pod
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if pod.DeletionTimestamp != nil {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if container.Image == image {
				matched = append(matched, pod)
				break
			}
		}
	}
	return matched
}

// expectGroupPhaseIdle asserts the coordination group Phase left Failed
// and settled at Idle — the terminal-state-exit acceptance.
func expectGroupPhaseIdle(namespace, isvcName string) {
	EventuallyWithOffset(1, func(g Gomega) {
		c := freshISVC(g, namespace, isvcName)
		g.Expect(c.Status.RolloutCoordination).NotTo(BeNil())
		g.Expect(c.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
		g.Expect(c.Status.RolloutCoordination.Groups[0].Phase).To(Equal(v1beta1.CoordinationPhaseIdle),
			"the coordination group must exit Failed once the corrective edit reconciles")
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

var _ = ginkgo.Describe("behavior - runtime corrective roll-forward recovery", ginkgo.Ordered, func() {
	const (
		name       = "rt-corr-fwd"
		goodImage  = "fake-serving:v1"
		badImage   = "ghcr.io/ome-projects/this-image-does-not-exist:rt-corr-fwd-bad"
		fixedImage = "fake-serving:v2"
	)

	var (
		ns     *corev1.Namespace
		csrN   string
		v1Rev  string
		badRev string
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rt-corr-fwd-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})

		configureFastDisposition()

		// Shared-runtime shape: the RUNTIME supplies the engine
		// container image; the ISVC omits it, so a runtime edit is the
		// rollout lever end to end (autoSync default true).
		fix := newCoordFixture(ns.Name, WithISVCName(name), WithoutDecoder(), engineOnlyBlueGreenGroup())
		csrN = fix.CSR.Name
		fix.CSR.Spec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "ome-container",
				Image: goodImage,
			}},
		}
		fix.ISVC.Spec.Engine.PodSpec.Containers[0].Image = ""
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
	})

	ginkgo.It("converges Ready on the runtime-supplied image", func() {
		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cur.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(enginePodsWithImageIn(g, ns.Name, name, goodImage)).To(HaveLen(1),
				"the engine pod must run the runtime-supplied image")
			v1Rev = cur.Status.CurrentRevision
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("holds the bad runtime revision after exhausting all attempts", func() {
		Eventually(func(g Gomega) {
			updateEngineRunnerImage(g, csrN, badImage)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(cur.Status.UpdateRevision).NotTo(Equal(v1Rev),
				"the runtime edit must cut a new target revision (autoSync)")
			badRev = cur.Status.UpdateRevision
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			b := findRetryBlock(retryBlocks(g, ns.Name, name, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil())
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("recovers with zero manual intervention when the runtime is fixed forward", func() {
		Eventually(func(g Gomega) {
			updateEngineRunnerImage(g, csrN, fixedImage)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Full convergence on the corrective revision: Instance Ready on
		// it, no leftover bad-revision pods anywhere (the dead surge
		// wreckage is cleaned, not just bypassed).
		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.UpdateRevision).NotTo(Equal(badRev))
			g.Expect(cur.Status.UpdateRevision).NotTo(Equal(v1Rev))
			g.Expect(cur.Status.CurrentRevision).To(Equal(cur.Status.UpdateRevision),
				"rollout must converge on the corrective revision")
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cur.Status.InstanceStatuses[0].RunningRevision).To(Equal(cur.Status.UpdateRevision))
			g.Expect(enginePodsWithImageIn(g, ns.Name, name, badImage)).To(BeEmpty(),
				"dead surge wreckage must be cleaned up")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		expectGroupPhaseIdle(ns.Name, name)
	})
})

var _ = ginkgo.Describe("behavior - runtime corrective roll-back recovery", ginkgo.Ordered, func() {
	const (
		name      = "rt-corr-back"
		goodImage = "fake-serving:v1"
		badImage  = "ghcr.io/ome-projects/this-image-does-not-exist:rt-corr-back-bad"
	)

	var (
		ns     *corev1.Namespace
		csrN   string
		v1Rev  string
		badRev string
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rt-corr-back-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})

		configureFastDisposition()

		fix := newCoordFixture(ns.Name, WithISVCName(name), WithoutDecoder(), engineOnlyBlueGreenGroup())
		csrN = fix.CSR.Name
		fix.CSR.Spec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "ome-container",
				Image: goodImage,
			}},
		}
		fix.ISVC.Spec.Engine.PodSpec.Containers[0].Image = ""
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
	})

	ginkgo.It("converges Ready then holds the bad runtime revision", func() {
		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cur.Status.CurrentRevision).NotTo(BeEmpty())
			v1Rev = cur.Status.CurrentRevision
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			updateEngineRunnerImage(g, csrN, badImage)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.UpdateRevision).NotTo(BeEmpty())
			g.Expect(cur.Status.UpdateRevision).NotTo(Equal(v1Rev))
			badRev = cur.Status.UpdateRevision
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			b := findRetryBlock(retryBlocks(g, ns.Name, name, v1beta1.EngineComponent), badRev)
			g.Expect(b).NotTo(BeNil())
			g.Expect(b.State).To(Equal(v1beta1.RetryBlockHeld))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("recovers with zero manual intervention when the runtime is reverted", func() {
		// The undo path from the settled Held state: the runtime returns
		// to the EXACT original image, so the corrective target is the
		// revision the instance already runs. Stock Deployments give
		// this for free; the single-pod wreckage (the dead pod parked
		// at the surge ordinal + Phase=Failed) must be cleaned even
		// though "no update" is needed.
		Eventually(func(g Gomega) {
			updateEngineRunnerImage(g, csrN, goodImage)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.CurrentRevision).To(Equal(cur.Status.UpdateRevision),
				"the reverted target must reconcile to a converged rollout")
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"the instance must leave Failed after the revert")
			g.Expect(enginePodsWithImageIn(g, ns.Name, name, badImage)).To(BeEmpty(),
				"the dead surge pod must be deleted even though no update is needed")
			g.Expect(enginePodsWithImageIn(g, ns.Name, name, goodImage)).To(HaveLen(1),
				"the original pod keeps serving through the undo")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		expectGroupPhaseIdle(ns.Name, name)

		// No further churn: the recovered state is stable.
		Consistently(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(15 * time.Second).WithPolling(500 * time.Millisecond).Should(Succeed())
	})
})

var _ = ginkgo.Describe("behavior - runtime corrective gang roll-back recovery", ginkgo.Ordered, func() {
	const (
		name      = "rt-corr-gback"
		goodImage = "fake-serving:v1"
		badImage  = "ghcr.io/ome-projects/this-image-does-not-exist:rt-corr-gang-bad"
	)

	var (
		ns   *corev1.Namespace
		csrN string
	)

	ginkgo.BeforeAll(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rt-corr-gback-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})

		configureFastDisposition()

		// Gang shape: leader + one
		// worker per Instance, images supplied by the shared runtime's
		// engineConfig.leader/worker runners.
		fix := newCoordFixture(ns.Name, WithISVCName(name), WithoutDecoder(),
			WithEngineWorkerSize(1), engineOnlyBlueGreenGroup())
		csrN = fix.CSR.Name
		fix.CSR.Spec.EngineConfig = &v1beta1.EngineSpec{
			Leader: &v1beta1.LeaderSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "ome-container",
				Image: goodImage,
			}}},
			Worker: &v1beta1.WorkerSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "ome-container",
				Image: goodImage,
			}}},
		}
		fix.ISVC.Spec.Engine.PodSpec.Containers[0].Image = ""
		fix.ISVC.Spec.Engine.Leader.PodSpec.Containers[0].Image = ""
		fix.ISVC.Spec.Engine.Worker.PodSpec.Containers[0].Image = ""
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
	})

	ginkgo.It("converges the gang Ready on the runtime-supplied image", func() {
		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1))
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cur.Status.CurrentRevision).NotTo(BeEmpty())
			g.Expect(enginePodsWithImageIn(g, ns.Name, name, goodImage)).To(HaveLen(2),
				"leader + worker must run the runtime-supplied image")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("recovers when the runtime is reverted at the dirty (Failed-with-wreckage) timing", func() {
		Eventually(func(g Gomega) {
			updateEngineGangRunnerImage(g, csrN, badImage)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Revert as soon as the escalated-failure shape appears (an
		// instance Failed while the crashed replacement gang is live) —
		// the prod undo timing. The wall-clock race with the abandon
		// continuation is inherent on a real cluster; the deterministic
		// dirty-timing wedge is pinned at the unit level
		// (TestCorrectiveRecovery_Gang_RollBack_MidWreckage).
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, name, v1beta1.EngineComponent)
			failed := false
			for _, s := range insts {
				if s.Phase == v1beta1.OMENativeInstanceFailed {
					failed = true
				}
			}
			g.Expect(failed).To(BeTrue(), "the failed rollout must escalate an instance to Failed")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			updateEngineGangRunnerImage(g, csrN, goodImage)
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Full recovery: exactly one instance Ready, the dead surge gang
		// deleted, the surge-target marker removed (not leaked as a
		// permanent Failed entry), the original gang serving throughout.
		Eventually(func(g Gomega) {
			cur := freshInferenceReplica(g, ns.Name, name, v1beta1.EngineComponent)
			g.Expect(cur.Status.CurrentRevision).To(Equal(cur.Status.UpdateRevision),
				"the reverted target must reconcile to a converged rollout")
			g.Expect(cur.Status.InstanceStatuses).To(HaveLen(1),
				"the gang surge-target marker must be cleaned up, not leaked as a permanent Failed entry")
			g.Expect(cur.Status.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(enginePodsWithImageIn(g, ns.Name, name, badImage)).To(BeEmpty(),
				"the crashed replacement gang's pods must be deleted")
			g.Expect(enginePodsWithImageIn(g, ns.Name, name, goodImage)).To(HaveLen(2),
				"the original gang keeps serving throughout the undo")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		expectGroupPhaseIdle(ns.Name, name)
	})
})
