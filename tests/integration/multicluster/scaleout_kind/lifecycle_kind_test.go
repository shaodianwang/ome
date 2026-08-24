package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Single-mode lifecycle against real member clusters: winner election, teardown,
// transient member loss, and spec propagation. Every claim here is one envtest
// cannot make, because it turns on pods a kubelet actually ran.
//
// Not Ordered: each spec builds its own namespaces, model, runtime, and
// WorkloadCluster registrations, so a failure in one must not skip the rest —
// on a suite where a single spec costs minutes, that signal matters.
var _ = ginkgo.Describe("Multi-cluster lifecycle (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	ginkgo.It("elects one winner whose pods go Ready, and deletes the loser's pods", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)

		ginkgo.By("waiting for the race to resolve to a single winner")
		winner := env.awaitWinner()

		ginkgo.By("asserting the winner's engine pod reaches Ready on a real kubelet")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(readyEnginePodsOn(g, winner.c, env.isvcNS, env.isvcName)).
				To(gomega.BeNumerically(">=", 1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"winner %s should have a Ready engine pod", winner.wcName)

		loser := env.other(winner)
		ginkgo.By("asserting the loser's derived is swept AND its pods are actually gone")
		gomega.Eventually(func() bool {
			return derivedSweptOn(loser.c, env.isvcNS, env.isvcName)
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
			"loser %s derived should be swept", loser.wcName)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(enginePodsOn(g, loser.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"loser %s should have no engine pods left", loser.wcName)
	})

	ginkgo.It("holds the placement across a transient member outage instead of re-racing", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)
		winner := env.awaitWinner()

		// Freeze the winner's node so its apiserver stops answering while its
		// WorkloadCluster registration stays intact and inside the connection
		// grace period. The placer must read this as a blip and hold, not as a
		// lost winner — re-racing here would tear down a healthy placement.
		ginkgo.By("pausing the winner's node container")
		pauseNode(winner.kindCluster)
		unpaused := false
		thaw := func() {
			if !unpaused {
				unpauseNode(winner.kindCluster)
				unpaused = true
			}
		}
		ginkgo.DeferCleanup(thaw)

		ginkgo.By("asserting the placement stays on the same winner throughout the outage")
		gomega.Consistently(func() string {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			if ps == nil {
				return ""
			}
			return ps.Cluster
		}).WithTimeout(10*time.Second).WithPolling(time.Second).Should(gomega.Equal(winner.wcName),
			"a paused member must not move the placement")

		ginkgo.By("thawing the node and asserting the winner is unchanged")
		thaw()
		gomega.Eventually(func() bool {
			return workloadClusterReady(winner.wcName)
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
			"winner should return to Ready after the outage")
		gomega.Consistently(func() string {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			if ps == nil {
				return ""
			}
			return ps.Cluster
		}).WithTimeout(5 * time.Second).WithPolling(time.Second).Should(gomega.Equal(winner.wcName))
	})

	ginkgo.It("tears down pods on every member and releases the finalizer when the source is deleted", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		src := env.createSource(model, runtime, v1beta1.PlacementModeAll, 1)

		ginkgo.By("waiting for both members to be running the workload")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).
					To(gomega.BeNumerically(">=", 1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"expected a Running engine pod on %s", m.wcName)
		}

		ginkgo.By("deleting the source InferenceService")
		gomega.Expect(controlCluster.client.Delete(controlCluster.ctx, src)).To(gomega.Succeed())

		ginkgo.By("asserting the source is fully gone — the placement finalizer was released")
		gomega.Eventually(func() bool {
			err := controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Namespace: env.isvcNS, Name: env.isvcName}, &v1beta1.InferenceService{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())

		ginkgo.By("asserting every member's derived AND its pods are gone")
		for _, m := range env.members {
			gomega.Eventually(func() bool {
				_, ok := getDerivedOn(m.c, env.isvcNS, env.isvcName)
				return ok
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeFalse(),
				"derived should be gone from %s", m.wcName)
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(enginePodsOn(g, m.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"pods should be gone from %s", m.wcName)
		}
	})

	ginkgo.It("leaves an unreachable member's derived for the GC to reap on reconnect", func() {
		// The placer's delete path only visits CONNECTED clusters, so a member
		// that is unreachable at delete time keeps its derived and the source's
		// finalizer is still released. That leak is by design — the orphan GC is
		// the backstop — and this asserts the whole contract, including the reap.
		model, runtime := env.seedModelRuntimeOnMembers()
		src := env.createSource(model, runtime, v1beta1.PlacementModeAll, 1)

		// The GC treats an empty live-ISVC list as suspect and skips the sweep, so
		// a surviving unrelated source must exist for the reap to run at all.
		keeper := env.createKeeperISVC()
		ginkgo.DeferCleanup(func() { deleteIfExists(controlCluster, keeper) })

		stranded := env.members[1]
		for _, m := range env.members {
			gomega.Eventually(func() bool {
				_, ok := getDerivedOn(m.c, env.isvcNS, env.isvcName)
				return ok
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
				"derived should exist on %s", m.wcName)
		}

		ginkgo.By("breaking the second member's kubeconfig so the control plane disconnects it")
		rotateSecretTo(controlCluster, env.secretNS, stranded.secretName, []byte("not-a-kubeconfig"))
		gomega.Eventually(func() bool {
			return workloadClusterReady(stranded.wcName)
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeFalse(),
			"%s should go NotReady on an invalid kubeconfig", stranded.wcName)

		ginkgo.By("deleting the source and asserting the finalizer is released anyway")
		gomega.Expect(controlCluster.client.Delete(controlCluster.ctx, src)).To(gomega.Succeed())
		gomega.Eventually(func() bool {
			err := controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Namespace: env.isvcNS, Name: env.isvcName}, &v1beta1.InferenceService{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
			"an unreachable member must not block teardown of the source")

		ginkgo.By("asserting the unreachable member kept its now-orphaned derived")
		_, ok := getDerivedOn(stranded.c, env.isvcNS, env.isvcName)
		gomega.Expect(ok).To(gomega.BeTrue(), "the derived should be stranded, not swept")

		ginkgo.By("restoring the kubeconfig and asserting the GC reaps the orphan")
		rotateSecretTo(controlCluster, env.secretNS, stranded.secretName, kubeconfigBytesFor(stranded.c))
		gomega.Eventually(func() bool {
			return workloadClusterReady(stranded.wcName)
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
		gomega.Eventually(func() bool {
			return derivedSweptOn(stranded.c, env.isvcNS, env.isvcName)
		}).WithTimeout(gcInterval+longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
			"the orphan GC should reap the stranded derived once the member is reachable again")
	})

	ginkgo.It("propagates a source spec change and rolls the winner's pods to the new image", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)
		winner := env.awaitWinner()

		ginkgo.By("waiting for the winner to run the original image")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(engineImagesOn(g, winner.c, env.isvcNS, env.isvcName)).
				To(gomega.ContainElement(engineImageV1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"winner should start on the original image")

		ginkgo.By("bumping the engine image on the SOURCE, from the control plane")
		gomega.Eventually(func(g gomega.Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Namespace: env.isvcNS, Name: env.isvcName}, fresh)).To(gomega.Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = engineImageV2
			g.Expect(controlCluster.client.Update(controlCluster.ctx, fresh)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("asserting the winner's derived picks up the new image")
		gomega.Eventually(func() string {
			derived, ok := getDerivedOn(winner.c, env.isvcNS, env.isvcName)
			if !ok || derived.Spec.Engine == nil || len(derived.Spec.Engine.PodSpec.Containers) == 0 {
				return ""
			}
			return derived.Spec.Engine.PodSpec.Containers[0].Image
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Equal(engineImageV2))

		ginkgo.By("asserting the pods actually roll to the new image and come back Running")
		gomega.Eventually(func(g gomega.Gomega) {
			images := engineImagesOn(g, winner.c, env.isvcNS, env.isvcName)
			g.Expect(images).NotTo(gomega.BeEmpty())
			g.Expect(images).To(gomega.HaveEach(engineImageV2), "every engine pod should be on the new image")
			g.Expect(runningEnginePodsOn(g, winner.c, env.isvcNS, env.isvcName)).To(gomega.BeNumerically(">=", 1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("asserting the update did not resurrect a derived on the loser")
		loser := env.other(winner)
		gomega.Consistently(func() bool {
			return derivedSweptOn(loser.c, env.isvcNS, env.isvcName)
		}).WithTimeout(5*time.Second).WithPolling(time.Second).Should(gomega.BeTrue(),
			"a spec update must not re-place onto the swept loser")
	})
})
