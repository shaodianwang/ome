package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Re-sharding and failure handling: what the placement does when a home goes
// away, comes back, loses a pod, or cannot run the workload at all. These are
// the paths where "it converged" is only a real claim if pods actually moved,
// so they belong on a kubelet rather than in envtest.
var _ = ginkgo.Describe("Multi-cluster re-shard and failure (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	ginkgo.It("tops the survivor up when a home is lost, without disturbing what it already runs", func() {
		const desired = 2

		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSplitSource(model, runtime, desired)

		survivor, lost := env.members[0], env.members[1]

		ginkgo.By("waiting for the spread: one replica on each home")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).
					To(gomega.Equal(1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"%s should be running exactly its share", m.wcName)
		}

		// Capture what the survivor is already running. A re-shard must ADD to it,
		// never restart it: recreating a serving replica to rebalance would be an
		// outage the user never asked for.
		incumbent := enginePodsOn(gomega.Default, survivor.c, env.isvcNS, env.isvcName)
		gomega.Expect(incumbent).To(gomega.HaveLen(1))
		incumbentUID := incumbent[0].UID

		ginkgo.By("disconnecting the other home")
		env.disconnectMember(lost)
		ginkgo.DeferCleanup(func() { env.reconnectMember(lost) })

		ginkgo.By("asserting the survivor takes over the full desired count")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, survivor.c, env.isvcNS, env.isvcName)).
				To(gomega.Equal(desired))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"%s should absorb the lost home's share", survivor.wcName)

		ginkgo.By("asserting the replica it was already serving was never restarted")
		var found bool
		for _, p := range enginePodsOn(gomega.Default, survivor.c, env.isvcNS, env.isvcName) {
			if p.UID == incumbentUID {
				found = true
			}
		}
		gomega.Expect(found).To(gomega.BeTrue(),
			"the incumbent replica must survive the re-shard — topping up is additive")
	})

	ginkgo.It("rebalances back onto a home that returns, and then holds steady", func() {
		const desired = 2

		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSplitSource(model, runtime, desired)

		survivor, returning := env.members[0], env.members[1]

		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).
					To(gomega.Equal(1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}

		ginkgo.By("losing a home, then bringing it back")
		env.disconnectMember(returning)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, survivor.c, env.isvcNS, env.isvcName)).
				To(gomega.Equal(desired))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		env.reconnectMember(returning)

		ginkgo.By("asserting the spread is restored across both homes")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).
					To(gomega.Equal(1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"%s should be back to its even share", m.wcName)
		}

		// Convergence is only useful if it settles. An apportionment that keeps
		// re-deciding would churn pods forever.
		ginkgo.By("asserting the distribution then stays put")
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect([]int{
				runningEnginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName),
				runningEnginePodsOn(g, env.members[1].c, env.isvcNS, env.isvcName),
			}).To(gomega.Equal([]int{1, 1}))
		}).WithTimeout(15*time.Second).WithPolling(time.Second).Should(gomega.Succeed(),
			"the re-shard must converge, not oscillate")
	})

	ginkgo.It("does not move the placement when the winner's pod is replaced", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)
		winner := env.awaitWinner()

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, winner.c, env.isvcNS, env.isvcName)).
				To(gomega.BeNumerically(">=", 1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		pods := enginePodsOn(gomega.Default, winner.c, env.isvcNS, env.isvcName)
		gomega.Expect(pods).NotTo(gomega.BeEmpty())
		oldUID := pods[0].UID

		// An Instance reports Admitted only while it has pods, so the window
		// between delete and replacement looks momentarily like lost admission.
		// The placement must ride that out rather than re-race — pod churn is
		// routine, and re-placing on it would move a healthy service.
		ginkgo.By("deleting the winner's engine pod")
		victim := &corev1.Pod{}
		gomega.Expect(winner.c.client.Get(winner.c.ctx,
			types.NamespacedName{Namespace: env.isvcNS, Name: pods[0].Name}, victim)).To(gomega.Succeed())
		gomega.Expect(winner.c.client.Delete(winner.c.ctx, victim)).To(gomega.Succeed())

		ginkgo.By("asserting the placement never leaves the winner while the pod is replaced")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Cluster).To(gomega.Equal(winner.wcName))
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
		}).WithTimeout(20*time.Second).WithPolling(time.Second).Should(gomega.Succeed(),
			"pod-UID rotation must not re-race the placement")

		ginkgo.By("asserting a fresh pod took its place")
		gomega.Eventually(func(g gomega.Gomega) {
			current := enginePodsOn(g, winner.c, env.isvcNS, env.isvcName)
			g.Expect(current).NotTo(gomega.BeEmpty())
			var running bool
			for _, p := range current {
				g.Expect(p.UID).NotTo(gomega.Equal(oldUID), "the old pod should be gone")
				if p.Status.Phase == corev1.PodRunning {
					running = true
				}
			}
			g.Expect(running).To(gomega.BeTrue(), "the replacement should reach Running")
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("marks a placement Failed when the workload cannot start, and stops re-placing", func() {
		// A short readiness budget so the member's backstop escalates inside the
		// suite's time; production defaults to 30 minutes.
		const readyTimeout = 45 * time.Second

		model, runtime := env.seedModelRuntimeOnMembers()
		env.createFailingSource(model, runtime, readyTimeout)

		ginkgo.By("waiting for the readiness backstop to escalate the placement to Failed")
		var failedOn string
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseFailed))
			failedOn = ps.Cluster
		}).WithTimeout(readyTimeout + longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// Failed is terminal on purpose: the spec needs fixing, and re-placing a
		// workload that cannot start just moves the failure around the fleet.
		ginkgo.By("asserting it stays Failed on the same cluster rather than looping")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseFailed))
			g.Expect(ps.Cluster).To(gomega.Equal(failedOn))
		}).WithTimeout(20*time.Second).WithPolling(2*time.Second).Should(gomega.Succeed(),
			"a terminally failed placement must not re-place into a fail loop")
	})
})
