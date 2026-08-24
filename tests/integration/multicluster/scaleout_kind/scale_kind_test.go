package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Scale and churn. Deliberately modest: these run on two single-node KIND
// clusters sharing one developer machine, so they check that convergence and
// stability hold at more than one replica per home and across repeated
// disruption — not fleet-scale numbers, which belong in the perf harness.
var _ = ginkgo.Describe("Multi-cluster scale and churn (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	ginkgo.It("converges a multi-replica spread across both homes", func() {
		const (
			desired   = 6
			perHome   = desired / 2
			converged = int32(desired)
		)

		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSplitSource(model, runtime, desired)

		ginkgo.By("asserting every home brings up its full share of running pods")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).
					To(gomega.Equal(perHome))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"%s should be running its %d-replica share", m.wcName, perHome)
		}

		ginkgo.By("asserting the fleet-wide ready count reaches the desired total")
		gomega.Eventually(func() int32 {
			return env.readyReplicasAcrossHomes()
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Equal(converged),
			"summed ready replicas should reach the desired count")

		// Reaching the count is half the claim; not overshooting is the other.
		// An apportionment that keeps over-requesting would sit above desired.
		ginkgo.By("asserting it settles at the desired count rather than overshooting")
		gomega.Consistently(func() int32 {
			return env.readyReplicasAcrossHomes()
		}).WithTimeout(15 * time.Second).WithPolling(time.Second).Should(gomega.Equal(converged))
	})

	ginkgo.It("keeps the winner and its endpoint stable across repeated loss of the other home", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)
		winner := env.awaitWinner()
		loser := env.other(winner)

		var published string
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Endpoint).NotTo(gomega.BeNil())
			published = ps.Endpoint.String()
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		pods := enginePodsOn(gomega.Default, winner.c, env.isvcNS, env.isvcName)
		gomega.Expect(pods).NotTo(gomega.BeEmpty())
		servingUID := pods[0].UID

		// Churn the home that is NOT serving. Nothing about the winner changes,
		// so every candidate-set recomputation this provokes must be a no-op for
		// the placement — the scaled-down soak for "capacity flux must not move a
		// healthy service".
		const cycles = 3
		for i := 0; i < cycles; i++ {
			ginkgo.By("disconnecting and restoring the non-serving home")
			env.disconnectMember(loser)
			env.reconnectMember(loser)

			ps := placementStatusOf(env.isvcNS, env.isvcName)
			gomega.Expect(ps).NotTo(gomega.BeNil())
			gomega.Expect(ps.Cluster).To(gomega.Equal(winner.wcName),
				"the winner must not move when an unrelated home flaps")
			gomega.Expect(ps.Endpoint).NotTo(gomega.BeNil(), "the endpoint must not blank")
			gomega.Expect(ps.Endpoint.String()).To(gomega.Equal(published),
				"the endpoint must not change")
		}

		ginkgo.By("asserting the serving pod was never restarted throughout the churn")
		current := enginePodsOn(gomega.Default, winner.c, env.isvcNS, env.isvcName)
		gomega.Expect(current).NotTo(gomega.BeEmpty())
		gomega.Expect(current[0].UID).To(gomega.Equal(servingUID),
			"churn on another home must not disturb the serving replica")
	})
})
