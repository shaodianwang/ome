package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Per-home endpoint reporting. What the control plane publishes in
// status.placement is the contract an external LB/DNS consumes to route across
// homes, and it is only meaningful once a member has actually stood the workload
// up — a URL is mirrored from the member's own derived status, which no envtest
// member ever produces.
//
// These specs assert the STATUS contract, not a programmed Gateway API backend:
// the endpoint publisher is a separate controller this suite does not wire.
var _ = ginkgo.Describe("Multi-cluster endpoints (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	ginkgo.It("publishes a per-home endpoint for every serving member and no top-level winner", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeAll, 1)

		ginkgo.By("waiting for both members to be running the workload")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).
					To(gomega.BeNumerically(">=", 1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"expected a Running engine pod on %s", m.wcName)
		}

		// Each home publishes the URL its own member reports. Under KIND those
		// URLs happen to be identical strings — the in-cluster DNS name is the
		// same on both members, since no per-cluster ingress exists here — so the
		// assertion is that every home carries ONE, not that they differ. Proving
		// homes are separately addressable needs real per-cluster ingress.
		ginkgo.By("asserting every serving home publishes its own endpoint")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))

			for _, m := range env.members {
				cand := candidateFor(ps, m.wcName)
				g.Expect(cand).NotTo(gomega.BeNil(), "no candidate entry for %s", m.wcName)
				g.Expect(cand.Phase).To(gomega.Equal(v1beta1.CandidatePhaseAdmitted),
					"%s should be admitted", m.wcName)
				g.Expect(cand.Endpoint).NotTo(gomega.BeNil(), "%s should publish an endpoint", m.wcName)
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// In All there is no single winner, so the per-home list is the whole
		// truth: a top-level Cluster/Endpoint would tell an LB to route everything
		// to one home.
		ginkgo.By("asserting All mode records no top-level winner cluster or endpoint")
		ps := placementStatusOf(env.isvcNS, env.isvcName)
		gomega.Expect(ps.Cluster).To(gomega.BeEmpty(), "All mode must not name a single winner")
		gomega.Expect(ps.Endpoint).To(gomega.BeNil(), "All mode must not publish a single top-level endpoint")
	})

	ginkgo.It("reports per-home admitted and ready replica counts that a weighted LB can split on", func() {
		const desired = 2

		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSplitSource(model, runtime, desired)

		ginkgo.By("asserting each home is apportioned a share and reports it as admitted AND ready")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))

			var totalReady, totalAdmitted int32
			for _, m := range env.members {
				cand := candidateFor(ps, m.wcName)
				g.Expect(cand).NotTo(gomega.BeNil(), "no candidate entry for %s", m.wcName)
				// Balanced spreads ceil(desired/homes) to each home, so every home
				// must carry a non-zero weight — a zero here means the spread
				// collapsed onto one home and an LB would send it all the traffic.
				g.Expect(cand.ReadyReplicas).To(gomega.BeNumerically(">=", 1),
					"%s should report ready replicas as its traffic weight", m.wcName)
				totalReady += cand.ReadyReplicas
				totalAdmitted += cand.AdmittedReplicas
			}
			g.Expect(totalAdmitted).To(gomega.BeNumerically(">=", int32(desired)),
				"admitted replicas across homes should cover the desired count")
			g.Expect(totalReady).To(gomega.BeNumerically(">=", int32(desired)),
				"ready replicas across homes should cover the desired count")
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("asserting the pods really landed on both members, not just the accounting")
		for _, m := range env.members {
			gomega.Expect(runningEnginePodsOn(gomega.Default, m.c, env.isvcNS, env.isvcName)).
				To(gomega.BeNumerically(">=", 1), "%s should be running its share", m.wcName)
		}
	})

	ginkgo.It("keeps the published endpoint stable while the winning member is unreachable", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)
		winner := env.awaitWinner()

		ginkgo.By("waiting for the winner to publish an endpoint")
		var published string
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Endpoint).NotTo(gomega.BeNil())
			published = ps.Endpoint.String()
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// A reachability blip must not blank the endpoint: an external LB watching
		// this field would deroute the service on every transient hiccup.
		ginkgo.By("pausing the winner's node and asserting the endpoint neither blanks nor changes")
		pauseNode(winner.kindCluster)
		unpaused := false
		thaw := func() {
			if !unpaused {
				unpauseNode(winner.kindCluster)
				unpaused = true
			}
		}
		ginkgo.DeferCleanup(thaw)

		gomega.Consistently(func() string {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			if ps == nil || ps.Endpoint == nil {
				return "<nil>"
			}
			return ps.Endpoint.String()
		}).WithTimeout(10*time.Second).WithPolling(time.Second).Should(gomega.Equal(published),
			"the endpoint must not flap to nil while the winner is briefly unreachable")

		thaw()
	})
})
