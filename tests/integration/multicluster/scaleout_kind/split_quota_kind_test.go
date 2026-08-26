package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Fractional placement against real Kueue quota — the subject of OEP-0018.1.
//
// Only here does quota mean anything: every spec grants an exact per-member
// budget, so "spreads because one cluster is too small" is a fact about
// admission rather than an arrangement the test made for itself. The engine
// carries a CPU request for the same reason — a zero-request pod is admitted
// against any quota, including none.
var _ = ginkgo.Describe("Split under real quota (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
	})

	ginkgo.It("spreads a service that no single home has room for", func() {
		const desired = 6
		perHome := desired / 2

		model, runtime := env.seedModelRuntimeOnMembers()

		// Each home can hold exactly half. Neither could run the service alone,
		// so a placement that converges proves the split is real capacity work
		// and not an even division chosen up front.
		for _, m := range env.members {
			env.grantQuota(m, env.cpuForReplicas(perHome))
		}

		env.createSplitSource(model, runtime, desired)

		ginkgo.By("asserting each home runs its share and the fleet total reaches the desired count")
		for _, m := range env.members {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).To(gomega.Equal(perHome))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"%s should run %d replicas", m.wcName, perHome)
		}
		gomega.Eventually(func() int32 {
			return env.admittedAcrossHomes()
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Equal(int32(desired)),
			"admitted replicas summed across homes should reach the desired count")
	})

	ginkgo.It("trims transient over-admission back to the desired count", func() {
		const desired = 2

		model, runtime := env.seedModelRuntimeOnMembers()

		// Both homes can hold the whole service. The fan-out over-requests on
		// purpose — it cannot know what fits without asking — so both admit and
		// the total briefly exceeds desired. Trim is what brings it back.
		for _, m := range env.members {
			env.grantQuota(m, env.cpuForReplicas(desired))
		}

		env.createSplitSourceWith(model, runtime, desired, &v1beta1.SplitSpec{})

		ginkgo.By("asserting the fleet converges to exactly the desired count")
		gomega.Eventually(func() int32 {
			return env.admittedAcrossHomes()
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Equal(int32(desired)))

		ginkgo.By("asserting it stays there rather than oscillating")
		gomega.Consistently(func() int32 {
			return env.admittedAcrossHomes()
		}).WithTimeout(20 * time.Second).WithPolling(2 * time.Second).Should(gomega.Equal(int32(desired)))

		ginkgo.By("asserting the running replicas total the desired count, not more")
		gomega.Eventually(func(g gomega.Gomega) {
			total := 0
			for _, m := range env.members {
				total += runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)
			}
			g.Expect(total).To(gomega.Equal(desired))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("fills the preferred home first and leaves the other empty when Packed", func() {
		const desired = 4

		model, runtime := env.seedModelRuntimeOnMembers()

		// Packed walks candidates in preference order, which is lexicographic on
		// WorkloadCluster name — not the order this suite registered them. Given
		// a preferred home with room for everything, the other should be left
		// unused entirely rather than merely under-filled.
		preferred := env.preferredMember()
		spare := env.other(preferred)
		env.grantQuota(preferred, env.cpuForReplicas(desired))
		env.grantQuota(spare, env.cpuForReplicas(desired))

		env.createSplitSourceWith(model, runtime, desired, &v1beta1.SplitSpec{})

		ginkgo.By("asserting the preferred home carries the whole service")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, preferred.c, env.isvcNS, env.isvcName)).To(gomega.Equal(desired))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("asserting the spare home is swept rather than left half-used")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(enginePodsOn(g, spare.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"Packed should not leave a partially-used second home")
	})

	ginkgo.It("honors the per-cluster ceiling even when a home could take more", func() {
		const (
			desired = 4
			ceiling = 2
		)

		model, runtime := env.seedModelRuntimeOnMembers()

		// Quota alone would let either home take everything; the ceiling is the
		// only thing forcing the spread, which is what makes it worth asserting.
		for _, m := range env.members {
			env.grantQuota(m, env.cpuForReplicas(desired))
		}

		env.createSplitSourceWith(model, runtime, desired, &v1beta1.SplitSpec{
			MaxReplicasPerCluster: ceiling,
		})

		ginkgo.By("asserting no home exceeds the ceiling and the total still converges")
		gomega.Eventually(func(g gomega.Gomega) {
			total := 0
			for _, m := range env.members {
				running := runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)
				g.Expect(running).To(gomega.BeNumerically("<=", ceiling),
					"%s must not exceed maxReplicasPerCluster", m.wcName)
				total += running
			}
			g.Expect(total).To(gomega.Equal(desired))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("drops a home that can only take a sliver and places the rest elsewhere", func() {
		const (
			desired = 3
			floor   = 2
		)

		model, runtime := env.seedModelRuntimeOnMembers()

		// One home has room for a single replica — below the floor, so keeping it
		// would leave a home serving an uneconomical fraction. Its share has to
		// come back to the deficit and land on the home that can hold it.
		sliver, roomy := env.members[0], env.members[1]
		env.grantQuota(sliver, env.cpuForReplicas(1))
		env.grantQuota(roomy, env.cpuForReplicas(desired))

		env.createSplitSourceWith(model, runtime, desired, &v1beta1.SplitSpec{
			MinReplicasPerCluster: floor,
		})

		ginkgo.By("asserting the sliver home is dropped entirely")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(enginePodsOn(g, sliver.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"a home under minReplicasPerCluster should be swept, not kept")

		ginkgo.By("asserting the whole service lands on the home that can hold it")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, roomy.c, env.isvcNS, env.isvcName)).To(gomega.Equal(desired))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("settles at a stable partial when the fleet cannot satisfy the request", func() {
		const (
			desired   = 6
			perHome   = 1
			reachable = perHome * 2
		)

		model, runtime := env.seedModelRuntimeOnMembers()

		// Far less quota than the service asks for. The interesting property is
		// not the shortfall itself but that it CONVERGES: an apportionment that
		// kept re-deciding would churn pods forever against a wall it cannot
		// clear.
		for _, m := range env.members {
			env.grantQuota(m, env.cpuForReplicas(perHome))
		}

		env.createSplitSource(model, runtime, desired)

		ginkgo.By("asserting the fleet admits everything it can and no more")
		gomega.Eventually(func() int32 {
			return env.admittedAcrossHomes()
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Equal(int32(reachable)))

		ginkgo.By("asserting the shortfall is steady rather than churning")
		gomega.Consistently(func() int32 {
			return env.admittedAcrossHomes()
		}).WithTimeout(20*time.Second).WithPolling(2*time.Second).Should(gomega.Equal(int32(reachable)),
			"a permanent shortfall must converge, not oscillate")

		// Worth stating explicitly: the control plane reports this as Placed.
		// There is no Degraded phase, so a service running 2 of 6 replicas is
		// indistinguishable from a healthy one at the phase level.
		ginkgo.By("recording that a shortfall is still reported as Placed")
		ps := placementStatusOf(env.isvcNS, env.isvcName)
		gomega.Expect(ps).NotTo(gomega.BeNil())
		gomega.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
	})
})
