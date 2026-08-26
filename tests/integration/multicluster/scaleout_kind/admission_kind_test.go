package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Real Kueue admission against a real kubelet. Kueue runs on both members, but
// manages only namespaces carrying kindfix.KueueManagedLabel, so these are the
// only specs whose pods are ever gated — every other spec in the suite creates
// unlabelled namespaces and sees admission as a no-op.
//
// This is the capability the rest of the Kueue-dependent backlog (gang
// admission, Split under real quota, preemption) is built on, so it is asserted
// on its own before anything depends on it.
var _ = ginkgo.Describe("Kueue admission (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		// 500m per replica against a 0 / 4 CPU quota: enough that quota is a real
		// constraint, small enough that a single-node KIND member can run it.
		env = setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
	})

	ginkgo.It("gates a workload under zero quota and admits it when quota is granted", func() {
		model, runtime := env.seedModelRuntimeOnMembers()

		// Zero quota everywhere: nothing can be admitted, on either member.
		ginkgo.By("granting zero CPU quota on both members")
		for _, m := range env.members {
			env.grantQuota(m, "0")
		}

		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)

		ginkgo.By("asserting the engine pods are gated rather than running")
		gomega.Eventually(func(g gomega.Gomega) {
			var gated int
			for _, m := range env.members {
				for _, p := range enginePodsOn(g, m.c, env.isvcNS, env.isvcName) {
					g.Expect(p.Spec.SchedulingGates).NotTo(gomega.BeEmpty(),
						"pod %s on %s should carry a Kueue scheduling gate", p.Name, m.wcName)
					gated++
				}
			}
			g.Expect(gated).To(gomega.BeNumerically(">=", 1), "expected at least one gated pod")
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// A gated workload is not admitted, so no cluster can win the race.
		ginkgo.By("asserting no placement is declared while every home is gated")
		gomega.Consistently(func() v1beta1.PlacementPhase {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			if ps == nil {
				return ""
			}
			return ps.Phase
		}).WithTimeout(10*time.Second).WithPolling(time.Second).ShouldNot(
			gomega.Equal(v1beta1.PlacementPhasePlaced),
			"a fully gated service must not be reported as placed")

		ginkgo.By("granting admissible quota on the first member only")
		admitting := env.members[0]
		env.grantQuota(admitting, "4")

		ginkgo.By("asserting that member's pods ungate and run")
		gomega.Eventually(func(g gomega.Gomega) {
			pods := enginePodsOn(g, admitting.c, env.isvcNS, env.isvcName)
			g.Expect(pods).NotTo(gomega.BeEmpty())
			for _, p := range pods {
				g.Expect(p.Spec.SchedulingGates).To(gomega.BeEmpty(),
					"pod %s should be ungated once quota admits it", p.Name)
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, admitting.c, env.isvcNS, env.isvcName)).
				To(gomega.BeNumerically(">=", 1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"the admitted member should actually run the workload")

		ginkgo.By("asserting the placement resolves to the member that had quota")
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(ps.Cluster).To(gomega.Equal(admitting.wcName),
				"only the member with quota can win")
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
