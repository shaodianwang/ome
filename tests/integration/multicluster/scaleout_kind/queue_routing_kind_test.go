package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Where a placed workload's quota is charged, as opposed to where the workload
// goes.
//
// The placer and the quota plane meet at exactly one point: the queue-name
// label stamped on a derived workload's pods. Everything else in this suite
// exercises one side or the other — the placement specs decide homes with
// admission a no-op, and the quota specs constrain a placement already decided.
// These two are about the label itself.
//
// Driven by hand on the kwok fleet first, as fixtures 61 and 68 under
// hack/kwok-sim; this is the same pair against a real kubelet.
var _ = ginkgo.Describe("Queue routing under real quota (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		// 500m per replica, matching the other quota specs: enough that quota
		// binds, small enough for a single-node KIND member.
		env = setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
	})

	// The happy path, and the claim is that each home's budget is its own. All
	// mode asks every matching home for the same two replicas while funding only
	// one apiece, so a home that ran both would have to be drawing on a budget
	// nobody gave it.
	//
	// Counted in pods rather than in status.placement: AdmittedReplicas is what
	// Split converges on, and All mode leaves it unset.
	ginkgo.It("charges every home's own quota when the service runs on all of them", func() {
		const asked = 2
		model, runtime := env.seedModelRuntimeOnMembers()

		for _, m := range env.members {
			env.grantQuota(m, env.cpuForReplicas(1))
		}

		env.createSource(model, runtime, v1beta1.PlacementModeAll, asked)

		ginkgo.By("asserting every home runs exactly what it funded, and gates the rest")
		for _, m := range env.members {
			member := m
			gomega.Eventually(func(g gomega.Gomega) {
				pods := enginePodsOn(g, member.c, env.isvcNS, env.isvcName)
				g.Expect(pods).To(gomega.HaveLen(asked),
					"%s should have both pods created", member.wcName)
				g.Expect(runningEnginePodsOn(g, member.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1),
					"%s funded one replica and should be running exactly one", member.wcName)
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}

		// Consistently, because the failure worth catching is a second replica
		// admitted late — a home reaching past its own budget into the cohort.
		ginkgo.By("asserting no home reaches past its own budget")
		gomega.Consistently(func(g gomega.Gomega) {
			for _, m := range env.members {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1),
					"%s ran more than it funded", m.wcName)
			}
		}).WithTimeout(20 * time.Second).WithPolling(pollInterval).Should(gomega.Succeed())
	})

	// The failure that looks like success from the control plane. A workload
	// labelled for a queue nobody provisioned is placed, derived, and healthy at
	// every level the hub can see — and never runs. The only signal is at the
	// pods, which is not where anyone looks first.
	ginkgo.It("gates a workload routed to a queue that does not exist", func() {
		// Routed wrong from the start. Re-routing a running workload would not
		// show this: its pods were admitted under the working queue and their
		// gate is already gone, and changing an annotation does not roll them.
		env = setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"),
			withSourceQueue("no-such-queue"))
		model, runtime := env.seedModelRuntimeOnMembers()

		// Generous quota on the queue this spec really funds, so nothing here can
		// be mistaken for a workload that simply could not be afforded.
		for _, m := range env.members {
			env.grantQuota(m, env.cpuForReplicas(4))
		}

		env.createSource(model, runtime, v1beta1.PlacementModeAll, 1)

		ginkgo.By("asserting the pods are created and gated, on every home")
		gomega.Eventually(func(g gomega.Gomega) {
			var gated int
			for _, m := range env.members {
				for _, p := range enginePodsOn(g, m.c, env.isvcNS, env.isvcName) {
					g.Expect(p.Spec.SchedulingGates).NotTo(gomega.BeEmpty(),
						"pod %s on %s carries no gate, so Kueue never saw it", p.Name, m.wcName)
					gated++
				}
			}
			g.Expect(gated).To(gomega.BeNumerically(">=", 1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// Quota is plentiful and still nothing is admitted, which is what
		// separates a misrouted workload from an unaffordable one.
		ginkgo.By("asserting nothing is admitted despite the budget being free")
		gomega.Consistently(func() int32 {
			return env.admittedAcrossHomes()
		}).WithTimeout(15 * time.Second).WithPolling(pollInterval).Should(gomega.BeZero())
	})
})
