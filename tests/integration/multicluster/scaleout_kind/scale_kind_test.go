package scaleout_kind

import (
	"slices"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

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
			desired = 6
			perHome = desired / 2
		)

		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSplitSource(model, runtime, desired)

		readyPods := func(g gomega.Gomega) map[string][]types.UID {
			out := map[string][]types.UID{}
			for _, m := range env.members {
				pods := enginePodsOn(g, m.c, env.isvcNS, env.isvcName)
				g.Expect(cmp.Diff(perHome, len(pods))).To(gomega.BeEmpty(), "pod count on %s", m.wcName)
				for _, pod := range pods {
					g.Expect(pod.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
					g.Expect(cmp.Diff(corev1.PodRunning, pod.Status.Phase)).To(gomega.BeEmpty())
					g.Expect(slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
						return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
					})).To(gomega.BeTrue(), "pod %s must be ready", pod.Name)
					out[m.wcName] = append(out[m.wcName], pod.UID)
				}
				slices.Sort(out[m.wcName])
			}
			return out
		}
		assertConvergedStatus := func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			g.Expect(ps.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(cmp.Diff(int64(desired), ps.Plan.AssignedReplicas)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(int32(0), ps.Plan.UnassignedReplicas)).To(gomega.BeEmpty())
			for _, m := range env.members {
				candidate := candidateFor(ps, m.wcName)
				g.Expect(candidate).NotTo(gomega.BeNil())
				g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
				g.Expect(candidate.ObservationKnown).To(gomega.BeTrue())
				g.Expect(cmp.Diff(ps.Plan.ID, candidate.AppliedPlanID)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff([3]int32{perHome, perHome, perHome}, [3]int32{candidate.Allocation.CurrentReplicas, candidate.Allocation.DesiredReplicas, candidate.ReadyReplicas})).To(gomega.BeEmpty(), "current, desired, ready replicas on %s", m.wcName)
			}
		}

		ginkgo.By("waiting for ready pods and acknowledgement of the settled plan")
		var serving map[string][]types.UID
		gomega.Eventually(func(g gomega.Gomega) {
			serving = readyPods(g)
			assertConvergedStatus(g)
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// A guarded inventory read can become unknown while member status changes.
		// Direct Pod identity and readiness prove workload stability through that gap.
		ginkgo.By("asserting the ready workloads stay stable without extra pods")
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(cmp.Diff(serving, readyPods(g))).To(gomega.BeEmpty())
		}).WithTimeout(15 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())
		gomega.Eventually(assertConvergedStatus).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
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
