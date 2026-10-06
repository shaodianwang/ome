package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

var _ = ginkgo.Describe("Single persisted admission race (KIND)", func() {
	ginkgo.It("commits a partially admitted winner and retains it across an affinity change", func() {
		env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
		model, runtime := env.seedModelRuntimeOnMembers()
		for _, member := range env.members {
			env.grantQuota(member, "0")
		}
		source := env.createSource(model, runtime, v1beta1.PlacementModeSingle, 2)
		key := client.ObjectKeyFromObject(source)
		ginkgo.By("observing full nominated floors while Kueue holds the race open")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			for _, member := range env.members {
				candidate := candidateFor(source.Status.Placement, member.wcName)
				g.Expect(candidate).NotTo(gomega.BeNil())
				g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
				g.Expect(candidate.Allocation.RaceCandidate).To(gomega.BeTrue())
				g.Expect(candidate.Allocation.DesiredReplicas).To(gomega.Equal(int32(2)))
				pods := enginePodsOn(g, member.c, env.isvcNS, env.isvcName)
				g.Expect(pods).To(gomega.HaveLen(2))
				for _, pod := range pods {
					g.Expect(pod.Spec.SchedulingGates).NotTo(gomega.BeEmpty())
				}
				derived, exists := getDerivedOn(member.c, env.isvcNS, env.isvcName)
				g.Expect(exists).To(gomega.BeTrue())
				policy, err := protocol.FromDerived(derived)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(policy).NotTo(gomega.BeNil())
				g.Expect(cmp.Diff(source.Status.Placement.Plan.ID, policy.PlanID)).To(gomega.BeEmpty())
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		raceID := source.Status.Placement.Plan.ID
		winner, loser := env.members[1], env.members[0]
		ginkgo.By("granting only one of the two requested replicas on one member")
		env.grantQuota(winner, env.cpuForReplicas(1))
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(cmp.Diff(winner.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(winner.wcName, source.Status.Placement.Cluster)).To(gomega.BeEmpty())
			g.Expect(source.Status.Placement.Plan.ID).NotTo(gomega.Equal(raceID))
			g.Expect(candidateFor(source.Status.Placement, winner.wcName).AdmittedReplicas).To(gomega.Equal(int32(1)))
			g.Expect(candidateFor(source.Status.Placement, loser.wcName).Allocation.DesiredReplicas).To(gomega.BeZero())
			g.Expect(readyEnginePodsOn(g, winner.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			g.Expect(enginePodsOn(g, loser.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			g.Expect(derivedSweptOn(loser.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
