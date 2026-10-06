package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

var _ = ginkgo.Describe("Single bounded replacement movement (KIND)", func() {
	ginkgo.DescribeTable("preserves bounded authority through replacement selection", func(restore bool) {
		env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
		model, runtime := env.seedModelRuntimeOnMembers()
		first, replacement := env.members[0], env.members[1]
		env.grantQuota(first, env.cpuForReplicas(2))
		env.grantQuota(replacement, "0")
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeSingle, 2)
		selectHome := func(name string) {
			source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}}}
		}
		selectHome(first.wcName)
		env.applyEngineResources(source)
		env.routeToSpecQueue(source)
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, source)
			for _, m := range env.members {
				gomega.Eventually(func() bool { return derivedSweptOn(m.c, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
			}
		})
		publish := capacityRoutePublisher(env)
		key := client.ObjectKeyFromObject(source)
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(first.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
			derived, ok := getDerivedOn(first.c, env.isvcNS, env.isvcName)
			g.Expect(ok).To(gomega.BeTrue())
			policy, err := protocol.FromDerived(derived)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(policy).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff([]v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}, policy.ReplicaFloors)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			selectHome(replacement.wcName)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("reserving one complete replacement while Kueue holds it pending")
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.SingleMove).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.SingleMove.Selected).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(first.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			pods := enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(2))
			for _, pod := range pods {
				g.Expect(pod.Spec.SchedulingGates).NotTo(gomega.BeEmpty())
			}
			g.Expect(readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("selecting on partial admission while retaining the full original home")
		env.grantQuota(replacement, env.cpuForReplicas(1))
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.SingleMove).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(replacement.wcName, source.Status.Placement.Plan.SingleMove.Selected)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(first.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			g.Expect(candidateFor(source.Status.Placement, first.wcName).Allocation.DrainRequested).To(gomega.BeFalse())
			g.Expect(readyEnginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			g.Expect(readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		if restore {
			ginkgo.By("verifying the original is physically absent before cancelling the move")
			original, ok := getDerivedOn(first.c, env.isvcNS, env.isvcName)
			gomega.Expect(ok).To(gomega.BeTrue())
			deleteIfExists(first.c, original)
			gomega.Eventually(func(g gomega.Gomega) {
				publish(g)
				g.Expect(enginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
				g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
				a := candidateFor(source.Status.Placement, first.wcName).Allocation
				g.Expect(cmp.Diff(int32(0), a.CurrentReplicas)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(int32(2), a.OriginalReplicas)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(first.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
				g.Expect(cmp.Diff(replacement.wcName, source.Status.Placement.Plan.SingleMove.Selected)).To(gomega.BeEmpty())
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
				selectHome(first.wcName)
				g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
			ginkgo.By("recreating the original from fresh policy and cleaning the selected replacement")
			peak := 0
			gomega.Eventually(func(g gomega.Gomega) {
				publish(g)
				old := enginePodsOn(g, first.c, env.isvcNS, env.isvcName)
				next := enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)
				peak = max(peak, len(old)+len(next))
				g.Expect(readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
				g.Expect(next).To(gomega.BeEmpty())
				g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
				g.Expect(source.Status.Placement.Plan.SingleMove).To(gomega.BeNil())
				g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
				g.Expect(cmp.Diff(first.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
				derived, ok := getDerivedOn(first.c, env.isvcNS, env.isvcName)
				g.Expect(ok).To(gomega.BeTrue())
				g.Expect(derived.UID).NotTo(gomega.Equal(original.UID))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
			gomega.Expect(peak).To(gomega.BeNumerically("<=", 4))
			return
		}
		ginkgo.By("completing the serving handoff within the shared allowance")
		env.grantQuota(replacement, env.cpuForReplicas(2))
		peak, lostCoverage := 0, false
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			old := enginePodsOn(g, first.c, env.isvcNS, env.isvcName)
			next := enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)
			peak = max(peak, len(old)+len(next))
			lostCoverage = lostCoverage || readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)+readyEnginePodsOn(g, replacement.c, env.isvcNS, env.isvcName) < 2
			g.Expect(old).To(gomega.BeEmpty())
			g.Expect(readyEnginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(source.Status.Placement.Plan.SingleMove).To(gomega.BeNil())
			g.Expect(cmp.Diff(replacement.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Expect(peak).To(gomega.BeNumerically("<=", 4))
		gomega.Expect(cmp.Diff(false, lostCoverage)).To(gomega.BeEmpty())
	},
		ginkgo.Entry("keeps the serving winner until the admitted replacement reaches its full floor", false),
		ginkgo.Entry("restores a missing original when the selected replacement is cancelled", true),
	)
})
