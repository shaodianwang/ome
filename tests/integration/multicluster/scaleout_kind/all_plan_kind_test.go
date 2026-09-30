package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

var _ = ginkgo.Describe("All full-policy movement (KIND)", func() {
	ginkgo.It("resumes a retarget after initial inventory recovers without dropping the serving home", func() {
		env := setupSpecEnv()
		model, runtime := env.seedModelRuntimeOnMembers()
		first, replacement := env.members[0], env.members[1]
		env.disconnectMember(replacement)
		reconnected := false
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeAll, 3)
		env.applyEngineResources(source)
		env.routeToSpecQueue(source)
		ginkgo.DeferCleanup(func() {
			if !reconnected {
				env.reconnectMember(replacement)
			}
			deleteIfExists(controlCluster, source)
			for _, member := range env.members {
				gomega.Eventually(func() bool { return derivedSweptOn(member.c, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
			}
		})
		mustCreate(controlCluster, source)
		publish := capacityRoutePublisher(env)
		key := client.ObjectKeyFromObject(source)
		ginkgo.By("serving the independent home while another member's initial inventory is unknown")
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.AdoptionDigest).NotTo(gomega.BeEmpty())
			candidate := candidateFor(source.Status.Placement, replacement.wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
			g.Expect(candidate.Allocation.InventoryPending).To(gomega.BeTrue())
			g.Expect(cmp.Diff(3, readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName))).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		original := source.Status.Placement.Plan.AdoptionDigest

		ginkgo.By("holding a change to the destination until the initial inventory is complete")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			before := source.DeepCopy()
			source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{replacement.wcName}}}}}
			source.Spec.Placement.MaxSurge = ptr.To[int32](3)
			g.Expect(controlCluster.client.Patch(controlCluster.ctx, source, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(cmp.Diff(original, source.Status.Placement.Plan.AdoptionDigest)).To(gomega.BeEmpty())
			candidate := candidateFor(source.Status.Placement, first.wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(int32(0), candidate.Allocation.OriginalReplicas)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(int32(3), candidate.Allocation.CurrentReplicas)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(int32(0), candidate.Allocation.DesiredReplicas)).To(gomega.BeEmpty())
			condition := source.Status.GetCondition(v1beta1.PlacementConverged)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff("AwaitingHomeInputs", condition.Reason)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(3, readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName))).To(gomega.BeEmpty())
			g.Expect(enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("reconnecting and replacing the whole home within the shared allowance")
		rotateSecretTo(controlCluster, env.secretNS, replacement.secretName, kubeconfigBytesFor(replacement.c))
		reconnected = true
		peak, lostCoverage := 0, false
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			old := enginePodsOn(g, first.c, env.isvcNS, env.isvcName)
			next := enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)
			peak = max(peak, len(old)+len(next))
			lostCoverage = lostCoverage || readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)+readyEnginePodsOn(g, replacement.c, env.isvcNS, env.isvcName) < 3
			g.Expect(workloadClusterReady(replacement.wcName)).To(gomega.BeTrue())
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.AdoptionDigest).To(gomega.BeEmpty())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(old).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(3, readyEnginePodsOn(g, replacement.c, env.isvcNS, env.isvcName))).To(gomega.BeEmpty())
			candidate := candidateFor(source.Status.Placement, replacement.wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(candidate.ObservationKnown).To(gomega.BeTrue())
			g.Expect(cmp.Diff(source.Status.Placement.Plan.ID, candidate.AppliedPlanID)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Expect(peak).To(gomega.BeNumerically("<=", 6))
		gomega.Expect(cmp.Diff(false, lostCoverage)).To(gomega.BeEmpty())
	})

	ginkgo.It("requires room for a whole home and drains only after its replacement serves", func() {
		env := setupSpecEnv()
		model, runtime := env.seedModelRuntimeOnMembers()
		first, replacement := env.members[0], env.members[1]
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeAll, 2)
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
			source.Spec.Placement.MaxSurge = ptr.To[int32](1)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			condition := source.Status.GetCondition(v1beta1.PlacementConverged)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(condition.Reason).To(gomega.Equal("SurgeBudgetExhausted"))
			g.Expect(readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
			g.Expect(enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			source.Spec.Placement.MaxSurge = ptr.To[int32](2)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
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
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Expect(peak).To(gomega.BeNumerically("<=", 4))
		gomega.Expect(cmp.Diff(false, lostCoverage)).To(gomega.BeEmpty())
	})
})
