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
