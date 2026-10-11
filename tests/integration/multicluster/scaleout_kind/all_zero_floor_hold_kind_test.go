package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

// A placement pass held on a reason keeps an idle zero-floor home Placed.
//
// An All-mode service with engine minReplicas 0 runs one idle copy on the
// cluster its affinity selects. The copy acknowledges the plan with no
// admitted replicas and the service reads Placed. The operator then points
// the affinity at the other cluster. A copy with a zero minimum cannot be
// moved, so every pass stops with PositiveFloorRequired on the
// PlacementConverged condition. The phase must stay Placed through those
// passes: the copy's placement did not move, the plan stays the idle plan,
// no pod and no second copy appears. A positive floor then releases the
// move, which proves the hold was a hold and not a wedge.
var _ = ginkgo.Describe("All zero-floor placement held on a reason (KIND)", func() {
	ginkgo.It("keeps an idle zero-floor home Placed while every pass stops on PositiveFloorRequired", func() {
		env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
		model, runtime := env.seedModelRuntimeOnMembers()
		first, replacement := env.members[0], env.members[1]
		for _, member := range env.members {
			env.grantQuota(member, "0")
		}
		selectHome := func(s *v1beta1.InferenceService, member memberReg) {
			s.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{member.wcName}}}}}
		}
		source := env.createSource(model, runtime, v1beta1.PlacementModeAll, 0, func(s *v1beta1.InferenceService) {
			s.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
			s.Spec.Engine.MaxReplicas = 4
			selectHome(s, first)
		})
		key := client.ObjectKeyFromObject(source)
		irKey := client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-engine"}
		publish := capacityRoutePublisher(env)

		// idleHome reads the one copy as the pass must keep seeing it: Placed,
		// the plan acknowledged, no replica admitted, no pod anywhere, the
		// other member swept.
		var firstUID types.UID
		idleHome := func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			g.Expect(source.Status.Placement.Plan.AssignedReplicas).To(gomega.BeZero())
			g.Expect(source.Status.Placement.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			candidate := candidateFor(source.Status.Placement, first.wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(source.Status.Placement.Plan.ID, candidate.AppliedPlanID)).To(gomega.BeEmpty(), "the idle home must acknowledge the plan that stands")
			g.Expect(candidate.ReadyReplicas).To(gomega.BeZero())
			ir := &v1beta1.InferenceReplica{}
			g.Expect(first.c.client.Get(first.c.ctx, irKey, ir)).To(gomega.Succeed())
			if firstUID == "" {
				firstUID = ir.UID
			}
			g.Expect(cmp.Diff(firstUID, ir.UID)).To(gomega.BeEmpty(), "the idle home must keep its InferenceReplica")
			g.Expect(cmp.Diff(ptr.To[int32](0), ir.Spec.Replicas)).To(gomega.BeEmpty())
			g.Expect(ir.Status.PlacementObservedGeneration).To(gomega.Equal(ir.Generation))
			g.Expect(ir.Spec.PlacementExecution).NotTo(gomega.BeNil())
			g.Expect(protocol.HasZeroReplicaFloor(ir.Spec.PlacementExecution, v1beta1.EngineComponent)).To(gomega.BeTrue())
			g.Expect(enginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			g.Expect(enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			g.Expect(derivedSweptOn(replacement.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue(), "no copy may appear on the cluster the floor cannot move to")
		}

		ginkgo.By("settling one idle zero-floor home as Placed")
		gomega.Eventually(idleHome).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		idlePlan := source.Status.Placement.Plan.DeepCopy()

		ginkgo.By("pointing the affinity at the other cluster, a move the zero floor cannot carry")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			selectHome(source, replacement)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		heldOnFloor := func(g gomega.Gomega) {
			idleHome(g)
			condition := source.Status.GetCondition(v1beta1.PlacementConverged)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff("PositiveFloorRequired", condition.Reason)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(idlePlan, source.Status.Placement.Plan)).To(gomega.BeEmpty(), "a held pass must not change the idle plan")
		}
		gomega.Eventually(heldOnFloor).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// Many held passes in a row: the window covers the placement requeue
		// many times over and the winner-lost grace once, so a pass that
		// demotes the idle copy to Admitting cannot hide between reads.
		ginkgo.By("holding: the phase stays Placed across the held passes")
		gomega.Consistently(heldOnFloor).WithTimeout(winnerLostGrace + placementRequeue).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("a positive floor releases the move")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			source.Spec.Engine.MinReplicas = ptr.To(2)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			pods := enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(2), "the positive floor must stage the copy on the selected cluster")
			for _, pod := range pods {
				g.Expect(pod.Spec.SchedulingGates).NotTo(gomega.BeEmpty(), "the staged copy waits on real admission")
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		env.grantQuota(replacement, env.cpuForReplicas(2))
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			g.Expect(source.Status.Placement.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(readyEnginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
			g.Expect(enginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			g.Expect(derivedSweptOn(first.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue(), "the retained home is released once the floor is positive")
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
