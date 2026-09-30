package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

var _ = ginkgo.Describe("All zero-floor placement (KIND)", func() {
	ginkgo.DescribeTable("retains externally scaled homes at zero and requires positive floors before movement", func(initialAll bool) {
		env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
		model, runtime := env.seedModelRuntimeOnMembers()
		first, replacement := env.members[0], env.members[1]
		for _, member := range env.members {
			env.grantQuota(member, "0")
		}
		source := env.createSource(model, runtime, v1beta1.PlacementModeAll, 0, func(s *v1beta1.InferenceService) {
			s.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
			s.Spec.Engine.MaxReplicas = 4
			if !initialAll {
				s.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{first.wcName}}}}}
			}
		})
		key := client.ObjectKeyFromObject(source)
		irKey := client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-engine"}
		publish := capacityRoutePublisher(env)
		var firstUID types.UID
		ginkgo.By("creating zero-floor homes without inventing admission or running pods")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			for _, member := range env.members {
				if !initialAll && member.wcName == replacement.wcName {
					g.Expect(derivedSweptOn(member.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
					continue
				}
				ir := &v1beta1.InferenceReplica{}
				g.Expect(member.c.client.Get(member.c.ctx, irKey, ir)).To(gomega.Succeed())
				g.Expect(cmp.Diff(ptr.To[int32](0), ir.Spec.Replicas)).To(gomega.BeEmpty())
				g.Expect(ir.Status.PlacementObservedGeneration).To(gomega.Equal(ir.Generation))
				g.Expect(ir.Spec.PlacementExecution).NotTo(gomega.BeNil())
				g.Expect(protocol.HasZeroReplicaFloor(ir.Spec.PlacementExecution, v1beta1.EngineComponent)).To(gomega.BeTrue())
				g.Expect(enginePodsOn(g, member.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
				if member.wcName == first.wcName {
					firstUID = ir.UID
				}
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		scaleFirst := func(replicas int32) {
			gomega.Eventually(func(g gomega.Gomega) {
				ir := &v1beta1.InferenceReplica{}
				g.Expect(first.c.client.Get(first.c.ctx, irKey, ir)).To(gomega.Succeed())
				g.Expect(cmp.Diff(firstUID, ir.UID)).To(gomega.BeEmpty())
				scale := &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: ir.Name, Namespace: ir.Namespace, ResourceVersion: ir.ResourceVersion}, Spec: autoscalingv1.ScaleSpec{Replicas: replicas}}
				g.Expect(first.c.client.SubResource("scale").Update(first.c.ctx, ir, client.WithSubResourceBody(scale))).To(gomega.Succeed())
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
		ginkgo.By("requiring real Kueue admission after external scale-up")
		scaleFirst(2)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			pods := enginePodsOn(g, first.c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(2))
			if source.Spec.Engine.MinReplicas != nil && *source.Spec.Engine.MinReplicas > 0 {
				g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeTrue())
			}
			for _, pod := range pods {
				g.Expect(pod.Spec.SchedulingGates).NotTo(gomega.BeEmpty())
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		env.grantQuota(first, env.cpuForReplicas(2))
		checkFirst := func(g gomega.Gomega, replicas int32) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(source.Status.Placement.Plan.SingleMove).To(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.AssignedReplicas).To(gomega.BeZero())
			g.Expect(source.Status.Placement.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
			g.Expect(readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(int(replicas)))
			g.Expect(enginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.HaveLen(int(replicas)))
			if initialAll {
				idle := &v1beta1.InferenceReplica{}
				g.Expect(replacement.c.client.Get(replacement.c.ctx, irKey, idle)).To(gomega.Succeed())
				g.Expect(cmp.Diff(ptr.To[int32](0), idle.Spec.Replicas)).To(gomega.BeEmpty())
				g.Expect(enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			} else {
				g.Expect(derivedSweptOn(replacement.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
			}
			ir := &v1beta1.InferenceReplica{}
			g.Expect(first.c.client.Get(first.c.ctx, irKey, ir)).To(gomega.Succeed())
			g.Expect(cmp.Diff(firstUID, ir.UID)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(ptr.To(replicas), ir.Spec.Replicas)).To(gomega.BeEmpty())
			g.Expect(ir.Status.PlacementObservedGeneration).To(gomega.Equal(ir.Generation))
			candidate := candidateFor(source.Status.Placement, first.wcName)
			if source.Generation == source.Status.Placement.Plan.ObservedGeneration {
				g.Expect(cmp.Diff(source.Status.Placement.Plan.ID, candidate.AppliedPlanID)).To(gomega.BeEmpty())
			} else {
				g.Expect(source.Status.GetCondition(v1beta1.PlacementSatisfied).Reason).To(gomega.Equal("AwaitingCurrentPlan"))
			}
			g.Expect(cmp.Diff(replicas, candidate.ReadyReplicas)).To(gomega.BeEmpty())
			tm := &v1beta1.TrafficMap{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, tm)).To(gomega.Succeed())
			var serving []string
			for _, entry := range tm.Spec.Entries {
				if entry.Weight > 0 {
					serving = append(serving, entry.Cluster)
				}
			}
			var want []string
			if replicas > 0 {
				want = []string{first.wcName}
			}
			g.Expect(cmp.Diff(want, serving)).To(gomega.BeEmpty())
		}
		gomega.Eventually(func(g gomega.Gomega) { checkFirst(g, 2) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("retaining every home and its external scaling authority at zero")
		scaleFirst(0)
		gomega.Eventually(func(g gomega.Gomega) { checkFirst(g, 0) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		idlePlan := source.Status.Placement.Plan.DeepCopy()
		gomega.Consistently(func(g gomega.Gomega) {
			checkFirst(g, 0)
			g.Expect(cmp.Diff(idlePlan, source.Status.Placement.Plan)).To(gomega.BeEmpty())
		}).WithTimeout(winnerLostGrace + placementRequeue).WithPolling(pollInterval).Should(gomega.Succeed())
		scaleFirst(2)
		gomega.Eventually(func(g gomega.Gomega) { checkFirst(g, 2) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("holding affinity movement while the retained home has no guaranteed floor")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{replacement.wcName}}}}}
			source.Spec.Placement.MaxSurge = ptr.To[int32](2)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			checkFirst(g, 2)
			condition := source.Status.GetCondition(v1beta1.PlacementConverged)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff("PositiveFloorRequired", condition.Reason)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("applying positive floors on retained homes before bounded movement")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			source.Spec.Engine.MinReplicas = ptr.To(2)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			g.Expect(readyEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
			pods := enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(2))
			if source.Spec.Engine.MinReplicas != nil && *source.Spec.Engine.MinReplicas > 0 {
				g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeTrue())
			}
			for _, pod := range pods {
				g.Expect(pod.Spec.SchedulingGates).NotTo(gomega.BeEmpty())
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		env.grantQuota(replacement, env.cpuForReplicas(2))
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.Winner).To(gomega.BeEmpty())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(source.Status.Placement.Plan.SingleMove).To(gomega.BeNil())
			g.Expect(readyEnginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)).To(gomega.Equal(2))
			g.Expect(enginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			g.Expect(derivedSweptOn(first.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	},
		ginkgo.Entry("retiring one of two retained zero-floor homes", true),
		ginkgo.Entry("moving to a new full home within the shared allowance", false),
	)
})
