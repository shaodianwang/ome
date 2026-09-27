package omenative_coordination_kind

import (
	"slices"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/protocol"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("placement member surge coordination", func() {
	ginkgo.It("holds member rollout surge while local services roll and assigned floors grow", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "placement-surge-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed()) })

		policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true}
		raw, err := protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		member := newCoordFixture(ns.Name, WithISVCName("member-service"), WithoutDecoder(), WithEngineUpdateStrategy(v1beta1.UpdateStrategySurgeThenDrain))
		if member.ISVC.Labels == nil {
			member.ISVC.Labels = map[string]string{}
		}
		if member.ISVC.Annotations == nil {
			member.ISVC.Annotations = map[string]string{}
		}
		member.ISVC.Labels[constants.PlacementOrigin] = string(policy.SourceUID)
		member.ISVC.Annotations[constants.PlacementExecution] = raw
		local := newCoordFixture(ns.Name, WithISVCName("local-service"), WithoutDecoder(), WithEngineUpdateStrategy(v1beta1.UpdateStrategySurgeThenDrain))
		if local.ISVC.Annotations == nil {
			local.ISVC.Annotations = map[string]string{}
		}
		local.ISVC.Annotations[constants.PlacementExecution] = "malformed, irrelevant to a local service"
		for _, fixture := range []*coordFixture{member, local} {
			applyFixture(Default, fixture)
			deferFixtureCleanup(fixture)
		}

		Eventually(func(g Gomega) {
			for _, fixture := range []*coordFixture{member, local} {
				ir := freshInferenceReplica(g, ns.Name, fixture.ISVC.Name, v1beta1.EngineComponent)
				g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
				g.Expect(ir.Status.ServingReplicas).To(Equal(int32(1)))
				g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
				want := policy
				if fixture == local {
					want = nil
				}
				g.Expect(cmp.Diff(want, ir.Spec.PlacementExecution)).To(BeEmpty())
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		podUIDs := func(g Gomega, name string) []types.UID {
			pods := listOMENativePods(g, ns.Name, name)
			uids := make([]types.UID, len(pods))
			for i := range pods {
				uids[i] = pods[i].UID
			}
			slices.Sort(uids)
			return uids
		}
		original := podUIDs(Default, member.ISVC.Name)
		for _, fixture := range []*coordFixture{member, local} {
			Eventually(func(g Gomega) {
				service := freshISVC(g, ns.Name, fixture.ISVC.Name)
				service.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
				g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}

		Eventually(func(g Gomega) {
			g.Expect(cmp.Diff(original, podUIDs(g, member.ISVC.Name))).To(BeEmpty(), "paused rollout must preserve the serving Pod")
			memberIR := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(memberIR.Spec.Runners[0].Template.Spec.Containers[0].Image).To(Equal("fake-serving:v2"))
			g.Expect(memberIR.Status.ObservedGeneration).To(Equal(memberIR.Generation))
			pods := listOMENativePods(g, ns.Name, local.ISVC.Name)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(pods[0].Spec.Containers[0].Image).To(Equal("fake-serving:v2"))
			g.Expect(podIsReady(&pods[0])).To(BeTrue())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			service := freshISVC(g, ns.Name, member.ISVC.Name)
			floor := 2
			service.Spec.Engine.MinReplicas = &floor
			g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, member.ISVC.Name)
			g.Expect(pods).To(HaveLen(2), "assigned floor growth must remain possible during a rollout pause")
			g.Expect(podUIDs(g, member.ISVC.Name)).To(ContainElement(original[0]))
			for i := range pods {
				g.Expect(podIsReady(&pods[i])).To(BeTrue())
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		policy.PlanID, policy.Revision, policy.PauseSurge = "plan-b", 2, false
		raw, err = protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			service := freshISVC(g, ns.Name, member.ISVC.Name)
			service.Annotations[constants.PlacementExecution] = raw
			g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(cmp.Diff(policy, ir.Spec.PlacementExecution)).To(BeEmpty())
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(2)))
			pods := listOMENativePods(g, ns.Name, member.ISVC.Name)
			g.Expect(pods).To(HaveLen(2))
			g.Expect(podUIDs(g, member.ISVC.Name)).NotTo(ContainElement(original[0]))
			for i := range pods {
				g.Expect(pods[i].Spec.Containers[0].Image).To(Equal("fake-serving:v2"))
				g.Expect(podIsReady(&pods[i])).To(BeTrue())
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
	ginkgo.It("defers member autoscaler growth while local scale and assigned floors advance", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "placement-scale-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed()) })
		policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true}
		raw, err := protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		member := newCoordFixture(ns.Name, WithISVCName("member-scale"), WithoutDecoder())
		local := newCoordFixture(ns.Name, WithISVCName("local-scale"), WithoutDecoder())
		for _, fixture := range []*coordFixture{member, local} {
			fixture.ISVC.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
			if fixture.ISVC.Annotations == nil {
				fixture.ISVC.Annotations = map[string]string{}
			}
		}
		member.ISVC.Labels = map[string]string{constants.PlacementOrigin: string(policy.SourceUID)}
		member.ISVC.Annotations[constants.PlacementExecution] = raw
		local.ISVC.Annotations[constants.PlacementExecution] = "irrelevant to local scale"
		for _, fixture := range []*coordFixture{member, local} {
			applyFixture(Default, fixture)
			deferFixtureCleanup(fixture)
		}
		Eventually(func(g Gomega) {
			for _, fixture := range []*coordFixture{member, local} {
				ir := freshInferenceReplica(g, ns.Name, fixture.ISVC.Name, v1beta1.EngineComponent)
				g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
				g.Expect(ir.Status.ServingReplicas).To(Equal(int32(1)))
				if fixture == member {
					g.Expect(ir.Spec.PlacementReplicaLimit).To(HaveValue(Equal(int32(1))))
					g.Expect(ir.Status.PlacementObservedGeneration).To(Equal(ir.Generation))
				} else {
					g.Expect(ir.Spec.PlacementReplicaLimit).To(BeNil())
					g.Expect(ir.Status.PlacementObservedGeneration).To(BeZero())
				}
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		for _, fixture := range []*coordFixture{member, local} {
			Eventually(func(g Gomega) {
				ir := freshInferenceReplica(g, ns.Name, fixture.ISVC.Name, v1beta1.EngineComponent)
				scale := &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: ir.Name, Namespace: ir.Namespace, ResourceVersion: ir.ResourceVersion}, Spec: autoscalingv1.ScaleSpec{Replicas: 3}}
				g.Expect(k8sClient.SubResource("scale").Update(suiteCtx, ir, client.WithSubResourceBody(scale))).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, local.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(3)))
			g.Expect(listOMENativePods(g, ns.Name, local.ISVC.Name)).To(HaveLen(3))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Replicas).To(HaveValue(Equal(int32(3))))
			g.Expect(ir.Spec.PlacementReplicaLimit).To(HaveValue(Equal(int32(1))))
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(1)))
			g.Expect(listOMENativePods(g, ns.Name, member.ISVC.Name)).To(HaveLen(1))
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		policy.PlanID, policy.Revision = "plan-b", 2
		raw, err = protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			service := freshISVC(g, ns.Name, member.ISVC.Name)
			floor := 2
			service.Spec.Engine.MinReplicas = &floor
			service.Annotations[constants.PlacementExecution] = raw
			g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Replicas).To(HaveValue(Equal(int32(3))))
			g.Expect(ir.Spec.PlacementReplicaLimit).To(HaveValue(Equal(int32(2))))
			g.Expect(ir.Status.PlacementObservedGeneration).To(Equal(ir.Generation))
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(2)))
			g.Expect(listOMENativePods(g, ns.Name, member.ISVC.Name)).To(HaveLen(2))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		policy.PlanID, policy.Revision, policy.PauseSurge = "plan-c", 3, false
		raw, err = protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			service := freshISVC(g, ns.Name, member.ISVC.Name)
			service.Annotations[constants.PlacementExecution] = raw
			g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.PlacementReplicaLimit).To(BeNil())
			g.Expect(ir.Status.PlacementObservedGeneration).To(Equal(ir.Generation))
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(3)))
			g.Expect(listOMENativePods(g, ns.Name, member.ISVC.Name)).To(HaveLen(3))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

})
