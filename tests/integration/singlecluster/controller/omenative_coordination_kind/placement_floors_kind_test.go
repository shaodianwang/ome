package omenative_coordination_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/protocol"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("placement member replica floors", func() {
	ginkgo.It("retains zero-floor member authority while external scaling grows and returns to zero", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "placement-zero-floor-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed()) })
		member := newCoordFixture(ns.Name, WithISVCName("member-zero-floor"), WithoutDecoder())
		local := newCoordFixture(ns.Name, WithISVCName("local-zero-floor"), WithoutDecoder())
		policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a",
			ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}}}
		raw, err := protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		member.ISVC.Labels = map[string]string{constants.PlacementOrigin: string(policy.SourceUID)}
		for _, fixture := range []*coordFixture{member, local} {
			fixture.ISVC.Spec.Engine.MinReplicas = ptr.To(0)
			fixture.ISVC.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
			if fixture.ISVC.Annotations == nil {
				fixture.ISVC.Annotations = map[string]string{}
			}
			fixture.ISVC.Annotations[constants.PlacementExecution] = raw
			deferFixtureCleanup(fixture)
			applyFixture(Default, fixture)
		}

		identities := map[string]types.UID{}
		check := func(replicas int32) {
			Eventually(func(g Gomega) {
				for _, fixture := range []*coordFixture{member, local} {
					wantReplicas := replicas
					if fixture == local && replicas == 0 {
						wantReplicas = 1
					}
					service := freshISVC(g, ns.Name, fixture.ISVC.Name)
					g.Expect(cmp.Diff(ptr.To(0), service.Spec.Engine.MinReplicas)).To(BeEmpty())
					ir := freshInferenceReplica(g, ns.Name, fixture.ISVC.Name, v1beta1.EngineComponent)
					if uid, tracked := identities[fixture.ISVC.Name]; tracked {
						g.Expect(cmp.Diff(uid, ir.UID)).To(BeEmpty())
					}
					wantPolicy := policy
					if fixture == local {
						wantPolicy = nil
					}
					g.Expect(cmp.Diff(wantPolicy, ir.Spec.PlacementExecution)).To(BeEmpty())
					g.Expect(cmp.Diff(ptr.To(wantReplicas), ir.Spec.Replicas)).To(BeEmpty())
					g.Expect(ir.Spec.PlacementReplicaLimit).To(BeNil())
					g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
					g.Expect(ir.Status.ReadyReplicas).To(Equal(wantReplicas))
					pods := listOMENativePods(g, ns.Name, fixture.ISVC.Name)
					g.Expect(pods).To(HaveLen(int(wantReplicas)))
					for i := range pods {
						g.Expect(podIsReady(&pods[i])).To(BeTrue())
					}
				}
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}
		check(0)
		for _, fixture := range []*coordFixture{member, local} {
			identities[fixture.ISVC.Name] = freshInferenceReplica(Default, ns.Name, fixture.ISVC.Name, v1beta1.EngineComponent).UID
		}
		for _, replicas := range []int32{2, 0} {
			for _, fixture := range []*coordFixture{member, local} {
				Eventually(func(g Gomega) {
					ir := freshInferenceReplica(g, ns.Name, fixture.ISVC.Name, v1beta1.EngineComponent)
					scale := &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: ir.Name, Namespace: ir.Namespace, ResourceVersion: ir.ResourceVersion}, Spec: autoscalingv1.ScaleSpec{Replicas: replicas}}
					g.Expect(k8sClient.SubResource("scale").Update(suiteCtx, ir, client.WithSubResourceBody(scale))).To(Succeed())
				}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
			}
			check(replicas)
		}
	})

	ginkgo.It("requires accepted runtime floors while local services follow runtime scaling", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "placement-floors-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed()) })
		member := newCoordFixture(ns.Name, WithISVCName("member-floor"), WithoutDecoder())
		local := newCoordFixture(ns.Name, WithISVCName("local-floor"), WithoutDecoder())
		policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true,
			ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 1}}}
		member.ISVC.Labels = map[string]string{constants.PlacementOrigin: string(policy.SourceUID)}
		raw, err := protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		for _, fixture := range []*coordFixture{member, local} {
			if fixture.CSR.Spec.EngineConfig == nil {
				fixture.CSR.Spec.EngineConfig = &v1beta1.EngineSpec{}
			}
			fixture.CSR.Spec.EngineConfig.MinReplicas = ptr.To(2)
			fixture.ISVC.Spec.Engine.MinReplicas = nil
			if fixture.ISVC.Annotations == nil {
				fixture.ISVC.Annotations = map[string]string{}
			}
			fixture.ISVC.Annotations[constants.PlacementExecution] = raw
			applyFixture(Default, fixture)
			deferFixtureCleanup(fixture)
		}
		Eventually(func(g Gomega) {
			g.Expect(freshInferenceReplica(g, ns.Name, local.ISVC.Name, v1beta1.EngineComponent).Status.ServingReplicas).To(Equal(int32(2)))
			condition := freshISVC(g, ns.Name, member.ISVC.Name).Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(corev1.ConditionFalse))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: member.ISVC.Name + "-engine"}, &v1beta1.InferenceReplica{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
			g.Expect(listOMENativePods(g, ns.Name, member.ISVC.Name)).To(BeEmpty())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		accept := func(floor int32) {
			policy.Revision++
			policy.ReplicaFloors[0].Replicas = floor
			raw, err := protocol.Encode(policy)
			Expect(err).NotTo(HaveOccurred())
			Eventually(func(g Gomega) {
				service := freshISVC(g, ns.Name, member.ISVC.Name)
				service.Annotations[constants.PlacementExecution] = raw
				g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
			Eventually(func(g Gomega) {
				ir := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
				g.Expect(cmp.Diff(policy, ir.Spec.PlacementExecution)).To(BeEmpty(), "CRD storage must retain the floor contract")
				g.Expect(ir.Spec.PlacementReplicaLimit).To(HaveValue(Equal(floor)))
				g.Expect(ir.Status.PlacementObservedGeneration).To(Equal(ir.Generation))
				g.Expect(ir.Status.ServingReplicas).To(Equal(floor))
				condition := freshISVC(g, ns.Name, member.ISVC.Name).Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
				g.Expect(condition).NotTo(BeNil())
				g.Expect(condition.Status).To(Equal(corev1.ConditionTrue))
			}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}
		accept(2)
		standing := freshInferenceReplica(Default, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
		original := listOMENativePods(Default, ns.Name, member.ISVC.Name)
		Expect(original).To(HaveLen(2))
		for _, fixture := range []*coordFixture{member, local} {
			Eventually(func(g Gomega) {
				runtime := &v1beta1.ClusterServingRuntime{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fixture.CSR), runtime)).To(Succeed())
				runtime.Spec.EngineConfig.MinReplicas = ptr.To(3)
				g.Expect(k8sClient.Update(suiteCtx, runtime)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, local.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(3)))
			g.Expect(ir.Spec.PlacementExecution).To(BeNil())
			condition := freshISVC(g, ns.Name, member.ISVC.Name).Status.GetCondition(v1beta1.PlacementReplicaFloorsReady)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(corev1.ConditionFalse))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			g.Expect(cmp.Diff(standing.Spec, freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent).Spec)).To(BeEmpty())
			pods := listOMENativePods(g, ns.Name, member.ISVC.Name)
			g.Expect(pods).To(HaveLen(2))
			for _, old := range original {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(&old), pod)).To(Succeed())
				g.Expect(cmp.Diff(old.UID, pod.UID)).To(BeEmpty())
				g.Expect(podIsReady(pod)).To(BeTrue())
			}
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		accept(3)
	})
})
