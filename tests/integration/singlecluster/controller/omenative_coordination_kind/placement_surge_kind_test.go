package omenative_coordination_kind

import (
	"slices"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/components"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/specdefaults"
	"github.com/ome-projects/ome/pkg/placement/capacity"
	"github.com/ome-projects/ome/pkg/placement/protocol"
	"github.com/ome-projects/ome/pkg/placement/resolution"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("placement member surge coordination", func() {
	ginkgo.It("requires matching demand before first projection and protects serving replicas on mismatch", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "placement-demand-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed()) })
		class := &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: ns.Name + "-runtime"}, Handler: "runc",
			Overhead:   &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")}},
			Scheduling: &nodev1.Scheduling{NodeSelector: map[string]string{corev1.LabelOSStable: "linux"}},
		}
		Expect(k8sClient.Create(suiteCtx, class)).To(Succeed())
		ginkgo.DeferCleanup(func() { Expect(k8sClient.Delete(suiteCtx, class)).To(Succeed()) })
		member := newCoordFixture(ns.Name, WithISVCName("member-demand"), WithoutDecoder())
		member.ISVC.Spec.Engine.RuntimeClassName = &class.Name
		local := newCoordFixture(ns.Name, WithISVCName("local-demand"), WithoutDecoder(), WithISVCAnnotation(constants.PlacementExecution, "irrelevant to local rendering"))
		kindfix.ApplyModelRuntime(suiteCtx, Default, k8sClient, member.CBM, member.CSR, kindfix.ApplyModelRuntimeOpts{CachedReader: kindutil.Suite().Manager.GetClient()})
		deferFixtureCleanup(member)
		hash := fixtureEngineRenderingHash(Default, member.ISVC)
		policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", Demand: &v1beta1.PlacementDemandContract{
			Fingerprint: strings.Repeat("a", 64), Components: []v1beta1.PlacementComponentDemand{{Component: v1beta1.EngineComponent, RenderingHash: strings.Repeat("b", 64)}},
		}}
		member.ISVC.Labels = map[string]string{constants.PlacementOrigin: string(policy.SourceUID)}
		raw, err := protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		if member.ISVC.Annotations == nil {
			member.ISVC.Annotations = map[string]string{}
		}
		member.ISVC.Annotations[constants.PlacementExecution] = raw
		Expect(k8sClient.Create(suiteCtx, member.ISVC)).To(Succeed())
		applyFixture(Default, local)
		deferFixtureCleanup(local)
		Eventually(func(g Gomega) {
			g.Expect(freshInferenceReplica(g, ns.Name, local.ISVC.Name, v1beta1.EngineComponent).Status.ServingReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: member.ISVC.Name + "-engine"}, &v1beta1.InferenceReplica{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "unverified demand must not create a workload")
			g.Expect(listOMENativePods(g, ns.Name, member.ISVC.Name)).To(BeEmpty())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		policy.Demand.Components[0].RenderingHash = hash
		raw, err = protocol.Encode(policy)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			service := freshISVC(g, ns.Name, member.ISVC.Name)
			service.Annotations[constants.PlacementExecution] = raw
			g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(1)))
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
			g.Expect(cmp.Diff(policy, ir.Spec.PlacementExecution)).To(BeEmpty(), "the API must retain the demand contract")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		standing := freshInferenceReplica(Default, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
		originalPods := listOMENativePods(Default, ns.Name, member.ISVC.Name)
		Expect(originalPods).To(HaveLen(1))
		Expect(cmp.Diff("10m", originalPods[0].Spec.Overhead.Cpu().String())).To(BeEmpty())
		Expect(cmp.Diff("linux", originalPods[0].Spec.NodeSelector[corev1.LabelOSStable])).To(BeEmpty())
		for _, fixture := range []*coordFixture{member, local} {
			Eventually(func(g Gomega) {
				service := freshISVC(g, ns.Name, fixture.ISVC.Name)
				service.Spec.Engine.Containers[0].Image = "fake-serving:v2"
				floor := 2
				service.Spec.Engine.MinReplicas = &floor
				g.Expect(k8sClient.Update(suiteCtx, service)).To(Succeed())
			}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		}
		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, local.ISVC.Name)
			g.Expect(pods).To(HaveLen(2))
			for i := range pods {
				g.Expect(pods[i].Spec.Containers[0].Image).To(Equal("fake-serving:v2"))
				g.Expect(podIsReady(&pods[i])).To(BeTrue())
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, member.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(cmp.Diff(standing.Spec, ir.Spec)).To(BeEmpty())
			pods := listOMENativePods(g, ns.Name, member.ISVC.Name)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(cmp.Diff(originalPods[0].UID, pods[0].UID)).To(BeEmpty())
			g.Expect(podIsReady(&pods[0])).To(BeTrue())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		pod := standing.Spec.Runners[0].Template.Spec.DeepCopy()
		pod.Containers[0].Image = "fake-serving:v2"
		hash, err = capacity.ComponentFingerprint(suiteCtx, k8sClient, v1beta1.EngineComponent, constants.OMENative, []capacity.PodSet{{Name: "primary", Count: 1, Spec: pod}})
		Expect(err).NotTo(HaveOccurred())
		policy.Revision++
		policy.Demand.Components[0].RenderingHash = hash
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
			g.Expect(ir.Status.ServingReplicas).To(Equal(int32(2)))
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
			pods := listOMENativePods(g, ns.Name, member.ISVC.Name)
			g.Expect(pods).To(HaveLen(2))
			for i := range pods {
				g.Expect(pods[i].Spec.Containers[0].Image).To(Equal("fake-serving:v2"))
				g.Expect(podIsReady(&pods[i])).To(BeTrue())
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
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

func fixtureEngineRenderingHash(g Gomega, service *v1beta1.InferenceService) string {
	rt, err := (resolution.Resolver{Client: k8sClient, OperatorNamespace: constants.OMENamespace}).Resolve(suiteCtx, service, nil)
	g.Expect(err).NotTo(HaveOccurred())
	cm := &corev1.ConfigMap{}
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName}, cm)).To(Succeed())
	cfg, deploy, err := controllerconfig.RenderingConfig(cm)
	g.Expect(err).NotTo(HaveOccurred())
	specdefaults.Engine(rt.Engine, constants.OMENative, deploy)
	templates, err := components.RenderEngineReplica(suiteCtx, &components.ComponentDeps{Client: k8sClient, Config: cfg}, components.ComponentInputs{
		DeploymentMode: constants.OMENative, Runtime: rt.Spec, RuntimeName: rt.Name, BaseModel: rt.Model, BaseModelMeta: rt.ModelMeta,
		ModelFormat: runtimeselector.New(k8sClient).GetSupportedModelFormat(suiteCtx, rt.Spec, rt.Model, true),
	}, service, rt.Engine)
	g.Expect(err).NotTo(HaveOccurred())
	sets, err := templates.PodSets(constants.OMENative, false, false)
	g.Expect(err).NotTo(HaveOccurred())
	hash, err := capacity.ComponentFingerprint(suiteCtx, k8sClient, v1beta1.EngineComponent, constants.OMENative, sets)
	g.Expect(err).NotTo(HaveOccurred())
	return hash
}
