package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

var _ = ginkgo.Describe("Multicluster member backend guard (KIND)", func() {
	ginkgo.It("holds opted-in backend changes and preserves local and legacy raw services", func() {
		env := setupSpecEnv()
		model, runtime := env.seedModelRuntimeOnMembers()
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeSingle, 1)
		source.Spec.DeploymentMode = ptr.To(constants.OMENative)
		source.Spec.Engine.Annotations = nil
		env.applyEngineResources(source)
		env.routeToSpecQueue(source)
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, source)
			for _, m := range env.members {
				gomega.Eventually(func() bool { return derivedSweptOn(m.c, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
			}
		})
		winner := env.awaitWinner()
		member := winner.c
		key := client.ObjectKeyFromObject(source)
		derived := &v1beta1.InferenceService{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(member.client.Get(member.ctx, key, derived)).To(gomega.Succeed())
			g.Expect(derived.Spec.Engine.Annotations[constants.DeploymentMode]).To(gomega.BeEmpty())
			g.Expect(runningEnginePodsOn(g, member, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		podUID := enginePodsOn(gomega.Default, member, env.isvcNS, env.isvcName)[0].UID
		originalRuntime := &v1beta1.ClusterServingRuntime{}
		gomega.Expect(member.client.Get(member.ctx, client.ObjectKey{Name: runtime}, originalRuntime)).To(gomega.Succeed())
		changeRuntime := func(mode constants.DeploymentModeType) {
			gomega.Eventually(func(g gomega.Gomega) {
				rt := &v1beta1.ClusterServingRuntime{}
				g.Expect(member.client.Get(member.ctx, client.ObjectKey{Name: runtime}, rt)).To(gomega.Succeed())
				rt.Spec.EngineConfig = originalRuntime.Spec.EngineConfig.DeepCopy()
				if mode != constants.OMENative {
					if rt.Spec.EngineConfig == nil {
						rt.Spec.EngineConfig = &v1beta1.EngineSpec{}
					}
					rt.Spec.EngineConfig.Annotations = map[string]string{constants.DeploymentMode: string(mode)}
				}
				g.Expect(member.client.Update(member.ctx, rt)).To(gomega.Succeed())
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
		awaitCondition := func(status corev1.ConditionStatus) {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(member.client.Get(member.ctx, key, derived)).To(gomega.Succeed())
				condition := derived.Status.GetCondition(v1beta1.PlacementBackendReady)
				g.Expect(condition).NotTo(gomega.BeNil())
				g.Expect(condition.Status).To(gomega.Equal(status))
				current := &v1beta1.InferenceService{}
				g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
				sourceCondition := current.Status.GetCondition(v1beta1.PlacementBackendReady)
				g.Expect(sourceCondition).NotTo(gomega.BeNil())
				g.Expect(sourceCondition.Status).To(gomega.Equal(status))
				g.Expect(current.Status.Placement.Cluster).To(gomega.Equal(winner.wcName))
				pods := enginePodsOn(g, member, env.isvcNS, env.isvcName)
				g.Expect(pods).To(gomega.HaveLen(1))
				g.Expect(cmp.Diff(podUID, pods[0].UID)).To(gomega.BeEmpty())
				g.Expect(runningEnginePodsOn(g, member, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
		changeRuntime(constants.RawDeployment)
		awaitCondition(corev1.ConditionFalse)

		unplannedServices := map[client.ObjectKey]types.UID{}
		for _, tc := range []struct{ name, origin string }{
			{name: "local-raw"},
			{name: "legacy-raw", origin: "legacy-source"},
		} {
			local := runnableSourceISVC(env.isvcNS, uniqueName(tc.name), model, runtime, v1beta1.PlacementModeSingle, 1)
			local.Spec.Placement = nil
			local.Spec.DeploymentMode = ptr.To(constants.RawDeployment)
			local.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
			local.Spec.Engine.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
			if tc.origin != "" {
				local.Annotations[constants.PlacementOriginUID] = tc.origin
			}
			env.applyEngineResources(local)
			mustCreate(member, local)
			ginkgo.DeferCleanup(func() { deleteIfExists(member, local) })
			gomega.Eventually(func(g gomega.Gomega) {
				deployment := &appsv1.Deployment{}
				g.Expect(member.client.Get(member.ctx, client.ObjectKey{Namespace: local.Namespace, Name: local.Name + "-engine"}, deployment)).To(gomega.Succeed())
				g.Expect(deployment.Status.AvailableReplicas).To(gomega.Equal(int32(1)))
				unplannedServices[client.ObjectKeyFromObject(deployment)] = deployment.UID
				g.Expect(member.client.Get(member.ctx, client.ObjectKeyFromObject(local), local)).To(gomega.Succeed())
				g.Expect(local.Status.GetCondition(v1beta1.PlacementBackendReady)).To(gomega.BeNil())
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}

		awaitCondition(corev1.ConditionFalse)
		changeRuntime(constants.OMENative)
		awaitCondition(corev1.ConditionTrue)
		for key, uid := range unplannedServices {
			deployment := &appsv1.Deployment{}
			gomega.Expect(member.client.Get(member.ctx, key, deployment)).To(gomega.Succeed())
			gomega.Expect(cmp.Diff(uid, deployment.UID)).To(gomega.BeEmpty())
			gomega.Expect(deployment.Status.AvailableReplicas).To(gomega.Equal(int32(1)))
		}
		changeRuntime(constants.RawDeployment)
		awaitCondition(corev1.ConditionFalse)
		deleteIfExists(controlCluster, source)
		gomega.Eventually(func() bool { return derivedSweptOn(member, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
	})
})

var _ = ginkgo.Describe("Multicluster source backend preflight (KIND)", func() {
	ginkgo.It("retains the blocked static share and creates its workload after backend recovery", func() {
		env := setupSpecEnv()
		model, runtime := env.seedModelRuntimeOnMembers()
		blocked := env.members[1]
		original := &v1beta1.ClusterServingRuntime{}
		gomega.Expect(blocked.c.client.Get(blocked.c.ctx, client.ObjectKey{Name: runtime}, original)).To(gomega.Succeed())
		unsupported := original.DeepCopy()
		unsupported.Spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
		gomega.Expect(blocked.c.client.Update(blocked.c.ctx, unsupported)).To(gomega.Succeed())

		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeSplit, 2)
		source.Spec.DeploymentMode = ptr.To(constants.OMENative)
		source.Spec.Engine.Annotations = nil
		source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
		env.applyEngineResources(source)
		env.routeToSpecQueue(source)
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, source)
			for _, member := range env.members {
				gomega.Eventually(func() bool { return derivedSweptOn(member.c, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
			}
		})
		publish := capacityRoutePublisher(env)
		key := client.ObjectKeyFromObject(source)
		gomega.Eventually(func(g gomega.Gomega) {
			current := &v1beta1.InferenceService{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
			condition := current.Status.GetCondition(v1beta1.PlacementBackendReady)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(condition.Status).To(gomega.Equal(corev1.ConditionFalse))
			g.Expect(current.Status.Placement).NotTo(gomega.BeNil())
			for _, member := range env.members {
				candidate := candidateFor(current.Status.Placement, member.wcName)
				g.Expect(candidate).NotTo(gomega.BeNil())
				g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
				g.Expect(candidate.Allocation.DesiredReplicas).To(gomega.Equal(int32(1)))
			}
			g.Expect(candidateFor(current.Status.Placement, blocked.wcName).Allocation.CurrentReplicas).To(gomega.BeZero())
			g.Expect(runningEnginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			err := blocked.c.client.Get(blocked.c.ctx, key, &v1beta1.InferenceService{})
			g.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		standingUID := enginePodsOn(gomega.Default, env.members[0].c, env.isvcNS, env.isvcName)[0].UID
		gomega.Eventually(func(g gomega.Gomega) {
			current := &v1beta1.ClusterServingRuntime{}
			g.Expect(blocked.c.client.Get(blocked.c.ctx, client.ObjectKey{Name: runtime}, current)).To(gomega.Succeed())
			current.Spec.EngineConfig = original.Spec.EngineConfig.DeepCopy()
			g.Expect(blocked.c.client.Update(blocked.c.ctx, current)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			current := &v1beta1.InferenceService{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, current)).To(gomega.Succeed())
			condition := current.Status.GetCondition(v1beta1.PlacementBackendReady)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(condition.Status).To(gomega.Equal(corev1.ConditionTrue))
			publish(g)
			for _, member := range env.members {
				g.Expect(runningEnginePodsOn(g, member.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			}
			pods := enginePodsOn(g, env.members[0].c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(1))
			g.Expect(cmp.Diff(standingUID, pods[0].UID)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
