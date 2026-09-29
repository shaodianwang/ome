package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

var _ = ginkgo.Describe("Single placement recovery after home loss (KIND)", func() {
	ginkgo.It("restarts admission with current affinity after a handed-off destination disappears", func() {
		env := setupSpecEnv(withKueueManagedNamespaces(), withEngineCPU("500m"))
		model, runtime := env.seedModelRuntimeOnMembers()
		original, destination := env.members[0], env.members[1]
		for _, member := range env.members {
			env.grantQuota(member, env.cpuForReplicas(1))
		}
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeSingle, 1)
		selectHome := func(name string) {
			source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}}}
		}
		selectHome(original.wcName)
		source.Spec.Placement.MaxSurge = ptr.To[int32](1)
		env.applyEngineResources(source)
		env.routeToSpecQueue(source)
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, source)
			for _, member := range env.members {
				gomega.Eventually(func() bool { return derivedSweptOn(member.c, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
			}
		})
		key := client.ObjectKeyFromObject(source)
		publish := capacityRoutePublisher(env)
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(original.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			g.Expect(readyEnginePodsOn(g, original.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		const retainedFinalizer = "example.com/verify-placement-cleanup"
		removeRetainedFinalizer := func() {
			gomega.Eventually(func(g gomega.Gomega) {
				member := &v1beta1.InferenceService{}
				err := original.c.client.Get(original.c.ctx, key, member)
				if apierrors.IsNotFound(err) {
					return
				}
				g.Expect(err).NotTo(gomega.HaveOccurred())
				if controllerutil.RemoveFinalizer(member, retainedFinalizer) {
					g.Expect(original.c.client.Update(original.c.ctx, member)).To(gomega.Succeed())
				}
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
		ginkgo.DeferCleanup(removeRetainedFinalizer)
		ginkgo.By("retaining the original service object so handoff cleanup stays active")
		gomega.Eventually(func(g gomega.Gomega) {
			member := &v1beta1.InferenceService{}
			g.Expect(original.c.client.Get(original.c.ctx, key, member)).To(gomega.Succeed())
			controllerutil.AddFinalizer(member, retainedFinalizer)
			g.Expect(original.c.client.Update(original.c.ctx, member)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			selectHome(destination.wcName)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.SingleMove).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(destination.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			member := &v1beta1.InferenceService{}
			g.Expect(original.c.client.Get(original.c.ctx, key, member)).To(gomega.Succeed())
			g.Expect(member.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
			g.Expect(readyEnginePodsOn(g, destination.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("losing the destination while new placement is temporarily ineligible")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			selectHome("unregistered-member")
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		member, ok := getDerivedOn(destination.c, env.isvcNS, env.isvcName)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(destination.c.client.Delete(destination.c.ctx, member)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			_, exists := getDerivedOn(destination.c, env.isvcNS, env.isvcName)
			g.Expect(exists).To(gomega.BeFalse())
			g.Expect(enginePodsOn(g, destination.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.SingleMove).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(destination.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("finishing physical cleanup and recovering through the configured loss grace")
		removeRetainedFinalizer()
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			selectHome(original.wcName)
			g.Expect(controlCluster.client.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.SingleMove).To(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
			g.Expect(cmp.Diff(original.wcName, source.Status.Placement.Plan.Winner)).To(gomega.BeEmpty())
			g.Expect(readyEnginePodsOn(g, original.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			g.Expect(enginePodsOn(g, destination.c, env.isvcNS, env.isvcName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
