package scaleout_kind

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

var _ = ginkgo.Describe("Member service recreation inventory (KIND)", func() {
	ginkgo.It("waits for a retained component before recreating its missing service", func() {
		env := setupSpecEnv()
		model, runtime := env.seedModelRuntimeOnMembers()
		member := env.members[0]
		source := runnableSourceISVC(env.isvcNS, env.isvcName, model, runtime, v1beta1.PlacementModeAll, 1)
		source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{member.wcName}}}}}
		env.applyEngineResources(source)
		env.routeToSpecQueue(source)
		mustCreate(controlCluster, source)
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, source)
			gomega.Eventually(func() bool { return derivedSweptOn(member.c, env.isvcNS, env.isvcName) }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue())
		})
		key := client.ObjectKeyFromObject(source)
		componentKey := client.ObjectKey{Namespace: source.Namespace, Name: source.Name + "-engine"}
		const retainedFinalizer = "example.com/retain-component"
		var serviceUID, componentUID types.UID
		gomega.Eventually(func(g gomega.Gomega) {
			derived := &v1beta1.InferenceService{}
			g.Expect(member.c.client.Get(member.c.ctx, key, derived)).To(gomega.Succeed())
			g.Expect(readyEnginePodsOn(g, member.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			component := &v1beta1.InferenceReplica{}
			g.Expect(member.c.client.Get(member.c.ctx, componentKey, component)).To(gomega.Succeed())
			controllerutil.AddFinalizer(component, retainedFinalizer)
			g.Expect(member.c.client.Update(member.c.ctx, component)).To(gomega.Succeed())
			serviceUID, componentUID = derived.UID, component.UID
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		releaseComponent := func() {
			gomega.Eventually(func(g gomega.Gomega) {
				component := &v1beta1.InferenceReplica{}
				err := member.c.client.Get(member.c.ctx, componentKey, component)
				if apierrors.IsNotFound(err) {
					return
				}
				g.Expect(err).NotTo(gomega.HaveOccurred())
				if component.UID == componentUID && controllerutil.RemoveFinalizer(component, retainedFinalizer) {
					g.Expect(member.c.client.Update(member.c.ctx, component)).To(gomega.Succeed())
				}
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
		ginkgo.DeferCleanup(releaseComponent)

		ginkgo.By("keeping component execution authority after forced service removal")
		derived := &v1beta1.InferenceService{}
		gomega.Expect(member.c.client.Get(member.c.ctx, key, derived)).To(gomega.Succeed())
		gomega.Expect(member.c.client.Delete(member.c.ctx, derived)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			component := &v1beta1.InferenceReplica{}
			g.Expect(member.c.client.Get(member.c.ctx, componentKey, component)).To(gomega.Succeed())
			g.Expect(cmp.Diff(componentUID, component.UID)).To(gomega.BeEmpty())
			g.Expect(component.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
			err := member.c.client.Get(member.c.ctx, key, derived)
			if apierrors.IsNotFound(err) {
				return
			}
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(cmp.Diff(serviceUID, derived.UID)).To(gomega.BeEmpty())
			g.Expect(derived.DeletionTimestamp.IsZero()).To(gomega.BeFalse())
			derived.Finalizers = nil
			g.Expect(client.IgnoreNotFound(member.c.client.Update(member.c.ctx, derived))).To(gomega.Succeed())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			condition := source.Status.GetCondition(v1beta1.PlacementConverged)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff("MemberApplicationFailed", condition.Reason)).To(gomega.BeEmpty())
			g.Expect(apierrors.IsNotFound(member.c.client.Get(member.c.ctx, key, &v1beta1.InferenceService{}))).To(gomega.BeTrue())
			component := &v1beta1.InferenceReplica{}
			g.Expect(member.c.client.Get(member.c.ctx, componentKey, component)).To(gomega.Succeed())
			g.Expect(cmp.Diff(componentUID, component.UID)).To(gomega.BeEmpty())
			candidate := candidateFor(source.Status.Placement, member.wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(int32(1), candidate.Allocation.CurrentReplicas)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("recreating the service after the retained component is removed")
		releaseComponent()
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(member.c.client.Get(member.c.ctx, key, derived)).To(gomega.Succeed())
			g.Expect(derived.UID).NotTo(gomega.Equal(serviceUID))
			g.Expect(readyEnginePodsOn(g, member.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			component := &v1beta1.InferenceReplica{}
			g.Expect(member.c.client.Get(member.c.ctx, componentKey, component)).To(gomega.Succeed())
			g.Expect(component.UID).NotTo(gomega.Equal(componentUID))
			g.Expect(controllerutil.ContainsFinalizer(component, retainedFinalizer)).To(gomega.BeFalse())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})
})
