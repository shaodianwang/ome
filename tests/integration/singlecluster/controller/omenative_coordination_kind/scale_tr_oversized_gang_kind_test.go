package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Oversized-gang wave admission. The suite budget is two Pods
// (kindLifecycleConfig), while a WorkerSize=2 gang costs three live
// Pods. Bounded scale-down must admit the first eligible gang ALONE via
// the oversized-Instance exception — a gang is indivisible, so refusing
// it outright would deadlock the scale-down forever — and it must not
// pile any other victim on top of the oversized wave.
//
// Invariants asserted:
//   - the first wave is exactly the highest extra index, despite its
//     cost exceeding the budget;
//   - all gang member Pods enter deletion together (selection is
//     gang-atomic);
//   - the deferred gang receives zero effects while the oversized wave
//     is open, and its PodGroup stays untouched;
//   - the next oversized gang is admitted only after the first wave's
//     status is removed, and the Component converges to one gang.
var _ = ginkgo.Describe("OMENative scale transitions — oversized gang wave (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "scale-tr-oversized-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("admits a three-pod gang alone when its cost exceeds the two-pod budget", func() {
		const isvcName = "scale-tr-oversized"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineWorkerSize(2),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for three Ready three-pod gangs and their PodGroups")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(3))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(boundedScaleDownPodsForInstance(g, ns.Name, isvcName, status.Index)).To(HaveLen(3))
				pg := &schedulingv1alpha1.PodGroup{}
				pgName := query.PodGroupName(isvcName, workloadtypes.ComponentEngine, status.Index)
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: pgName}, pg)).To(Succeed())
				g.Expect(pg.Spec.MinMember).To(Equal(int32(3)))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		victims := make(map[int32][]client.ObjectKey, 2)
		for index := int32(1); index < 3; index++ {
			pods := boundedScaleDownPodsForInstance(Default, ns.Name, isvcName, index)
			Expect(pods).To(HaveLen(3))
			for i := range pods {
				key := client.ObjectKeyFromObject(&pods[i])
				victims[index] = append(victims[index], key)
				setBoundedScaleDownHold(key, true)
			}
		}
		ginkgo.DeferCleanup(func() {
			for _, keys := range victims {
				for _, key := range keys {
					setBoundedScaleDownHold(key, false)
				}
			}
		})

		ginkgo.By("reducing desired gangs from three to one")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			one := 1
			current.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("observing the highest gang admitted alone despite exceeding the budget")
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{2}), "the oversized gang must be admitted alone, highest index first")
			activePods := boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 2)
			g.Expect(activePods).To(HaveLen(3))
			for _, pod := range activePods {
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil(), "every member of the oversized gang must enter deletion together")
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("holding the oversized wave open and asserting no other victim is admitted")
		Consistently(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{2}), "the oversized exception must never pile a second victim onto active work")
			deferredPods := boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 1)
			g.Expect(deferredPods).To(HaveLen(3))
			for _, pod := range deferredPods {
				g.Expect(pod.DeletionTimestamp).To(BeNil(), "the deferred gang must receive zero scale-down effects")
			}
			deferredPG := &schedulingv1alpha1.PodGroup{}
			deferredPGName := query.PodGroupName(isvcName, workloadtypes.ComponentEngine, 1)
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: deferredPGName}, deferredPG)).To(Succeed())
			g.Expect(deferredPG.DeletionTimestamp).To(BeNil(), "the deferred gang's PodGroup must stay untouched")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the first gang and observing the second oversized wave")
		for _, key := range victims[2] {
			setBoundedScaleDownHold(key, false)
		}
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{1}), "the next gang must be admitted only after the first wave closes")
			pgName := query.PodGroupName(isvcName, workloadtypes.ComponentEngine, 2)
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: pgName}, &schedulingv1alpha1.PodGroup{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the completed gang's PodGroup must be finalized; err=%v", err)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the second gang and converging with one gang")
		for _, key := range victims[1] {
			setBoundedScaleDownHold(key, false)
		}
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(1))
			g.Expect(statuses[0].Index).To(Equal(int32(0)))
			g.Expect(statuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 0)).To(HaveLen(3))
			pgs := &schedulingv1alpha1.PodGroupList{}
			g.Expect(k8sClient.List(suiteCtx, pgs, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(pgs.Items).To(HaveLen(1))
			g.Expect(pgs.Items[0].Name).To(Equal(query.PodGroupName(isvcName, workloadtypes.ComponentEngine, 0)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
