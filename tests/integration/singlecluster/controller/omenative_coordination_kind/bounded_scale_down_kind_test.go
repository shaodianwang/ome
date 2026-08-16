package omenative_coordination_kind

import (
	"sort"
	"strconv"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

const boundedScaleDownHoldFinalizer = "example.com/hold-scale-down"

func deleteOwnedInstanceIndices(statuses []v1beta1.OMENativeInstanceStatus) []int32 {
	indices := make([]int32, 0)
	for _, status := range statuses {
		if status.Phase != v1beta1.OMENativeInstanceDeleting || status.Operation == nil ||
			status.Operation.Type != v1beta1.InstanceOperationDelete {
			continue
		}
		indices = append(indices, status.Index)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	return indices
}

func setBoundedScaleDownHold(key client.ObjectKey, present bool) {
	Eventually(func(g Gomega) {
		pod := &corev1.Pod{}
		err := k8sClient.Get(suiteCtx, key, pod)
		if apierrors.IsNotFound(err) && !present {
			return
		}
		g.Expect(err).NotTo(HaveOccurred())
		if controllerutil.ContainsFinalizer(pod, boundedScaleDownHoldFinalizer) == present {
			return
		}
		if present {
			controllerutil.AddFinalizer(pod, boundedScaleDownHoldFinalizer)
		} else {
			controllerutil.RemoveFinalizer(pod, boundedScaleDownHoldFinalizer)
		}
		g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

func boundedScaleDownPodsForInstance(g Gomega, namespace, isvcName string, index int32) []corev1.Pod {
	pods := listOMENativePods(g, namespace, isvcName)
	selected := make([]corev1.Pod, 0)
	for i := range pods {
		if pods[i].Labels[query.LabelInstanceIdx] == strconv.Itoa(int(index)) {
			selected = append(selected, pods[i])
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Name < selected[j].Name })
	return selected
}

var _ = ginkgo.Describe("OMENative bounded scale-down (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "bounded-scaledown-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("scales a singleton Component 5 -> 1 in two highest-index-first waves", func() {
		const isvcName = "bounded-scaledown"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(5),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for five Ready singleton Instances")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(5))
			ready := make(map[int32]bool, len(statuses))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				ready[status.Index] = true
			}
			for index := int32(0); index < 5; index++ {
				g.Expect(ready).To(HaveKey(index))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		pod0 := aliveEnginePodForInstance(Default, ns.Name, isvcName, 0)
		pod0UID := pod0.UID
		victims := make(map[int32]client.ObjectKey, 4)
		for index := int32(1); index < 5; index++ {
			pod := aliveEnginePodForInstance(Default, ns.Name, isvcName, index)
			victims[index] = client.ObjectKeyFromObject(pod)
			setBoundedScaleDownHold(victims[index], true)
		}
		ginkgo.DeferCleanup(func() {
			for index := int32(1); index < 5; index++ {
				setBoundedScaleDownHold(victims[index], false)
			}
		})

		ginkgo.By("reducing desired replicas from five to one")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			one := 1
			current.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("observing the first budget-two wave select the highest indices")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			deleting := deleteOwnedInstanceIndices(statuses)
			g.Expect(deleting).To(HaveLen(2), "active Delete work must not exceed the configured Pod budget")
			g.Expect(deleting).To(Equal([]int32{3, 4}), "the first wave must prefer the highest extra indices")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			for _, index := range []int32{3, 4} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, victims[index], pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil(), "first-wave pod %d must enter deletion", index)
			}
			for _, index := range []int32{1, 2} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, victims[index], pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp).To(BeNil(), "deferred pod %d must remain untouched", index)
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("restarting the controller manager while the first wave is active")
		Expect(kindutil.Suite().RestartManager()).To(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{3, 4}), "the restarted controller must reconstruct the committed wave")
			for _, index := range []int32{3, 4} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, victims[index], pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil())
			}
			for _, index := range []int32{1, 2} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, victims[index], pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp).To(BeNil(), "restart must not admit a deferred victim")
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the first wave and observing the second")
		setBoundedScaleDownHold(victims[3], false)
		setBoundedScaleDownHold(victims[4], false)
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			deleting := deleteOwnedInstanceIndices(statuses)
			g.Expect(deleting).To(HaveLen(2), "active Delete work must remain within the configured Pod budget")
			g.Expect(deleting).To(Equal([]int32{1, 2}), "the second wave must start only after the first closes")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			for _, index := range []int32{1, 2} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, victims[index], pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil(), "second-wave pod %d must enter deletion", index)
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the second wave and waiting for steady state")
		setBoundedScaleDownHold(victims[1], false)
		setBoundedScaleDownHold(victims[2], false)
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(1))
			g.Expect(statuses[0].Index).To(Equal(int32(0)))
			g.Expect(statuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			remaining := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod0), remaining)).To(Succeed())
			g.Expect(remaining.UID).To(Equal(pod0UID), "scale-down must not replace instance 0")
			g.Expect(remaining.DeletionTimestamp).To(BeNil())
			for _, key := range victims {
				err := k8sClient.Get(suiteCtx, key, &corev1.Pod{})
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "victim Pod %s must be gone; err=%v", key.Name, err)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("keeps two-Pod gangs atomic and retains each PodGroup until its gang is gone", func() {
		const isvcName = "bounded-gang"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineWorkerSize(1),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for three Ready two-Pod gangs and their PodGroups")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(3))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(boundedScaleDownPodsForInstance(g, ns.Name, isvcName, status.Index)).To(HaveLen(2))
				pg := &schedulingv1alpha1.PodGroup{}
				pgName := query.PodGroupName(isvcName, workload.ComponentEngine, status.Index)
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: pgName}, pg)).To(Succeed())
				g.Expect(pg.Spec.MinMember).To(Equal(int32(2)))
				g.Expect(pg.DeletionTimestamp).To(BeNil())
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		victims := make(map[int32][]client.ObjectKey, 2)
		for index := int32(1); index < 3; index++ {
			pods := boundedScaleDownPodsForInstance(Default, ns.Name, isvcName, index)
			Expect(pods).To(HaveLen(2))
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

		ginkgo.By("observing one complete highest-index gang in the first budget-two wave")
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{2}))
			activePods := boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 2)
			g.Expect(activePods).To(HaveLen(2))
			for _, pod := range activePods {
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil(), "every selected gang member must enter deletion")
			}
			deferredPods := boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 1)
			g.Expect(deferredPods).To(HaveLen(2))
			for _, pod := range deferredPods {
				g.Expect(pod.DeletionTimestamp).To(BeNil(), "the deferred gang must remain untouched")
			}

			activePG := &schedulingv1alpha1.PodGroup{}
			activePGName := query.PodGroupName(isvcName, workload.ComponentEngine, 2)
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: activePGName}, activePG)).To(Succeed())
			g.Expect(activePG.DeletionTimestamp).To(BeNil(), "PodGroup must outlive terminating member Pods")
			deferredPG := &schedulingv1alpha1.PodGroup{}
			deferredPGName := query.PodGroupName(isvcName, workload.ComponentEngine, 1)
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: deferredPGName}, deferredPG)).To(Succeed())
			g.Expect(deferredPG.DeletionTimestamp).To(BeNil())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the first gang and observing PodGroup finalization before the next wave")
		for _, key := range victims[2] {
			setBoundedScaleDownHold(key, false)
		}
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{1}))

			firstPGName := query.PodGroupName(isvcName, workload.ComponentEngine, 2)
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: firstPGName}, &schedulingv1alpha1.PodGroup{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the completed gang's PodGroup must be finalized; err=%v", err)

			secondWavePods := boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 1)
			g.Expect(secondWavePods).To(HaveLen(2))
			for _, pod := range secondWavePods {
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil(), "every second-wave gang member must enter deletion")
			}
			secondPG := &schedulingv1alpha1.PodGroup{}
			secondPGName := query.PodGroupName(isvcName, workload.ComponentEngine, 1)
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: secondPGName}, secondPG)).To(Succeed())
			g.Expect(secondPG.DeletionTimestamp).To(BeNil(), "the second PodGroup must remain while its member Pods exist")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the second gang and converging with only instance zero")
		for _, key := range victims[1] {
			setBoundedScaleDownHold(key, false)
		}
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(1))
			g.Expect(statuses[0].Index).To(Equal(int32(0)))
			g.Expect(statuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 0)).To(HaveLen(2))

			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(pods).To(HaveLen(2))
			pgs := &schedulingv1alpha1.PodGroupList{}
			g.Expect(k8sClient.List(suiteCtx, pgs, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(pgs.Items).To(HaveLen(1))
			g.Expect(pgs.Items[0].Name).To(Equal(query.PodGroupName(isvcName, workload.ComponentEngine, 0)))
			g.Expect(pgs.Items[0].DeletionTimestamp).To(BeNil())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("finishes a committed wave after desired replicas rebound", func() {
		const isvcName = "bounded-rebound"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(5),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for five Ready singleton Instances")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(5))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		originalUIDs := make(map[int32]types.UID, 5)
		victims := make(map[int32]client.ObjectKey, 2)
		for index := int32(0); index < 5; index++ {
			pod := aliveEnginePodForInstance(Default, ns.Name, isvcName, index)
			originalUIDs[index] = pod.UID
			if index >= 3 {
				victims[index] = client.ObjectKeyFromObject(pod)
				setBoundedScaleDownHold(victims[index], true)
			}
		}
		ginkgo.DeferCleanup(func() {
			for _, key := range victims {
				setBoundedScaleDownHold(key, false)
			}
		})

		ginkgo.By("admitting the highest two indices for a five-to-one scale-down")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			one := 1
			current.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{3, 4}))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("restoring desired replicas while the committed wave is active")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			five := 5
			current.Spec.Engine.MinReplicas = &five
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{3, 4}), "desired rebound must not cancel durable Delete ownership")
			for _, index := range []int32{3, 4} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, victims[index], pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil())
			}
			for _, index := range []int32{0, 1, 2} {
				pod := aliveEnginePodForInstance(g, ns.Name, isvcName, index)
				g.Expect(pod.UID).To(Equal(originalUIDs[index]))
				g.Expect(pod.DeletionTimestamp).To(BeNil())
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the committed victims and reconciling replacement replicas")
		for _, key := range victims {
			setBoundedScaleDownHold(key, false)
		}
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(5))
			byIndex := make(map[int32]v1beta1.OMENativeInstanceStatus, len(statuses))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				byIndex[status.Index] = status
			}
			for index := int32(0); index < 5; index++ {
				g.Expect(byIndex).To(HaveKey(index))
				pod := aliveEnginePodForInstance(g, ns.Name, isvcName, index)
				if index < 3 {
					g.Expect(pod.UID).To(Equal(originalUIDs[index]), "non-admitted instance %d must survive", index)
				} else {
					g.Expect(pod.UID).NotTo(Equal(originalUIDs[index]), "committed victim %d must be recreated", index)
				}
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
