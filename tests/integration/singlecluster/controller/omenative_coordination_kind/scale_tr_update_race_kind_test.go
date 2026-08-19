package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Update racing an in-flight bounded scale-down. While any extra or
// delete-owned Instance remains incomplete, the workload pipeline must
// not advance Update: the retained Instance keeps its pod and revision
// untouched until every committed victim finishes, then the deferred
// update runs to completion. Advancing Update mid-wave would combine
// destructive scale-down load with a rollout and could recreate
// capacity the wave is still tearing down.
//
// Invariants asserted:
//   - with a delete wave held open, an image bump produces zero effects
//     on the retained Instance (same pod UID, old image, phase Ready);
//   - no replacement or surge pod appears while the wave is open;
//   - after the wave completes, the update proceeds and the survivor
//     converges onto the new image with a rotated UID.
var _ = ginkgo.Describe("OMENative scale transitions — update racing scale-down (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "scale-tr-updrace-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("defers an image update until the in-flight scale-down wave completes", func() {
		const isvcName = "scale-tr-updrace"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineUpdateStrategy(v1beta1.UpdateStrategyRecreatePod),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for three Ready singleton Instances")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(3))
			for _, status := range statuses {
				g.Expect(status.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		pod0 := aliveEnginePodForInstance(Default, ns.Name, isvcName, 0)
		pod0UID := pod0.UID
		victims := make(map[int32]client.ObjectKey, 2)
		for index := int32(1); index < 3; index++ {
			pod := aliveEnginePodForInstance(Default, ns.Name, isvcName, index)
			victims[index] = client.ObjectKeyFromObject(pod)
			setBoundedScaleDownHold(victims[index], true)
		}
		ginkgo.DeferCleanup(func() {
			for _, key := range victims {
				setBoundedScaleDownHold(key, false)
			}
		})

		ginkgo.By("reducing desired replicas from three to one and observing the committed wave")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			one := 1
			current.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(deleteOwnedInstanceIndices(instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent))).
				To(Equal([]int32{1, 2}))
			for _, key := range victims {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, key, pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil())
			}
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping the engine image while the wave is held open")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			current.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the update produces zero effects while delete work is incomplete")
		Consistently(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			byIndex := make(map[int32]v1beta1.OMENativeInstanceStatus, len(statuses))
			for _, status := range statuses {
				byIndex[status.Index] = status
			}
			g.Expect(byIndex).To(HaveKey(int32(0)))
			g.Expect(byIndex[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"the retained Instance must not enter Updating while the wave is open")
			g.Expect(deleteOwnedInstanceIndices(statuses)).To(Equal([]int32{1, 2}))

			survivor := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod0), survivor)).To(Succeed())
			g.Expect(survivor.UID).To(Equal(pod0UID), "the retained pod must not be recreated mid-wave")
			g.Expect(survivor.DeletionTimestamp).To(BeNil())
			g.Expect(survivor.Spec.Containers[0].Image).To(Equal("fake-serving:v1"),
				"the retained pod must keep the old image while the wave is open")

			g.Expect(listOMENativePods(g, ns.Name, isvcName)).To(HaveLen(3),
				"no surge or replacement pod may appear while the wave is open")
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the wave and expecting the deferred update to converge on v2")
		for _, key := range victims {
			setBoundedScaleDownHold(key, false)
		}
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(1))
			g.Expect(statuses[0].Index).To(Equal(int32(0)))
			g.Expect(statuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			survivor := aliveEnginePodForInstance(g, ns.Name, isvcName, 0)
			g.Expect(survivor.Spec.Containers[0].Image).To(Equal("fake-serving:v2"),
				"the deferred update must run once scale-down completes")
			g.Expect(survivor.UID).NotTo(Equal(pod0UID),
				"RecreatePod must rotate the retained pod's UID for the deferred update")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
