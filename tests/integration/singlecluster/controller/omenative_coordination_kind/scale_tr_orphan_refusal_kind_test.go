package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Statusless-orphan refusal on the normal scale-down path. A pod that
// matches the OMENative selector but whose instance index has no
// InstanceStatus is durable evidence the controller does not own — it
// must be neither adopted nor deleted during ordinary scale-down, and
// it must not block the surrounding waves from converging. Only IR
// teardown (deletionTimestamp as durable authorization) or an operator
// may remove it.
//
// Invariants asserted:
//   - scale-down converges normally around the orphan;
//   - the orphan pod survives with its UID and no deletionTimestamp;
//   - no InstanceStatus is invented for the orphan's index.
var _ = ginkgo.Describe("OMENative scale transitions — statusless orphan refusal (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "scale-tr-orphan-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("leaves a statusless orphan pod untouched while scale-down converges around it", func() {
		const isvcName = "scale-tr-orphan"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(3),
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

		// The orphan carries every label the OMENative pod selector and
		// instance bucketing match on, at an index far outside the plan,
		// so the controller observes it but finds no InstanceStatus.
		ginkgo.By("planting a statusless orphan pod at an out-of-plan instance index")
		grace := int64(0)
		orphan := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "scale-tr-orphan-pod",
				Namespace: ns.Name,
				Labels: map[string]string{
					util.LabelManagedByKey:    util.ManagedByOMENativeValue,
					"ome.io/inferenceservice": isvcName,
					"component":               "engine",
					"ome.io/instance-index":   "7",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "main", Image: "fake-serving:v1"}},
				NodeSelector: map[string]string{
					"models.ome.io/clusterbasemodel." + isvcName + "-cbm": "Ready",
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, orphan)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, orphan, &client.DeleteOptions{GracePeriodSeconds: &grace})
		})
		var orphanUID types.UID
		Eventually(func(g Gomega) {
			fresh := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(orphan), fresh)).To(Succeed())
			orphanUID = fresh.UID
			g.Expect(orphanUID).NotTo(BeEmpty())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		pod0 := aliveEnginePodForInstance(Default, ns.Name, isvcName, 0)

		ginkgo.By("reducing desired replicas from three to one")
		Eventually(func(g Gomega) {
			current := freshISVC(g, ns.Name, isvcName)
			one := 1
			current.Spec.Engine.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, current)).To(Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the scale-down to converge around the orphan")
		Eventually(func(g Gomega) {
			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(1))
			g.Expect(statuses[0].Index).To(Equal(int32(0)))
			g.Expect(statuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 1)).To(BeEmpty())
			g.Expect(boundedScaleDownPodsForInstance(g, ns.Name, isvcName, 2)).To(BeEmpty())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting the orphan was neither adopted nor deleted")
		Consistently(func(g Gomega) {
			fresh := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(orphan), fresh)).To(Succeed())
			g.Expect(fresh.UID).To(Equal(orphanUID), "the orphan pod must not be replaced")
			g.Expect(fresh.DeletionTimestamp).To(BeNil(), "normal scale-down must never delete a statusless orphan")

			statuses := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(statuses).To(HaveLen(1), "no InstanceStatus may be invented for the orphan index")
			g.Expect(statuses[0].Index).To(Equal(int32(0)))

			survivor := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(pod0), survivor)).To(Succeed())
			g.Expect(survivor.UID).To(Equal(pod0.UID), "the retained instance must survive untouched")
			g.Expect(survivor.DeletionTimestamp).To(BeNil())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
