package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// spec.engine.lifecycle.updateStrategy.type edited while a SurgeThenDrain roll
// is already past its point of no return.
//
// UpdateStrategy is not part of the revision payload, so the edit retargets
// nothing: the same roll continues under a different mechanism. Once the surge
// has drained its source, the cycle must run to completion — the source is
// deleted and ActiveOrdinal advances onto the replacement's slot. A mode that
// reuses one pod name has no concept of that second slot: it patches the
// drained source back to life and promotes, leaving the Instance permanently at
// two pods on one slot, with the source held out of rotation by a surge drain
// entry only its deletion would have released.
//
// The window is opened deterministically rather than raced. SurgeThenDrain
// parks at its settle step for gracePeriodSeconds after the source leaves the
// EndpointSlice, so a long grace gives the edit a wide, reliable window to land
// in. The spec gates the edit on the Instance actually reporting that step, and
// re-checks the source afterwards, so a surge that finished early fails loudly
// instead of passing vacuously.
var _ = ginkgo.Describe("behavior — update strategy edited mid-surge", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "strat-switch-kss1-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("finishes the surge cycle when the edit lands after the source has drained", func() {
		const isvcName = "strat-switch-kss1"
		sourcePod := isvcName + "-engine-0-default-0"
		replacementPod := isvcName + "-engine-0-default-1"
		settleGrace := int32(120)

		fix := newCoordFixture(ns.Name,
			WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))

		// SurgeThenDrain honors gracePeriodSeconds as the settle wait between
		// unrouting the source and deleting it.
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategySurgeThenDrain,
				InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
					GracePeriodSeconds: &settleGrace,
				},
			},
		}

		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the engine Ready at ordinal 0")
		Eventually(func(g Gomega) {
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].ActiveOrdinal).To(Equal(int32(0)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping the image to open a SurgeThenDrain roll")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("waiting for the settle window: replacement serving, source drained but alive")
		var replacementUID string
		Eventually(func(g Gomega) {
			// The Instance parks here for gracePeriodSeconds. Gating on the step
			// itself, rather than on pod shape alone, is what makes the edit
			// below land inside the window instead of racing it.
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Operation).NotTo(BeNil())
			g.Expect(insts[0].Operation.Step).To(Equal("SurgeDrainSettle"))

			replacement := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: replacementPod,
			}, replacement)).To(Succeed())
			g.Expect(podIsReady(replacement)).To(BeTrue())
			replacementUID = string(replacement.UID)

			// The source has left rotation but the settle wait keeps it alive:
			// the surge is past its point of no return.
			source := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: sourcePod,
			}, source)).To(Succeed())
			g.Expect(source.DeletionTimestamp).To(BeNil())
			g.Expect(podIsReady(source)).To(BeFalse())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("editing the strategy to InPlaceIfPossible inside the settle window")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.Lifecycle.UpdateStrategy.Type = v1beta1.UpdateStrategyInPlaceIfPossible
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Proves the edit landed while the cycle was still in its settle step.
		// Without this the spec would pass vacuously if the surge had already
		// finished on its own.
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: ns.Name, Name: sourcePod,
		}, &corev1.Pod{})).To(Succeed(),
			"source was already deleted before the edit landed; the settle window was missed")

		ginkgo.By("expecting the surge cycle to finish rather than hand over mid-flight")
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: sourcePod,
			}, &corev1.Pod{})
			g.Expect(err).To(HaveOccurred(),
				"drained source survived: it is held out of rotation by a surge drain entry "+
					"that only its deletion releases, and the Instance is left at two pods")

			replacement := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: replacementPod,
			}, replacement)).To(Succeed())
			g.Expect(string(replacement.UID)).To(Equal(replacementUID))
			g.Expect(podIsReady(replacement)).To(BeTrue())

			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].ActiveOrdinal).To(Equal(int32(1)),
				"the surge promote must advance ActiveOrdinal onto the replacement's slot")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
