package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("InferenceReplica pause circuit breaker", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ir-pause-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("does not recreate a deleted pod while paused and resumes after unpause", func() {
		const isvcName = "ir-pause"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName), WithoutDecoder())
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var originalUID types.UID
		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(podreadiness.IsContainersReady(&pods[0])).To(BeTrue())
			originalUID = pods[0].UID
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.ReadyReplicas).To(Equal(int32(1)))
			// The pod serves as soon as its gate is written, but the Instance
			// is promoted only once PodReady and the availability window
			// agree; pausing before that promote would park an open Create
			// attempt, which is not the shape this spec exercises.
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("pausing the parent InferenceService and waiting for the annotation to project onto the IR")
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, isvcName)
			if isvc.Annotations == nil {
				isvc.Annotations = map[string]string{}
			}
			isvc.Annotations[constants.PausedRolloutAnnotation] = "true"
			g.Expect(k8sClient.Update(suiteCtx, isvc)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue())
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		pods := listOMENativePods(Default, ns.Name, isvcName)
		Expect(pods).To(HaveLen(1))
		zero := int64(0)
		Expect(k8sClient.Delete(suiteCtx, &pods[0], &client.DeleteOptions{GracePeriodSeconds: &zero})).To(Succeed())

		ginkgo.By("holding the missing-pod state without restart or create churn, with truthful status")
		Eventually(func(g Gomega) {
			g.Expect(listOMENativePods(g, ns.Name, isvcName)).To(BeEmpty())
			// The truth pass must stop the Instance from claiming Ready
			// with zero pods while its recovery stays parked by the pause.
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstancePending),
				"a paused Instance that lost every pod must not report Ready")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		Consistently(func(g Gomega) {
			g.Expect(listOMENativePods(g, ns.Name, isvcName)).To(BeEmpty())
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue())
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(1))
			g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstancePending),
				"the demotion must hold for the whole paused window")
		}, kindSteadyStateWindow, kindPollInterval).Should(Succeed())

		ginkgo.By("removing the parent InferenceService pause annotation")
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, isvcName)
			delete(isvc.Annotations, constants.PausedRolloutAnnotation)
			g.Expect(k8sClient.Update(suiteCtx, isvc)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(pods).To(HaveLen(1))
			g.Expect(pods[0].UID).NotTo(Equal(originalUID))
			g.Expect(podreadiness.IsContainersReady(&pods[0])).To(BeTrue())
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeFalse())
			g.Expect(ir.Status.ReadyReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
