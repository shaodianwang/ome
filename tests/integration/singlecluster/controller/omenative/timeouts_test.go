package omenative

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Operation-deadline / timeout-recovery specs. The reconciler's
// ExpireOperationsIfDeadlinePassed handler runs at the top of every
// reconcile pass — these specs synthesize an elapsed Deadline by
// writing directly to InstanceStatus.Operation.Deadline so we don't
// have to wait for the real wall-clock timeout to fire.
var _ = ginkgo.Describe("OMENative operation deadlines", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-to-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// In-place update past its Deadline falls back to recreate. The
	// fallback path clears Phase=Updating to Ready, drops Operation,
	// and stamps the InPlaceUpdateFallback condition. detectUpdateTrigger
	// then re-fires on the unchanged RunningRevision vs target gap and
	// the recreate flow takes over.
	//
	// envtest naturally stalls in-place rollouts at podRuntimeImagesMatch
	// (no kubelet to update Status.ContainerStatuses[*].Image), which
	// makes this scenario easy to reproduce — we just need to fast-
	// forward the Deadline.
	ginkgo.It("in-place update past Deadline falls back via InPlaceUpdateFallback condition", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("to-inplace-llama"),
			WithCSRName("to-inplace-runtime"),
			WithISVCName("to-inplace-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for initial Ready at v1")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("triggering in-place update by flipping image to v2")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("catching Phase=Updating (in-place stalls in envtest by design)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			s := cs.OMENative.InstanceStatuses[0]
			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceUpdating),
				"expected Updating, got %q", s.Phase)
			g.Expect(s.Operation).NotTo(BeNil())
			g.Expect(s.Operation.Type).To(Equal(v1beta1.InstanceOperationUpdate))
			g.Expect(s.Operation.Step).To(Equal("InPlace"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("fast-forwarding the Deadline into the past")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs, ok := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			cs.OMENative.InstanceStatuses[0].Operation.Deadline = metav1.NewTime(metav1.Now().Add(-time.Minute))
			fresh.Status.Components[v1beta1.EngineComponent] = cs
			g.Expect(k8sClient.Status().Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting fallback: Phase=Ready + InPlaceUpdateFallback condition True")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			s := cs.OMENative.InstanceStatuses[0]
			cond := meta.FindStatusCondition(s.Conditions, "InPlaceUpdateFallback")
			g.Expect(cond).NotTo(BeNil(), "InPlaceUpdateFallback condition must be set")
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue),
				"InPlaceUpdateFallback should flip True after timeout fallback fires; got %+v", cond)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting an InPlaceTimeoutFallback warning event")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "InPlaceTimeoutFallback" && e.InvolvedObject.Name == fx.ISVC.Name {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected InPlaceTimeoutFallback warning event; got %d events", len(events.Items))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("simulating recovery: clear Phase=Updating to Ready and write RunningRevision=target")
		// envtest can't drive a full in-flight recreate to convergence in
		// reasonable time (multiple drain + delete + create + ready cycles
		// against a stalled kubelet sim). The fallback condition's
		// recovery flip is independent of how the Instance reaches steady-
		// Ready — directly synthesize the post-recovery shape and assert
		// the next reconcile flips the breadcrumb back.
		var targetRev string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs, ok := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.UpdateRevision).NotTo(BeEmpty())
			targetRev = cs.OMENative.UpdateRevision
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			cs.OMENative.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceReady
			cs.OMENative.InstanceStatuses[0].Operation = nil
			cs.OMENative.InstanceStatuses[0].RunningRevision = targetRev
			cs.OMENative.InstanceStatuses[0].TargetRevision = ""
			fresh.Status.Components[v1beta1.EngineComponent] = cs
			g.Expect(k8sClient.Status().Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting InPlaceUpdateFallback to flip back to False on the next reconcile")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			s := cs.OMENative.InstanceStatuses[0]
			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(s.Operation).To(BeNil(), "Operation must remain cleared post-recovery")
			cond := meta.FindStatusCondition(s.Conditions, "InPlaceUpdateFallback")
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionFalse),
				"InPlaceUpdateFallback must flip back to False once Instance returns to steady-Ready; got %+v", cond)
			g.Expect(cond.Reason).To(Equal("NoInPlaceFallback"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
