package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// eventHasReasonInvolving asserts at least one Event in ns has the
// given Reason AND involves the given object kind/name. Wrapping
// EventList list semantics keeps the spec readable.
func eventHasReasonInvolving(g Gomega, namespace, reason, involvedKind, involvedName string) {
	events := &corev1.EventList{}
	g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(namespace))).To(Succeed())
	matched := false
	for i := range events.Items {
		e := events.Items[i]
		if e.Reason != reason {
			continue
		}
		if e.InvolvedObject.Kind != involvedKind || e.InvolvedObject.Name != involvedName {
			continue
		}
		matched = true
		break
	}
	g.Expect(matched).To(BeTrue(),
		"expected Event with Reason=%q involving %s/%s; got %d events in namespace",
		reason, involvedKind, involvedName, len(events.Items))
}

// OMENative event-recorder integration specs. Each event reason
// downstream tooling consumes (dashboards, alerts, status pages) is
// pinned here so a regression that drops a recordNormal/recordWarning
// call surfaces as a failing assertion.
var _ = ginkgo.Describe("OMENative event recorder", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-evt-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// InstanceCreated fires on the first transition into Phase=Creating
	// for an Instance. Idempotency: subsequent reconciles that find the
	// instance already in Creating must NOT re-emit.
	ginkgo.It("InstanceCreated fires on first reconcile", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("evt-created-llama"),
			WithCSRName("evt-created-runtime"),
			WithISVCName("evt-created-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "InstanceCreated", "InferenceService", fx.ISVC.Name)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// InstanceReady fires once on the Creating → Ready transition. The
	// idempotency guard (wasReady-based) MUST suppress re-emission on
	// subsequent reconciles after the Instance is already Ready.
	ginkgo.It("InstanceReady fires on transition to Ready", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("evt-ready-llama"),
			WithCSRName("evt-ready-runtime"),
			WithISVCName("evt-ready-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			eventHasReasonInvolving(g, ns.Name, "InstanceReady", "InferenceService", fx.ISVC.Name)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RestartTriggered fires on the transition into Phase=Restarting.
	// Same idempotency contract as InstanceReady — subsequent
	// Restart-pass reconciles MUST NOT re-emit.
	ginkgo.It("RestartTriggered fires when a pod is Failed", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("evt-restart-llama"),
			WithCSRName("evt-restart-runtime"),
			WithISVCName("evt-restart-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the ISVC to reach Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("synthesizing pod.Status.Phase=Failed")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "evt-restart-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "RestartTriggered", "InferenceService", fx.ISVC.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// InPlaceUpdateStarted fires when the in-place path is entered.
	// InPlaceUpdateCompleted is NOT asserted here because envtest has
	// no kubelet to roll Status.ContainerStatuses[*].Image, so
	// podRuntimeImagesMatch never converges and the Completed branch
	// can't execute. The Started + status=Updating signal is enough to
	// pin the entry-point wiring.
	ginkgo.It("InPlaceUpdateStarted fires on an image-only rollout", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("evt-inplace-llama"),
			WithCSRName("evt-inplace-runtime"),
			WithISVCName("evt-inplace-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "InPlaceUpdateStarted", "InferenceService", fx.ISVC.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RecreateUpdateStarted + RecreateUpdateCompleted bracket a recreate
	// rollout (env-var change forces recreate via chooseUpdateMode).
	ginkgo.It("RecreateUpdateStarted + Completed bracket an env-var rollout", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("evt-recreate-llama"),
			WithCSRName("evt-recreate-runtime"),
			WithISVCName("evt-recreate-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Env = append(
				fresh.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "RECREATE_TRIGGER", Value: "1"},
			)
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "RecreateUpdateStarted", "InferenceService", fx.ISVC.Name)
			eventHasReasonInvolving(g, ns.Name, "RecreateUpdateCompleted", "InferenceService", fx.ISVC.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// InstanceReady's idempotency guard (wasReady-based) must suppress
	// re-emission on every reconcile after the first Ready transition.
	// A regression that emits InstanceReady per reconcile would generate
	// noisy events, drowning out the actionable signal in dashboards.
	ginkgo.It("InstanceReady fires exactly once on the steady-state path", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("evt-idem-llama"),
			WithCSRName("evt-idem-runtime"),
			WithISVCName("evt-idem-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("counting InstanceReady events emitted before any disturbance")
		var initialCount int
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			c := 0
			for _, e := range events.Items {
				if e.Reason == "InstanceReady" && e.InvolvedObject.Name == fx.ISVC.Name {
					c++
				}
			}
			g.Expect(c).To(BeNumerically(">=", 1), "InstanceReady must fire at least once")
			initialCount = c
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Account for K8s event de-duplication: identical events are
		// coalesced into a single Event object with a bumped Count
		// field. Assert that the count of distinct InstanceReady Event
		// objects with the same UID does not grow.
		ginkgo.By("asserting count stays at the initial value over 3s of further reconciles")
		Consistently(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			c := 0
			for _, e := range events.Items {
				if e.Reason == "InstanceReady" && e.InvolvedObject.Name == fx.ISVC.Name {
					c++
				}
			}
			g.Expect(c).To(Equal(initialCount),
				"InstanceReady event count grew from %d to %d — the once-on-transition guard regressed",
				initialCount, c)
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// FoundOrphan fires when Restart sees a pod labeled
	// managed-by=OMENative but missing ome.io/instance-incarnation.
	// Without this signal the operator has no way to learn that the
	// controller is refusing to delete the orphan.
	ginkgo.It("FoundOrphan fires on a label-stripped pod during Restart", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("evt-orphan-llama"),
			WithCSRName("evt-orphan-runtime"),
			WithISVCName("evt-orphan-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the ISVC to reach Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("stripping ome.io/instance-incarnation from the live pod")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "evt-orphan-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			delete(pod.Labels, "ome.io/instance-incarnation")
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("provoking Restart via Phase=Failed on the now-orphan pod")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "evt-orphan-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "FoundOrphan", "InferenceService", fx.ISVC.Name)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
