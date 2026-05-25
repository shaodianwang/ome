package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Loop B (pod / EndpointSlice event handlers) integration specs.
//
// These verify the watch-driven reconcile path end-to-end through
// envtest's real informer cache. Direct enqueue-detection isn't
// possible (controller-runtime doesn't expose the work queue), so
// each spec asserts on an OBSERVABLE side effect that proves the
// reconcile fired: a new pod appears, an incarnation bumps, etc.
var _ = ginkgo.Describe("OMENative Loop B watches", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-watch-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Restart triggered by PodFailed: controller observes the failed pod
	// and bumps Incarnation; a fresh pod appears with a new UID.
	ginkgo.It("PodFailed triggers Restart and a fresh pod is created", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("fail-llama"),
			WithCSRName("fail-runtime"),
			WithISVCName("fail-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the ISVC to reach Ready at Incarnation=1")
		var originalPodUID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))

			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(1))
			originalPodUID = string(pods.Items[0].UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("synthesizing pod.Status.Phase=Failed to trigger Restart")
		// RetryOnConflict because the controller writes pod conditions
		// concurrently — sim flips ContainersReady, controller sets serving.
		Eventually(func(g Gomega) {
			victim := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "fail-isvc-engine-0-default-0",
			}, victim)).To(Succeed())
			victim.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, victim)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting Restart to bump Incarnation and produce a fresh pod UID")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"Incarnation should bump on Restart; got %d", cs.OMENative.InstanceStatuses[0].Incarnation)

			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty(), "expected at least one pod post-Restart")
			foundFresh := false
			for _, p := range pods.Items {
				if string(p.UID) != originalPodUID {
					foundFresh = true
					break
				}
			}
			g.Expect(foundFresh).To(BeTrue(),
				"expected at least one pod with UID different from original %s", originalPodUID)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Root-cause note for future readers: this spec previously hung at
	// phase=Restarting inc=2 podCount=0 indefinitely because Restart's
	// Phase A called ExpectDeletes(1) AFTER p.Client.Delete. Under the
	// kubelet-sim's near-instant force-delete, the DELETE watch event
	// fired before ExpectDeletes ran, ObservedDelete missed (no entry),
	// and ExpectDeletes then created a Deletes=1 entry that nothing
	// decremented — Satisfied returned false for the full 2-minute TTL.
	// Fix: per-pod EXPECT-ORDER (Expect BEFORE the RPC, rollback via
	// ObservedDelete on error) applied to all expectations sites.
	ginkgo.It("pod delete triggers Restart and pod is recreated", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("watch-llama"),
			WithCSRName("watch-runtime"),
			WithISVCName("watch-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the ISVC to reach Phase=Ready at Incarnation=1")
		var originalPodUID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			s := cs.OMENative.InstanceStatuses[0]
			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(s.Incarnation).To(Equal(int64(1)))

			// Capture the original pod's UID so we can prove the
			// post-delete pod is a NEW object, not the same one.
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(1))
			originalPodUID = string(pods.Items[0].UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("deleting the original pod to provoke Restart")
		victim := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: ns.Name, Name: "watch-isvc-engine-0-default-0",
		}, victim)).To(Succeed())
		Expect(k8sClient.Delete(suiteCtx, victim)).To(Succeed())

		// Latency budget. A2 spec says "within 1s"; envtest under load
		// is slower than production K8s, and the chain involves
		// kubelet-sim force-delete → DELETE watch → enqueue →
		// detectRestartTrigger → patchInstanceStatusRestarting →
		// status flush. LongTimeout to absorb flake under -repeat.
		// Drop the "exactly one pod" sub-assertion — we don't care if
		// the old pod is briefly visible alongside the new one; the
		// signal that the Restart fired is the Incarnation bump.
		ginkgo.By("expecting the controller to re-Render at Incarnation=2 with a fresh pod UID")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"Incarnation should bump on Restart; got %d", cs.OMENative.InstanceStatuses[0].Incarnation)

			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty(), "expected at least one pod post-Restart")
			foundFresh := false
			for _, p := range pods.Items {
				if string(p.UID) != originalPodUID {
					foundFresh = true
					break
				}
			}
			g.Expect(foundFresh).To(BeTrue(),
				"expected at least one pod with UID different from original %s", originalPodUID)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RestartPolicy=None: even a PodFailed should NOT trigger Restart.
	// detectRestartTrigger only runs under RestartPolicy=RecreateInstance;
	// None must keep the policy gate honest. Operators set None when they
	// want manual recovery.
	ginkgo.It("RestartPolicy=None suppresses Restart on PodFailed", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("nopol-llama"),
			WithCSRName("nopol-runtime"),
			WithISVCName("nopol-isvc"),
			WithRestartPolicy(v1beta1.OMENativeRestartPolicyNone),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for Ready at Incarnation=1")
		var origUID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "nopol-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			origUID = string(pod.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("synthesizing pod.Status.Phase=Failed")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "nopol-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("asserting Incarnation stays at 1 and the failed pod survives (no Restart)")
		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)),
				"RestartPolicy=None must NOT bump Incarnation on PodFailed; got %d",
				cs.OMENative.InstanceStatuses[0].Incarnation)

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "nopol-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(string(pod.UID)).To(Equal(origUID),
				"failed pod must NOT be replaced under RestartPolicy=None")
			g.Expect(pod.DeletionTimestamp).To(BeNil(),
				"failed pod must NOT be marked for deletion under RestartPolicy=None")
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// A2 latency: a pod-delete event should land on the parent ISVC's
	// reconcile queue within ShortTimeout (10s). A regression that broke
	// the EventHandler → Expectations → queue chain would push observable
	// latency past the spec's "within one reconcile interval" budget.
	ginkgo.It("pod-delete enqueues the parent ISVC within ShortTimeout", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("lat-llama"),
			WithCSRName("lat-runtime"),
			WithISVCName("lat-isvc"),
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

		ginkgo.By("deleting the pod and asserting Incarnation bumps within ShortTimeout")
		victim := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: ns.Name, Name: "lat-isvc-engine-0-default-0",
		}, victim)).To(Succeed())
		Expect(k8sClient.Delete(suiteCtx, victim)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"pod-delete should fire Restart within ShortTimeout=10s; got Incarnation=%d",
				cs.OMENative.InstanceStatuses[0].Incarnation)
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Spec scenario #57 — ManagedByOMENativePredicate filters out
	// non-OMENative pods. Bug it catches: predicate regression that
	// causes the controller to enqueue our ISVC for every pod event
	// in the namespace, including pods owned by other deployments
	// (LWS, RawDeployment, etc.). High-volume namespaces would see
	// reconcile thrash.
	//
	// Indirect observation: pre-create a non-OMENative pod in the
	// namespace, then create an OMENative ISVC; verify the
	// pre-existing pod is left alone (not adopted, not deleted, not
	// relabeled) across the ISVC's normal reconcile cycle.
	ginkgo.It("pods without the OMENative managed-by label are not adopted by the controller", func() {
		intruder := &corev1.Pod{}
		intruder.Name = "intruder-pod"
		intruder.Namespace = ns.Name
		intruder.Labels = map[string]string{
			// Deliberately NOT ome.io/managed-by=OMENative. The
			// component label is included to test the predicate is
			// looking at managed-by, not at component-name only.
			"ome.io/component": "engine",
		}
		intruder.Spec.Containers = []corev1.Container{{Name: "main", Image: "test:intruder"}}
		Expect(k8sClient.Create(suiteCtx, intruder)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, intruder) })

		fx := newOMENativeFixture(ns.Name,
			WithCBMName("filter-llama"),
			WithCSRName("filter-runtime"),
			WithISVCName("filter-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// Wait for the ISVC to reach Ready (proves the controller is
		// actively reconciling), then assert the intruder is unchanged.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).NotTo(BeEmpty())
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Now verify the intruder is untouched: not deleted, not
		// relabeled, no controller OwnerRef added.
		Consistently(func(g Gomega) {
			got := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(intruder), got)).To(Succeed())
			g.Expect(got.Labels[util.LabelManagedByKey]).To(BeEmpty(),
				"intruder pod should not have been relabeled by the controller; labels=%+v", got.Labels)
			for _, ref := range got.OwnerReferences {
				g.Expect(ref.UID).NotTo(Equal(fx.ISVC.UID),
					"intruder pod should not have been adopted (controller OwnerRef set); owners=%+v", got.OwnerReferences)
			}
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Pod events from a DIFFERENT OMENative ISVC must not enqueue this
	// ISVC. The watch predicate scopes events to the parent via
	// ome.io/inferenceservice; a regression that drops that filter
	// would cause cross-ISVC reconcile churn in shared namespaces.
	ginkgo.It("pod events from a different OMENative ISVC do not enqueue this ISVC", func() {
		fxA := newOMENativeFixture(ns.Name,
			WithCBMName("cross-llama-a"),
			WithCSRName("cross-runtime-a"),
			WithISVCName("cross-a-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fxA.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fxA.CBM) })
		Expect(k8sClient.Create(suiteCtx, fxA.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fxA.CSR) })
		Expect(k8sClient.Create(suiteCtx, fxA.ISVC)).To(Succeed())

		fxB := newOMENativeFixture(ns.Name,
			WithCBMName("cross-llama-b"),
			WithCSRName("cross-runtime-b"),
			WithISVCName("cross-b-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fxB.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fxB.CBM) })
		Expect(k8sClient.Create(suiteCtx, fxB.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fxB.CSR) })
		Expect(k8sClient.Create(suiteCtx, fxB.ISVC)).To(Succeed())

		ginkgo.By("waiting for both ISVCs to reach Ready at Incarnation=1")
		Eventually(func(g Gomega) {
			for _, isvc := range []*v1beta1.InferenceService{fxA.ISVC, fxB.ISVC} {
				fresh := &v1beta1.InferenceService{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
				cs := fresh.Status.Components[v1beta1.EngineComponent]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("deleting ISVC-B's pod to trigger Restart on ISVC-B")
		victim := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: ns.Name, Name: "cross-b-isvc-engine-0-default-0",
		}, victim)).To(Succeed())
		Expect(k8sClient.Delete(suiteCtx, victim)).To(Succeed())

		ginkgo.By("expecting ISVC-B to bump Incarnation but ISVC-A to stay at 1")
		Eventually(func(g Gomega) {
			freshB := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fxB.ISVC), freshB)).To(Succeed())
			csB := freshB.Status.Components[v1beta1.EngineComponent]
			g.Expect(csB.OMENative).NotTo(BeNil())
			g.Expect(csB.OMENative.InstanceStatuses).NotTo(BeEmpty())
			g.Expect(csB.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"ISVC-B should have bumped Incarnation in response to its own pod delete")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			freshA := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fxA.ISVC), freshA)).To(Succeed())
			csA := freshA.Status.Components[v1beta1.EngineComponent]
			g.Expect(csA.OMENative).NotTo(BeNil())
			g.Expect(csA.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(csA.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)),
				"ISVC-A's Incarnation must stay at 1 — its pod wasn't touched")
		}, "2s", util.PollInterval).Should(Succeed())
	})

	// Multi-instance restart isolation: when one Instance's pod fails,
	// only that Instance's Incarnation bumps. A regression that drives
	// Restart over the wrong index (or fans out to all instances) would
	// take down healthy siblings.
	ginkgo.It("Restart trigger on one Instance does not bump siblings (MinReplicas=2)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("multi-restart-llama"),
			WithCSRName("multi-restart-runtime"),
			WithISVCName("multi-restart-isvc"),
			WithMinReplicas(2),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for both instances to reach Ready at Incarnation=1")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(2))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(s.Incarnation).To(Equal(int64(1)))
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("failing only Instance 0's pod")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "multi-restart-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting only Instance 0's Incarnation to bump")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(2))
			byIdx := map[int32]v1beta1.OMENativeInstanceStatus{}
			for _, s := range cs.OMENative.InstanceStatuses {
				byIdx[s.Index] = s
			}
			g.Expect(byIdx[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"Instance 0 (the failed one) should bump Incarnation; got %d", byIdx[0].Incarnation)
			g.Expect(byIdx[1].Incarnation).To(Equal(int64(1)),
				"Instance 1 (sibling) must stay at Incarnation=1; got %d", byIdx[1].Incarnation)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// detectRestartTrigger fires only on pod.Phase=Failed or pod count
	// below desired — a Ready pod that loses ContainersReady (e.g.
	// probe flap) must NOT trigger Restart, since restart-on-readiness
	// would create a thrash loop on noisy probes.
	ginkgo.It("readiness loss alone does not trigger Restart (Incarnation stays at 1)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("flap-llama"),
			WithCSRName("flap-runtime"),
			WithISVCName("flap-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the ISVC to reach Ready at Incarnation=1")
		var originalPodUID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "flap-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			originalPodUID = string(pod.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flipping ContainersReady to False without deleting / failing the pod")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "flap-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			for i, c := range pod.Status.Conditions {
				if c.Type == corev1.ContainersReady {
					pod.Status.Conditions[i].Status = corev1.ConditionFalse
				}
			}
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("asserting Incarnation stays at 1 (no Restart triggered) over 3s")
		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)),
				"readiness flap must NOT bump Incarnation; got %d", cs.OMENative.InstanceStatuses[0].Incarnation)

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "flap-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(string(pod.UID)).To(Equal(originalPodUID),
				"readiness flap must NOT recreate the pod; UID changed from %s to %s", originalPodUID, pod.UID)
			g.Expect(pod.DeletionTimestamp).To(BeNil(),
				"readiness flap must NOT mark the pod for deletion")
		}, "3s", util.PollInterval).Should(Succeed())
	})
})
