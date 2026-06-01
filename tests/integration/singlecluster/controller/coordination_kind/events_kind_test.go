package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// eventHasReasonInvolving asserts at least one Event in ns has the
// given Reason AND involves the given object kind/name.
func eventHasReasonInvolving(g Gomega, namespace, reason, involvedKind, involvedName string) {
	events := &corev1.EventList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, events, client.InNamespace(namespace))).To(Succeed())
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
	g.ExpectWithOffset(1, matched).To(BeTrue(),
		"expected Event with Reason=%q involving %s/%s; got %d events in namespace",
		reason, involvedKind, involvedName, len(events.Items))
}

// OMENative event-recorder specs. Each event reason downstream tooling
// (dashboards, alerts, status pages) consumes is pinned here so a
// regression that drops a recordNormal/recordWarning call surfaces as
// a failing assertion.
var _ = ginkgo.Describe("OMENative event recorder (KIND)", ginkgo.Ordered, func() {
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
	// for an Instance.
	ginkgo.It("InstanceCreated fires on first reconcile", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("evt-created-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "InstanceCreated", "InferenceService", "evt-created-isvc")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// InstanceReady fires once on the Creating → Ready transition.
	ginkgo.It("InstanceReady fires on transition to Ready", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("evt-ready-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "evt-ready-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			eventHasReasonInvolving(g, ns.Name, "InstanceReady", "InferenceService", "evt-ready-isvc")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// RestartTriggered fires on the transition into Phase=Restarting.
	// On real kubelet under KIND we provoke restart by force-deleting
	// the pod; detectRestartTrigger fires on the live-pod-count drop
	// (the envtest sim used pod.Status.Phase=Failed, which doesn't
	// stick on real kubelet because kubelet owns pod status).
	ginkgo.It("RestartTriggered fires when a pod is deleted under RecreateInstance policy", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("evt-restart-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the ISVC to reach Ready")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "evt-restart-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("force-deleting the live engine pod to trigger Restart")
		grace := int64(0)
		_ = k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "evt-restart-isvc-engine-0-default-0"},
		}, &client.DeleteOptions{GracePeriodSeconds: &grace})

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "RestartTriggered", "InferenceService", "evt-restart-isvc")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// InPlaceUpdateStarted fires when the in-place path is entered.
	// On KIND with real kubelet the Started + Completed bracket can
	// both be observed (kubelet does eventually roll
	// Status.ContainerStatuses[*].Image after the in-place patch).
	ginkgo.It("InPlaceUpdateStarted fires on an image-only rollout", func() {
		// InPlaceUpdateStarted only fires when chooseUpdateMode picks
		// the in-place path: that requires UpdateStrategy=InPlaceIfPossible
		// (default SurgeThenDrain routes through the surge path and emits
		// RecreateUpdate* instead). Mirrors the canonical pattern in
		// inplace_kind_test.go. The 3-replica fixture exercises
		// the multi-Instance dispatch loop so the InPlaceUpdateStarted
		// event fires once per Instance.
		fix := newCoordFixture(ns.Name, WithISVCName("evt-inplace-isvc"),
			WithoutDecoder(),
			WithEngineReplicas(3),
			WithEngineImage("fake-serving:v1"))
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{
				Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "evt-inplace-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(3))
			for i, inst := range cs.Lifecycle.InstanceStatuses {
				g.Expect(inst.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"Instance %d should be Ready before bumping image (got Phase=%s)",
					i, inst.Phase)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "evt-inplace-isvc")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "InPlaceUpdateStarted", "InferenceService", "evt-inplace-isvc")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// RecreateUpdateStarted + RecreateUpdateCompleted bracket a recreate
	// rollout (env-var change forces recreate via chooseUpdateMode).
	ginkgo.It("RecreateUpdateStarted + Completed bracket an env-var rollout", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("evt-recreate-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "evt-recreate-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "evt-recreate-isvc")
			cur.Spec.Engine.PodSpec.Containers[0].Env = append(
				cur.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "RECREATE_TRIGGER", Value: "1"},
			)
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "RecreateUpdateStarted", "InferenceService", "evt-recreate-isvc")
			eventHasReasonInvolving(g, ns.Name, "RecreateUpdateCompleted", "InferenceService", "evt-recreate-isvc")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// InstanceReady's idempotency guard (wasReady-based) must suppress
	// re-emission on every reconcile after the first Ready transition.
	// K8s coalesces identical events into a single object with bumped
	// Count, so we assert the count of distinct Event objects with this
	// Reason does NOT grow.
	ginkgo.It("InstanceReady fires exactly once on the steady-state path", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("evt-idem-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for Ready")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "evt-idem-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("counting InstanceReady Event objects emitted before any disturbance")
		var initialCount int
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			c := 0
			for _, e := range events.Items {
				if e.Reason == "InstanceReady" && e.InvolvedObject.Name == "evt-idem-isvc" {
					c++
				}
			}
			g.Expect(c).To(BeNumerically(">=", 1), "InstanceReady must fire at least once")
			initialCount = c
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("asserting count stays at the initial value over 5s of further reconciles")
		Consistently(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			c := 0
			for _, e := range events.Items {
				if e.Reason == "InstanceReady" && e.InvolvedObject.Name == "evt-idem-isvc" {
					c++
				}
			}
			g.Expect(c).To(Equal(initialCount),
				"InstanceReady event count grew from %d to %d — the once-on-transition guard regressed",
				initialCount, c)
		}, "5s", kindPollInterval).Should(Succeed())
	})

	// FoundOrphan fires when Restart sees a pod labeled
	// managed-by=OMENative but missing ome.io/instance-incarnation.
	// Without this signal the operator has no way to learn that the
	// controller is refusing to delete the orphan.
	ginkgo.It("FoundOrphan fires on a label-stripped pod during Restart", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("evt-orphan-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for the ISVC to reach Ready")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "evt-orphan-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("stripping ome.io/instance-incarnation from the live pod")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "evt-orphan-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			delete(pod.Labels, "ome.io/instance-incarnation")
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("force-deleting the now-orphan pod to provoke Restart")
		// Real kubelet: delete the pod to drive live-pod-count below
		// desired. The Restart pass scans the live pod set and finds
		// the orphan (the one we just stripped — but it'll be gone
		// before Restart runs; what we want is the controller to
		// observe ANY orphan during its scan over time, including
		// other orphans from race windows). Stamp the orphan label on
		// a NEW pod created with the right name shape after a fresh
		// delete.
		//
		// Simpler approach: just delete the live pod. The controller's
		// Restart pass kicks off, then on the next reconcile a fresh
		// pod gets created. We've already stripped the incarnation
		// label from the live pod — but the delete will catch that
		// pod before Restart's orphan scan runs. To make FoundOrphan
		// fire reliably we need a pod with managed-by but no
		// incarnation label present at scan time, so we re-label after
		// the fresh pod appears.
		//
		// Alternative: directly create an orphan-shaped pod alongside
		// the live one. The controller's Restart pass scans and emits
		// FoundOrphan on the orphan.
		grace := int64(0)
		_ = k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "evt-orphan-isvc-engine-0-default-0"},
		}, &client.DeleteOptions{GracePeriodSeconds: &grace})

		// Now create a deliberate orphan-shaped pod that the Restart
		// scan will find: same labels as a normal OMENative pod EXCEPT
		// no instance-incarnation. The controller's orphan scan emits
		// FoundOrphan and refuses to delete it (operator must clean up).
		orphan := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "orphan-shaped",
				Namespace: ns.Name,
				Labels: map[string]string{
					util.LabelManagedByKey:    util.ManagedByOMENativeValue,
					"ome.io/inferenceservice": "evt-orphan-isvc",
					"ome.io/component":        "engine",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "main", Image: "fake-serving:v1"}},
				NodeSelector: map[string]string{
					"models.ome.io/clusterbasemodel.evt-orphan-isvc-cbm": "Ready",
				},
			},
		}
		err := k8sClient.Create(suiteCtx, orphan)
		Expect(err == nil || apierrors.IsAlreadyExists(err)).To(BeTrue(),
			"orphan pod create: got %v", err)
		ginkgo.DeferCleanup(func() {
			_ = k8sClient.Delete(suiteCtx, orphan, &client.DeleteOptions{GracePeriodSeconds: &grace})
		})

		Eventually(func(g Gomega) {
			eventHasReasonInvolving(g, ns.Name, "FoundOrphan", "InferenceService", "evt-orphan-isvc")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
