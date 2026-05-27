package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative status surface specs ported from envtest status_test.go.
// Pin the parts of AggregateAndWriteStatus that downstream tooling
// depends on (HPA, kubectl wait, dashboards).
//
// Coverage already in status_surface_kind_test.go and skipped here:
//   - Component.ObservedGeneration tracks ISVC.metadata.generation
//   - per-Instance Conditions populate the AllPods* triad
//   - per-Instance PodCount/ReadyPodCount/AvailablePodCount counters
var _ = ginkgo.Describe("OMENative status surface (extras)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-stat-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Component-level Conditions.Available reaches True once every
	// Instance reaches AvailableReplicas == Replicas. Drives
	// downstream `kubectl wait --for=condition=Available`.
	ginkgo.It("Component.Conditions.Available reaches True on full availability", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("avail-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "avail-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			cond := meta.FindStatusCondition(cs.OMENative.Conditions, "Available")
			g.Expect(cond).NotTo(BeNil(), "Available condition missing")
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue),
				"Available should be True once all pods are ready; got %+v", cond)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// DRAIN-1: a pod with DeletionTimestamp must be excluded from
	// AvailableReplicas immediately, before the EndpointSlice
	// publishes Ready=false. endpointAvailable masks terminating
	// endpoints from the available set; without this, AvailableReplicas
	// stays inflated during graceful termination windows.
	ginkgo.It("DRAIN-1: terminating pod is excluded from AvailableReplicas", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("term-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		// RestartPolicy=None so deleting the pod here doesn't trigger
		// Restart on the controller side — we want the pod stuck in
		// terminating state for the AvailableReplicas assertion.
		restartNone := v1beta1.OMENativeRestartPolicyNone
		fix.ISVC.Spec.Engine.OMENative = &v1beta1.OMENativeComponentSpec{
			RestartPolicy: &restartNone,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for Ready + AvailableReplicas=1")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "term-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.AvailableReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("setting a long graceful termination on the pod, then deleting")
		// 60s grace period gives a wide window where the pod has a
		// DeletionTimestamp but kubelet hasn't actually force-removed it.
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: ns.Name, Name: "term-isvc-engine-0-default-0",
		}, pod)).To(Succeed())
		grace := int64(60)
		Expect(k8sClient.Delete(suiteCtx, pod, &client.DeleteOptions{
			GracePeriodSeconds: &grace,
		})).To(Succeed())

		ginkgo.By("expecting AvailableReplicas to drop to 0 within ShortTimeout")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "term-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.AvailableReplicas).To(Equal(int32(0)),
				"terminating pod must be excluded from AvailableReplicas; got %d",
				cs.OMENative.AvailableReplicas)
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Component.UpdateRevision + CurrentRevision converge after a
	// fresh ISVC reaches Ready. UpdateRevision is the target revision;
	// CurrentRevision is the promoted revision after rolloutComplete
	// returns true. Drives downstream tooling that watches "deployment
	// is at version X".
	ginkgo.It("Component.UpdateRevision + CurrentRevision converge after a fresh ISVC reaches Ready", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("rev-promote-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "rev-promote-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.UpdateRevision).NotTo(BeEmpty(),
				"UpdateRevision should be stamped once the target is computed")
			g.Expect(cs.OMENative.CurrentRevision).To(Equal(cs.OMENative.UpdateRevision),
				"CurrentRevision should converge to UpdateRevision once rollout completes")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
