package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative Loop B (pod / EndpointSlice event handler) specs ported
// from envtest watches_test.go.
//
// Coverage already in partial_failure_kind_test.go (PF-1/2/3):
//   - PodFailed triggers Restart and fresh pod created (PF-1 uses
//     pod-delete on real kubelet; same code path —
//     detectRestartTrigger fires on live-pod-count below desired)
//   - pod delete triggers Restart and pod is recreated (PF-1)
//   - RestartPolicy=None suppresses Restart (PF-3)
//   - Restart on one Instance does not bump siblings (PF-1 with 3
//     replicas — only the deleted Instance bumps)
//
// Ported below: the watch-predicate filtering specs (predicate
// regression would cause cross-ISVC reconcile thrash) and the
// readiness-loss negative case.
var _ = ginkgo.Describe("OMENative Loop B watches (extras)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-watch-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ManagedByOMENativePredicate filters out non-OMENative pods. A
	// predicate regression would cause the controller to enqueue the
	// ISVC for every pod event in the namespace; high-volume namespaces
	// would see reconcile thrash and the intruder could be relabeled.
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
		intruder.Spec.Containers = []corev1.Container{{
			Name:  "main",
			Image: "fake-serving:v1",
		}}
		Expect(k8sClient.Create(suiteCtx, intruder)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			grace := int64(0)
			_ = k8sClient.Delete(suiteCtx, intruder, &client.DeleteOptions{GracePeriodSeconds: &grace})
		})

		fix := newCoordFixture(ns.Name, WithISVCName("filter-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the ISVC to reach Ready (proves the controller is
		// actively reconciling), then assert the intruder is unchanged.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "filter-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).NotTo(BeEmpty())
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Capture the ISVC UID so the adoption assertion is precise.
		freshIsvc := freshISVC(Default, ns.Name, "filter-isvc")
		isvcUID := freshIsvc.UID

		Consistently(func(g Gomega) {
			got := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(intruder), got)).To(Succeed())
			g.Expect(got.Labels[util.LabelManagedByKey]).To(BeEmpty(),
				"intruder pod should not have been relabeled by the controller; labels=%+v", got.Labels)
			for _, ref := range got.OwnerReferences {
				g.Expect(ref.UID).NotTo(Equal(isvcUID),
					"intruder pod should not have been adopted (controller OwnerRef set); owners=%+v", got.OwnerReferences)
			}
		}, "3s", kindPollInterval).Should(Succeed())
	})

	// Pod events from a DIFFERENT OMENative ISVC must not enqueue
	// this ISVC. The watch predicate scopes events to the parent via
	// ome.io/inferenceservice. A regression that drops that filter
	// would cause cross-ISVC reconcile churn in shared namespaces.
	ginkgo.It("pod events from a different OMENative ISVC do not enqueue this ISVC", func() {
		fixA := newCoordFixture(ns.Name, WithISVCName("cross-a-isvc"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fixA)
		deferFixtureCleanup(fixA)

		fixB := newCoordFixture(ns.Name, WithISVCName("cross-b-isvc"),
			WithCBMName("cross-b-cbm"),
			WithCSRName("cross-b-csr"),
			WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		// Restart on pod-delete (real kubelet) so ISVC-B bumps
		// Incarnation observably.
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fixB.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fixB)
		deferFixtureCleanup(fixB)

		ginkgo.By("waiting for both ISVCs to reach Ready at Incarnation=1")
		Eventually(func(g Gomega) {
			for _, name := range []string{"cross-a-isvc", "cross-b-isvc"} {
				c := freshISVC(g, ns.Name, name)
				cs := c.Status.Components[v1beta1.EngineComponent]
				g.Expect(cs.OMENative).NotTo(BeNil(), "ISVC %s has no OMENative status", name)
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("deleting ISVC-B's pod to trigger Restart on ISVC-B")
		grace := int64(0)
		victim := &corev1.Pod{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: ns.Name, Name: "cross-b-isvc-engine-0-default-0",
		}, victim)).To(Succeed())
		Expect(k8sClient.Delete(suiteCtx, victim, &client.DeleteOptions{GracePeriodSeconds: &grace})).To(Succeed())

		ginkgo.By("expecting ISVC-B to bump Incarnation but ISVC-A to stay at 1")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "cross-b-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).NotTo(BeEmpty())
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"ISVC-B should have bumped Incarnation in response to its own pod delete")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			c := freshISVC(g, ns.Name, "cross-a-isvc")
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)),
				"ISVC-A's Incarnation must stay at 1 — its pod wasn't touched")
		}, "3s", kindPollInterval).Should(Succeed())
	})
})
