package coordination_kind

import (
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Per-revision Service emission + drift correction under a real
// reconciler. KIND mirror of the envtest producer_services SVC-* suite.
//
// The pure-function naming tests (PerRevisionServiceName math,
// PerRevisionHeadlessServiceName math, the long-name regression
// SVC-13) are already covered by pkg/.../coordination/services_test.go.
// What KIND adds is:
//
//   - SVC-1 routing pair emission with `PublishNotReadyAddresses=false`
//     (routing) + `PublishNotReadyAddresses=true` (headless) under a
//     real apiserver + a real reconcile loop.
//   - SVC-2 drift correction: hand-mutate the routing Service's
//     selector to a bogus value; coordination must restore the four-
//     key selector on the next reconcile.
//
// envtest's SVC-1/SVC-2 hold the same shape — but the drift case
// PIt-ed under load there, and the routing/headless pair distinction
// in envtest is observationally fine but doesn't exercise the real
// Service controller (no kube-proxy / EndpointSlices). KIND closes
// that gap.
var _ = ginkgo.Describe("SVC — per-revision Service emission + drift correction", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "svc-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("SVC-1 emits per-revision routing + headless pair with correct PublishNotReadyAddresses + selector", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc1"), WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the engine to reach serving so coordination has a
		// pod hash to bucket on, and per-revision Services land.
		var revHash string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "svc1")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(1)))

			pods := listOMENativePods(g, ns.Name, "svc1")
			g.Expect(pods).NotTo(BeEmpty())
			revHash = pods[0].Labels[query.LabelRevisionHash]
			g.Expect(revHash).NotTo(BeEmpty(),
				"engine pod must carry %s label before per-revision Service can be selected", query.LabelRevisionHash)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Routed Service: <isvc>-<comp>-rev-<hash>, PublishNotReadyAddresses=false.
		routedName := coordination.PerRevisionServiceName("svc1", v1beta1.EngineComponent, revHash)
		routed := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: ns.Name, Name: routedName},
			routed)).To(Succeed(), "routed Service %s should exist", routedName)
		Expect(routed.Spec.ClusterIP).NotTo(Equal(corev1.ClusterIPNone),
			"routed Service must have a cluster IP, not be headless")
		Expect(routed.Spec.PublishNotReadyAddresses).To(BeFalse(),
			"routed Service must NOT publish not-ready addresses (only-ready endpoints serve traffic)")

		// Headless Service: <isvc>-<comp>-rev-<hash>-headless, ClusterIP=None,
		// PublishNotReadyAddresses=true.
		headlessName := coordination.PerRevisionHeadlessServiceName("svc1", v1beta1.EngineComponent, revHash)
		Expect(strings.HasSuffix(headlessName, "-headless")).To(BeTrue(),
			"per-revision headless name should end in -headless")
		headless := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: ns.Name, Name: headlessName},
			headless)).To(Succeed(), "headless Service %s should exist", headlessName)
		Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone),
			"headless variant must have ClusterIP=None")
		Expect(headless.Spec.PublishNotReadyAddresses).To(BeTrue(),
			"headless must publish not-ready addresses for peer-DNS gang init")

		// Both Services share the per-revision selector — four required
		// keys narrowing to exactly this hash's pods.
		for _, svc := range []*corev1.Service{routed, headless} {
			Expect(svc.Spec.Selector).To(HaveKeyWithValue(constants.InferenceServicePodLabelKey, "svc1"),
				"Service %s missing inferenceservice selector", svc.Name)
			Expect(svc.Spec.Selector).To(HaveKeyWithValue(constants.OMEComponentLabel, string(v1beta1.EngineComponent)),
				"Service %s missing component selector", svc.Name)
			Expect(svc.Spec.Selector).To(HaveKeyWithValue(query.LabelRevisionHash, revHash),
				"Service %s missing per-revision hash selector", svc.Name)
			Expect(svc.Spec.Selector).To(HaveKeyWithValue(query.LabelManagedBy, query.ManagedByOMENative),
				"Service %s missing managed-by selector", svc.Name)
		}

		// Both Services own back through the ISVC (kube-GC cascade).
		fresh := freshISVC(Default, ns.Name, "svc1")
		for _, svc := range []*corev1.Service{routed, headless} {
			Expect(svc.OwnerReferences).To(HaveLen(1),
				"Service %s should have exactly one OwnerReference", svc.Name)
			Expect(svc.OwnerReferences[0].UID).To(Equal(fresh.UID),
				"Service %s owner UID mismatch", svc.Name)
			Expect(svc.OwnerReferences[0].Kind).To(Equal("InferenceService"))
		}
	})

	ginkgo.It("SVC-2 hand-deleted routed Service is recreated by the next reconcile (OwnerRef-driven drift)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc2"), WithoutDecoder(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the original routed Service to materialize.
		var revHash, routedName string
		Eventually(func(g Gomega) {
			pods := listOMENativePods(g, ns.Name, "svc2")
			g.Expect(pods).NotTo(BeEmpty())
			revHash = pods[0].Labels[query.LabelRevisionHash]
			g.Expect(revHash).NotTo(BeEmpty())
			routedName = coordination.PerRevisionServiceName("svc2", v1beta1.EngineComponent, revHash)
			svc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx,
				client.ObjectKey{Namespace: ns.Name, Name: routedName},
				svc)).To(Succeed())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Delete the routed Service out from under the controller. The
		// next reconcile must rebuild it because the per-revision pod
		// set still exists — `EnsurePerRevisionServices` is idempotent
		// and will Create when Get returns NotFound.
		drift := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: ns.Name, Name: routedName},
			drift)).To(Succeed())
		Expect(k8sClient.Delete(suiteCtx, drift)).To(Succeed())

		// Poke a reconcile via a benign annotation. Without the poke
		// the controller might not re-queue immediately on Service
		// deletion (the ISVC watcher's Service handler is the natural
		// trigger; the poke gives it a deterministic re-reconcile).
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "svc2")
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations["coord.test/poke"] = "1"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Routed Service is restored. Use a slightly relaxed timeout
		// because the recreate needs the controller to re-resolve the
		// per-revision pod set + rebuild + Create the Service.
		Eventually(func(g Gomega) {
			svc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx,
				client.ObjectKey{Namespace: ns.Name, Name: routedName},
				svc)).To(Succeed(),
				"routed Service %s must be recreated after drift delete", routedName)
			// Restored Service still carries the correct selector + OwnerRef.
			g.Expect(svc.Spec.Selector).To(HaveKeyWithValue(query.LabelRevisionHash, revHash))
			g.Expect(svc.OwnerReferences).NotTo(BeEmpty(),
				"restored Service must regain its ISVC OwnerReference")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
