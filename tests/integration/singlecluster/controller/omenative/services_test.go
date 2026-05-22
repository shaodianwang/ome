package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Headless Service specs — pin OEP-0007 PR B's contract.
//
// The OMENative reconciler creates a per-Component headless Service
// at the first reconcile pass. Drain checks (IsPodDrained,
// IsPodInRotation) list EndpointSlices for THIS service; if the
// service is missing or shaped wrong, every downstream drain
// silently fails-open or hangs.
var _ = ginkgo.Describe("OMENative headless Service", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-svc-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Spec scenario #60. Bug it catches: B regression — Service not
	// created at all (kube-proxy never publishes endpoints, drain
	// silently completes against an empty slice list);
	// PublishNotReadyAddresses=false (NotReady pods never appear in
	// slices, so drain checks can't observe terminating pods);
	// wrong selector (slices target the wrong pod set).
	ginkgo.It("headless Service is created at first reconcile with the right shape", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("svc-llama-7b"),
			WithCSRName("svc-runtime"),
			WithISVCName("svc-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var svc *corev1.Service
		Eventually(func(g Gomega) {
			svcs := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, svcs, client.InNamespace(ns.Name))).To(Succeed())
			for i := range svcs.Items {
				if svcs.Items[i].Spec.ClusterIP == corev1.ClusterIPNone {
					svc = &svcs.Items[i]
					break
				}
			}
			g.Expect(svc).NotTo(BeNil(), "expected a headless Service for the OMENative Component")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Shape assertions — every property documented in PR B that
		// downstream drain logic depends on.
		Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone),
			"headless Service must have ClusterIP=None so kube-proxy publishes per-pod endpoints")
		Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue(),
			"PublishNotReadyAddresses must be true so drain checks can observe NotReady endpoints")
		Expect(svc.Spec.Selector).NotTo(BeEmpty(),
			"selector must be set so EndpointSlices target the right pods")
		// The selector should include managed-by=OMENative to scope to
		// the component's pods specifically.
		Expect(svc.Spec.Selector["ome.io/managed-by"]).To(Equal("OMENative"),
			"selector must include ome.io/managed-by=OMENative; got %+v", svc.Spec.Selector)
	})

	// Spec scenario #61. Bug it catches: a regression where two
	// components share a service (cross-component endpoint pollution
	// would make drain checks for one component see endpoints for
	// the other, breaking drain semantics). This suite's ISVCs are
	// single-component (engine only) so we verify by name pattern
	// rather than by side-by-side multi-component setup.
	ginkgo.It("headless Service name follows the per-Component pattern", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("svc-name-llama"),
			WithCSRName("svc-name-runtime"),
			WithISVCName("svc-name-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// Name pattern from pkg/.../omenative/render.go:headlessServiceName
		// is "<isvc>-<component>-headless".
		expectedName := "svc-name-isvc-engine-headless"
		Eventually(func(g Gomega) {
			svc := &corev1.Service{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: expectedName}, svc)
			g.Expect(err).NotTo(HaveOccurred(),
				"expected Service named %q; pattern is <isvc>-<component>-headless", expectedName)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
