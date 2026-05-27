package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative top-level ISVC status plumbing.
//
// The OMENative reconciler populates the per-Component sub-status
// (status.components.<c>.omenative.{availableReplicas,conditions,
// instanceStatuses,...}). The rest of OME — ingress, traffic routing,
// model-status, and `kubectl wait --for=condition=Ready` — reads the
// TOP-level conditions (EngineReady / DecoderReady / RouterReady /
// IngressReady / Ready) and the per-Component .url.
//
// PR #228 wired the {Engine,Decoder,Router}Ready conditions from the
// OMENative ReadyReplicas counter. STAT-1 / STAT-2 below pin that
// contract. The per-Component URL plumbing (STAT-3) is still missing
// and stays PIt.
var _ = ginkgo.Describe("OMENative top-level status plumbing", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-status-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("STAT-1 engine-only ISVC: EngineReady condition flips to True after OMENative converges", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("stat1-llama"),
			WithCSRName("stat1-runtime"),
			WithISVCName("stat1-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// Wait for OMENative-level convergence first. Without this gate
		// the top-level assertions could pass for the wrong reason
		// (still Initializing because nothing's happened yet).
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.ReadyReplicas).To(Equal(int32(1)),
				"OMENative engine must reach ReadyReplicas=1 before checking top-level")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// EngineReady flips with the OMENative ReadyReplicas counter
		// (the contract PropagateRawStatus / PropagateMultiNodeStatus
		// share for Deployment / LWS). Aggregate top-level Ready is
		// out of scope here — it depends on IngressReady which the
		// envtest suite doesn't wire.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engineReady := fresh.Status.GetCondition(v1beta1.EngineReady)
			g.Expect(engineReady).NotTo(BeNil(), "EngineReady condition must exist after OMENative converges")
			g.Expect(engineReady.Status).To(Equal(corev1.ConditionTrue),
				"EngineReady must flip to True once OMENative ReadyReplicas == Replicas")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("STAT-2 PD ISVC (engine+decoder+router): all three component-ready conditions flip to True", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("stat2-llama"),
			WithCSRName("stat2-runtime"),
			WithISVCName("stat2-isvc"),
			WithDecoder(),
			WithRouter(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// OMENative-level convergence on all three Components.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
				cs := fresh.Status.Components[c]
				g.Expect(cs.OMENative).NotTo(BeNil(), "OMENative sub-status must exist for %s", c)
				g.Expect(cs.OMENative.ReadyReplicas).To(Equal(int32(1)),
					"%s OMENative.ReadyReplicas must reach 1", c)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Each Component's top-level Ready condition flips to True.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, ct := range []knapis.ConditionType{v1beta1.EngineReady, v1beta1.DecoderReady, v1beta1.RouterReady} {
				cond := fresh.Status.GetCondition(ct)
				g.Expect(cond).NotTo(BeNil(), "%s condition must exist after OMENative converges", ct)
				g.Expect(cond.Status).To(Equal(corev1.ConditionTrue),
					"%s must flip to True once OMENative AvailableReplicas == desired", ct)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("STAT-3 per-Component url is populated in status.components.<c>.url after convergence [BLOCKED: OMENative-side URL propagation missing — separate fix from #228]", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("stat3-llama"),
			WithCSRName("stat3-runtime"),
			WithISVCName("stat3-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// OMENative convergence first.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := fresh.Status.Components[c]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.AvailableReplicas).To(Equal(int32(1)))
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Per-Component URL is set — operators read this for traffic-
		// routing wiring and ops dashboards (the real-cluster status
		// dump in the bug report showed status.components.<c>.url
		// populated for RawDeployment-mode ISVCs but missing for
		// OMENative).
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := fresh.Status.Components[c]
				g.Expect(cs.URL).NotTo(BeNil(), "%s status.url must be populated after convergence", c)
				g.Expect(cs.URL.Host).NotTo(BeEmpty(), "%s status.url.Host must be a non-empty hostname", c)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
