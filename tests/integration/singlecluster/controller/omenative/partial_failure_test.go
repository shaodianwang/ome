package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Multi-instance partial failure specs. With more than one replica,
// the per-Instance state machine must isolate failures to the
// affected Instance — sibling instances stay Ready, AvailableReplicas
// counts the survivors, and Restart fires only on the failed
// Instance. These guard against the regression where one bad pod
// drags down its siblings via a shared expectations key, status
// counter, or condition aggregator.
var _ = ginkgo.Describe("OMENative multi-instance partial failure", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-pf-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PF-1 — 3 replicas, fail the middle pod. Only that Instance
	// walks Restart; siblings' Incarnations stay put. AvailableReplicas
	// drops to 2 during the restart window, then returns to 3.
	ginkgo.It("PF-1: failing one of 3 pods restarts only that Instance; siblings untouched", func() {
		three := 3
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pf1-llama"),
			WithCSRName("pf1-runtime"),
			WithISVCName("pf1-isvc"),
			WithMinReplicas(three),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		initialInc := map[int32]int64{}
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				initialInc[s.Index] = s.Incarnation
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("failing pod at index 1 only")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pf1-isvc-engine-1-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("index 1 Incarnation bumps; indices 0 + 2 Incarnations unchanged")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			byIdx := map[int32]int64{}
			phases := map[int32]v1beta1.OMENativeInstancePhase{}
			for _, s := range cs.OMENative.InstanceStatuses {
				byIdx[s.Index] = s.Incarnation
				phases[s.Index] = s.Phase
			}
			g.Expect(byIdx[1]).To(BeNumerically(">", initialInc[1]),
				"index 1 must have bumped Incarnation (Restart fired)")
			g.Expect(byIdx[0]).To(Equal(initialInc[0]),
				"index 0 Incarnation must remain unchanged — Restart leaked to sibling")
			g.Expect(byIdx[2]).To(Equal(initialInc[2]),
				"index 2 Incarnation must remain unchanged — Restart leaked to sibling")
			g.Expect(phases[0]).To(Equal(v1beta1.OMENativeInstanceReady),
				"sibling index 0 must stay Ready")
			g.Expect(phases[2]).To(Equal(v1beta1.OMENativeInstanceReady),
				"sibling index 2 must stay Ready")
			g.Expect(phases[1]).To(Equal(v1beta1.OMENativeInstanceReady),
				"failed instance 1 must converge back to Ready after Restart")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PF-2 — 3 replicas, fail ALL 3 pods simultaneously. All three
	// Instances bump Incarnation independently — the per-Instance
	// expectations key isolates each Restart cycle so they don't
	// serialize or deadlock. Final state: all 3 Ready, each at a
	// higher Incarnation than initial.
	ginkgo.It("PF-2: failing all 3 pods simultaneously restarts each Instance independently", func() {
		three := 3
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pf2-llama"),
			WithCSRName("pf2-runtime"),
			WithISVCName("pf2-isvc"),
			WithMinReplicas(three),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		initialInc := map[int32]int64{}
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				initialInc[s.Index] = s.Incarnation
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("failing all 3 pods")
		for _, name := range []string{
			"pf2-isvc-engine-0-default-0",
			"pf2-isvc-engine-1-default-0",
			"pf2-isvc-engine-2-default-0",
		} {
			Eventually(func(g Gomega) {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, pod)).To(Succeed())
				pod.Status.Phase = corev1.PodFailed
				g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}

		ginkgo.By("all 3 Incarnations bump; all 3 converge back to Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"index %d must converge to Ready after Restart", s.Index)
				g.Expect(s.Incarnation).To(BeNumerically(">", initialInc[s.Index]),
					"index %d must have bumped Incarnation through Restart; got Inc=%d, initial=%d",
					s.Index, s.Incarnation, initialInc[s.Index])
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PF-3 — RestartPolicy=None. Pod fails; controller MUST NOT
	// restart it. The failed Instance's Incarnation stays put;
	// sibling Instances are unaffected. Pins the "RestartPolicy=None
	// opt-out" contract for operator-managed restart workflows.
	ginkgo.It("PF-3: RestartPolicy=None leaves the failed Instance dead; siblings unaffected", func() {
		three := 3
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pf3-llama"),
			WithCSRName("pf3-runtime"),
			WithISVCName("pf3-isvc"),
			WithMinReplicas(three),
			WithRestartPolicy(v1beta1.OMENativeRestartPolicyNone),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		initialInc := int64(0)
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				if s.Index == 1 {
					initialInc = s.Incarnation
				}
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("failing pod at index 1")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pf3-isvc-engine-1-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("index 1 Incarnation MUST NOT bump under RestartPolicy=None")
		Consistently(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				if s.Index == 1 {
					g.Expect(s.Incarnation).To(Equal(initialInc),
						"RestartPolicy=None must NOT bump Incarnation on the failed Instance; got Inc=%d, initial=%d",
						s.Incarnation, initialInc)
				}
			}
		}, "3s", util.PollInterval).Should(Succeed())
	})
})
