package omenative

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

// Status / Component-Condition / counter specs. AggregateAndWriteStatus
// composes the per-Instance counters, the Component-level rollup, and
// the Condition surface (Available, Progressing, OperationStuck, etc.).
// These specs pin the parts that downstream tooling (HPA, kubectl wait,
// dashboards) depends on.
var _ = ginkgo.Describe("OMENative status surface", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-status-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Component-level Conditions.Available must reach True once every
	// Instance reaches AvailableReplicas == Replicas. Drives downstream
	// `kubectl wait --for=condition=Available`. Recovery to False on
	// readiness flap is NOT integration-testable here — the kubelet
	// simulator deterministically re-stamps ContainersReady=True on the
	// next tick, racing our manual flip; that side of the contract is
	// pinned at the unit level in conditions_test.go.
	ginkgo.It("Component.Conditions.Available reaches True on full availability", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("avail-llama"),
			WithCSRName("avail-runtime"),
			WithISVCName("avail-isvc"),
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
			cond := meta.FindStatusCondition(cs.OMENative.Conditions, "Available")
			g.Expect(cond).NotTo(BeNil(), "Available condition missing")
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue),
				"Available should be True once all pods are ready; got %+v", cond)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Per-Instance Conditions surface AllPodsScheduled / AllPodsReady /
	// AllPodsAvailable. Status=True with the matching Reason on the
	// happy path. Catches a regression that drops the condition or
	// reports the wrong status.
	ginkgo.It("per-Instance Conditions populate the AllPods* triad", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("inst-cond-llama"),
			WithCSRName("inst-cond-runtime"),
			WithISVCName("inst-cond-isvc"),
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
			s := cs.OMENative.InstanceStatuses[0]
			for _, t := range []string{"AllPodsScheduled", "AllPodsReady", "AllPodsAvailable"} {
				c := meta.FindStatusCondition(s.Conditions, t)
				g.Expect(c).NotTo(BeNil(), "InstanceStatus[0] missing Condition %q", t)
				g.Expect(c.Status).To(Equal(metav1.ConditionTrue),
					"InstanceStatus[0].Conditions[%s] should be True at Ready; got %+v", t, c)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Per-Instance PodCount / ReadyPodCount / AvailablePodCount must
	// converge with the live pod set. At MinReplicas=2 + both pods Ready:
	// PodCount=2, ReadyPodCount=2, AvailablePodCount=2.
	ginkgo.It("per-Instance counters converge with the live pod set (MinReplicas=2)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("counter-llama"),
			WithCSRName("counter-runtime"),
			WithISVCName("counter-isvc"),
			WithMinReplicas(2),
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
			g.Expect(cs.OMENative.Replicas).To(Equal(int32(2)))
			g.Expect(cs.OMENative.ReadyReplicas).To(Equal(int32(2)))
			g.Expect(cs.OMENative.AvailableReplicas).To(Equal(int32(2)))
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(2))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.PodCount).To(Equal(int32(1)),
					"instance %d PodCount: got %d want 1", s.Index, s.PodCount)
				g.Expect(s.ReadyPodCount).To(Equal(int32(1)),
					"instance %d ReadyPodCount", s.Index)
				g.Expect(s.AvailablePodCount).To(Equal(int32(1)),
					"instance %d AvailablePodCount", s.Index)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
