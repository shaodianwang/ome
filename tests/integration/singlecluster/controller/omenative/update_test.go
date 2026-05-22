package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative spec-update specs. The reconciler's Update path branches on
// chooseUpdateMode (in-place when the diff is container images only;
// recreate otherwise). These specs pin the two branches end-to-end and
// the revision-side fields they touch.
var _ = ginkgo.Describe("OMENative Update", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-upd-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Image-only spec change must in-place patch the existing pod
	// (preserving its UID) rather than recreating. Without inPlaceEligible
	// + chooseUpdateMode returning InPlace, the rollout would fall through
	// to recreate and the pod's UID would change.
	ginkgo.It("image-only spec change patches container image in-place (pod UID preserved)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("inplace-llama"),
			WithCSRName("inplace-runtime"),
			WithISVCName("inplace-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the initial pod to be Ready at v1")
		var origUID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "inplace-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:v1"))
			origUID = string(pod.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flipping the engine container image to v2")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the pod's image to flip to v2 with the SAME UID (no recreate)")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "inplace-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:v2"),
				"container image should be patched to v2")
			g.Expect(string(pod.UID)).To(Equal(origUID),
				"in-place update must preserve pod UID; got %s want %s", pod.UID, origUID)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// RecreatePod strategy forces recreate even when the diff IS image-
	// only (would otherwise be in-place eligible). Pod UID changes and
	// Incarnation bumps regardless of eligibility.
	ginkgo.It("RecreatePod strategy forces recreate on an image-only diff", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("forcerec-llama"),
			WithCSRName("forcerec-runtime"),
			WithISVCName("forcerec-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
			WithUpdateStrategy(v1beta1.OMENativeUpdateStrategyRecreatePod),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the initial pod to be Ready")
		var origUID string
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "forcerec-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(Equal(int64(1)))
			origUID = string(pod.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flipping the engine container image (would be in-place eligible under default strategy)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting recreate semantics (UID changes + Incarnation bumps)")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "forcerec-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(string(pod.UID)).NotTo(Equal(origUID),
				"RecreatePod strategy must replace the pod; UID stuck at %s", origUID)
			g.Expect(pod.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:v2"))

			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"recreate must bump Incarnation")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// InPlaceOnly + ineligible diff (env-var change) must reject the
	// update with InPlaceUpdateNotPossible event and NOT touch the pod.
	// chooseUpdateMode regression that downgrades to recreate on
	// InPlaceOnly would silently violate operator intent.
	ginkgo.It("InPlaceOnly strategy rejects an env-var diff (InPlaceUpdateNotPossible)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("inplaceonly-llama"),
			WithCSRName("inplaceonly-runtime"),
			WithISVCName("inplaceonly-isvc"),
			WithUpdateStrategy(v1beta1.OMENativeUpdateStrategyInPlaceOnly),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the initial pod to be Ready")
		var origUID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "inplaceonly-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			origUID = string(pod.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("adding an env var (ineligible for in-place)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Env = append(
				fresh.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "SHOULD_BE_REJECTED", Value: "yes"},
			)
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting an InPlaceUpdateNotPossible warning event")
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, e := range events.Items {
				if e.Reason == "InPlaceUpdateNotPossible" && e.InvolvedObject.Name == fx.ISVC.Name {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(),
				"expected InPlaceUpdateNotPossible event; got %d events", len(events.Items))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("asserting the pod was NOT touched (UID unchanged, env var not applied)")
		Consistently(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "inplaceonly-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(string(pod.UID)).To(Equal(origUID),
				"InPlaceOnly rejection must not recreate the pod; UID changed")
			for _, e := range pod.Spec.Containers[0].Env {
				g.Expect(e.Name).NotTo(Equal("SHOULD_BE_REJECTED"),
					"env var must NOT be applied to the live pod under InPlaceOnly rejection")
			}
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Non-image diff (env var) must fall through to recreate — the pod
	// UID changes and Incarnation bumps. chooseUpdateMode regression
	// would route env diffs through in-place which silently no-ops.
	ginkgo.It("env-var change falls through to recreate update (pod UID changes, Incarnation bumps)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("recreate-llama"),
			WithCSRName("recreate-runtime"),
			WithISVCName("recreate-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the initial pod to be Ready at Incarnation=1")
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
				Namespace: ns.Name, Name: "recreate-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			origUID = string(pod.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("adding an env var to the engine container (non-image diff)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Env = append(
				fresh.Spec.Engine.PodSpec.Containers[0].Env,
				corev1.EnvVar{Name: "EXTRA_SETTING", Value: "yes"},
			)
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting Incarnation to bump and a fresh pod to appear (UID changed)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">=", int64(2)),
				"recreate update must bump Incarnation; got %d", cs.OMENative.InstanceStatuses[0].Incarnation)

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "recreate-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(string(pod.UID)).NotTo(Equal(origUID),
				"recreate update must produce a new pod; UID stayed %s", origUID)
			foundEnv := false
			for _, e := range pod.Spec.Containers[0].Env {
				if e.Name == "EXTRA_SETTING" && e.Value == "yes" {
					foundEnv = true
					break
				}
			}
			g.Expect(foundEnv).To(BeTrue(),
				"new pod must reflect the new env var; got %+v", pod.Spec.Containers[0].Env)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

})
