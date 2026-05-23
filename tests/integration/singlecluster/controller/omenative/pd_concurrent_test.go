package omenative

import (
	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Concurrent state-machine operations across PD Components. Stress
// tests for the per-Component contract: two simultaneous migrations
// (one Engine, one Decoder), a migration on one Component while
// another Component restarts. Pins that the reconciler isolates
// in-flight ops by (component, instance) tuple under load.
var _ = ginkgo.Describe("OMENative PD-disaggregated concurrent ops", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-pda-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PDA-1 — Simultaneous migration on Engine + Decoder. Two
	// distinct migration request annotations on the same ISVC, one
	// per component. The audit ledger (shared across components but
	// component-tagged per entry) tracks both UUIDs separately and
	// both migrations complete.
	ginkgo.It("PDA-1: simultaneous Engine + Decoder migration requests both complete", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pda1-llama"),
			WithCSRName("pda1-runtime"),
			WithISVCName("pda1-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var engineNode, decoderNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := fresh.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			engPod, decPod := &corev1.Pod{}, &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pda1-isvc-engine-0-default-0",
			}, engPod)).To(Succeed())
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pda1-isvc-decoder-0-default-0",
			}, decPod)).To(Succeed())
			engineNode = engPod.Spec.NodeName
			decoderNode = decPod.Spec.NodeName
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing two migration requests on the same ISVC — one engine, one decoder")
		engineUUID := uuid.NewString()
		decoderUUID := uuid.NewString()
		writeMigrationRequestWithComponent(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), engineUUID, engineNode, 0, string(v1beta1.EngineComponent))
		writeMigrationRequestWithComponent(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), decoderUUID, decoderNode, 0, string(v1beta1.DecoderComponent))

		ginkgo.By("both Engine and Decoder converge to a non-zero index (each migrated off its source)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := fresh.Status.Components[comp]
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(cs.OMENative.InstanceStatuses[0].Index).NotTo(Equal(int32(0)),
					"%s must have migrated off index 0", comp)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("the shared audit ledger carries both UUIDs with the correct Component tagging")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
			seen := map[string]string{} // uuid -> component
			for _, e := range ledger.Entries {
				if e.Phase == "Completed" {
					seen[e.RequestUUID] = e.Component
				}
			}
			g.Expect(seen).To(HaveKeyWithValue(engineUUID, string(v1beta1.EngineComponent)),
				"engine UUID must be in ledger tagged with engine component; got %+v", seen)
			g.Expect(seen).To(HaveKeyWithValue(decoderUUID, string(v1beta1.DecoderComponent)),
				"decoder UUID must be in ledger tagged with decoder component; got %+v", seen)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PDA-2 — Engine pod fails during Decoder migration. Tests that
	// per-Component reconciles run independently: Decoder's
	// migration completes, AND the Engine's Restart fires from the
	// PodFailed event without interfering with the Decoder reconcile.
	ginkgo.It("PDA-2: Engine restart during Decoder migration both complete independently", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pda2-llama"),
			WithCSRName("pda2-runtime"),
			WithISVCName("pda2-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var decoderNode string
		var initialEngInc int64
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := fresh.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			initialEngInc = fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Incarnation
			decPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pda2-isvc-decoder-0-default-0",
			}, decPod)).To(Succeed())
			decoderNode = decPod.Spec.NodeName
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("fire Decoder migration AND fail Engine pod")
		decoderUUID := uuid.NewString()
		writeMigrationRequestWithComponent(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), decoderUUID, decoderNode, 0, string(v1beta1.DecoderComponent))
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pda2-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Decoder migrates (Index 0 → other); Engine restarts in place (Incarnation bumps)")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses[0].Index).NotTo(Equal(int32(0)),
				"Decoder must have migrated off index 0")
			g.Expect(decCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"Engine must stay at index 0 (restart preserves Index)")
			g.Expect(engCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">", initialEngInc),
				"Engine must have bumped Incarnation through Restart")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

})
