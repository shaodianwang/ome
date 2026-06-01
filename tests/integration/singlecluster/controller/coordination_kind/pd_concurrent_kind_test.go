package coordination_kind

import (
	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Concurrent state-machine operations across PD Components. Stress
// tests for the per-Component contract: two simultaneous migrations
// (one Engine, one Decoder); a migration on one Component while
// another Component restarts.
var _ = ginkgo.Describe("OMENative PD-disaggregated concurrent ops (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-pda-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PDA-1 — Simultaneous migration on Engine + Decoder. Two distinct
	// migration request annotations on the same ISVC, one per
	// component. The audit ledger (shared across components but
	// component-tagged per entry) tracks both UUIDs separately and
	// both migrations complete.
	ginkgo.It("PDA-1: simultaneous Engine + Decoder migration requests both complete", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pda1-isvc"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var engineNode, decoderNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pda1-isvc")
			for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil())
				g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("writing two migration requests on the same ISVC — one engine, one decoder")
		engineUUID := uuid.NewString()
		decoderUUID := uuid.NewString()
		writeMigrationRequestWithComponent(Default, client.ObjectKeyFromObject(fix.ISVC), engineUUID, engineNode, 0, string(v1beta1.EngineComponent))
		writeMigrationRequestWithComponent(Default, client.ObjectKeyFromObject(fix.ISVC), decoderUUID, decoderNode, 0, string(v1beta1.DecoderComponent))

		ginkgo.By("both Engine and Decoder converge to a non-zero index (each migrated off its source)")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pda1-isvc")
			for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Index).NotTo(Equal(int32(0)),
					"%s must have migrated off index 0", comp)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the shared audit ledger carries both UUIDs with the correct Component tagging")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, "pda1-isvc")
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PDA-2 — Engine pod fails during Decoder migration. Per-Component
	// reconciles run independently: Decoder's migration completes, AND
	// the Engine's Restart fires from the pod-delete event without
	// interfering with the Decoder reconcile.
	ginkgo.It("PDA-2: Engine restart during Decoder migration both complete independently", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pda2-isvc"),
			WithEngineImage("fake-serving:v1"))
		// Restart on pod-delete (real kubelet flavor).
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var decoderNode string
		var initialEngInc int64
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pda2-isvc")
			for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				cs := c.Status.Components[comp]
				g.Expect(cs.Lifecycle).NotTo(BeNil())
				g.Expect(cs.Lifecycle.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			initialEngInc = c.Status.Components[v1beta1.EngineComponent].Lifecycle.InstanceStatuses[0].Incarnation
			decPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pda2-isvc-decoder-0-default-0",
			}, decPod)).To(Succeed())
			decoderNode = decPod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("fire Decoder migration AND delete Engine pod")
		decoderUUID := uuid.NewString()
		writeMigrationRequestWithComponent(Default, client.ObjectKeyFromObject(fix.ISVC), decoderUUID, decoderNode, 0, string(v1beta1.DecoderComponent))
		grace := int64(0)
		_ = k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "pda2-isvc-engine-0-default-0"},
		}, &client.DeleteOptions{GracePeriodSeconds: &grace})

		ginkgo.By("Decoder migrates (Index 0 → other); Engine restarts in place (Incarnation bumps)")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pda2-isvc")
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.Lifecycle.InstanceStatuses[0].Index).NotTo(Equal(int32(0)),
				"Decoder must have migrated off index 0")
			g.Expect(decCS.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.Lifecycle.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.Lifecycle.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"Engine must stay at index 0 (restart preserves Index)")
			g.Expect(engCS.Lifecycle.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(engCS.Lifecycle.InstanceStatuses[0].Incarnation).To(BeNumerically(">", initialEngInc),
				"Engine must have bumped Incarnation through Restart")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
