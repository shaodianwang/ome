package omenative_coordination_kind

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

// OMENative PD-disaggregated foundation specs.
// Each spec asserts a slice of the per-Component isolation
// contract — none rely on coordination policies (those are
// covered in the sibling bluegreen / sequential / independent / pause
// / inplace specs in this package).
//
// Already covered by pd_isolation_kind_test.go (dropped here):
//   - each Component owns a distinct ControllerRevision
//   - each Component owns a distinct headless Service
//   - Engine image bump leaves Decoder CR set untouched
//   - Engine MinReplicas scale doesn't change Decoder
//
// Already covered by cleanup_lifecycle_kind_test.go LIFE-DEL (KIND
// runs real kube-controller-manager so cascade actually fires, not
// just the OwnerRef shape check the envtest spec did):
//   - deleting the ISVC cascades to both Components' children
var _ = ginkgo.Describe("OMENative PD-disaggregated foundation (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-pd-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// Both Engine and Decoder reach Ready independently when
	// each carries the OMENative annotation. Without the
	// DetermineDeploymentModes fix the Decoder would be silently
	// routed to RawDeployment.
	ginkgo.It("Engine + Decoder ISVC converges with both InstanceStatuses Ready", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pd1-isvc"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("expecting pods at OMENative naming for BOTH components")
		Eventually(func(g Gomega) {
			for _, name := range []string{
				"pd1-isvc-engine-0-default-0",
				"pd1-isvc-decoder-0-default-0",
			} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: ns.Name, Name: name,
				}, pod)).To(Succeed(),
					"pod %q must exist at OMENative shape; if missing the controller routed to RawDeployment", name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting both per-Component InstanceStatuses to reach Ready")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pd1-isvc")

			engCS := c.Status.Components[v1beta1.EngineComponent]
			insts := instanceStatuses(g, ns.Name, "pd1-isvc", v1beta1.EngineComponent)
			g.Expect(engCS.Lifecycle).NotTo(BeNil(), "Engine must have OMENative status")
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.Lifecycle).NotTo(BeNil(), "Decoder must have OMENative status")
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Component-scoped migration only walks the named
	// Component. Engine-targeted request must not perturb Decoder.
	// Without the component-filter in pendingMigrationRequest, the
	// Decoder reconcile would pick up the Engine request and surge
	// against the wrong pod.
	ginkgo.It("a Component-scoped migration request only walks the named Component", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pd5-isvc"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var engineNode string
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pd5-isvc")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "pd5-isvc", v1beta1.EngineComponent)
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd5-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			engineNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("writing a migration request scoped to Engine only")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, engineNode, 0)

		ginkgo.By("Engine completes the migration; Decoder index 0 stays put on its original pod")
		Eventually(func(g Gomega) {
			_ = freshISVC(g, ns.Name, "pd5-isvc")
			engInsts := instanceStatuses(g, ns.Name, "pd5-isvc", v1beta1.EngineComponent)
			g.Expect(engInsts).To(HaveLen(1))
			g.Expect(engInsts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(engInsts[0].Index).NotTo(Equal(int32(0)),
				"Engine migration must move off index 0")

			decInsts := instanceStatuses(g, ns.Name, "pd5-isvc", v1beta1.DecoderComponent)
			g.Expect(decInsts).To(HaveLen(1),
				"Decoder must NOT have a surge instance from an Engine-only request")
			g.Expect(decInsts[0].Index).To(Equal(int32(0)),
				"Decoder must remain at its original index — engine migration leaked into decoder dispatch")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("audit ledger carries an Engine-tagged Completed entry only (no Decoder entry)")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, "pd5-isvc")
			var engineHit, decoderHit bool
			for _, e := range ledger.Entries {
				if e.RequestUUID != reqUUID {
					continue
				}
				switch e.Component {
				case string(v1beta1.EngineComponent):
					engineHit = true
				case string(v1beta1.DecoderComponent):
					decoderHit = true
				}
			}
			g.Expect(engineHit).To(BeTrue(),
				"audit ledger must record the Engine migration")
			g.Expect(decoderHit).To(BeFalse(),
				"audit ledger must NOT record a Decoder entry for an Engine-only request")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// Restart on one Component must NOT bump the other's
	// Incarnation. Fail an Engine pod; the Engine instance restarts and
	// the Engine Incarnation bumps. Decoder Incarnation stays put.
	// Without per-Component expectations key, a stale ObservedDelete
	// against the wrong (namespace, isvc, component, instance) could
	// stall Decoder progress.
	ginkgo.It("failing an Engine pod restarts Engine without touching Decoder Incarnation", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pd7-isvc"),
			WithEngineImage("fake-serving:v1"))
		// Restart on pod-delete (real kubelet flavor).
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var decoderInc int64
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pd7-isvc")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, "pd7-isvc", v1beta1.EngineComponent)
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts).To(HaveLen(1))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(insts[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			decoderInc = insts[0].Incarnation
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("force-deleting the Engine pod to trigger Restart")
		grace := int64(0)
		_ = k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "pd7-isvc-engine-0-default-0"},
		}, &client.DeleteOptions{GracePeriodSeconds: &grace})

		ginkgo.By("Engine Incarnation bumps; Decoder Incarnation remains unchanged")
		Eventually(func(g Gomega) {
			_ = freshISVC(g, ns.Name, "pd7-isvc")
			engInsts := instanceStatuses(g, ns.Name, "pd7-isvc", v1beta1.EngineComponent)
			decInsts := instanceStatuses(g, ns.Name, "pd7-isvc", v1beta1.DecoderComponent)
			g.Expect(engInsts).NotTo(BeEmpty())
			g.Expect(engInsts[0].Incarnation).To(BeNumerically(">", int64(1)),
				"Engine must have bumped Incarnation through Restart")
			g.Expect(decInsts).NotTo(BeEmpty())
			g.Expect(decInsts[0].Incarnation).To(Equal(decoderInc),
				"Decoder Incarnation must remain unchanged — Engine restart leaked into Decoder reconcile")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
