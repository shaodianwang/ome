package omenative

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative PD-disaggregated foundation specs.
//
// These exercise multi-Component ISVCs (Engine + Decoder) where each
// Component independently opts into OMENative via its own annotation.
// They are the foundation gate for OEP-0007.1 coordinated rollout —
// before adding cross-Component coordination, the v1 "each Component
// rolls independently" contract must hold per OEP-0007 README:842-849.
//
// Each spec asserts one slice of the per-Component isolation contract:
// pods, ControllerRevisions, headless Services, migration ledger
// entries, scale, restart, cascade-delete. None of them rely on the
// new OEP-0007.1 surface (Per-Revision Services, Status.Components.*.
// Traffic[], rolloutCoordination groups). Adding those later should
// not break these specs.
var _ = ginkgo.Describe("OMENative PD-disaggregated foundation", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-pd-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PD-1 — Both Engine and Decoder reach Ready independently when
	// each component carries the OMENative annotation. Without the
	// DetermineDeploymentModes fix the Decoder is silently routed to
	// RawDeployment so this exact spec would never see Decoder pods at
	// the OMENative-shape `<isvc>-decoder-0-default-0` name.
	ginkgo.It("PD-1: Engine + Decoder ISVC converges with both InstanceStatuses Ready", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd1-llama"),
			WithCSRName("pd1-runtime"),
			WithISVCName("pd1-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("expecting pods at the OMENative naming for BOTH components")
		Eventually(func(g Gomega) {
			engPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd1-isvc-engine-0-default-0",
			}, engPod)).To(Succeed(),
				"Engine pod must exist at OMENative shape; if missing the controller routed Engine to RawDeployment")
			decPod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd1-isvc-decoder-0-default-0",
			}, decPod)).To(Succeed(),
				"Decoder pod must exist at OMENative shape; if missing the controller routed Decoder to RawDeployment (DetermineDeploymentModes annotation bug)")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting both per-Component InstanceStatuses to reach Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())

			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil(), "Engine must have OMENative status")
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.OMENative).NotTo(BeNil(), "Decoder must have OMENative status; if nil the Decoder reconciler is still on the Raw path")
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PD-2 — Per-component ControllerRevisions: each component gets
	// its own CR stream named `<isvc>-<component>-rev-<hash>`. The
	// retention loop walks each component's set independently;
	// without per-component naming the two sets would collide on hash
	// and corrupt each other's revision history.
	ginkgo.It("PD-2: each Component owns a distinct ControllerRevision (independent revision streams)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd2-llama"),
			WithCSRName("pd2-runtime"),
			WithISVCName("pd2-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())

			var engineCount, decoderCount int
			for _, cr := range crs.Items {
				switch {
				case strings.HasPrefix(cr.Name, "pd2-isvc-engine-"):
					engineCount++
				case strings.HasPrefix(cr.Name, "pd2-isvc-decoder-"):
					decoderCount++
				default:
					ginkgo.Fail(fmt.Sprintf("unexpected CR name %q does not match per-component prefix", cr.Name))
				}
			}
			g.Expect(engineCount).To(Equal(1),
				"expected exactly one Engine CR; got %d", engineCount)
			g.Expect(decoderCount).To(Equal(1),
				"expected exactly one Decoder CR; got %d", decoderCount)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PD-3 — Per-component headless Services: each component owns
	// `<isvc>-<component>-headless`. Required so the per-Component
	// drain check (which reads EndpointSlices by Service name) sees
	// only its own pods. Foundation for OEP-0007.1 Per-Revision
	// Services (which adds revision-suffixed Services alongside
	// these).
	ginkgo.It("PD-3: each Component owns a distinct headless Service", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd3-llama"),
			WithCSRName("pd3-runtime"),
			WithISVCName("pd3-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			engSvc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd3-isvc-engine-headless",
			}, engSvc)).To(Succeed())
			g.Expect(engSvc.Spec.ClusterIP).To(Equal("None"),
				"Engine peer Service must be headless")
			g.Expect(engSvc.Spec.Selector[constants.OMEComponentLabel]).To(Equal("engine"),
				"Engine Service selector must scope to engine pods only")

			decSvc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd3-isvc-decoder-headless",
			}, decSvc)).To(Succeed())
			g.Expect(decSvc.Spec.ClusterIP).To(Equal("None"),
				"Decoder peer Service must be headless")
			g.Expect(decSvc.Spec.Selector[constants.OMEComponentLabel]).To(Equal("decoder"),
				"Decoder Service selector must scope to decoder pods only — overlap would cross-route peer DNS")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PD-4 — Image bump on Engine only must NOT churn Decoder.
	// Validates per-component revision/restart isolation. This is the
	// "Independent" baseline OEP-0007.1's Atomic / PairedMixable /
	// Sequential policies will compose ON TOP of: if the v1 default
	// already cascades changes across components, none of the
	// coordination policies can claim isolation.
	ginkgo.It("PD-4: Engine image bump produces a new Engine CR but leaves Decoder CR set untouched", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd4-llama"),
			WithCSRName("pd4-runtime"),
			WithISVCName("pd4-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var initialDecoderCR string
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			haveEng, haveDec := false, false
			for _, cr := range crs.Items {
				if strings.HasPrefix(cr.Name, "pd4-isvc-engine-") {
					haveEng = true
				}
				if strings.HasPrefix(cr.Name, "pd4-isvc-decoder-") {
					haveDec = true
					initialDecoderCR = cr.Name
				}
			}
			g.Expect(haveEng).To(BeTrue())
			g.Expect(haveDec).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("bumping ONLY the Engine container image")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting a NEW Engine CR; Decoder set must remain at exactly one CR with the same name")
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())

			var engineNames, decoderNames []string
			for _, cr := range crs.Items {
				switch {
				case strings.HasPrefix(cr.Name, "pd4-isvc-engine-"):
					engineNames = append(engineNames, cr.Name)
				case strings.HasPrefix(cr.Name, "pd4-isvc-decoder-"):
					decoderNames = append(decoderNames, cr.Name)
				}
			}
			g.Expect(len(engineNames)).To(BeNumerically(">=", 2),
				"Engine bump must produce a second CR; got %v", engineNames)
			g.Expect(decoderNames).To(HaveLen(1),
				"Decoder must NOT receive a new CR on an Engine-only bump; got %v", decoderNames)
			g.Expect(decoderNames[0]).To(Equal(initialDecoderCR),
				"Decoder CR name must be unchanged — coordination v1 contract")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PD-5 — Per-component migration scope. A migration request that
	// targets `component=engine` must only walk the Engine state
	// machine. The Decoder InstanceStatus must NOT enter Migrating,
	// even though both share the per-ISVC audit ConfigMap. Without
	// the `if req.Component != string(component) { continue }` guard
	// in pendingMigrationRequest, a Decoder reconcile could pick up
	// the Engine request and run a surge against the wrong pod.
	ginkgo.It("PD-5: a Component-scoped migration request only walks the named Component", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd5-llama"),
			WithCSRName("pd5-runtime"),
			WithISVCName("pd5-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var engineNode string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(decCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd5-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			engineNode = pod.Spec.NodeName
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing a migration request scoped to Engine only")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, engineNode, 0)

		ginkgo.By("Engine completes the migration; Decoder index 0 stays put on its original pod")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Index).NotTo(Equal(int32(0)),
				"Engine migration must move off index 0")

			// Decoder must still have ONLY its original instance and remain at index 0.
			// Phase may briefly transition (e.g., status-write race against migration's
			// ISVC mutations); the contract is "no new surge", which is what this asserts.
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1),
				"Decoder must NOT have a surge instance from an Engine-only request")
			g.Expect(decCS.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"Decoder must remain at its original index — engine migration leaked into decoder dispatch")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Decoder phase stays Ready throughout the Engine migration —
		// the lifecycle annotation filter in revisionTemplateMeta means
		// the migration request landing on the ISVC does NOT drift the
		// Decoder's revision hash, so no phantom Updating transition.
		ginkgo.By("Decoder Phase stays Ready throughout the Engine migration")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"Decoder must stay Ready while Engine migrates; if Updating, a lifecycle annotation is leaking into the Decoder's revision hash")

			// Exactly one Decoder CR — proves the migration annotation
			// did NOT drift the Decoder's pod-template hash.
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue, "component": "decoder"},
			)).To(Succeed())
			g.Expect(crs.Items).To(HaveLen(1),
				"Decoder must have exactly one CR; multiple means lifecycle annotations are still drifting the hash")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// The audit ledger is shared but each entry must be Component-
		// tagged. The Engine entry must have Component=engine; there
		// must be NO Decoder entry for the same UUID.
		ginkgo.By("asserting the shared audit ledger carries an Engine-tagged Completed entry only")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, fx.ISVC.Name)
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
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PD-6 — Cascade-delete. Removing the ISVC must drop both
	// Components' children (CRs, headless Services). The parent
	// ISVC's OwnerReference cascade does the work; envtest doesn't
	// run GC so we assert deletionTimestamps land and the apiserver
	// blocks new traffic against them. Validates per-component
	// OwnerReference fan-out doesn't leak orphans on either side.
	ginkgo.It("PD-6: deleting the ISVC stamps deletionTimestamps on both Components' children", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd6-llama"),
			WithCSRName("pd6-runtime"),
			WithISVCName("pd6-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			engSvc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd6-isvc-engine-headless",
			}, engSvc)).To(Succeed())
			decSvc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd6-isvc-decoder-headless",
			}, decSvc)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("deleting the ISVC")
		Expect(k8sClient.Delete(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("ISVC vanishes from the apiserver")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"ISVC must be NotFound after delete; got err=%v", err)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Verify the OwnerReference on the per-component children
		// points at the deleted ISVC. envtest lacks the GC controller,
		// so the Services may persist with the OwnerReference still
		// pointing at the gone ISVC; that's the K8s contract — a real
		// cluster's GC reconciler would then sweep them.
		ginkgo.By("each component's headless Service still carries an OwnerReference back to the deleted ISVC")
		for _, svcName := range []string{"pd6-isvc-engine-headless", "pd6-isvc-decoder-headless"} {
			svc := &corev1.Service{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: svcName}, svc)
			if apierrors.IsNotFound(err) {
				continue
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(svc.OwnerReferences).NotTo(BeEmpty(),
				"%s must have an OwnerReference for cascade GC", svcName)
			Expect(svc.OwnerReferences[0].Name).To(Equal(fx.ISVC.Name),
				"%s OwnerReference must name the parent ISVC", svcName)
		}
	})

	// PD-7 — Restart trigger on one Component must NOT bump the other.
	// Fails an Engine pod; the Engine instance walks through Restart
	// and the Engine Incarnation bumps. The Decoder InstanceStatus
	// Incarnation MUST remain unchanged. Without the per-Component
	// expectations key (verified above), a stale ObservedDelete
	// against the wrong namespace+isvc+component+instance tuple could
	// blow up Decoder progress here.
	ginkgo.It("PD-7: failing an Engine pod restarts Engine without touching Decoder Incarnation", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd7-llama"),
			WithCSRName("pd7-runtime"),
			WithISVCName("pd7-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var decoderInc int64
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(decCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			decoderInc = decCS.OMENative.InstanceStatuses[0].Incarnation
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("forcing the Engine pod into PodFailed to trigger Restart")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd7-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Engine Incarnation bumps; Decoder Incarnation must remain at its original value")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative.InstanceStatuses[0].Incarnation).To(BeNumerically(">", int64(1)),
				"Engine must have bumped Incarnation through Restart")
			g.Expect(decCS.OMENative.InstanceStatuses[0].Incarnation).To(Equal(decoderInc),
				"Decoder Incarnation must remain unchanged — Engine restart leaked into Decoder reconcile")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PD-8 — Scale Engine MinReplicas independently of Decoder. After
	// scale-up Engine has more InstanceStatuses; Decoder stays at its
	// original replica count. The plan builder, scale-down, and
	// extraInstanceIndices logic ALL run per-Component — this spec is
	// the integration-level guard that a scale-up on one component
	// doesn't cause a spurious scale-down on the other.
	ginkgo.It("PD-8: bumping Engine MinReplicas does not change Decoder replica count", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pd8-llama"),
			WithCSRName("pd8-runtime"),
			WithISVCName("pd8-isvc"),
			WithDecoder(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("scaling Engine MinReplicas 1 → 3 without touching Decoder")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			three := 3
			fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(3),
				"Engine must have 3 InstanceStatuses after scale-up")
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1),
				"Decoder must remain at 1 InstanceStatus — Engine scale leaked into Decoder plan")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
