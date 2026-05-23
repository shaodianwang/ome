package omenative

import (
	"strings"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Three-way and mixed-mode PD topologies (Router + Engine + Decoder).
// Operator deployment shapes for OEP-0007.1: a Router fronts the PD
// pair, and operators sometimes opt one component out of OMENative.
// These specs extend the 2-way isolation contract in pd_test.go to
// three Components and to OMENative + Raw mixed deployments.
var _ = ginkgo.Describe("OMENative PD three-way + mixed-mode topologies", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-pdx-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PDX-1 — Three-way OMENative: Router + Engine + Decoder all opt
	// into OMENative. All three Components must reach Ready
	// independently. The single most-critical OEP-0007.1 topology
	// gate: if this doesn't converge, no coordination policy can rest
	// on top of it.
	ginkgo.It("PDX-1: Router + Engine + Decoder all OMENative converge to Ready", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pdx1-llama"),
			WithCSRName("pdx1-runtime"),
			WithISVCName("pdx1-isvc"),
			WithDecoder(),
			WithRouter(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("expecting OMENative-shape pods for all three components")
		Eventually(func(g Gomega) {
			for _, name := range []string{
				"pdx1-isvc-engine-0-default-0",
				"pdx1-isvc-decoder-0-default-0",
				"pdx1-isvc-router-0-default-0",
			} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, pod)).To(Succeed(),
					"missing OMENative-shape pod %s; if component was routed to RawDeployment this Get fails", name)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting all three Components' OMENative status to reach Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := fresh.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil(), "%s must have OMENative status", comp)
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1), "%s replicas mismatch", comp)
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"%s instance must reach Ready", comp)
			}
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PDX-2 — Three-way headless Services: each Component gets its
	// own per-Component headless Service. Pins selector isolation
	// across all three Components — overlap would cross-route peer
	// discovery (e.g., Engine resolving "router" via the
	// router-headless DNS picks up decoder pods).
	ginkgo.It("PDX-2: each of Engine/Decoder/Router has its own headless Service with a tight selector", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pdx2-llama"),
			WithCSRName("pdx2-runtime"),
			WithISVCName("pdx2-isvc"),
			WithDecoder(),
			WithRouter(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			for compName, expectedSelector := range map[string]string{
				"pdx2-isvc-engine-headless":  "engine",
				"pdx2-isvc-decoder-headless": "decoder",
				"pdx2-isvc-router-headless":  "router",
			} {
				svc := &corev1.Service{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: compName}, svc)).To(Succeed())
				g.Expect(svc.Spec.ClusterIP).To(Equal("None"))
				g.Expect(svc.Spec.Selector["component"]).To(Equal(expectedSelector),
					"%s selector must be {component=%s}; any other value cross-routes peer DNS", compName, expectedSelector)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PDX-3 — Three-way per-Component CR streams. CR retention,
	// detectUpdateTrigger, and EnsureControllerRevision ALL key off
	// (isvc, component) — collisions would mix histories or trigger
	// phantom rollouts. With three Components the matrix is bigger:
	// 3 streams, each independent.
	ginkgo.It("PDX-3: each Component owns a distinct ControllerRevision (3 streams of 1)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pdx3-llama"),
			WithCSRName("pdx3-runtime"),
			WithISVCName("pdx3-isvc"),
			WithDecoder(),
			WithRouter(),
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

			counts := map[string]int{"engine": 0, "decoder": 0, "router": 0}
			for _, cr := range crs.Items {
				for c := range counts {
					if strings.HasPrefix(cr.Name, "pdx3-isvc-"+c+"-") {
						counts[c]++
					}
				}
			}
			g.Expect(counts["engine"]).To(Equal(1), "expected 1 Engine CR; got %d (names: %+v)", counts["engine"], crNames(crs.Items))
			g.Expect(counts["decoder"]).To(Equal(1), "expected 1 Decoder CR; got %d", counts["decoder"])
			g.Expect(counts["router"]).To(Equal(1), "expected 1 Router CR; got %d", counts["router"])
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PDX-4 — In a three-way ISVC, bumping the Router image must
	// only churn Router's CR stream; Engine and Decoder stay frozen.
	// Closes the third-component side of the per-Component update
	// isolation contract (PD-4 covers Engine↔Decoder).
	ginkgo.It("PDX-4: Router image bump produces a new Router CR; Engine + Decoder CR sets untouched", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pdx4-llama"),
			WithCSRName("pdx4-runtime"),
			WithISVCName("pdx4-isvc"),
			WithImage("ghcr.io/test/runtime:v1"),
			WithDecoder(),
			WithRouter(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var initialEngineCR, initialDecoderCR string
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			seen := 0
			for _, cr := range crs.Items {
				switch {
				case strings.HasPrefix(cr.Name, "pdx4-isvc-engine-"):
					initialEngineCR = cr.Name
					seen++
				case strings.HasPrefix(cr.Name, "pdx4-isvc-decoder-"):
					initialDecoderCR = cr.Name
					seen++
				case strings.HasPrefix(cr.Name, "pdx4-isvc-router-"):
					seen++
				}
			}
			g.Expect(seen).To(Equal(3), "expected one CR per component before any bump; got %d", seen)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("bumping ONLY the Router image")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			fresh.Spec.Router.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Router gets a 2nd CR; Engine + Decoder CR names unchanged")
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			engineNames, decoderNames, routerNames := []string{}, []string{}, []string{}
			for _, cr := range crs.Items {
				switch {
				case strings.HasPrefix(cr.Name, "pdx4-isvc-engine-"):
					engineNames = append(engineNames, cr.Name)
				case strings.HasPrefix(cr.Name, "pdx4-isvc-decoder-"):
					decoderNames = append(decoderNames, cr.Name)
				case strings.HasPrefix(cr.Name, "pdx4-isvc-router-"):
					routerNames = append(routerNames, cr.Name)
				}
			}
			g.Expect(engineNames).To(HaveLen(1), "Engine must NOT receive a new CR on Router bump; got %v", engineNames)
			g.Expect(engineNames[0]).To(Equal(initialEngineCR))
			g.Expect(decoderNames).To(HaveLen(1), "Decoder must NOT receive a new CR on Router bump; got %v", decoderNames)
			g.Expect(decoderNames[0]).To(Equal(initialDecoderCR))
			g.Expect(len(routerNames)).To(BeNumerically(">=", 2), "Router bump must produce a second CR; got %v", routerNames)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PDX-5 — Per-Component migration with all three components
	// running OMENative. Engine-targeted migration must not perturb
	// Decoder or Router. Strengthens PD-5 by adding a third
	// component to the isolation matrix.
	ginkgo.It("PDX-5: Engine migration in a three-way deployment leaves Decoder + Router untouched", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pdx5-llama"),
			WithCSRName("pdx5-runtime"),
			WithISVCName("pdx5-isvc"),
			WithDecoder(),
			WithRouter(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var engineNode string
		var initialDecInc, initialRouterInc int64
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := fresh.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			initialDecInc = fresh.Status.Components[v1beta1.DecoderComponent].OMENative.InstanceStatuses[0].Incarnation
			initialRouterInc = fresh.Status.Components[v1beta1.RouterComponent].OMENative.InstanceStatuses[0].Incarnation

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pdx5-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			engineNode = pod.Spec.NodeName
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("writing an Engine-scoped migration request")
		reqUUID := uuid.NewString()
		writeMigrationRequest(NewWithT(ginkgo.GinkgoT()), client.ObjectKeyFromObject(fx.ISVC), reqUUID, engineNode, 0)

		ginkgo.By("Engine migrates; Decoder + Router stay on their original instance + Incarnation")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())

			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Index).NotTo(Equal(int32(0)),
				"Engine must have migrated off index 0")

			decCS := fresh.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"Decoder must not gain a surge from Engine-only migration")
			g.Expect(decCS.OMENative.InstanceStatuses[0].Incarnation).To(Equal(initialDecInc),
				"Decoder Incarnation must remain unchanged")

			routerCS := fresh.Status.Components[v1beta1.RouterComponent]
			g.Expect(routerCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(routerCS.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"Router must not gain a surge from Engine-only migration")
			g.Expect(routerCS.OMENative.InstanceStatuses[0].Incarnation).To(Equal(initialRouterInc),
				"Router Incarnation must remain unchanged")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// PDX-6 — Mixed mode: Engine OMENative + Decoder RawDeployment.
	// The OMENative dispatcher must NOT touch the Decoder, and the
	// Raw dispatcher must NOT touch the Engine. envtest's Raw
	// reconciler doesn't run kubelet sim on its pods, so we assert
	// shape: OMENative-shape Engine pod exists, no OMENative-shape
	// Decoder pod exists, Engine's OMENative InstanceStatus reaches
	// Ready.
	ginkgo.It("PDX-6: mixed mode (Engine OMENative + Decoder Raw) dispatches each to its own reconciler", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pdx6-llama"),
			WithCSRName("pdx6-runtime"),
			WithISVCName("pdx6-isvc"),
			WithDecoder(),
			WithDecoderRawMode(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("Engine reaches Ready under OMENative")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			engCS := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("no OMENative-shape pod exists for Decoder (Raw dispatcher owns it)")
		Consistently(func(g Gomega) {
			pod := &corev1.Pod{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pdx6-isvc-decoder-0-default-0",
			}, pod)
			g.Expect(err).To(HaveOccurred(),
				"OMENative-shape Decoder pod must NOT exist; Decoder is Raw and uses a Deployment-shape pod")
		}, "2s", util.PollInterval).Should(Succeed())

		ginkgo.By("Engine's OMENative status set; Decoder.OMENative is nil (Raw doesn't populate it)")
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
		Expect(fresh.Status.Components[v1beta1.EngineComponent].OMENative).NotTo(BeNil())
		// Decoder.OMENative may be nil OR have empty InstanceStatuses;
		// the contract is "OMENative state machine didn't run for it",
		// which both shapes prove.
		decCS := fresh.Status.Components[v1beta1.DecoderComponent]
		if decCS.OMENative != nil {
			Expect(decCS.OMENative.InstanceStatuses).To(BeEmpty(),
				"Decoder OMENative state must be empty when running under Raw — non-empty means the OMENative dispatcher leaked")
		}
	})

	// PDX-7 — Per-Component restart in a three-way deployment.
	// Failing the Router pod must walk Router through Restart without
	// perturbing Engine or Decoder. Tests the third-component
	// boundary of the per-Component expectations key.
	ginkgo.It("PDX-7: failing the Router pod restarts Router without touching Engine + Decoder", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("pdx7-llama"),
			WithCSRName("pdx7-runtime"),
			WithISVCName("pdx7-isvc"),
			WithDecoder(),
			WithRouter(),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		var initialEngInc, initialDecInc int64
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := fresh.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			initialEngInc = fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Incarnation
			initialDecInc = fresh.Status.Components[v1beta1.DecoderComponent].OMENative.InstanceStatuses[0].Incarnation
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("forcing Router pod PodFailed")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pdx7-isvc-router-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Router Incarnation bumps; Engine + Decoder Incarnations remain unchanged")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			routerInc := fresh.Status.Components[v1beta1.RouterComponent].OMENative.InstanceStatuses[0].Incarnation
			engInc := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Incarnation
			decInc := fresh.Status.Components[v1beta1.DecoderComponent].OMENative.InstanceStatuses[0].Incarnation
			g.Expect(routerInc).To(BeNumerically(">", int64(1)),
				"Router must have bumped Incarnation through Restart")
			g.Expect(engInc).To(Equal(initialEngInc),
				"Engine Incarnation must remain unchanged — Router restart leaked into Engine reconcile")
			g.Expect(decInc).To(Equal(initialDecInc),
				"Decoder Incarnation must remain unchanged — Router restart leaked into Decoder reconcile")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
