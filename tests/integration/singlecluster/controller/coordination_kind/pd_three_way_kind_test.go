package coordination_kind

import (
	"strings"

	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Three-way and mixed-mode PD topologies (Router + Engine + Decoder).
// Operator deployment shapes for OEP-0007.1: a Router fronts the PD
// pair, and operators sometimes opt one component out of OMENative.
// These specs extend the 2-way contracts in pd_kind / pd_isolation_kind
// to three Components and to OMENative + Raw mixed deployments.
var _ = ginkgo.Describe("OMENative PD three-way + mixed-mode topologies (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-pdx-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// PDX-1 — All three Components (Router + Engine + Decoder) reach
	// Ready under OMENative. The single most-critical OEP-0007.1
	// topology gate.
	ginkgo.It("PDX-1: Router + Engine + Decoder all OMENative converge to Ready", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pdx1-isvc"),
			WithRouter(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("expecting OMENative-shape pods for all three components")
		Eventually(func(g Gomega) {
			for _, name := range []string{
				"pdx1-isvc-engine-0-default-0",
				"pdx1-isvc-decoder-0-default-0",
				"pdx1-isvc-router-0-default-0",
			} {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, pod)).To(Succeed(),
					"missing OMENative-shape pod %s", name)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("expecting all three Components' OMENative status to reach Ready")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pdx1-isvc")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil(), "%s must have OMENative status", comp)
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1), "%s replicas mismatch", comp)
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"%s instance must reach Ready", comp)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PDX-2 — Each of the three Components has its own headless
	// Service with a tight selector. Overlap would cross-route peer
	// DNS (e.g., Engine resolving "router" via the router-headless DNS
	// picks up decoder pods).
	ginkgo.It("PDX-2: each of Engine/Decoder/Router has its own headless Service with a tight selector", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pdx2-isvc"),
			WithRouter(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PDX-3 — Each Component owns a distinct ControllerRevision (3
	// streams of 1). CR retention, detectUpdateTrigger, and
	// EnsureControllerRevision ALL key off (isvc, component) — collisions
	// would mix histories or trigger phantom rollouts.
	ginkgo.It("PDX-3: each Component owns a distinct ControllerRevision (3 streams of 1)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pdx3-isvc"),
			WithRouter(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())

			counts := map[string]int{"engine": 0, "decoder": 0, "router": 0}
			for _, cr := range crs.Items {
				for c := range counts {
					if strings.HasPrefix(cr.Name, "pdx3-isvc-"+c+"-") {
						counts[c]++
					}
				}
			}
			g.Expect(counts["engine"]).To(Equal(1), "expected 1 Engine CR; got %d", counts["engine"])
			g.Expect(counts["decoder"]).To(Equal(1), "expected 1 Decoder CR; got %d", counts["decoder"])
			g.Expect(counts["router"]).To(Equal(1), "expected 1 Router CR; got %d", counts["router"])
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PDX-4 — Bumping the Router image must only churn Router's CR
	// stream; Engine and Decoder stay frozen. Closes the third-component
	// side of per-Component update isolation (PD-4 covers Engine↔Decoder).
	ginkgo.It("PDX-4: Router image bump produces a new Router CR; Engine + Decoder CR sets untouched", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pdx4-isvc"),
			WithRouter(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var initialEngineCR, initialDecoderCR string
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping ONLY the Router image")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pdx4-isvc")
			cur.Spec.Router.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("Router gets a 2nd CR; Engine + Decoder CR names unchanged")
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
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
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PDX-5 — Per-Component migration with all three components running
	// OMENative. Engine-targeted migration must not perturb Decoder or
	// Router.
	ginkgo.It("PDX-5: Engine migration in a three-way deployment leaves Decoder + Router untouched", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pdx5-isvc"),
			WithRouter(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var engineNode string
		var initialDecInc, initialRouterInc int64
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pdx5-isvc")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			initialDecInc = c.Status.Components[v1beta1.DecoderComponent].OMENative.InstanceStatuses[0].Incarnation
			initialRouterInc = c.Status.Components[v1beta1.RouterComponent].OMENative.InstanceStatuses[0].Incarnation

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pdx5-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			g.Expect(pod.Spec.NodeName).NotTo(BeEmpty())
			engineNode = pod.Spec.NodeName
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("writing an Engine-scoped migration request")
		reqUUID := uuid.NewString()
		writeMigrationRequest(Default, client.ObjectKeyFromObject(fix.ISVC), reqUUID, engineNode, 0)

		ginkgo.By("Engine migrates; Decoder + Router stay on their original instance + Incarnation")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pdx5-isvc")

			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Index).NotTo(Equal(int32(0)),
				"Engine must have migrated off index 0")

			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(decCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(decCS.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"Decoder must not gain a surge from Engine-only migration")
			g.Expect(decCS.OMENative.InstanceStatuses[0].Incarnation).To(Equal(initialDecInc),
				"Decoder Incarnation must remain unchanged")

			routerCS := c.Status.Components[v1beta1.RouterComponent]
			g.Expect(routerCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(routerCS.OMENative.InstanceStatuses[0].Index).To(Equal(int32(0)),
				"Router must not gain a surge from Engine-only migration")
			g.Expect(routerCS.OMENative.InstanceStatuses[0].Incarnation).To(Equal(initialRouterInc),
				"Router Incarnation must remain unchanged")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// PDX-6 — Mixed mode: Engine OMENative + Decoder RawDeployment.
	// The OMENative dispatcher must NOT touch the Decoder, and the
	// Raw dispatcher must NOT touch the Engine.
	ginkgo.It("PDX-6: mixed mode (Engine OMENative + Decoder Raw) dispatches each to its own reconciler", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pdx6-isvc"),
			WithDecoderRawMode(),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("Engine reaches Ready under OMENative")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pdx6-isvc")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(engCS.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("no OMENative-shape pod exists for Decoder (Raw dispatcher owns it)")
		Consistently(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pdx6-isvc-decoder-0-default-0",
			}, &corev1.Pod{})
			g.Expect(err).To(HaveOccurred(),
				"OMENative-shape Decoder pod must NOT exist; Decoder is Raw")
		}, "3s", kindPollInterval).Should(Succeed())

		ginkgo.By("Engine's OMENative status set; Decoder.OMENative is nil or empty")
		c := freshISVC(Default, ns.Name, "pdx6-isvc")
		Expect(c.Status.Components[v1beta1.EngineComponent].OMENative).NotTo(BeNil())
		decCS := c.Status.Components[v1beta1.DecoderComponent]
		if decCS.OMENative != nil {
			Expect(decCS.OMENative.InstanceStatuses).To(BeEmpty(),
				"Decoder OMENative state must be empty when running under Raw — non-empty means the OMENative dispatcher leaked")
		}
	})

	// PDX-7 — Per-Component restart in a three-way deployment. Failing
	// the Router pod must walk Router through Restart without
	// perturbing Engine or Decoder.
	ginkgo.It("PDX-7: failing the Router pod restarts Router without touching Engine + Decoder", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pdx7-isvc"),
			WithRouter(),
			WithEngineImage("fake-serving:v1"))
		// Restart on pod-delete for the Router component.
		restartRecreate := v1beta1.InstanceRestartPolicyRecreateInstance
		fix.ISVC.Spec.Router.Lifecycle = &v1beta1.LifecycleSpec{
			RestartPolicy: &restartRecreate,
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var initialEngInc, initialDecInc int64
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pdx7-isvc")
			for _, comp := range []v1beta1.ComponentType{
				v1beta1.EngineComponent,
				v1beta1.DecoderComponent,
				v1beta1.RouterComponent,
			} {
				cs := c.Status.Components[comp]
				g.Expect(cs.OMENative).NotTo(BeNil())
				g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
				g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			}
			initialEngInc = c.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Incarnation
			initialDecInc = c.Status.Components[v1beta1.DecoderComponent].OMENative.InstanceStatuses[0].Incarnation
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("force-deleting Router pod to trigger Restart")
		grace := int64(0)
		_ = k8sClient.Delete(suiteCtx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "pdx7-isvc-router-0-default-0"},
		}, &client.DeleteOptions{GracePeriodSeconds: &grace})

		ginkgo.By("Router Incarnation bumps; Engine + Decoder Incarnations remain unchanged")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pdx7-isvc")
			routerInc := c.Status.Components[v1beta1.RouterComponent].OMENative.InstanceStatuses[0].Incarnation
			engInc := c.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Incarnation
			decInc := c.Status.Components[v1beta1.DecoderComponent].OMENative.InstanceStatuses[0].Incarnation
			g.Expect(routerInc).To(BeNumerically(">", int64(1)),
				"Router must have bumped Incarnation through Restart")
			g.Expect(engInc).To(Equal(initialEngInc),
				"Engine Incarnation must remain unchanged — Router restart leaked into Engine reconcile")
			g.Expect(decInc).To(Equal(initialDecInc),
				"Decoder Incarnation must remain unchanged — Router restart leaked into Decoder reconcile")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
