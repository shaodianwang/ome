package coordination_kind

import (
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

// PD-isolation contracts. Each Component (engine, decoder) owns its
// own revision stream and its own headless Service; an image bump on
// one Component must not churn the other; scaling one Component must
// not move the other. These contracts underpin every coordination
// policy — if the per-Component isolation breaks, BlueGreen /
// RollingUpdate / Sequential / Independent all break with it.
//
// Specs use the basic newCoordFixture (no WithCoordinationGroup)
// because PD isolation is orthogonal to the coord state machine.
var _ = ginkgo.Describe("PD per-Component isolation", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "pd-iso-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("PD-2 each Component owns a distinct ControllerRevision", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pd2-iso"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			var engineCount, decoderCount int
			for _, cr := range crs.Items {
				switch {
				case strings.HasPrefix(cr.Name, "pd2-iso-engine-"):
					engineCount++
				case strings.HasPrefix(cr.Name, "pd2-iso-decoder-"):
					decoderCount++
				default:
					ginkgo.Fail(fmt.Sprintf("unexpected CR name %q does not match per-Component prefix", cr.Name))
				}
			}
			g.Expect(engineCount).To(Equal(1), "expected exactly one Engine CR; got %d", engineCount)
			g.Expect(decoderCount).To(Equal(1), "expected exactly one Decoder CR; got %d", decoderCount)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("PD-3 each Component owns a distinct headless Service", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pd3-iso"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			engSvc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd3-iso-engine-headless",
			}, engSvc)).To(Succeed())
			g.Expect(engSvc.Spec.ClusterIP).To(Equal("None"))
			g.Expect(engSvc.Spec.Selector[constants.OMEComponentLabel]).To(Equal("engine"))

			decSvc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "pd3-iso-decoder-headless",
			}, decSvc)).To(Succeed())
			g.Expect(decSvc.Spec.ClusterIP).To(Equal("None"))
			g.Expect(decSvc.Spec.Selector[constants.OMEComponentLabel]).To(Equal("decoder"))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("PD-4 engine image bump produces a new Engine CR; decoder CR set untouched", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pd4-iso"),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		var initialDecoderCR string
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			haveEng, haveDec := false, false
			for _, cr := range crs.Items {
				if strings.HasPrefix(cr.Name, "pd4-iso-engine-") {
					haveEng = true
				}
				if strings.HasPrefix(cr.Name, "pd4-iso-decoder-") {
					haveDec = true
					initialDecoderCR = cr.Name
				}
			}
			g.Expect(haveEng).To(BeTrue())
			g.Expect(haveDec).To(BeTrue())
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Bump only engine image.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pd4-iso")
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// New engine CR appears; decoder set is unchanged (still exactly
		// one CR with the same name).
		Eventually(func(g Gomega) {
			crs := &appsv1.ControllerRevisionList{}
			g.Expect(k8sClient.List(suiteCtx, crs,
				client.InNamespace(ns.Name),
				client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
			)).To(Succeed())
			var engineCount, decoderCount int
			var decoderNames []string
			for _, cr := range crs.Items {
				if strings.HasPrefix(cr.Name, "pd4-iso-engine-") {
					engineCount++
				}
				if strings.HasPrefix(cr.Name, "pd4-iso-decoder-") {
					decoderCount++
					decoderNames = append(decoderNames, cr.Name)
				}
			}
			g.Expect(engineCount).To(BeNumerically(">=", 2),
				"PD-4: engine bump should produce at least 2 engine CRs (v1 + v2); got %d", engineCount)
			g.Expect(decoderCount).To(Equal(1),
				"PD-4: decoder must NOT churn on engine bump; got %d decoder CRs (%v)", decoderCount, decoderNames)
			g.Expect(decoderNames[0]).To(Equal(initialDecoderCR),
				"PD-4: decoder CR name must be unchanged after engine bump")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("PD-8 engine scale-up does not change decoder replica count", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pd8-iso"),
			WithEngineReplicas(1),
			WithEngineImage("fake-serving:v1"))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the initial 1+1 state.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pd8-iso")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.Replicas).To(Equal(int32(1)))
			g.Expect(decCS.Lifecycle.Replicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Scale engine up to 2.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pd8-iso")
			two := 2
			cur.Spec.Engine.MinReplicas = &two
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Engine reaches 2; decoder stays at 1.
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "pd8-iso")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle.Replicas).To(Equal(int32(2)),
				"PD-8: engine scale-up should reach 2; got %d", engCS.Lifecycle.Replicas)
			g.Expect(decCS.Lifecycle.Replicas).To(Equal(int32(1)),
				"PD-8: decoder must NOT change on engine scale-up; got %d", decCS.Lifecycle.Replicas)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
