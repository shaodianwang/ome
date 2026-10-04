package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A-6 — PD baseline without a rolloutCoordination block.
//
// Asserts the OMENative reconciler runs to convergence WITHOUT a
// coordination spec: per-Instance status populates, OMENative-level
// AvailableReplicas hits 1 per Component, and status.rolloutCoordination
// stays nil. Catches regressions where the controller mistakenly
// requires a coord block to advance.
var _ = ginkgo.Describe("baseline — PD without rolloutCoordination", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "nocoord-a6-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("A-6 engine + decoder converge with no rolloutCoordination block; status.rolloutCoordination stays nil", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("nocoord-a6"),
			WithEngineImage("fake-serving:v1"))
		// No WithCoordinationGroup → fixture leaves Spec.Rollout
		// nil. Both Components are still OMENative; the coordination layer
		// just doesn't engage.
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "nocoord-a6")
			g.Expect(c.Status.RolloutCoordination).To(BeNil(),
				"status.rolloutCoordination must stay nil when spec omits it")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ReadyReplicas).To(Equal(int32(1)))
			g.Expect(decCS.Lifecycle.ReadyReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	// The same convergence under a service name long enough that every name
	// derived from it is bounded: the model's node-label key and the
	// per-revision headless Service names. A bounded name must be read back
	// exactly as it was written, or the pods never schedule and every
	// Instance stays Creating.
	ginkgo.It("A-6 engine + decoder converge under a service name whose derived names are bounded", func() {
		const isvcName = "nocoord-a6-with-a-long-service-name"
		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithEngineImage("fake-serving:v1"))
		Expect(constants.GetClusterBaseModelLabel(fix.CBM.Name)).NotTo(HaveSuffix("."+fix.CBM.Name),
			"premise: the model name must be bounded by the node-label helper")
		Expect(query.PerRevisionHeadlessServiceName(isvcName, workloadtypes.ComponentEngine, "abcd1234")).
			NotTo(Equal(isvcName+"-engine-rev-abcd1234-headless"),
				"premise: the per-revision headless Service name must be bounded")
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
				rows := instanceStatuses(g, ns.Name, isvcName, component)
				g.Expect(rows).To(HaveLen(1), "%s rows", component)
				g.Expect(rows[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady), "%s row: %+v", component, rows[0])
				g.Expect(rows[0].RunningRevision).NotTo(BeEmpty(), "%s row runs no revision", component)
			}
			pods := listOMENativePods(g, ns.Name, isvcName)
			g.Expect(pods).To(HaveLen(2))
			for _, pod := range pods {
				g.Expect(pod.Spec.NodeName).NotTo(BeEmpty(), "pod %s is not scheduled", pod.Name)
			}
			c := freshISVC(g, ns.Name, isvcName)
			g.Expect(c.Status.RolloutCoordination).To(BeNil(),
				"status.rolloutCoordination must stay nil when spec omits it")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.Lifecycle).NotTo(BeNil())
			g.Expect(decCS.Lifecycle).NotTo(BeNil())
			g.Expect(engCS.Lifecycle.ReadyReplicas).To(Equal(int32(1)))
			g.Expect(decCS.Lifecycle.ReadyReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})
