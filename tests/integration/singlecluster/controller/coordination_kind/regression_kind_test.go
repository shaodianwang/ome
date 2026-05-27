package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Regression contracts ported from the envtest regression_test.go.
//
// Most envtest REG-* specs are either pure-function tests (REG-Pure-Aux,
// REG-9, REG-10 — covered by pkg/.../coordination/{groups,reconciler}_test.go)
// or PIt'd hedges against features outside this suite (REG-2/3/5/6/7/8).
// REG-1 is already structurally pinned by baseline_no_coord_kind A-6
// ("status.rolloutCoordination must stay nil when spec omits it").
//
// What remains worth a KIND mirror is the negative-condition assertion
// REG-1 / REG-10 both touch: when an ISVC has no rolloutCoordination
// block, the controller must NOT stamp a `RolloutCoordinationReady`
// status condition. That's distinct from REG-1's "Status.RolloutCoordination
// is nil" (the struct field) and from A-6's reachability check — a future
// regression where the controller defaults the condition to True / False
// for OEP-0011 ingress synergy could silently break operators that key
// dashboards on this condition.
//
// One spec covers the contract; the PIts are dropped as redundant /
// out-of-scope per the envtest sibling's own annotations.
var _ = ginkgo.Describe("REG — coordination regression contracts", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "reg-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("REG-1+REG-10 ISVC without rolloutCoordination must not stamp the RolloutCoordinationReady condition", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("reg-noready"),
			WithEngineImage("fake-serving:v1"))
		// Explicit nil — fixture default is already nil but make the
		// regression's intent unambiguous in the spec.
		Expect(fix.ISVC.Spec.RolloutCoordination).To(BeNil())
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for the ISVC to actually reach steady state so we know
		// the controller has run a full reconcile (otherwise an empty
		// Conditions slice could just mean it hasn't run yet).
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, "reg-noready")
			engCS := c.Status.Components[v1beta1.EngineComponent]
			decCS := c.Status.Components[v1beta1.DecoderComponent]
			g.Expect(engCS.OMENative).NotTo(BeNil())
			g.Expect(decCS.OMENative).NotTo(BeNil())
			g.Expect(engCS.OMENative.ServingReplicas).To(Equal(int32(1)))
			g.Expect(decCS.OMENative.ServingReplicas).To(Equal(int32(1)))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// REG-1: Status.RolloutCoordination stays nil. (Pinned by A-6
		// too; re-asserting here keeps the regression scope honest.)
		c := freshISVC(Default, ns.Name, "reg-noready")
		Expect(c.Status.RolloutCoordination).To(BeNil(),
			"Status.RolloutCoordination must not be populated when spec.rolloutCoordination is nil")

		// REG-10: no RolloutCoordinationReady condition is stamped.
		// Check the API condition type constant directly so a rename
		// trips this test immediately rather than silently passing on
		// the old string.
		for _, cond := range c.Status.Conditions {
			Expect(string(cond.Type)).NotTo(Equal(string(v1beta1.RolloutCoordinationReady)),
				"RolloutCoordinationReady condition must NOT be stamped on ISVCs without a coord block; got %+v", cond)
		}
	})
})
