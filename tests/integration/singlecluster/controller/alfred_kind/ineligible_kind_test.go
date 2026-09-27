package alfred_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("alfred surfaces ineligible instances as advisories", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "alfred-inelig-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("migrationPolicy Never is advisory only", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("inelig-never"), withMigrationMode(v1beta1.MigrationPolicyModeNever))
		readyOn(fix, src, 1)

		// alfred reads the migration policy from the InferenceReplica, so the
		// ISVC's lifecycle block matters only through the controller's projection.
		ginkgo.By("the policy is projected onto the InferenceReplica")
		Eventually(func(g Gomega) {
			ir := freshIR(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Lifecycle).NotTo(BeNil())
			g.Expect(ir.Spec.Lifecycle.MigrationPolicy).NotTo(BeNil())
			g.Expect(ir.Spec.Lifecycle.MigrationPolicy.Mode).To(Equal(v1beta1.MigrationPolicyModeNever))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		markUnhealthy(src)
		expectIneligibleAdvisory(fix, src)
	})

	ginkgo.It("a paused rollout is never dispatched", func() {
		src := workers[0]
		fix := newAlfredFixture(ns.Name, withName("inelig-paused"))
		readyOn(fix, src, 1)

		// The controller projects the ISVC pause onto the InferenceReplica's
		// spec.paused, which is what alfred reads. The pause annotation is not
		// part of the revision hash, so the workload stays steady and its
		// status stays current while paused.
		ginkgo.By("the pause is projected onto the InferenceReplica")
		annotateISVC(Default, isvcKey(fix), map[string]string{constants.PausedRolloutAnnotation: "true"})
		Eventually(func(g Gomega) {
			ir := freshIR(g, ns.Name, fix.ISVC.Name, v1beta1.EngineComponent)
			g.Expect(ir.Spec.Paused).To(BeTrue())
			g.Expect(ir.Status.ObservedGeneration).To(Equal(ir.Generation))
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		markUnhealthy(src)
		expectIneligibleAdvisory(fix, src)
	})
})
