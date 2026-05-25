package coordination

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Coordination regression suite (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-reg-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("REG-9 OEP-0007.1 stayed in-CRD: RolloutCoordination is a *field* on InferenceService, not its own CRD", func() {
		var spec v1beta1.InferenceServiceSpec
		var status v1beta1.InferenceServiceStatus
		_ = spec.RolloutCoordination
		_ = status.RolloutCoordination
		Expect(spec.RolloutCoordination).To(BeNil())
		Expect(status.RolloutCoordination).To(BeNil())
	})

	ginkgo.It("REG-10 RolloutCoordinationReady condition NOT stamped when no groups declared", func() {
		isvc := &v1beta1.InferenceService{}
		isvc.Name = "reg10"
		isvc.Namespace = ns.Name
		coordination.SetRolloutCoordinationReady(isvc, nil, time.Now())
		for _, c := range isvc.Status.Conditions {
			Expect(string(c.Type)).NotTo(Equal(v1beta1.RolloutCoordinationReady),
				"with no groups, RolloutCoordinationReady must NOT be stamped")
		}
	})

	ginkgo.It("REG-Pure-Aux ResolveGroups on nil RolloutCoordinationSpec → empty slice (no-op preserved)", func() {
		groups := coordination.ResolveGroups(nil)
		Expect(groups).To(BeEmpty())
	})

	ginkgo.It("REG-1 ISVC without coordination block has no Status.RolloutCoordination + no Ready condition", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("reg1"))
		fix.ISVC.Spec.RolloutCoordination = nil
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "reg1")
			g.Expect(isvc.Status.RolloutCoordination).To(BeNil(),
				"Status.RolloutCoordination should not be populated without a coord block")
			for _, c := range isvc.Status.Conditions {
				g.Expect(string(c.Type)).NotTo(Equal(v1beta1.RolloutCoordinationReady),
					"RolloutCoordinationReady condition must not be stamped")
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("REG-2 RawDeployment Component in mixed-mode — needs mixed-mode controller setup not in this suite", func() {})
	ginkgo.PIt("REG-3 OEP-0011 traffic translator reader — belongs in ingress suite", func() {})

	ginkgo.It("REG-4 OEP-0007 single-Component lifecycle unchanged (no coord block → behaves like before)", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("reg4"), WithoutDecoder())
		fix.ISVC.Spec.RolloutCoordination = nil
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// With no coord block, per-revision Services + Traffic[] still
		// emit for any OMENative Component; coord status stays nil.
		Eventually(func(g Gomega) {
			isvc := freshISVC(g, ns.Name, "reg4")
			g.Expect(isvc.Status.RolloutCoordination).To(BeNil(),
				"no coord block → no coord status")
			traffic := trafficForComponent(isvc, v1beta1.EngineComponent)
			g.Expect(traffic).NotTo(BeEmpty(),
				"data plane (Traffic[]) should still emit for OMENative Component")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.PIt("REG-5 migration verb cross-feature — covered by OMENative MIGRATE-* suite", func() {})
	ginkgo.PIt("REG-6 webhook throughput — belongs in a benchmark suite", func() {})
	ginkgo.PIt("REG-7 OMENative integration suite passes — covered by OMENative suite's own CI job", func() {})
	ginkgo.PIt("REG-8 fault-injection retry — needs fault-injecting client wrapper not wired into this suite", func() {})
})
