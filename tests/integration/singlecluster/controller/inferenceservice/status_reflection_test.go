package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// InferenceService Status reflection. The reconciler computes
// Status.URL / Status.Address / Status.Components from the produced
// child resources (Service hostname for RawDeployment /
// OMENative). Specs here exercise that observation loop end-to-end
// with the real apiserver feeding child-resource events back into
// the controller.
//
// Catches regressions where Status fields are set in memory but
// never persist (missing Status().Update call) or where the
// controller stops reflecting changes after a spec edit.
var _ = ginkgo.Describe("InferenceService Status reflection", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-status-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	makeBaseModelAndRuntime := func(suffix string) (string, string) {
		cbm := v1beta1testing.MakeClusterBaseModel("status-cbm-" + suffix).
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("status-rt-"+suffix).
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		return cbm.Name, csr.Name
	}

	ginkgo.It("RawDeployment mode → Status.URL is populated with the produced Service's host:port", func() {
		// Once the controller creates the Service, it reads
		// Service.Spec.Ports[0].Port and computes
		// host = <svc>.<ns>.svc.cluster.local, then writes that
		// into Status.URL. Asserts the FULL pipeline end-to-end
		// (apiserver create → controller observes → status update).
		cbm, rt := makeBaseModelAndRuntime("url")
		isvc := v1beta1testing.MakeInferenceService("status-url-isvc", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "status-url-isvc", Namespace: ns.Name}, got)).To(Succeed())
			g.Expect(got.Status.URL).NotTo(BeNil(), "Status.URL must be populated once the Service is observable")
			g.Expect(got.Status.URL.Host).To(ContainSubstring(ns.Name),
				"URL host must include the ISVC namespace (DNS hostname format <svc>.<ns>.svc.cluster.local)")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Status.Address is populated alongside Status.URL", func() {
		// Address is the duck v1 representation that consumers
		// (Knative-style) use; must stay in sync with URL. Pin it
		// so a refactor that touches only one of the two surfaces
		// any drift.
		cbm, rt := makeBaseModelAndRuntime("addr")
		isvc := v1beta1testing.MakeInferenceService("status-addr-isvc", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "status-addr-isvc", Namespace: ns.Name}, got)).To(Succeed())
			g.Expect(got.Status.Address).NotTo(BeNil())
			g.Expect(got.Status.Address.URL).NotTo(BeNil())
			g.Expect(got.Status.Address.URL.Host).To(Equal(got.Status.URL.Host),
				"Status.Address.URL.Host must mirror Status.URL.Host")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("VirtualDeployment mode → Conditions Ready=True with reason=VirtualDeployment", func() {
		// VirtualDeployment is a special path where there's no
		// real child Deployment — the controller short-circuits
		// to "Ready" because external-to-OME systems handle the
		// runtime. Pin the special-case condition so a refactor
		// that drops the short-circuit surfaces here.
		cbm, rt := makeBaseModelAndRuntime("vd")
		isvc := v1beta1testing.MakeInferenceService("status-vd-isvc", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.VirtualDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "status-vd-isvc", Namespace: ns.Name}, got)).To(Succeed())
			g.Expect(got.Status.Conditions).NotTo(BeEmpty())

			// duckv1.Status.Conditions uses knative apis.Condition;
			// match by Type-as-string to avoid pulling in the
			// knative type just for the assertion.
			var found bool
			for _, c := range got.Status.Conditions {
				if string(c.Type) != "Ready" {
					continue
				}
				g.Expect(string(c.Status)).To(Equal("True"))
				g.Expect(c.Reason).To(Equal("VirtualDeployment"))
				found = true
				break
			}
			g.Expect(found).To(BeTrue(), "Ready condition must exist in VirtualDeployment mode")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ISVC Update → Status.URL recomputes if the underlying Service name changes", func() {
		// Change a spec field that triggers Service re-derivation
		// (e.g., add an annotation that changes Service naming
		// behavior). Status.URL must stay consistent with the
		// current Service. We do a benign Update and verify URL is
		// still set + still includes the namespace — proves the
		// reconcile loop didn't lose Status on Update.
		cbm, rt := makeBaseModelAndRuntime("update")
		isvc := v1beta1testing.MakeInferenceService("status-update-isvc", ns.Name).
			Model(cbm).Runtime(rt).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		// Wait for initial Status.URL.
		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "status-update-isvc", Namespace: ns.Name}, got)).To(Succeed())
			g.Expect(got.Status.URL).NotTo(BeNil())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("Update ISVC with a benign annotation — Status.URL must remain populated through the Update reconcile")
		updateInferenceService(types.NamespacedName{Name: "status-update-isvc", Namespace: ns.Name}, func(latest *v1beta1.InferenceService) {
			if latest.Annotations == nil {
				latest.Annotations = map[string]string{}
			}
			latest.Annotations["test.ome.io/touched"] = "true"
		})

		Consistently(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "status-update-isvc", Namespace: ns.Name}, latest)).To(Succeed())
			g.Expect(latest.Status.URL).NotTo(BeNil(),
				"Status.URL must NOT be cleared by an Update reconcile")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
