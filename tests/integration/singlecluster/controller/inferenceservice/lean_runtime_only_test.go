package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Lean path: spec.model omitted, spec.runtime named directly. The
// controller skips model fetch + runtime auto-select + runtime-vs-model
// validation, and reconciles using the runtime spec as-is.
var _ = ginkgo.Describe("InferenceService lean path (runtime only, no model)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-lean-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("ISVC without spec.model reconciles to a Deployment using the named runtime's image", func() {
		csr := v1beta1testing.MakeClusterServingRuntime("lean-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:lean",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080, Name: "http1"}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("lean-isvc", ns.Name).
			Runtime("lean-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		// No .Model(...) call — spec.model stays nil.
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting a Service whose selector matches the lean ISVC")
		Eventually(func(g Gomega) {
			services := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, services, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, s := range services.Items {
				if s.Spec.Selector["ome.io/inferenceservice"] == "lean-isvc" {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue(), "lean ISVC must produce a Service")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

})
