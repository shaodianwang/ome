package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Controller-side counterpart to the webhook's
// TestResolveModelAndRuntime_ExplicitRuntimeCompatIsAdvisory: when a
// runtime is named explicitly, a pure compatibility mismatch (the runtime does
// not declare the model's architecture/format/framework) must be downgraded to
// an advisory event and the Deployment built anyway — the operator's deliberate
// choice wins over the runtime's declared supportedModelFormats. The webhook
// already admits such an ISVC; without the same downgrade in the controller the
// reconcile would hard-fail and the ISVC would never get pods.
var _ = ginkgo.Describe("Explicit-runtime compatibility is advisory (controller)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-advisory-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("explicit runtime that does not declare the model's architecture still produces a Deployment", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("advisory-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("a runtime that declares a DIFFERENT architecture — ValidateRuntime returns a compatibility error")
		csr := v1beta1testing.MakeClusterServingRuntime("advisory-runtime").
			SupportsModelFormat("safetensors", false /* autoSelect */, 10).
			WithLastFormatArchitecture("MistralForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:advisory"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("naming the runtime explicitly on the ISVC")
		isvc := v1beta1testing.MakeInferenceService("advisory-isvc", ns.Name).
			Model("advisory-llama").
			Runtime("advisory-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting the Deployment to be built from the named runtime despite the declared-format mismatch")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1),
				"explicit runtime must be honored (advisory) instead of hard-failing the reconcile")
			containers := deps.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Image).To(Equal("ghcr.io/test/runtime:advisory"),
				"the explicitly-named runtime's image must be used")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
