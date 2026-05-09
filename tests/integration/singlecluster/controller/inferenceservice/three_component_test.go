package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// ISVC with all three components: engine + decoder + router. The
// controller creates one reconciler per non-nil merged component
// (controller.go:284-356), each producing its own children. In the
// simplest case (all 3 in RawDeployment) we expect three distinct
// Deployments — one per component — each running the matching runner
// image declared on the runtime.
var _ = ginkgo.Describe("InferenceService 3-component (engine + decoder + router)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-comp3-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("Engine + Decoder + Router → 3 distinct Deployments with the right images each", func() {
		ginkgo.By("creating BaseModel + Runtime baseline (engine + decoder + router runners)")
		cbm := v1beta1testing.MakeClusterBaseModel("comp3-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("comp3-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:engine"}).
			DecoderRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:decoder"}).
			RouterRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:router"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with empty Engine + Decoder + Router stubs (containers come from the runtime)")
		isvc := v1beta1testing.MakeInferenceService("comp3-isvc", ns.Name).
			Model("comp3-llama").
			Runtime("comp3-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).
			Decoder(&v1beta1.DecoderSpec{}).
			Router(&v1beta1.RouterSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting EXACTLY 3 Deployments — one per component — with the right images")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(3),
				"3-component ISVC must produce exactly 3 Deployments (engine + decoder + router)")

			imgSet := map[string]bool{}
			for _, dep := range deps.Items {
				containers := dep.Spec.Template.Spec.Containers
				g.Expect(containers).NotTo(BeEmpty(),
					"every Deployment must have at least one container")
				imgSet[containers[0].Image] = true
			}
			g.Expect(imgSet).To(HaveKey("ghcr.io/test/runtime:engine"))
			g.Expect(imgSet).To(HaveKey("ghcr.io/test/runtime:decoder"))
			g.Expect(imgSet).To(HaveKey("ghcr.io/test/runtime:router"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
