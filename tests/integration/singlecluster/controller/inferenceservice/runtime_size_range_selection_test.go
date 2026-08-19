package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// ModelSizeRange contracts of auto-selection: an out-of-range runtime is
// excluded outright (priority cannot rescue it), the tighter range wins a
// score tie, and the gate only applies when the model actually declares a
// parameter size.
var _ = ginkgo.Describe("Runtime auto-selection model-size gating", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "model-ru-size-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	expectEngineImage := func(namespace, wantImage, because string) {
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(namespace))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			containers := deps.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Image).To(Equal(wantImage), because)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	}

	ginkgo.It("out-of-range runtime is excluded even at higher priority; in-range runtime wins", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-size-gate-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-size-gate-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		cbm.Spec.ModelParameterSize = ptr.To("7B")
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a HIGH-priority runtime whose size range excludes the 7B model")
		oversize := v1beta1testing.MakeClusterServingRuntime("model-ru-size-gate-oversize").
			SupportsModelFormat("model-ru-size-gate-fmt", true, 100).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:oversize"}).Obj()
		oversize.Spec.ModelSizeRange = &v1beta1.ModelSizeRangeSpec{Min: ptr.To("100B"), Max: ptr.To("200B")}
		Expect(k8sClient.Create(suiteCtx, oversize)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, oversize) })

		ginkgo.By("creating a LOW-priority runtime whose size range contains the model")
		fit := v1beta1testing.MakeClusterServingRuntime("model-ru-size-gate-fit").
			SupportsModelFormat("model-ru-size-gate-fmt", true, 1).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:fit"}).Obj()
		fit.Spec.ModelSizeRange = &v1beta1.ModelSizeRangeSpec{Min: ptr.To("1B"), Max: ptr.To("13B")}
		Expect(k8sClient.Create(suiteCtx, fit)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fit) })

		isvc := v1beta1testing.MakeInferenceService("model-ru-size-gate-isvc", ns.Name).
			Model("model-ru-size-gate-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImage(ns.Name, "ghcr.io/test/runtime:fit",
			"size-range exclusion is a hard gate; priority must not override it")
	})

	ginkgo.It("on a score tie, the runtime with the tighter size range wins over name order", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-size-tie-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-size-tie-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		cbm.Spec.ModelParameterSize = ptr.To("7B")
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// The wide-range runtime sorts alphabetically FIRST, so a win by
		// the tight-range runtime proves size distance (not name order)
		// broke the tie.
		wide := v1beta1testing.MakeClusterServingRuntime("model-ru-size-tie-a-wide").
			SupportsModelFormat("model-ru-size-tie-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:wide"}).Obj()
		wide.Spec.ModelSizeRange = &v1beta1.ModelSizeRangeSpec{Min: ptr.To("1B"), Max: ptr.To("70B")}
		Expect(k8sClient.Create(suiteCtx, wide)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, wide) })

		tight := v1beta1testing.MakeClusterServingRuntime("model-ru-size-tie-b-tight").
			SupportsModelFormat("model-ru-size-tie-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:tight"}).Obj()
		tight.Spec.ModelSizeRange = &v1beta1.ModelSizeRangeSpec{Min: ptr.To("5B"), Max: ptr.To("10B")}
		Expect(k8sClient.Create(suiteCtx, tight)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, tight) })

		isvc := v1beta1testing.MakeInferenceService("model-ru-size-tie-isvc", ns.Name).
			Model("model-ru-size-tie-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImage(ns.Name, "ghcr.io/test/runtime:tight",
			"equal scores must prefer the runtime whose size range fits the model most tightly")
	})

	ginkgo.It("runtime with a size range still matches a model that declares no parameter size", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-size-nosize-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-size-nosize-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		// No ModelParameterSize on purpose: the size gate must not fire.
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("model-ru-size-nosize-csr").
			SupportsModelFormat("model-ru-size-nosize-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:nosize"}).Obj()
		csr.Spec.ModelSizeRange = &v1beta1.ModelSizeRangeSpec{Min: ptr.To("1B"), Max: ptr.To("13B")}
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("model-ru-size-nosize-isvc", ns.Name).
			Model("model-ru-size-nosize-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImage(ns.Name, "ghcr.io/test/runtime:nosize",
			"a size range constrains only models that declare a size; sizeless models pass the gate")
	})
})
