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

// Runtime-selection edge cases. These exercise the
// pkg/runtimeselector matcher's filtering logic at integration
// level: only AutoSelect=true and not-Disabled runtimes are
// candidates; among candidates, highest Priority wins.
var _ = ginkgo.Describe("Runtime selection edge cases", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-rtsel-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("AutoSelect=false runtime is skipped during auto-selection (no Deployment)", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("rtsel-as-false-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating ONE matching runtime with AutoSelect=false (must NOT be auto-picked)")
		csr := v1beta1testing.MakeClusterServingRuntime("rtsel-as-false-runtime").
			SupportsModelFormat("safetensors", false /* autoSelect */, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("rtsel-as-false-isvc", ns.Name).
			Model("rtsel-as-false-llama").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting NO Deployment — selector skips AutoSelect=false runtimes")
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(), "AutoSelect=false runtime must not be auto-picked")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Disabled runtime is skipped (even when AutoSelect=true)", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("rtsel-disabled-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("rtsel-disabled-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Disabled(true).
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("rtsel-disabled-isvc", ns.Name).
			Model("rtsel-disabled-llama").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(), "Disabled runtime must not produce a Deployment")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	ginkgo.It("among multiple matching runtimes, highest Priority wins", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("rtsel-prio-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating two matching runtimes — low priority (1) + high priority (100)")
		lowCSR := v1beta1testing.MakeClusterServingRuntime("rtsel-prio-low").
			SupportsModelFormat("safetensors", true, 1).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:low"}).Obj()
		Expect(k8sClient.Create(suiteCtx, lowCSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, lowCSR) })

		highCSR := v1beta1testing.MakeClusterServingRuntime("rtsel-prio-high").
			SupportsModelFormat("safetensors", true, 100).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:high"}).Obj()
		Expect(k8sClient.Create(suiteCtx, highCSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, highCSR) })

		isvc := v1beta1testing.MakeInferenceService("rtsel-prio-isvc", ns.Name).
			Model("rtsel-prio-llama").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting the Deployment to use the high-priority runtime's image")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			containers := deps.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Image).To(Equal("ghcr.io/test/runtime:high"),
				"highest Priority runtime must win auto-selection")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
