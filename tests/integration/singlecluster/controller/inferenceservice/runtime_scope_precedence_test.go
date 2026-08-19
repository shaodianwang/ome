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

// Scope-precedence contracts of runtime auto-selection: namespace-scoped
// ServingRuntimes are tenant overrides and always outrank cluster-scoped
// ones regardless of score, and equally-scored candidates resolve
// deterministically by name.
var _ = ginkgo.Describe("Runtime auto-selection scope precedence", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "model-ru-prec-")
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

	ginkgo.It("namespaced ServingRuntime outranks a higher-priority ClusterServingRuntime", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-prec-scope-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-prec-scope-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a HIGH-priority cluster runtime (would win on score alone)")
		csr := v1beta1testing.MakeClusterServingRuntime("model-ru-prec-scope-csr").
			SupportsModelFormat("model-ru-prec-scope-fmt", true, 100).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:cluster-scoped"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating a LOW-priority namespaced runtime in the ISVC namespace")
		sr := v1beta1testing.MakeServingRuntime("model-ru-prec-scope-sr", ns.Name).
			SupportsModelFormat("model-ru-prec-scope-fmt", true, 1).Obj()
		sr.Spec.SupportedModelFormats[0].ModelArchitecture = ptr.To("LlamaForCausalLM")
		sr.Spec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name: "ome-container", Image: "ghcr.io/test/runtime:namespace-scoped",
			}},
		}
		Expect(k8sClient.Create(suiteCtx, sr)).To(Succeed())

		isvc := v1beta1testing.MakeInferenceService("model-ru-prec-scope-isvc", ns.Name).
			Model("model-ru-prec-scope-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImage(ns.Name, "ghcr.io/test/runtime:namespace-scoped",
			"namespace-scoped runtime must win auto-selection even against a higher-priority cluster runtime")
	})

	ginkgo.It("namespaced ServingRuntime in ANOTHER namespace is invisible; cluster runtime wins", func() {
		otherNS, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "model-ru-prec-other-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = util.DeleteNamespace(suiteCtx, k8sClient, otherNS.Name) })

		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-prec-cross-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-prec-cross-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a matching namespaced runtime in a DIFFERENT namespace")
		sr := v1beta1testing.MakeServingRuntime("model-ru-prec-cross-sr", otherNS.Name).
			SupportsModelFormat("model-ru-prec-cross-fmt", true, 100).Obj()
		sr.Spec.SupportedModelFormats[0].ModelArchitecture = ptr.To("LlamaForCausalLM")
		sr.Spec.EngineConfig = &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name: "ome-container", Image: "ghcr.io/test/runtime:wrong-namespace",
			}},
		}
		Expect(k8sClient.Create(suiteCtx, sr)).To(Succeed())

		csr := v1beta1testing.MakeClusterServingRuntime("model-ru-prec-cross-csr").
			SupportsModelFormat("model-ru-prec-cross-fmt", true, 1).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:cluster-visible"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("model-ru-prec-cross-isvc", ns.Name).
			Model("model-ru-prec-cross-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImage(ns.Name, "ghcr.io/test/runtime:cluster-visible",
			"a namespaced runtime outside the ISVC namespace must not participate in selection")
	})

	ginkgo.It("equal-score cluster runtimes resolve deterministically to the alphabetically-first name", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("model-ru-prec-tie-model").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("model-ru-prec-tie-fmt").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating the alphabetically-LATER runtime first so creation order can't masquerade as the tiebreaker")
		later := v1beta1testing.MakeClusterServingRuntime("model-ru-prec-tie-zeta").
			SupportsModelFormat("model-ru-prec-tie-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:zeta"}).Obj()
		Expect(k8sClient.Create(suiteCtx, later)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, later) })

		earlier := v1beta1testing.MakeClusterServingRuntime("model-ru-prec-tie-alpha").
			SupportsModelFormat("model-ru-prec-tie-fmt", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:alpha"}).Obj()
		Expect(k8sClient.Create(suiteCtx, earlier)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, earlier) })

		isvc := v1beta1testing.MakeInferenceService("model-ru-prec-tie-isvc", ns.Name).
			Model("model-ru-prec-tie-model").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectEngineImage(ns.Name, "ghcr.io/test/runtime:alpha",
			"score ties must break alphabetically by runtime name, independent of creation order")
	})
})
