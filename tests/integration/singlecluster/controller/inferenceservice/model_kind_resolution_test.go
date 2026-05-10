package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// BaseModel kind precedence and resolution. utils/reconciliation.go's
// GetBaseModel resolves a model name by:
//  1. Try BaseModel in the ISVC's namespace
//  2. Fall back to ClusterBaseModel cluster-wide
//
// These specs pin both halves of that contract.
var _ = ginkgo.Describe("ISVC + BaseModel kind resolution", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-mkind-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("namespaced BaseModel wins over same-name ClusterBaseModel during resolution", func() {
		// Both a CBM and a namespaced BM exist with the same name.
		// CBM uses one image, namespaced BM uses metadata that
		// matches a runtime image. We then assert the resulting
		// Deployment used the namespaced BM's metadata path (proven
		// by the ISVC reaching Deployment-creation given ONLY a
		// runtime that supports the namespaced-BM's format).
		ginkgo.By("creating a ClusterBaseModel with a format NO runtime supports — would fail if picked")
		cbm := v1beta1testing.MakeClusterBaseModel("dual-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("orphan-format-only-on-cbm").
			ModelArchitecture("OrphanArch").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a namespaced BaseModel with a matching format")
		bm := v1beta1testing.MakeBaseModel("dual-llama", ns.Name).
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		csr := v1beta1testing.MakeClusterServingRuntime("dual-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("ISVC.Spec.Model with default Kind=ClusterBaseModel still resolves the namespaced one (GetBaseModel checks namespaced first)")
		isvc := v1beta1testing.MakeInferenceService("dual-isvc", ns.Name).
			Model("dual-llama").
			Runtime("dual-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("Deployment must materialize — proves resolution found the namespaced BM (whose format matches the runtime)")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).NotTo(BeEmpty(),
				"Deployment must be created; if the ClusterBaseModel had been picked instead, runtime selection would fail")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		_ = k8sClient.Delete(suiteCtx, bm)
	})

	ginkgo.It("ISVC referencing a name with NO namespaced BM falls through to ClusterBaseModel", func() {
		// Only a CBM exists; no namespaced BM. ISVC.Model.Name resolves
		// to the cluster-scoped one. Per-node cluster-scoped label is
		// what shows up in the NodeSelector.
		ginkgo.By("creating a ClusterBaseModel only (no namespaced BM with the same name)")
		cbm := v1beta1testing.MakeClusterBaseModel("only-cbm-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("only-cbm-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("only-cbm-isvc", ns.Name).
			Model("only-cbm-llama").
			Runtime("only-cbm-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting NodeSelector to carry the CLUSTER-scoped label (not the namespaced one)")
		expectedClusterLabel := constants.GetClusterBaseModelLabel("only-cbm-llama")
		expectedNamespacedLabel := constants.GetBaseModelLabel(ns.Name, "only-cbm-llama")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).NotTo(BeEmpty())
			pod := deps.Items[0].Spec.Template.Spec
			g.Expect(pod.NodeSelector).To(HaveKeyWithValue(expectedClusterLabel, "Ready"),
				"cluster-scoped label %q must be the model-ready selector", expectedClusterLabel)
			g.Expect(pod.NodeSelector).NotTo(HaveKey(expectedNamespacedLabel),
				"the namespaced label must NOT be set when only the CBM exists")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
