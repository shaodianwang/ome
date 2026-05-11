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

// Per-node BaseModel + ISVC integration. The most-common production
// pattern: BaseModel uses oci:// (or hf://) → model-agent downloads
// to selected nodes → labels them ready → ISVC reconciler injects a
// preferred node selector for that label so pods land on nodes that
// have the model bytes locally.
//
// These specs verify that flow end-to-end at the ISVC level (the
// model-agent's labeling itself is unit-tested in pkg/modelagent;
// here we confirm the SCHEDULING-side contract: the right label key
// shows up in the Deployment's nodeAffinity).
var _ = ginkgo.Describe("InferenceService + per-node BaseModel", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-pernode-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("ClusterBaseModel (cluster-scoped) → Deployment has preferred nodeAffinity for the cluster-scoped model-ready label", func() {
		ginkgo.By("creating a per-node ClusterBaseModel (oci:// URI; per-node is the default Distribution)")
		cbm := v1beta1testing.MakeClusterBaseModel("pernode-llama-cluster").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("pernode-runtime-cluster").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("pernode-isvc-cluster", ns.Name).
			Model("pernode-llama-cluster").
			Runtime("pernode-runtime-cluster").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting the Deployment's pod template to carry the cluster-scoped model-ready NodeSelector entry")
		expectedLabel := constants.GetClusterBaseModelLabel("pernode-llama-cluster")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			pod := deps.Items[0].Spec.Template.Spec
			g.Expect(pod.NodeSelector).NotTo(BeNil(),
				"per-node BaseModel must produce a NodeSelector on the pod template")
			g.Expect(pod.NodeSelector).To(HaveKeyWithValue(expectedLabel, "Ready"),
				"pod template must require label %q=%q (cluster-scoped per-node model readiness)", expectedLabel, "Ready")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ISVC referencing a namespaced BaseModel → preferred nodeAffinity for the namespaced model-ready label", func() {
		ginkgo.By("creating a namespaced BaseModel in the ISVC's namespace")
		bm := v1beta1testing.MakeBaseModel("pernode-llama-ns", ns.Name).
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		csr := v1beta1testing.MakeClusterServingRuntime("pernode-runtime-ns").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("ISVC.Spec.Model.Kind=BaseModel — selects the namespaced one over any same-name CBM")
		isvc := v1beta1testing.MakeInferenceService("pernode-isvc-ns", ns.Name).
			NamespacedModel("pernode-llama-ns").
			Runtime("pernode-runtime-ns").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		expectedLabel := constants.GetBaseModelLabel(ns.Name, "pernode-llama-ns")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(1))
			pod := deps.Items[0].Spec.Template.Spec
			g.Expect(pod.NodeSelector).To(HaveKeyWithValue(expectedLabel, "Ready"),
				"pod template must require label %q=Ready (namespaced per-node model readiness)", expectedLabel)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		_ = k8sClient.Delete(suiteCtx, bm)
	})
})
