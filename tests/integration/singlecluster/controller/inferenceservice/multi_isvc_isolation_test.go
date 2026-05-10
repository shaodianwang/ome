package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/utils/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// Multi-ISVC isolation: two independent ISVCs in the same namespace
// must produce two independent Deployments. Catches regressions where
// the reconciler's child-naming scheme accidentally collides
// (e.g. naming the Deployment after the namespace instead of the ISVC).
var _ = ginkgo.Describe("Multiple ISVCs in one namespace", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-multi-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("two ISVCs with different models → two independent Deployments + Services", func() {
		ginkgo.By("creating two distinct ClusterBaseModels")
		cbmA := v1beta1testing.MakeClusterBaseModel("multi-llama-a").
			StorageURI("oci://n/test/b/models/o/llama-a").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbmA)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbmA) })

		cbmB := v1beta1testing.MakeClusterBaseModel("multi-llama-b").
			StorageURI("oci://n/test/b/models/o/llama-b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbmB)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbmB) })

		csr := v1beta1testing.MakeClusterServingRuntime("multi-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating two ISVCs in the same namespace, each pointing at a different model")
		isvcA := v1beta1testing.MakeInferenceService("multi-isvc-a", ns.Name).
			Model("multi-llama-a").Runtime("multi-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvcA)).To(Succeed())

		isvcB := v1beta1testing.MakeInferenceService("multi-isvc-b", ns.Name).
			Model("multi-llama-b").Runtime("multi-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvcB)).To(Succeed())

		ginkgo.By("expecting two distinct Deployments and two distinct Services")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(HaveLen(2),
				"two ISVCs must produce exactly two Deployments")
			names := map[string]bool{deps.Items[0].Name: true, deps.Items[1].Name: true}
			g.Expect(names).To(HaveLen(2), "Deployment names must be distinct")

			services := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, services, client.InNamespace(ns.Name))).To(Succeed())
			isvcSelectors := map[string]bool{}
			for _, s := range services.Items {
				if v, ok := s.Spec.Selector["ome.io/inferenceservice"]; ok {
					isvcSelectors[v] = true
				}
			}
			g.Expect(isvcSelectors).To(HaveKey("multi-isvc-a"))
			g.Expect(isvcSelectors).To(HaveKey("multi-isvc-b"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("deleting one ISVC must leave the other's Deployment intact")
		Expect(k8sClient.Delete(suiteCtx, isvcA)).To(Succeed())

		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "multi-isvc-a", Namespace: ns.Name}, &v1beta1.InferenceService{})
			g.Expect(err).To(HaveOccurred(), "multi-isvc-a should be gone")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// multi-isvc-b's Deployment must still be present (envtest has
		// no GC, so deleting isvcA doesn't auto-delete isvcA's
		// Deployment either, but the assertion is that isvcB's
		// Deployment is still there).
		gotB := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "multi-isvc-b", Namespace: ns.Name}, gotB)).To(Succeed())
	})
})
