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

// Negative-path InferenceService specs. Each asserts that an
// invalid/incomplete configuration:
//   - Surfaces an error event (covered indirectly via no Deployment),
//     OR
//   - Leaves the ISVC's child workloads absent
//
// We intentionally don't poll for specific ISVC.Status condition
// reasons in most of these because the controller's status-update
// path is conservative on error (it logs + emits events, doesn't
// always rewrite conditions). Asserting "no Deployment after a
// reasonable wait" is the more reliable contract.
var _ = ginkgo.Describe("InferenceService negative paths", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-neg-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("ISVC referencing a missing BaseModel → no Deployment created", func() {
		ginkgo.By("creating an ISVC pointing at a Model that doesn't exist")
		isvc := v1beta1testing.MakeInferenceService("missing-bm-isvc", ns.Name).
			Model("does-not-exist").
			Runtime("also-does-not-exist").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name: "ome-container", Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting NO Deployment to be created for the lifetime of the test")
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(), "missing BaseModel must not produce a Deployment")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ISVC referencing a Disabled BaseModel → no Deployment created", func() {
		ginkgo.By("creating a Disabled ClusterBaseModel")
		cbm := v1beta1testing.MakeClusterBaseModel("disabled-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Disabled(true).Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("disabled-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the ISVC referencing the Disabled BM")
		isvc := v1beta1testing.MakeInferenceService("disabled-isvc", ns.Name).
			Model("disabled-llama").
			Runtime("disabled-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name: "ome-container", Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting NO Deployment (ReconcileBaseModel rejects disabled models)")
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(), "Disabled BaseModel must not produce a Deployment")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ISVC with no auto-selectable runtime → no Deployment created", func() {
		ginkgo.By("creating a BaseModel with a unique format that no runtime supports")
		cbm := v1beta1testing.MakeClusterBaseModel("orphan-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("nonexistent-format-orphan").
			ModelArchitecture("OrphanArchitecture").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating an ISVC with NO explicit Runtime (forces auto-selection)")
		isvc := v1beta1testing.MakeInferenceService("orphan-isvc", ns.Name).
			Model("orphan-llama").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name: "ome-container", Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting NO Deployment (no matching SupportedModelFormat across all CSRs)")
		Consistently(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).To(BeEmpty(), "no matching runtime must not produce a Deployment")
		}, util.ShortTimeout, util.PollInterval).Should(Succeed())
	})

})
