package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// RawDeployment-mode InferenceService specs.
//
// Minimum-viable ISVC reconcile chain in this mode:
//
//	ISVC → ClusterBaseModel resolved → ClusterServingRuntime resolved
//	     → MergeRuntimeSpecs → DetermineDeploymentModes (Raw)
//	     → Engine/Decoder/Router component reconcile
//	     → Deployment + Service produced (HPA + PDB depend on extra
//	       spec; we don't assert on them in the minimum-viable spec)
//
// Each spec creates its objects in a per-test namespace and AfterEach
// drops the namespace, which the cleanup registry's hooks unblock by
// stripping ISVC + BaseModel finalizers.
var _ = ginkgo.Describe("InferenceService RawDeployment", ginkgo.Ordered, func() {
	var (
		ns *corev1.Namespace
	)

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-raw-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("ISVC with explicit Runtime → Deployment is created", func() {
		ginkgo.By("creating a ClusterBaseModel referenced by the ISVC")
		cbm := v1beta1testing.MakeClusterBaseModel("raw-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("creating a ClusterServingRuntime whose SupportedModelFormat matches the BaseModel's format AND architecture (selector is strict)")
		csr := v1beta1testing.MakeClusterServingRuntime("raw-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the InferenceService with Engine spec (MergeEngineSpec returns nil if isvc.Spec.Engine is nil → DetermineDeploymentModes errors)")
		isvc := v1beta1testing.MakeInferenceService("raw-isvc", ns.Name).
			Model("raw-llama-7b").
			Runtime("raw-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting a Deployment to appear in the ISVC namespace within MediumTimeout")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).NotTo(BeEmpty(), "expected at least one Deployment to be created for the ISVC")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ISVC Engine container image change propagates to the Deployment template", func() {
		ginkgo.By("creating BaseModel + Runtime baseline")
		cbm := v1beta1testing.MakeClusterBaseModel("update-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("update-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the ISVC with image v1")
		isvc := v1beta1testing.MakeInferenceService("update-isvc", ns.Name).
			Model("update-llama-7b").
			Runtime("update-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name: "ome-container", Image: "ghcr.io/test/runtime:v1",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("waiting for the v1 Deployment to materialize")
		var depName string
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).NotTo(BeEmpty())
			depName = deps.Items[0].Name
			g.Expect(deps.Items[0].Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(deps.Items[0].Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:v1"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("updating the ISVC Engine image to v2")
		latest := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "update-isvc", Namespace: ns.Name}, latest)).To(Succeed())
		latest.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())

		ginkgo.By("expecting the Deployment template to update to v2")
		Eventually(func(g Gomega) {
			dep := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: depName, Namespace: ns.Name}, dep)).To(Succeed())
			g.Expect(dep.Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(dep.Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/test/runtime:v2"),
				"ISVC spec change must propagate to the owned Deployment template")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ISVC deletion clears the finalizer (envtest has no GC; child cleanup is Tier-3)", func() {
		ginkgo.By("creating BaseModel + Runtime + ISVC and waiting for the finalizer to land")
		cbm := v1beta1testing.MakeClusterBaseModel("del-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("del-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("del-isvc", ns.Name).
			Model("del-llama-7b").
			Runtime("del-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name: "ome-container", Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "del-isvc", Namespace: ns.Name}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement("inferenceservice.finalizers"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("deleting the ISVC and waiting for it to fully disappear (finalizer cleared)")
		Expect(k8sClient.Delete(suiteCtx, isvc)).To(Succeed())
		Eventually(func() error {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "del-isvc", Namespace: ns.Name}, &v1beta1.InferenceService{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed(),
			"ISVC must be fully gone after the controller clears its finalizer")
	})

	ginkgo.It("ISVC without explicit Runtime → auto-selected from SupportedModelFormats", func() {
		ginkgo.By("creating a BaseModel + a single autoSelect=true Runtime that matches its format/architecture")
		cbm := v1beta1testing.MakeClusterBaseModel("auto-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("auto-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating the ISVC WITHOUT a Runtime ref — selector should pick auto-runtime by autoSelect=true")
		isvc := v1beta1testing.MakeInferenceService("auto-isvc", ns.Name).
			Model("auto-llama-7b").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name: "ome-container", Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).NotTo(BeEmpty(), "auto-selection failed to produce a Deployment")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// _ keeps apierrors imported; reserved for negative-path specs that
// will land alongside this happy-path test.
var _ = apierrors.IsNotFound

// _ keeps metav1 imported; reserved for selectors / status assertions
// in subsequent specs.
var _ = metav1.GetOptions{}
