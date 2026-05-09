package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// RawDeployment-mode child resources beyond the Deployment itself:
// the controller produces a Service for traffic routing and (when
// MinReplicas != MaxReplicas) an HPA for autoscaling. PDB is gated
// on a separate config we'll cover in a follow-up.
//
// Service is unconditional. HPA only fires when the autoscaler
// classification resolves to HPA AND the spec asks for variable
// replicas.
var _ = ginkgo.Describe("InferenceService RawDeployment children", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-children-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("RawDeployment ISVC produces a Service alongside the Deployment", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("svc-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("svc-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  "ome-container",
				Image: "ghcr.io/test/runtime:test",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080, Name: "http1"}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("svc-isvc", ns.Name).
			Model("svc-llama").
			Runtime("svc-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting at least one Service in the ISVC namespace pointing at the engine")
		Eventually(func(g Gomega) {
			services := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, services, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(services.Items).NotTo(BeEmpty(), "RawDeployment ISVC must produce a Service")

			// Find one whose selector matches our ISVC label.
			labelKey := "ome.io/inferenceservice"
			var matched *corev1.Service
			for i := range services.Items {
				if services.Items[i].Spec.Selector[labelKey] == "svc-isvc" {
					matched = &services.Items[i]
					break
				}
			}
			g.Expect(matched).NotTo(BeNil(),
				"at least one Service must select pods labeled %s=svc-isvc", labelKey)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("RawDeployment ISVC with MinReplicas<MaxReplicas → HPA created (default autoscaler class is HPA)", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("hpa-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("hpa-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with Engine.MinReplicas=2 and MaxReplicas=5 (variable → HPA needed)")
		minR, maxR := 2, 5
		isvc := v1beta1testing.MakeInferenceService("hpa-isvc", ns.Name).
			Model("hpa-llama").
			Runtime("hpa-runtime").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR,
					MaxReplicas: maxR,
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting an HPA with the right min/max in the ISVC namespace")
		Eventually(func(g Gomega) {
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			g.Expect(k8sClient.List(suiteCtx, hpas, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(hpas.Items).NotTo(BeEmpty(), "variable-replica ISVC must produce an HPA under the default autoscaler class")
			h := hpas.Items[0]
			g.Expect(h.Spec.MinReplicas).NotTo(BeNil())
			g.Expect(*h.Spec.MinReplicas).To(BeNumerically("==", int32(minR)))
			g.Expect(h.Spec.MaxReplicas).To(BeNumerically("==", int32(maxR)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
