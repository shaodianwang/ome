package inferenceservice

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// RawDeployment-mode child resources beyond the Deployment itself:
// the controller produces a Service for traffic routing and (when
// MinReplicas != MaxReplicas) an HPA for autoscaling.
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

	ginkgo.It("RawDeployment ISVC with Engine.Autoscaler Class=KEDA → ScaledObject created (KEDA CRD present)", func() {
		// Positive counterpart to the inferenceservice_no_keda suite: with the
		// KEDA CRD present, a Class=KEDA autoscaler must produce a ScaledObject.
		cbm := v1beta1testing.MakeClusterBaseModel("keda-llama").
			StorageURI("oci://n/test/b/models/o/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("keda-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating ISVC with Engine.Autoscaler Class=KEDA + a prometheus trigger")
		minR, maxR := 2, 5
		isvc := v1beta1testing.MakeInferenceService("keda-isvc", ns.Name).
			Model("keda-llama").
			Runtime("keda-runtime").
			DeploymentMode(constants.RawDeployment).
			// RawDeployment resolves the autoscaler class from the
			// ome.io/autoscalerClass annotation (getAutoscalerClass), not the
			// Engine.Autoscaler.Class field — the field-based DispatchAutoscaler
			// is not wired into the raw reconciler. Without this annotation the
			// class defaults to HPA and no ScaledObject is created.
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minR,
					MaxReplicas: maxR,
					Autoscaler: &v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerKEDA,
						Keda: &v1beta1.KedaAutoscaler{
							Triggers: []kedav1.ScaleTriggers{{
								Type: "prometheus",
								Metadata: map[string]string{
									"serverAddress": "http://prometheus.monitoring.svc:9090",
									"query":         "vllm:num_requests_running",
									"threshold":     "10",
								},
							}},
						},
					},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting a ScaledObject with the right min/max + trigger in the ISVC namespace")
		Eventually(func(g Gomega) {
			sos := &kedav1.ScaledObjectList{}
			g.Expect(k8sClient.List(suiteCtx, sos, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(sos.Items).NotTo(BeEmpty(), "Class=KEDA ISVC must produce a ScaledObject when the KEDA CRD is present")
			so := sos.Items[0]
			g.Expect(so.Spec.MinReplicaCount).NotTo(BeNil())
			g.Expect(*so.Spec.MinReplicaCount).To(BeNumerically("==", int32(minR)))
			g.Expect(so.Spec.MaxReplicaCount).NotTo(BeNil())
			g.Expect(*so.Spec.MaxReplicaCount).To(BeNumerically("==", int32(maxR)))
			g.Expect(so.Spec.Triggers).To(HaveLen(1))
			g.Expect(so.Spec.Triggers[0].Type).To(Equal("prometheus"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
