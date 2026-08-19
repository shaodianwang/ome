package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// RawDeployment replica-bound defaulting at the reconciler layer.
// Stored InferenceServices can reach reconciliation without admission
// defaults, so the Raw dispatch must tolerate omitted MinReplicas /
// MaxReplicas: an omitted min falls back to 1 and an omitted max
// collapses to min. These specs drive full reconciles and pin the
// resulting child-resource shapes, so a regression that turns omitted
// bounds back into a dispatch error fails loudly here.
var _ = ginkgo.Describe("InferenceService RawDeployment replica defaults", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "raw-depl-defaults-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	createModelAndRuntime := func(prefix string) {
		ginkgo.GinkgoHelper()
		cbm := v1beta1testing.MakeClusterBaseModel(prefix + "-cbm").
			StorageURI("oci://n/example/b/models/o/" + prefix).
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime(prefix+"-csr").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/engine:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}).
			DecoderRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/decoder:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
	}

	getHPA := func(namespace, name string) *autoscalingv2.HorizontalPodAutoscaler {
		ginkgo.GinkgoHelper()
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: namespace, Name: name}, hpa)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		return hpa
	}

	ginkgo.It("engine with fully omitted replica bounds converges to a Deployment and a 1/1 HPA", func() {
		createModelAndRuntime("raw-depl-omitted")

		isvc := v1beta1testing.MakeInferenceService("raw-depl-omitted-isvc", ns.Name).
			Model("raw-depl-omitted-cbm").
			Runtime("raw-depl-omitted-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		engineName := constants.EngineServiceName(isvc.Name)

		ginkgo.By("the engine Deployment exists")
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: engineName}, &appsv1.Deployment{})
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("the default-class HPA is created with min=1 and max=1 (omitted bounds collapse to the floor)")
		hpa := getHPA(ns.Name, engineName)
		Expect(hpa.Spec.MinReplicas).NotTo(BeNil())
		Expect(*hpa.Spec.MinReplicas).To(BeNumerically("==", 1))
		Expect(hpa.Spec.MaxReplicas).To(BeNumerically("==", 1))
		Expect(hpa.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
		Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal(engineName))

		ginkgo.By("reconciliation completed past autoscaler dispatch: the engine autoscaler status is published")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: isvc.Name}, latest)).To(Succeed())
			engineStatus, ok := latest.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			g.Expect(engineStatus.Autoscaler).NotTo(BeNil())
			g.Expect(engineStatus.Autoscaler.Class).To(Equal(v1beta1.AutoscalerHPA))
			g.Expect(engineStatus.ScaleTargetRef).NotTo(BeNil())
			g.Expect(engineStatus.ScaleTargetRef.Name).To(Equal(engineName))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("engine with only minReplicas set collapses the omitted max onto min", func() {
		createModelAndRuntime("raw-depl-minonly")

		minReplicas := 2
		isvc := v1beta1testing.MakeInferenceService("raw-depl-minonly-isvc", ns.Name).
			Model("raw-depl-minonly-cbm").
			Runtime("raw-depl-minonly-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		hpa := getHPA(ns.Name, constants.EngineServiceName(isvc.Name))
		Expect(hpa.Spec.MinReplicas).NotTo(BeNil())
		Expect(*hpa.Spec.MinReplicas).To(BeNumerically("==", 2))
		Expect(hpa.Spec.MaxReplicas).To(BeNumerically("==", 2))
	})

	ginkgo.It("engine and decoder keep their own replica bounds on their own HPAs and Services", func() {
		// The Raw path funnels every component through a synthetic
		// Engine wrapper, so cross-component bound bleed-through is a
		// plausible failure shape — pin per-component isolation.
		createModelAndRuntime("raw-depl-percomp")

		engineMin, decoderMin := 1, 2
		isvc := v1beta1testing.MakeInferenceService("raw-depl-percomp-isvc", ns.Name).
			Model("raw-depl-percomp-cbm").
			Runtime("raw-depl-percomp-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &engineMin, MaxReplicas: 3},
			}).
			Decoder(&v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &decoderMin, MaxReplicas: 5},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		engineName := constants.EngineServiceName(isvc.Name)
		decoderName := constants.DecoderServiceName(isvc.Name)

		engineHPA := getHPA(ns.Name, engineName)
		Expect(*engineHPA.Spec.MinReplicas).To(BeNumerically("==", 1))
		Expect(engineHPA.Spec.MaxReplicas).To(BeNumerically("==", 3))
		Expect(engineHPA.Spec.ScaleTargetRef.Name).To(Equal(engineName))

		decoderHPA := getHPA(ns.Name, decoderName)
		Expect(*decoderHPA.Spec.MinReplicas).To(BeNumerically("==", 2))
		Expect(decoderHPA.Spec.MaxReplicas).To(BeNumerically("==", 5))
		Expect(decoderHPA.Spec.ScaleTargetRef.Name).To(Equal(decoderName))

		for _, name := range []string{engineName, decoderName} {
			Eventually(func() error {
				return k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: name}, &corev1.Service{})
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		}
	})

	ginkgo.It("engine deploymentStrategy Recreate is honored on the generated Deployment", func() {
		createModelAndRuntime("raw-depl-strategy")

		minReplicas := 1
		isvc := v1beta1testing.MakeInferenceService("raw-depl-strategy-isvc", ns.Name).
			Model("raw-depl-strategy-cbm").
			Runtime("raw-depl-strategy-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minReplicas,
					MaxReplicas: 1,
					DeploymentStrategy: &appsv1.DeploymentStrategy{
						Type: appsv1.RecreateDeploymentStrategyType,
					},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: ns.Name,
				Name:      constants.EngineServiceName(isvc.Name),
			}, deployment)).To(Succeed())
			g.Expect(deployment.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
			g.Expect(deployment.Spec.Strategy.RollingUpdate).To(BeNil())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
