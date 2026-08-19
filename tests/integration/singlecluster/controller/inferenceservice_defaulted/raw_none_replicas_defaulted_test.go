package inferenceservice_defaulted

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// RawDeployment replica ownership through the admission chain. An
// admitted InferenceService never reaches the reconciler with
// minReplicas unset: the defaulter stamps 1. For autoscaler class None
// that stamped floor is controller-owned exactly like an explicit one —
// the Deployment is created with spec.replicas=1 and manual drift
// converges back to 1, even though the user never declared a count.
var _ = ginkgo.Describe("InferenceService RawDeployment defaulted replica ownership", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "raw-none-defaulted-")
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
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
	}

	expectReplicas := func(namespace, name string, replicas int32) {
		ginkgo.GinkgoHelper()
		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: namespace, Name: name}, deployment)).To(Succeed())
			g.Expect(deployment.Spec.Replicas).NotTo(BeNil())
			g.Expect(*deployment.Spec.Replicas).To(BeNumerically("==", replicas))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	}

	ginkgo.It("None-class engine with omitted minReplicas gets 1 from the defaulter and the controller owns it", func() {
		createModelAndRuntime("raw-none-defaulted")

		isvc := v1beta1testing.MakeInferenceService("raw-none-defaulted-isvc", ns.Name).
			Model("raw-none-defaulted-cbm").
			Runtime("raw-none-defaulted-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					// MinReplicas deliberately omitted.
					Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("the defaulter stamps minReplicas=1 on the admitted spec")
		admitted := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: isvc.Name}, admitted)).To(Succeed())
		Expect(admitted.Spec.Engine).NotTo(BeNil())
		Expect(admitted.Spec.Engine.MinReplicas).To(HaveValue(Equal(1)))

		engineName := constants.EngineServiceName(isvc.Name)

		ginkgo.By("the engine Deployment is created with spec.replicas=1")
		expectReplicas(ns.Name, engineName, 1)

		ginkgo.By("a manual replica edit converges back to the defaulted floor, proving controller ownership")
		Eventually(func() error {
			deployment := &appsv1.Deployment{}
			if err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: engineName}, deployment); err != nil {
				return err
			}
			edited := int32(3)
			deployment.Spec.Replicas = &edited
			return k8sClient.Update(suiteCtx, deployment)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		expectReplicas(ns.Name, engineName, 1)
	})
})
