package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// RawDeployment replica ownership. With autoscaler class None no scaler
// writes Deployment.spec.replicas, so the ISVC controller owns the count:
// it stamps minReplicas on create and converges manual drift back to the
// declared floor. With a scaler class (HPA here) the scaler owns the count
// and the controller must preserve the live value across its own updates.
var _ = ginkgo.Describe("InferenceService RawDeployment replica ownership", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "raw-none-replicas-")
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

	getDeployment := func(namespace, name string) *appsv1.Deployment {
		ginkgo.GinkgoHelper()
		deployment := &appsv1.Deployment{}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: namespace, Name: name}, deployment)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		return deployment
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

	ginkgo.It("None-class engine stamps minReplicas on the Deployment and re-stamps after a manual edit", func() {
		createModelAndRuntime("raw-none-stamp")

		minReplicas := 3
		isvc := v1beta1testing.MakeInferenceService("raw-none-stamp-isvc", ns.Name).
			Model("raw-none-stamp-cbm").
			Runtime("raw-none-stamp-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minReplicas,
					Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		engineName := constants.EngineServiceName(isvc.Name)

		ginkgo.By("the engine Deployment is created with spec.replicas equal to minReplicas")
		expectReplicas(ns.Name, engineName, 3)

		ginkgo.By("a manual replica edit converges back to the declared floor")
		Eventually(func() error {
			deployment := &appsv1.Deployment{}
			if err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: engineName}, deployment); err != nil {
				return err
			}
			edited := int32(5)
			deployment.Spec.Replicas = &edited
			return k8sClient.Update(suiteCtx, deployment)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		expectReplicas(ns.Name, engineName, 3)
	})

	ginkgo.It("HPA-class engine preserves a scaler-written replica count across ISVC reconciles", func() {
		createModelAndRuntime("raw-hpa-preserve")

		minReplicas := 1
		isvc := v1beta1testing.MakeInferenceService("raw-hpa-preserve-isvc", ns.Name).
			Model("raw-hpa-preserve-cbm").
			Runtime("raw-hpa-preserve-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minReplicas,
					MaxReplicas: 5,
					Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		engineName := constants.EngineServiceName(isvc.Name)
		getDeployment(ns.Name, engineName)

		ginkgo.By("a scaler writes a replica count above the floor")
		Eventually(func() error {
			deployment := &appsv1.Deployment{}
			if err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: engineName}, deployment); err != nil {
				return err
			}
			scaled := int32(4)
			deployment.Spec.Replicas = &scaled
			return k8sClient.Update(suiteCtx, deployment)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("a runtime image change forces a Deployment update through a full ISVC reconcile")
		Eventually(func(g Gomega) {
			csr := &v1beta1.ClusterServingRuntime{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "raw-hpa-preserve-csr"}, csr)).To(Succeed())
			csr.Spec.EngineConfig.Runner.Image = "example.com/engine:v2"
			err := k8sClient.Update(suiteCtx, csr)
			g.Expect(err).To(Or(BeNil(), Satisfy(apierrors.IsConflict)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// The runtime watch covers this in a real cluster; in envtest an
		// inert annotation nudge removes the requeue lag.
		ginkgo.By("triggering an ISVC reconcile by updating an inert annotation")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: isvc.Name}, latest)).To(Succeed())
			if latest.Annotations == nil {
				latest.Annotations = map[string]string{}
			}
			latest.Annotations["ome.io/test-trigger"] = "v2"
			err := k8sClient.Update(suiteCtx, latest)
			g.Expect(err).To(Or(BeNil(), Satisfy(apierrors.IsConflict)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("the Deployment picks up the new image while keeping the scaler-written replicas")
		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns.Name, Name: engineName}, deployment)).To(Succeed())
			g.Expect(deployment.Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(Equal("example.com/engine:v2"))
			g.Expect(deployment.Spec.Replicas).NotTo(BeNil())
			g.Expect(*deployment.Spec.Replicas).To(BeNumerically("==", 4))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
