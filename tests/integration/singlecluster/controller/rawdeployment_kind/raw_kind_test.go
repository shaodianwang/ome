package rawdeployment_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("RawDeployment", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "raw-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		_ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)
	})

	ginkgo.It("engine Deployment reaches N ready replicas and a Service is created", func() {
		const replicas = 2
		cbm := kindfix.StandardClusterBaseModel("raw-cbm", kindfix.StandardModelStorageURI)
		csr := kindfix.StandardClusterServingRuntime("raw-csr")
		kindfix.ApplyModelRuntime(suiteCtx, Default, k8sClient, cbm, csr, kindfix.ApplyModelRuntimeOpts{
			CachedReader: kindutil.Suite().Manager.GetClient(),
		})

		minReplicas := replicas
		isvc := v1beta1testing.MakeInferenceService("raw-isvc", ns.Name).
			Model("raw-cbm").
			Runtime("raw-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas},
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}}},
			}).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, cbm, csr, isvc)
		})

		ginkgo.By("the engine Deployment reports all replicas Ready (real kubelet ran the pods)")
		Eventually(func(g Gomega) {
			deps := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(deps.Items).NotTo(BeEmpty(), "expected a Deployment for the RawDeployment ISVC")
			var ready int32
			for i := range deps.Items {
				ready += deps.Items[i].Status.ReadyReplicas
			}
			g.Expect(ready).To(BeNumerically("==", replicas))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("a Service is created for the ISVC")
		Eventually(func(g Gomega) {
			svcs := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, svcs, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(svcs.Items).NotTo(BeEmpty(), "expected a Service for the RawDeployment ISVC")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})

	ginkgo.It("engine image bump rolls the Deployment to the new image and stays Ready", func() {
		const replicas = 2
		cbm := kindfix.StandardClusterBaseModel("raw-img-cbm", kindfix.StandardModelStorageURI)
		csr := kindfix.StandardClusterServingRuntime("raw-img-csr")
		kindfix.ApplyModelRuntime(suiteCtx, Default, k8sClient, cbm, csr, kindfix.ApplyModelRuntimeOpts{
			CachedReader: kindutil.Suite().Manager.GetClient(),
		})

		minReplicas := replicas
		isvc := v1beta1testing.MakeInferenceService("raw-img-isvc", ns.Name).
			Model("raw-img-cbm").
			Runtime("raw-img-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas},
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}}},
			}).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, cbm, csr, isvc)
		})

		ginkgo.By("the Deployment is Ready on v1")
		Eventually(func(g Gomega) {
			dep := engineDeployment(g, ns.Name)
			g.Expect(containerImage(dep, constants.MainContainerName)).To(Equal("fake-serving:v1"))
			g.Expect(dep.Status.ReadyReplicas).To(BeNumerically("==", replicas))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("bumping the engine image to v2")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			fresh.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the Deployment rolls fully to v2 and reports all replicas Ready")
		Eventually(func(g Gomega) {
			dep := engineDeployment(g, ns.Name)
			g.Expect(containerImage(dep, constants.MainContainerName)).To(Equal("fake-serving:v2"))
			g.Expect(dep.Status.UpdatedReplicas).To(BeNumerically("==", replicas))
			g.Expect(dep.Status.ReadyReplicas).To(BeNumerically("==", replicas))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
	ginkgo.It("scaling MinReplicas grows the Deployment and the new pods become Ready", func() {
		cbm := kindfix.StandardClusterBaseModel("raw-scale-cbm", kindfix.StandardModelStorageURI)
		csr := kindfix.StandardClusterServingRuntime("raw-scale-csr")
		kindfix.ApplyModelRuntime(suiteCtx, Default, k8sClient, cbm, csr, kindfix.ApplyModelRuntimeOpts{
			CachedReader: kindutil.Suite().Manager.GetClient(),
		})

		minReplicas := 2
		isvc := v1beta1testing.MakeInferenceService("raw-scale-isvc", ns.Name).
			Model("raw-scale-cbm").
			Runtime("raw-scale-csr").
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &minReplicas},
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}}},
			}).
			Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			kindfix.CleanupModelRuntime(suiteCtx, k8sClient, cbm, csr, isvc)
		})

		ginkgo.By("the Deployment is Ready at 2 replicas")
		Eventually(func(g Gomega) {
			g.Expect(engineDeployment(g, ns.Name).Status.ReadyReplicas).To(BeNumerically("==", 2))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("scaling MinReplicas to 3")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			three := 3
			fresh.Spec.Engine.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("the Deployment grows to 3 replicas, all Ready")
		Eventually(func(g Gomega) {
			g.Expect(engineDeployment(g, ns.Name).Status.ReadyReplicas).To(BeNumerically("==", 3))
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	})
})

// engineDeployment returns the Deployment whose pod template carries the
// ome-container, failing the assertion if none exists yet.
func engineDeployment(g Gomega, namespace string) *appsv1.Deployment {
	deps := &appsv1.DeploymentList{}
	g.Expect(k8sClient.List(suiteCtx, deps, client.InNamespace(namespace))).To(Succeed())
	for i := range deps.Items {
		if containerImage(&deps.Items[i], constants.MainContainerName) != "" {
			return &deps.Items[i]
		}
	}
	g.Expect(deps.Items).To(ContainElement(HaveField("Spec.Template.Spec.Containers",
		ContainElement(HaveField("Name", constants.MainContainerName)))), "no engine Deployment found")
	return nil
}

// containerImage returns the image of the named container in a Deployment's pod
// template, or "" if the container is absent.
func containerImage(dep *appsv1.Deployment, name string) string {
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name == name {
			return c.Image
		}
	}
	return ""
}
