package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Runtime-only ServingRuntime behavior", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "runtime-only-behavior-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("preserves children while disabled and resumes runtime updates after re-enable", func() {
		const (
			runtimeName = "runtime-only-behavior"
			isvcName    = "runtime-only-behavior"
			imageV1     = "ghcr.io/test/runtime-only:v1"
			imageV2     = "ghcr.io/test/runtime-only:v2"
		)

		csr := v1beta1testing.MakeClusterServingRuntime(runtimeName).
			EngineRunner(corev1.Container{Name: "ome-container", Image: imageV1}).Obj()
		csr.Spec.NodeSelector = map[string]string{
			"accelerator": "gpu",
			"zone":        "runtime-zone",
		}
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService(isvcName, ns.Name).
			Runtime(runtimeName).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{PodSpec: v1beta1.PodSpec{
				NodeSelector: map[string]string{"zone": "component-zone"},
			}}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		var deploymentKey types.NamespacedName
		Eventually(func(g Gomega) {
			deployments := &appsv1.DeploymentList{}
			g.Expect(k8sClient.List(
				suiteCtx,
				deployments,
				client.InNamespace(ns.Name),
				client.MatchingLabels{constants.InferenceServiceLabel: isvcName},
			)).To(Succeed())
			g.Expect(deployments.Items).To(HaveLen(1))
			deployment := deployments.Items[0]
			g.Expect(deployment.Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(Equal(imageV1))
			g.Expect(deployment.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{
				"accelerator": "gpu",
				"zone":        "component-zone",
			}))
			deploymentKey = types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		updatedRuntime := &v1beta1.ClusterServingRuntime{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: runtimeName}, updatedRuntime)).To(Succeed())
		disabled := true
		updatedRuntime.Spec.Disabled = &disabled
		updatedRuntime.Spec.EngineConfig.Runner.Container.Image = imageV2
		Expect(k8sClient.Update(suiteCtx, updatedRuntime)).To(Succeed())

		liveISVC := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: isvcName, Namespace: ns.Name}, liveISVC)).To(Succeed())
		Eventually(func(g Gomega) {
			events := &corev1.EventList{}
			g.Expect(k8sClient.List(suiteCtx, events, client.InNamespace(ns.Name))).To(Succeed())
			matched := false
			for _, event := range events.Items {
				if event.Reason == "RuntimeValidationError" && event.InvolvedObject.UID == liveISVC.UID {
					matched = true
					break
				}
			}
			g.Expect(matched).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Consistently(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, deploymentKey, deployment)).To(Succeed())
			g.Expect(deployment.Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(Equal(imageV1))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		reenabledRuntime := &v1beta1.ClusterServingRuntime{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: runtimeName}, reenabledRuntime)).To(Succeed())
		disabled = false
		reenabledRuntime.Spec.Disabled = &disabled
		Expect(k8sClient.Update(suiteCtx, reenabledRuntime)).To(Succeed())

		Eventually(func(g Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(k8sClient.Get(suiteCtx, deploymentKey, deployment)).To(Succeed())
			g.Expect(deployment.Spec.Template.Spec.Containers).NotTo(BeEmpty())
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(Equal(imageV2))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
