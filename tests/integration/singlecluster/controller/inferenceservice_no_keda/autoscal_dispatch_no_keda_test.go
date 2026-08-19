package inferenceservicenokeda

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Autoscaler dispatch consults the canonical ScaledObject on every class —
// including HPA and None — so each branch must tolerate a cluster where the
// KEDA CRD is not installed. These specs drive a full reconcile with the
// ScaledObject scheme registered but its CRD absent: the HPA class must still
// converge and a class transition to None must still clean up.
var _ = ginkgo.Describe("InferenceService autoscaler dispatch with the KEDA CRD absent", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "autoscal-nokeda-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("typed HPA converges and a switch to None cleans up without touching ScaledObjects", func() {
		runtime := v1beta1testing.MakeClusterServingRuntime("autoscal-nokeda-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/runtime:test",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })

		minReplicas := 1
		// Lean runtime-only ISVC: no spec.model, so the suite does not need
		// the BaseModel reconciler to drive a model lifecycle.
		isvc := v1beta1testing.MakeInferenceService("autoscal-nokeda-isvc", ns.Name).
			Runtime(runtime.Name).
			DeploymentMode(constants.RawDeployment).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &minReplicas,
					MaxReplicas: 2,
					Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		componentName := isvc.Name + "-engine"
		hpaKey := types.NamespacedName{Namespace: ns.Name, Name: componentName}

		ginkgo.By("expecting the HPA to converge despite the absent ScaledObject mapping")
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, hpaKey, hpa)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
		Expect(hpa.Spec.ScaleTargetRef.APIVersion).To(Equal(appsv1.SchemeGroupVersion.String()))
		Expect(hpa.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
		Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal(componentName))

		ginkgo.By("expecting autoscaler status to report the OME-managed HPA")
		Eventually(func(g Gomega) {
			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: ns.Name,
				Name:      isvc.Name,
			}, latest)).To(Succeed())
			status, ok := latest.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			g.Expect(status.Autoscaler).NotTo(BeNil())
			g.Expect(status.Autoscaler.Class).To(Equal(v1beta1.AutoscalerHPA))
			g.Expect(status.Autoscaler.ManagedBy).To(Equal(v1beta1.AutoscalerManagedByOME))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("switching the class to None and expecting the HPA to be deleted")
		Eventually(func() error {
			latest := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: ns.Name,
				Name:      isvc.Name,
			}, latest); err != nil {
				return err
			}
			latest.Spec.Engine.ComponentExtensionSpec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
			return k8sClient.Update(suiteCtx, latest)
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, hpaKey, &autoscalingv2.HorizontalPodAutoscaler{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())
	})
})
