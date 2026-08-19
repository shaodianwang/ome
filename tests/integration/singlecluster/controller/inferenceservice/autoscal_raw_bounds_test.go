package inferenceservice

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// RawDeployment bound generation: the typed KEDA path is the only one that
// admits a zero floor (scale-to-zero), while HPA bounds forward the explicit
// values and clamp an omitted max up to the min. These specs pin the rendered
// HPA / ScaledObject bounds through the full reconcile loop.
var _ = ginkgo.Describe("InferenceService Raw autoscaler replica bounds", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "autoscal-raw-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("typed KEDA with minReplicas zero renders a zero-floor ScaledObject", func() {
		minReplicas := 0
		componentName := createAutoscalRawBoundsFixture(ns.Name, "autoscal-keda-zero", &minReplicas, 3, &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaAutoscaler{
				Triggers: []kedav1.ScaleTriggers{{
					Type: "prometheus",
					Metadata: map[string]string{
						"serverAddress": "https://metrics.example.com",
						"query":         "requests_in_flight",
						"threshold":     "5",
					},
				}},
			},
		})

		so := &kedav1.ScaledObject{}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: ns.Name,
				Name:      isvcutils.GetScaledObjectName(componentName),
			}, so)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(so.Spec.MinReplicaCount).NotTo(BeNil())
		Expect(*so.Spec.MinReplicaCount).To(Equal(int32(0)))
		Expect(so.Spec.MaxReplicaCount).NotTo(BeNil())
		Expect(*so.Spec.MaxReplicaCount).To(Equal(int32(3)))
		Expect(so.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
		Expect(so.Spec.ScaleTargetRef.Name).To(Equal(componentName))
	})

	ginkgo.It("typed HPA forwards explicit min and max bounds", func() {
		minReplicas := 2
		componentName := createAutoscalRawBoundsFixture(ns.Name, "autoscal-hpa-bounds", &minReplicas, 5,
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA})

		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: ns.Name,
				Name:      componentName,
			}, hpa)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(hpa.Spec.MinReplicas).NotTo(BeNil())
		Expect(*hpa.Spec.MinReplicas).To(Equal(int32(2)))
		Expect(hpa.Spec.MaxReplicas).To(Equal(int32(5)))
	})

	ginkgo.It("typed HPA with omitted maxReplicas clamps the ceiling to the floor", func() {
		minReplicas := 3
		componentName := createAutoscalRawBoundsFixture(ns.Name, "autoscal-hpa-clamp", &minReplicas, 0,
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA})

		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		Eventually(func() error {
			return k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: ns.Name,
				Name:      componentName,
			}, hpa)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(hpa.Spec.MinReplicas).NotTo(BeNil())
		Expect(*hpa.Spec.MinReplicas).To(Equal(int32(3)))
		Expect(hpa.Spec.MaxReplicas).To(Equal(int32(3)))
	})
})

// createAutoscalRawBoundsFixture creates a ClusterBaseModel +
// ClusterServingRuntime + RawDeployment InferenceService triple with the
// given engine bounds and typed autoscaler; returns the component name the
// generated Deployment / scaler share.
func createAutoscalRawBoundsFixture(
	namespace string,
	prefix string,
	minReplicas *int,
	maxReplicas int,
	typedAutoscaler *v1beta1.ComponentAutoscaler,
) string {
	modelName := prefix + "-model"
	runtimeName := prefix + "-runtime"
	isvcName := prefix + "-isvc"

	model := v1beta1testing.MakeClusterBaseModel(modelName).
		StorageURI("oci://n/example/b/models/o/" + prefix).
		ModelFormat("safetensors").
		ModelArchitecture("LlamaForCausalLM").
		ModelFramework("Transformers").Obj()
	Expect(k8sClient.Create(suiteCtx, model)).To(Succeed())
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, model) })

	runtime := v1beta1testing.MakeClusterServingRuntime(runtimeName).
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		EngineRunner(corev1.Container{
			Name:  constants.MainContainerName,
			Image: "example.com/runtime:test",
			Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
		}).Obj()
	Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })

	isvc := v1beta1testing.MakeInferenceService(isvcName, namespace).
		Model(modelName).
		Runtime(runtimeName).
		DeploymentMode(constants.RawDeployment).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: minReplicas,
				MaxReplicas: maxReplicas,
				Autoscaler:  typedAutoscaler.DeepCopy(),
			},
		}).Obj()
	Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

	return isvcName + "-engine"
}
