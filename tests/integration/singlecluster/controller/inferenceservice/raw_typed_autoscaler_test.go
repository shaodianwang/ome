package inferenceservice

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

type rawTypedAutoscalerFixture struct {
	isvc          *v1beta1.InferenceService
	componentName string
}

const rawTypedKEDAControlAnnotation = "scaledobject.keda.sh/transfer-hpa-ownership"

var _ = ginkgo.Describe("InferenceService Raw typed autoscalers", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-raw-as-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	for _, tt := range []struct {
		name        string
		prefix      string
		annotations map[string]string
	}{
		{
			name:   "Raw typed None overrides the default HPA when legacy annotations are absent",
			prefix: "typed-none-default",
		},
		{
			name:   "Raw typed None overrides the legacy HPA annotation",
			prefix: "typed-none-legacy",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
			},
		},
	} {
		tt := tt
		ginkgo.It(tt.name, func() {
			fixture := createRawTypedAutoscalerFixture(
				ns.Name,
				tt.prefix,
				&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
				nil,
				tt.annotations,
			)

			waitForRawTypedAutoscalerStatus(fixture, v1beta1.AutoscalerNone, v1beta1.AutoscalerManagedByNone, "isvc")
			expectRawTypedHPAAbsent(fixture)
			expectRawTypedScaledObjectAbsent(fixture)
		})
	}

	ginkgo.It("Raw typed KEDA creates a Deployment-target ScaledObject owned by the InferenceService", func() {
		fixture := createRawTypedAutoscalerFixture(ns.Name, "typed-keda", rawTypedKEDAAutoscaler(), nil, map[string]string{
			constants.AutoscalerClass:         string(constants.AutoscalerClassHPA),
			rawTypedKEDAControlAnnotation:     "true",
			"example.com/autoscaler-metadata": "keda",
		})

		so := expectRawTypedScaledObject(fixture)
		Expect(so.Spec.ScaleTargetRef.APIVersion).To(Equal(appsv1.SchemeGroupVersion.String()))
		Expect(so.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
		Expect(so.Spec.ScaleTargetRef.Name).To(Equal(fixture.componentName))
		Expect(so.Labels).To(HaveKeyWithValue(constants.InferenceServicePodLabelKey, fixture.isvc.Name))
		Expect(so.Labels).To(HaveKeyWithValue(constants.OMEComponentLabel, string(v1beta1.EngineComponent)))
		Expect(so.Annotations).To(HaveKeyWithValue(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)))
		Expect(so.Annotations).To(HaveKeyWithValue(rawTypedKEDAControlAnnotation, "true"))
		Expect(so.Annotations).To(HaveKeyWithValue("example.com/autoscaler-metadata", "keda"))
		expectRawTypedControllerOwner(so, fixture.isvc)
		expectRawTypedHPAAbsent(fixture)
		waitForRawTypedAutoscalerStatus(fixture, v1beta1.AutoscalerKEDA, v1beta1.AutoscalerManagedByOME, "isvc")
	})

	ginkgo.It("Raw typed HPA targets the component Deployment", func() {
		fixture := createRawTypedAutoscalerFixture(
			ns.Name,
			"typed-hpa",
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			nil,
			map[string]string{"example.com/autoscaler-metadata": "hpa"},
		)

		hpa := expectRawTypedHPA(fixture)
		Expect(hpa.Spec.ScaleTargetRef.APIVersion).To(Equal(appsv1.SchemeGroupVersion.String()))
		Expect(hpa.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
		Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal(fixture.componentName))
		Expect(hpa.Labels).To(HaveKeyWithValue(constants.InferenceServicePodLabelKey, fixture.isvc.Name))
		Expect(hpa.Labels).To(HaveKeyWithValue(constants.OMEComponentLabel, string(v1beta1.EngineComponent)))
		Expect(hpa.Annotations).To(HaveKeyWithValue("example.com/autoscaler-metadata", "hpa"))
		expectRawTypedControllerOwner(hpa, fixture.isvc)
		expectRawTypedScaledObjectAbsent(fixture)
		waitForRawTypedAutoscalerStatus(fixture, v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "isvc")
	})

	ginkgo.It("Raw typed External leaves both OME autoscaler resources absent", func() {
		fixture := createRawTypedAutoscalerFixture(
			ns.Name,
			"typed-external",
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			nil,
			nil,
		)

		waitForRawTypedAutoscalerStatus(fixture, v1beta1.AutoscalerExternal, v1beta1.AutoscalerManagedByExternal, "isvc")
		expectRawTypedHPAAbsent(fixture)
		expectRawTypedScaledObjectAbsent(fixture)
	})

	for _, tt := range []struct {
		name       string
		prefix     string
		autoscaler *v1beta1.ComponentAutoscaler
		class      v1beta1.AutoscalerClass
		managedBy  v1beta1.AutoscalerManagedBy
		wantKEDA   bool
	}{
		{
			name:       "Raw runtime-inherited KEDA creates a Deployment-target ScaledObject",
			prefix:     "runtime-keda",
			autoscaler: rawTypedKEDAAutoscaler(),
			class:      v1beta1.AutoscalerKEDA,
			managedBy:  v1beta1.AutoscalerManagedByOME,
			wantKEDA:   true,
		},
		{
			name:       "Raw runtime-inherited External leaves both OME autoscaler resources absent",
			prefix:     "runtime-external",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			class:      v1beta1.AutoscalerExternal,
			managedBy:  v1beta1.AutoscalerManagedByExternal,
		},
	} {
		tt := tt
		ginkgo.It(tt.name, func() {
			fixture := createRawTypedAutoscalerFixture(ns.Name, tt.prefix, nil, tt.autoscaler, nil)

			waitForRawTypedAutoscalerStatus(fixture, tt.class, tt.managedBy, "runtime")
			if tt.wantKEDA {
				so := expectRawTypedScaledObject(fixture)
				Expect(so.Spec.ScaleTargetRef.APIVersion).To(Equal(appsv1.SchemeGroupVersion.String()))
				Expect(so.Spec.ScaleTargetRef.Kind).To(Equal("Deployment"))
				Expect(so.Spec.ScaleTargetRef.Name).To(Equal(fixture.componentName))
				expectRawTypedControllerOwner(so, fixture.isvc)
			} else {
				expectRawTypedScaledObjectAbsent(fixture)
			}
			expectRawTypedHPAAbsent(fixture)
		})
	}

	ginkgo.It("Raw HPA to KEDA to None deletes OME-owned scalers and preserves a foreign canonical HPA", func() {
		fixture := createRawTypedAutoscalerFixture(
			ns.Name,
			"typed-transition",
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			nil,
			nil,
		)

		hpa := expectRawTypedHPA(fixture)
		expectRawTypedControllerOwner(hpa, fixture.isvc)

		updateRawTypedAutoscaler(fixture, rawTypedKEDAAutoscaler())
		so := expectRawTypedScaledObject(fixture)
		expectRawTypedControllerOwner(so, fixture.isvc)
		expectRawTypedHPAAbsent(fixture)

		foreignOwner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "foreign-autoscaler-owner", Namespace: ns.Name}}
		Expect(k8sClient.Create(suiteCtx, foreignOwner)).To(Succeed())
		foreignMin := int32(1)
		foreignHPA := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fixture.componentName,
				Namespace:       ns.Name,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(foreignOwner, corev1.SchemeGroupVersion.WithKind("ConfigMap"))},
			},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       "Deployment",
					Name:       fixture.componentName,
				},
				MinReplicas: &foreignMin,
				MaxReplicas: 2,
				Metrics:     rawTypedCPUMetrics(70),
			},
		}
		Expect(k8sClient.Create(suiteCtx, foreignHPA)).To(Succeed())
		foreignUID := foreignHPA.UID

		updateRawTypedAutoscaler(fixture, &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone})
		waitForRawTypedAutoscalerStatus(fixture, v1beta1.AutoscalerNone, v1beta1.AutoscalerManagedByNone, "isvc")
		expectRawTypedScaledObjectAbsent(fixture)

		preserved := expectRawTypedHPA(fixture)
		Expect(preserved.UID).To(Equal(foreignUID))
		controller := metav1.GetControllerOf(preserved)
		Expect(controller).NotTo(BeNil())
		Expect(controller.UID).To(Equal(foreignOwner.UID))
	})

	ginkgo.It("Raw legacy HPA fallback preserves target utilization and reports legacy status source", func() {
		fixture := createRawTypedAutoscalerFixture(
			ns.Name,
			"legacy-hpa",
			nil,
			nil,
			map[string]string{
				constants.AutoscalerClass:             string(constants.AutoscalerClassHPA),
				constants.TargetUtilizationPercentage: "67",
			},
		)

		hpa := expectRawTypedHPA(fixture)
		Expect(hpa.Spec.Metrics).To(HaveLen(1))
		Expect(hpa.Spec.Metrics[0].Resource).NotTo(BeNil())
		Expect(hpa.Spec.Metrics[0].Resource.Target.AverageUtilization).NotTo(BeNil())
		Expect(*hpa.Spec.Metrics[0].Resource.Target.AverageUtilization).To(Equal(int32(67)))
		waitForRawTypedAutoscalerStatus(fixture, v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "legacy")
	})

	ginkgo.It("Raw legacy KEDA without triggers rejects reconciliation and preserves the prior owned HPA", func() {
		fixture := createRawTypedAutoscalerFixture(
			ns.Name,
			"legacy-keda-invalid",
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			nil,
			nil,
		)

		hpa := expectRawTypedHPA(fixture)
		originalUID := hpa.UID
		originalSpec := hpa.DeepCopy().Spec
		waitForRawTypedAutoscalerStatus(fixture, v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "isvc")

		updateRawTypedAutoscalerWithLegacyClass(fixture, nil, constants.AutoscalerClassKEDA)

		Consistently(func(g Gomega) {
			preserved := &autoscalingv2.HorizontalPodAutoscaler{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      fixture.componentName,
			}, preserved)).To(Succeed())
			g.Expect(preserved.UID).To(Equal(originalUID))
			g.Expect(preserved.Spec).To(Equal(originalSpec))

			err := k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      isvcutils.GetScaledObjectName(fixture.componentName),
			}, &kedav1.ScaledObject{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())

			latest := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      fixture.isvc.Name,
			}, latest)).To(Succeed())
			status := latest.Status.Components[v1beta1.EngineComponent]
			g.Expect(status.Autoscaler).NotTo(BeNil())
			g.Expect(status.Autoscaler.Class).To(Equal(v1beta1.AutoscalerHPA))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

func createRawTypedAutoscalerFixture(
	namespace string,
	prefix string,
	typedAutoscaler *v1beta1.ComponentAutoscaler,
	runtimeAutoscaler *v1beta1.ComponentAutoscaler,
	annotations map[string]string,
) rawTypedAutoscalerFixture {
	modelName := prefix + "-model"
	runtimeName := prefix + "-runtime"
	isvcName := prefix + "-isvc"
	minReplicas := 1
	maxReplicas := 4

	model := v1beta1testing.MakeClusterBaseModel(modelName).
		StorageURI("oci://n/example/b/models/o/" + prefix).
		ModelFormat("safetensors").
		ModelArchitecture("LlamaForCausalLM").
		ModelFramework("Transformers").Obj()
	Expect(k8sClient.Create(suiteCtx, model)).To(Succeed())
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, model) })

	runtimeEngine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			MinReplicas: &minReplicas,
			MaxReplicas: maxReplicas,
			Autoscaler:  copyRawTypedAutoscaler(runtimeAutoscaler),
		},
		Runner: &v1beta1.RunnerSpec{
			Container: corev1.Container{Name: constants.MainContainerName, Image: "example.com/runtime:test"},
		},
	}
	runtime := v1beta1testing.MakeClusterServingRuntime(runtimeName).
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		EngineConfig(runtimeEngine).Obj()
	Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })

	isvcBuilder := v1beta1testing.MakeInferenceService(isvcName, namespace).
		Model(modelName).
		Runtime(runtimeName).
		DeploymentMode(constants.RawDeployment).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas,
				MaxReplicas: maxReplicas,
				Autoscaler:  copyRawTypedAutoscaler(typedAutoscaler),
			},
		})
	for key, value := range annotations {
		isvcBuilder.Annotation(key, value)
	}
	isvc := isvcBuilder.Obj()
	Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

	return rawTypedAutoscalerFixture{
		isvc:          isvc,
		componentName: isvcName + "-engine",
	}
}

func rawTypedKEDAAutoscaler() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
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
	}
}

func rawTypedCPUMetrics(utilization int32) []autoscalingv2.MetricSpec {
	return []autoscalingv2.MetricSpec{{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name: corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{
				Type:               autoscalingv2.UtilizationMetricType,
				AverageUtilization: &utilization,
			},
		},
	}}
}

func copyRawTypedAutoscaler(autoscaler *v1beta1.ComponentAutoscaler) *v1beta1.ComponentAutoscaler {
	if autoscaler == nil {
		return nil
	}
	return autoscaler.DeepCopy()
}

func waitForRawTypedAutoscalerStatus(
	fixture rawTypedAutoscalerFixture,
	class v1beta1.AutoscalerClass,
	managedBy v1beta1.AutoscalerManagedBy,
	source string,
) {
	Eventually(func(g Gomega) {
		latest := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.isvc.Name,
		}, latest)).To(Succeed())
		status, ok := latest.Status.Components[v1beta1.EngineComponent]
		g.Expect(ok).To(BeTrue())
		g.Expect(status.Autoscaler).NotTo(BeNil())
		g.Expect(status.Autoscaler.Class).To(Equal(class))
		g.Expect(status.Autoscaler.ManagedBy).To(Equal(managedBy))
		g.Expect(status.Autoscaler.SpecSource).To(Equal(source))
		g.Expect(status.ScaleTargetRef).NotTo(BeNil())
		g.Expect(status.ScaleTargetRef.APIVersion).To(Equal(appsv1.SchemeGroupVersion.String()))
		g.Expect(status.ScaleTargetRef.Kind).To(Equal("Deployment"))
		g.Expect(status.ScaleTargetRef.Name).To(Equal(fixture.componentName))
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
}

func expectRawTypedHPA(fixture rawTypedAutoscalerFixture) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	Eventually(func() error {
		return k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.componentName,
		}, hpa)
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	return hpa
}

func expectRawTypedHPAAbsent(fixture rawTypedAutoscalerFixture) {
	Eventually(func() bool {
		err := k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.componentName,
		}, &autoscalingv2.HorizontalPodAutoscaler{})
		return apierrors.IsNotFound(err)
	}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(BeTrue())
}

func expectRawTypedScaledObject(fixture rawTypedAutoscalerFixture) *kedav1.ScaledObject {
	so := &kedav1.ScaledObject{}
	Eventually(func() error {
		return k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      isvcutils.GetScaledObjectName(fixture.componentName),
		}, so)
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	return so
}

func expectRawTypedScaledObjectAbsent(fixture rawTypedAutoscalerFixture) {
	Eventually(func() bool {
		err := k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      isvcutils.GetScaledObjectName(fixture.componentName),
		}, &kedav1.ScaledObject{})
		return apierrors.IsNotFound(err)
	}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(BeTrue())
}

func expectRawTypedControllerOwner(object metav1.Object, isvc *v1beta1.InferenceService) {
	controller := metav1.GetControllerOf(object)
	Expect(controller).NotTo(BeNil())
	Expect(controller.APIVersion).To(Equal(v1beta1.SchemeGroupVersion.String()))
	Expect(controller.Kind).To(Equal("InferenceService"))
	Expect(controller.Name).To(Equal(isvc.Name))
	Expect(controller.UID).To(Equal(isvc.UID))
}

func updateRawTypedAutoscaler(fixture rawTypedAutoscalerFixture, autoscaler *v1beta1.ComponentAutoscaler) {
	Eventually(func() error {
		latest := &v1beta1.InferenceService{}
		if err := k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.isvc.Name,
		}, latest); err != nil {
			return err
		}
		latest.Spec.Engine.ComponentExtensionSpec.Autoscaler = copyRawTypedAutoscaler(autoscaler)
		return k8sClient.Update(suiteCtx, latest)
	}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
}

func updateRawTypedAutoscalerWithLegacyClass(
	fixture rawTypedAutoscalerFixture,
	autoscaler *v1beta1.ComponentAutoscaler,
	class constants.AutoscalerClassType,
) {
	Eventually(func() error {
		latest := &v1beta1.InferenceService{}
		if err := k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.isvc.Name,
		}, latest); err != nil {
			return err
		}
		latest.Spec.Engine.ComponentExtensionSpec.Autoscaler = copyRawTypedAutoscaler(autoscaler)
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[constants.AutoscalerClass] = string(class)
		return k8sClient.Update(suiteCtx, latest)
	}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
}
