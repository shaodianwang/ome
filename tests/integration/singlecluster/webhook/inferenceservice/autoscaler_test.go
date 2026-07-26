package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Webhook validation for the per-Component Autoscaler
// block. One envtest spec per rejection
// rule, plus the annotation/block conflict rule; reach the apiserver
// over the validating webhook (no direct unit-test bypass).
//
// The status-field-twin treatment of `none` and `external` and
// the absence of a KEDA-installed probe are reflected in the absence of negative
// specs for those classes.
var _ = ginkgo.Describe("InferenceService webhook Autoscaler validation", func() {
	ginkgo.BeforeEach(ensureNamespace)

	// Helper to wrap a ComponentAutoscaler block onto a minimal
	// Engine-only ISVC. The model name is bogus and the runtime is
	// omitted — both validators downstream of the Autoscaler check
	// will fire too, but the webhook returns the first rejection so
	// the Autoscaler rule fires first when malformed.
	withAutoscaler := func(name string, as *v1beta1.ComponentAutoscaler) *v1beta1.InferenceService {
		return v1beta1testing.MakeInferenceService(name, "isvc-webhook").
			Model("any-model").
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: ptr.To[int](2),
					Autoscaler:  as,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
	}

	ginkgo.It("rejects class=keda with empty Triggers", func() {
		isvc := withAutoscaler("bad-keda-empty-triggers", &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerKEDA,
			Keda:  &v1beta1.KedaAutoscaler{Triggers: nil},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		// CRD's required-Triggers marker catches the
		// nil-triggers-list case at schema time with the apiserver's
		// "Required value" message. The webhook's own
		// "class=keda requires at least 1 trigger" message fires for
		// the nil-Keda-block case (covered in unit tests) which the
		// CRD marker cannot express.
		Expect(err.Error()).To(SatisfyAny(
			ContainSubstring("class=keda requires at least 1 trigger"),
			ContainSubstring("should have at least 1 items"),
			ContainSubstring("triggers: Required value"),
		))
	})

	ginkgo.It("rejects class=hpa with Type=Resource and Resource=nil", func() {
		isvc := withAutoscaler("bad-hpa-resource-nil", &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerHPA,
			HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.ResourceMetricSourceType,
			}}},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("type=Resource but"))
	})

	ginkgo.It("rejects class=keda with IdleReplicaCount >= MinReplicas", func() {
		// Engine.MinReplicas=2 via helper above; IdleReplicaCount=2
		// triggers the strictly-less rule.
		validTriggers := []kedav1.ScaleTriggers{{
			Type: "prometheus",
			Metadata: map[string]string{
				"serverAddress": "http://prom:9090",
				"query":         "rate(metric[1m])",
				"threshold":     "20",
			},
		}}
		isvc := withAutoscaler("bad-keda-idle-vs-min", &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaAutoscaler{
				Triggers:         validTriggers,
				IdleReplicaCount: ptr.To[int32](2),
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("keda.idleReplicaCount must be < minReplicas"))
	})

	ginkgo.It("rejects bogus class value not in CRD enum (defense-in-depth)", func() {
		// The CRD's +kubebuilder:validation:Enum marker should catch
		// this at the schema layer with a different message. The
		// webhook is defense-in-depth for stale-CRD clients; the
		// schema error here is fine — both reject.
		isvc := withAutoscaler("bad-class-value", &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerClass("knative"),
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(SatisfyAny(
			ContainSubstring("is not one of HPA|KEDA|External|None"),
			ContainSubstring("Unsupported value"),
		))
	})

	ginkgo.It("rejects ISVC with legacy autoscaler annotation AND Autoscaler block", func() {
		// Q2: annotation + block must not coexist. The validator
		// returns the AutoscalerAnnotationConflict reason regardless
		// of which Component carries the block.
		isvc := v1beta1testing.MakeInferenceService("conflict-annotation-block", "isvc-webhook").
			Model("any-model").
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassHPA)).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("AutoscalerAnnotationConflict"))
	})

	// Positive control: a valid Autoscaler block on each class shape
	// makes it past the new webhook rules (the spec is otherwise
	// malformed because the model doesn't exist; that's expected and
	// proves the autoscaler rule is not what's rejecting these).
	ginkgo.It("accepts class=external (Q5 status-field twin of none)", func() {
		isvc := withAutoscaler("ok-external", &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerExternal,
		})
		// The non-existent model trips the model-existence check
		// downstream of ValidateComponentAutoscaler, so this still
		// errors — but with a different reason, proving the
		// Autoscaler rule allowed it through.
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).NotTo(ContainSubstring("AutoscalerClassUnknown"))
		Expect(err.Error()).NotTo(ContainSubstring("AutoscalerAnnotationConflict"))
		Expect(err.Error()).NotTo(ContainSubstring("KedaTriggersRequired"))
		Expect(err.Error()).NotTo(ContainSubstring("HPAMetricMalformed"))
	})

	ginkgo.It("accepts class=hpa with Metrics that include a valid Resource entry", func() {
		cpu80 := resource.MustParse("80")
		isvc := withAutoscaler("ok-hpa-resource", &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerHPA,
			HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:         autoscalingv2.UtilizationMetricType,
						AverageValue: &cpu80,
					},
				},
			}}},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		// Same as above: model-existence trip is fine — none of the
		// new Autoscaler reason strings should appear.
		Expect(err.Error()).NotTo(ContainSubstring("HPAMetricMalformed"))
	})
})
