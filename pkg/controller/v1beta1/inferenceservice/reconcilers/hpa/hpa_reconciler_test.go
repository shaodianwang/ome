package hpa

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/webhook/admission/isvc"
)

// TestCreateHPAFromComponentExt exercises the Raw Deployment path: the
// existing Raw Deployment constructor flows through
// createHPAFromComponentExt, which reads from the Autoscaler.HPA
// block (the canonical location) plus the legacy
// ome.io/targetUtilizationPercentage annotation override.
func TestCreateHPAFromComponentExt(t *testing.T) {

	type args struct {
		objectMeta   metav1.ObjectMeta
		componentExt *v1beta1.ComponentExtensionSpec
	}

	cpuMetric := func(util int32) autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: v1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: &util,
				},
			},
		}
	}
	memMetric := func(util int32) autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name: v1.ResourceMemory,
				Target: autoscalingv2.MetricTarget{
					Type:               autoscalingv2.UtilizationMetricType,
					AverageUtilization: &util,
				},
			},
		}
	}

	testInput := map[string]args{
		"igdefaulthpa": {
			objectMeta: metav1.ObjectMeta{
				Name:      "basic-ig",
				Namespace: "basic-ig-namespace",
				Annotations: map[string]string{
					"annotation": "annotation-value",
				},
				Labels: map[string]string{
					"label":                 "label-value",
					"ome.io/inferencegraph": "basic-ig",
				},
			},
			componentExt: &v1beta1.ComponentExtensionSpec{},
		},
		"igAutoscalerCPU30": {
			objectMeta: metav1.ObjectMeta{
				Name:      "basic-ig",
				Namespace: "basic-ig-namespace",
				Annotations: map[string]string{
					"annotation": "annotation-value",
				},
				Labels: map[string]string{
					"label":                 "label-value",
					"ome.io/inferencegraph": "basic-ig",
				},
			},
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: isvc.GetIntReference(2),
				MaxReplicas: 5,
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA: &v1beta1.HPAAutoscaler{
						Metrics: []autoscalingv2.MetricSpec{cpuMetric(30)},
					},
				},
			},
		},
		"predictordefaulthpa": {
			objectMeta: metav1.ObjectMeta{},
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: nil,
				MaxReplicas: 0,
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA: &v1beta1.HPAAutoscaler{
						Metrics: []autoscalingv2.MetricSpec{memMetric(80)},
					},
				},
			},
		},
		"predictorAutoscalerCPU50": {
			objectMeta: metav1.ObjectMeta{},
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: isvc.GetIntReference(5),
				MaxReplicas: 10,
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA: &v1beta1.HPAAutoscaler{
						Metrics: []autoscalingv2.MetricSpec{cpuMetric(50)},
					},
				},
			},
		},
		"invalidinputhpa": {
			objectMeta: metav1.ObjectMeta{},
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: isvc.GetIntReference(0),
				MaxReplicas: -10,
				Autoscaler: &v1beta1.ComponentAutoscaler{
					Class: v1beta1.AutoscalerHPA,
					HPA: &v1beta1.HPAAutoscaler{
						Metrics: []autoscalingv2.MetricSpec{memMetric(80)},
					},
				},
			},
		},
		"annotationOverrideCPU60": {
			objectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.TargetUtilizationPercentage: "60",
				},
			},
			// No Autoscaler block — annotation drives the metric.
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: isvc.GetIntReference(1),
				MaxReplicas: 3,
			},
		},
	}

	defaultminreplicas := int32(1)
	igminreplicas := int32(2)
	predictorminreplicas := int32(5)

	expectedHPASpecs := map[string]*autoscalingv2.HorizontalPodAutoscaler{
		"igdefaulthpa": {
			ObjectMeta: testInput["igdefaulthpa"].objectMeta,
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       testInput["igdefaulthpa"].objectMeta.Name,
				},
				MinReplicas: &defaultminreplicas,
				MaxReplicas: 1,
				Metrics:     []autoscalingv2.MetricSpec{cpuMetric(80)},
				Behavior:    &autoscalingv2.HorizontalPodAutoscalerBehavior{},
			},
		},
		"igAutoscalerCPU30": {
			ObjectMeta: testInput["igAutoscalerCPU30"].objectMeta,
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       testInput["igAutoscalerCPU30"].objectMeta.Name,
				},
				MinReplicas: &igminreplicas,
				MaxReplicas: 5,
				Metrics:     []autoscalingv2.MetricSpec{cpuMetric(30)},
				Behavior:    &autoscalingv2.HorizontalPodAutoscalerBehavior{},
			},
		},
		"predictordefaulthpa": {
			ObjectMeta: metav1.ObjectMeta{},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
				},
				MinReplicas: &defaultminreplicas,
				MaxReplicas: 1,
				Metrics:     []autoscalingv2.MetricSpec{memMetric(80)},
				Behavior:    &autoscalingv2.HorizontalPodAutoscalerBehavior{},
			},
		},
		"predictorAutoscalerCPU50": {
			ObjectMeta: metav1.ObjectMeta{},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
				},
				MinReplicas: &predictorminreplicas,
				MaxReplicas: 10,
				Metrics:     []autoscalingv2.MetricSpec{cpuMetric(50)},
				Behavior:    &autoscalingv2.HorizontalPodAutoscalerBehavior{},
			},
		},
		"annotationOverrideCPU60": {
			ObjectMeta: testInput["annotationOverrideCPU60"].objectMeta,
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
				},
				MinReplicas: &defaultminreplicas,
				MaxReplicas: 3,
				Metrics:     []autoscalingv2.MetricSpec{cpuMetric(60)},
				Behavior:    &autoscalingv2.HorizontalPodAutoscalerBehavior{},
			},
		},
	}

	tests := []struct {
		name     string
		args     args
		expected *autoscalingv2.HorizontalPodAutoscaler
	}{
		{
			name: "inference graph default hpa",
			args: args{
				objectMeta:   testInput["igdefaulthpa"].objectMeta,
				componentExt: testInput["igdefaulthpa"].componentExt,
			},
			expected: expectedHPASpecs["igdefaulthpa"],
		},
		{
			name: "inference graph autoscaler-block cpu 30%",
			args: args{
				objectMeta:   testInput["igAutoscalerCPU30"].objectMeta,
				componentExt: testInput["igAutoscalerCPU30"].componentExt,
			},
			expected: expectedHPASpecs["igAutoscalerCPU30"],
		},
		{
			name: "predictor default hpa",
			args: args{
				objectMeta:   testInput["predictordefaulthpa"].objectMeta,
				componentExt: testInput["predictordefaulthpa"].componentExt,
			},
			expected: expectedHPASpecs["predictordefaulthpa"],
		},
		{
			name: "predictor autoscaler-block cpu 50%",
			args: args{
				objectMeta:   testInput["predictorAutoscalerCPU50"].objectMeta,
				componentExt: testInput["predictorAutoscalerCPU50"].componentExt,
			},
			expected: expectedHPASpecs["predictorAutoscalerCPU50"],
		},
		{
			name: "invalid bounds collapse to defaults",
			args: args{
				objectMeta:   testInput["invalidinputhpa"].objectMeta,
				componentExt: testInput["invalidinputhpa"].componentExt,
			},
			expected: expectedHPASpecs["predictordefaulthpa"],
		},
		{
			name: "annotation overrides default cpu 60%",
			args: args{
				objectMeta:   testInput["annotationOverrideCPU60"].objectMeta,
				componentExt: testInput["annotationOverrideCPU60"].componentExt,
			},
			expected: expectedHPASpecs["annotationOverrideCPU60"],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createHPAFromComponentExt(tt.args.objectMeta, tt.args.componentExt)
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("Test %q unexpected hpa (-want +got): %v", tt.name, diff)
			}
		})
	}
}

// TestCreateHPA_IRScaleTarget verifies the IR-managed wiring: when the
// caller passes an InferenceReplica scaleTargetRef and a nil HPAAutoscaler,
// the generator emits an HPA targeting the IR with the default CPU=80% metric.
func TestCreateHPA_IRScaleTarget(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "foo-engine",
		Namespace: "ns",
	}
	scaleTargetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}

	got := createHPA(componentMeta, scaleTargetRef, nil, 1, 5)

	if diff := cmp.Diff(scaleTargetRef, got.Spec.ScaleTargetRef); diff != "" {
		t.Errorf("ScaleTargetRef mismatch (-want +got): %s", diff)
	}
	if got.Spec.MinReplicas == nil || *got.Spec.MinReplicas != 1 {
		t.Errorf("MinReplicas: want 1, got %v", got.Spec.MinReplicas)
	}
	if got.Spec.MaxReplicas != 5 {
		t.Errorf("MaxReplicas: want 5, got %d", got.Spec.MaxReplicas)
	}
	if len(got.Spec.Metrics) != 1 {
		t.Fatalf("Expected 1 default metric, got %d", len(got.Spec.Metrics))
	}
	m := got.Spec.Metrics[0]
	if m.Type != autoscalingv2.ResourceMetricSourceType {
		t.Errorf("Expected Resource metric, got %v", m.Type)
	}
	if m.Resource == nil || m.Resource.Name != v1.ResourceCPU {
		t.Errorf("Expected default Resource=cpu, got %+v", m.Resource)
	}
	if m.Resource.Target.AverageUtilization == nil || *m.Resource.Target.AverageUtilization != 80 {
		t.Errorf("Expected default AverageUtilization=80, got %v", m.Resource.Target.AverageUtilization)
	}
	if got.Spec.Behavior == nil {
		t.Error("Expected non-nil Behavior (empty stub), got nil")
	}
}

// TestCreateHPA_CustomMetrics verifies that custom Metrics (Object,
// External, etc.) on HPAAutoscaler are forwarded verbatim, displacing the
// default CPU=80% fallback. Two metrics included so the test catches any
// accidental single-element collapse.
func TestCreateHPA_CustomMetrics(t *testing.T) {
	componentMeta := metav1.ObjectMeta{Name: "foo-engine", Namespace: "ns"}
	scaleTargetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	avg := int32(60)
	memUtil := int32(75)
	hpaSpec := &v1beta1.HPAAutoscaler{
		Metrics: []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: v1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: &avg,
					},
				},
			},
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: v1.ResourceMemory,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: &memUtil,
					},
				},
			},
		},
	}

	got := createHPA(componentMeta, scaleTargetRef, hpaSpec, 2, 8)

	if diff := cmp.Diff(hpaSpec.Metrics, got.Spec.Metrics); diff != "" {
		t.Errorf("Metrics not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateHPA_Behavior verifies that the HPA Behavior block is forwarded
// verbatim. The Behavior field exists on the generated HPA only when the
// caller supplies one; absent caller-supplied Behavior the generator emits
// an empty stub (preserves legacy semantics).
func TestCreateHPA_Behavior(t *testing.T) {
	componentMeta := metav1.ObjectMeta{Name: "foo-engine", Namespace: "ns"}
	scaleTargetRef := autoscalingv2.CrossVersionObjectReference{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	stabilization := int32(300)
	behavior := &autoscalingv2.HorizontalPodAutoscalerBehavior{
		ScaleDown: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: &stabilization,
		},
	}
	hpaSpec := &v1beta1.HPAAutoscaler{
		Behavior: behavior,
	}

	got := createHPA(componentMeta, scaleTargetRef, hpaSpec, 1, 3)

	if diff := cmp.Diff(behavior, got.Spec.Behavior); diff != "" {
		t.Errorf("Behavior not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateHPA_ComponentExtShim_Identical pins the byte-identical
// contract for the Raw Deployment path: createHPAFromComponentExt must
// emit the same HPA as createHPA when given an equivalent Autoscaler.HPA
// block. The metric configuration lives entirely in Autoscaler.HPA.Metrics
// — operators who used to set ScaleTarget=55 + ScaleMetric=cpu now declare
// a Resource{cpu, 55%} entry in Autoscaler.HPA.Metrics.
func TestCreateHPA_ComponentExtShim_Identical(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "raw-engine",
		Namespace: "ns",
		Labels:    map[string]string{"app": "raw-engine"},
	}
	utilization := int32(55)
	componentExt := &v1beta1.ComponentExtensionSpec{
		MinReplicas: isvc.GetIntReference(3),
		MaxReplicas: 7,
		Autoscaler: &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerHPA,
			HPA: &v1beta1.HPAAutoscaler{
				Metrics: []autoscalingv2.MetricSpec{
					{
						Type: autoscalingv2.ResourceMetricSourceType,
						Resource: &autoscalingv2.ResourceMetricSource{
							Name: v1.ResourceCPU,
							Target: autoscalingv2.MetricTarget{
								Type:               autoscalingv2.UtilizationMetricType,
								AverageUtilization: &utilization,
							},
						},
					},
				},
			},
		},
	}
	minReplicas := int32(3)
	expected := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: componentMeta,
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "raw-engine",
			},
			MinReplicas: &minReplicas,
			MaxReplicas: 7,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: v1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: &utilization,
						},
					},
				},
			},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{},
		},
	}

	got := createHPAFromComponentExt(componentMeta, componentExt)

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("ComponentExt shim is not byte-identical (-want +got): %s", diff)
	}
}

func TestSemanticHPAEquals(t *testing.T) {
	assert.True(t, semanticHPAEquals(
		&autoscalingv2.HorizontalPodAutoscaler{
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{},
		}))

	assert.False(t, semanticHPAEquals(
		&autoscalingv2.HorizontalPodAutoscaler{
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(4)},
		}))

	assert.False(t, semanticHPAEquals(
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.AutoscalerClass: "hpa"}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.AutoscalerClass: "external"}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		}))

	assert.False(t, semanticHPAEquals(
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.AutoscalerClass: "external"}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		}))

	assert.True(t, semanticHPAEquals(
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.AutoscalerClass: "hpa"}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{constants.AutoscalerClass: "hpa"}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		}))

	assert.True(t, semanticHPAEquals(
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		}))

	assert.True(t, semanticHPAEquals(
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"unrelated": "true"}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"unrelated": "false"}},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: ptr.Int32(3)},
		}))
}
