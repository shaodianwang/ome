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

// TestCreateHPA exercises the legacy-shim path: the existing Raw Deployment
// constructor flows through createHPAFromLegacyComponentExt, which must keep
// producing the byte-identical pre-refactor HPA. Once Slice 6 deletes the
// legacy ScaleTarget / ScaleMetric fields these cases collapse into the
// IRScaleTarget / CustomMetrics tests below.
func TestCreateHPA(t *testing.T) {

	type args struct {
		objectMeta   metav1.ObjectMeta
		componentExt *v1beta1.ComponentExtensionSpec
	}

	cpuResource := v1beta1.MetricCPU
	memoryResource := v1beta1.MetricMemory

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
		"igspecifiedhpa": {
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
				ScaleTarget: isvc.GetIntReference(30),
				ScaleMetric: &cpuResource,
			},
		},
		"predictordefaulthpa": {
			objectMeta: metav1.ObjectMeta{},
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: nil,
				MaxReplicas: 0,
				ScaleTarget: nil,
				ScaleMetric: &memoryResource,
			},
		},
		"predictorspecifiedhpa": {
			objectMeta: metav1.ObjectMeta{},
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: isvc.GetIntReference(5),
				MaxReplicas: 10,
				ScaleTarget: isvc.GetIntReference(50),
				ScaleMetric: &cpuResource,
			},
		},
		"invalidinputhpa": {
			objectMeta: metav1.ObjectMeta{},
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: isvc.GetIntReference(0),
				MaxReplicas: -10,
				ScaleTarget: nil,
				ScaleMetric: &memoryResource,
			},
		},
	}

	defaultminreplicas := int32(1)
	defaultutilization := int32(80)
	igminreplicas := int32(2)
	igutilization := int32(30)
	predictorminreplicas := int32(5)
	predictorutilization := int32(50)

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
				Metrics: []autoscalingv2.MetricSpec{
					{
						Type: autoscalingv2.ResourceMetricSourceType,
						Resource: &autoscalingv2.ResourceMetricSource{
							Name: v1.ResourceName("cpu"),
							Target: autoscalingv2.MetricTarget{
								Type:               "Utilization",
								AverageUtilization: &defaultutilization,
							},
						},
					},
				},
				Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{},
			},
		},
		"igspecifiedhpa": {
			ObjectMeta: testInput["igspecifiedhpa"].objectMeta,
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       testInput["igspecifiedhpa"].objectMeta.Name,
				},
				MinReplicas: &igminreplicas,
				MaxReplicas: 5,
				Metrics: []autoscalingv2.MetricSpec{
					{
						Type: autoscalingv2.ResourceMetricSourceType,
						Resource: &autoscalingv2.ResourceMetricSource{
							Name: v1.ResourceName("cpu"),
							Target: autoscalingv2.MetricTarget{
								Type:               "Utilization",
								AverageUtilization: &igutilization,
							},
						},
					},
				},
				Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{},
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
				Metrics: []autoscalingv2.MetricSpec{
					{
						Type: autoscalingv2.ResourceMetricSourceType,
						Resource: &autoscalingv2.ResourceMetricSource{
							Name: v1.ResourceName("memory"),
							Target: autoscalingv2.MetricTarget{
								Type:               "Utilization",
								AverageUtilization: &defaultutilization,
							},
						},
					},
				},
				Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{},
			},
		},
		"predictorspecifiedhpa": {
			ObjectMeta: metav1.ObjectMeta{},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
				},
				MinReplicas: &predictorminreplicas,
				MaxReplicas: 10,
				Metrics: []autoscalingv2.MetricSpec{
					{
						Type: autoscalingv2.ResourceMetricSourceType,
						Resource: &autoscalingv2.ResourceMetricSource{
							Name: v1.ResourceName("cpu"),
							Target: autoscalingv2.MetricTarget{
								Type:               "Utilization",
								AverageUtilization: &predictorutilization,
							},
						},
					},
				},
				Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{},
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
			name: "inference graph specified hpa",
			args: args{
				objectMeta:   testInput["igspecifiedhpa"].objectMeta,
				componentExt: testInput["igspecifiedhpa"].componentExt,
			},
			expected: expectedHPASpecs["igspecifiedhpa"],
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
			name: "predictor specified hpa",
			args: args{
				objectMeta:   testInput["predictorspecifiedhpa"].objectMeta,
				componentExt: testInput["predictorspecifiedhpa"].componentExt,
			},
			expected: expectedHPASpecs["predictorspecifiedhpa"],
		},
		{
			name: "invalid input for hpa",
			args: args{
				objectMeta:   testInput["invalidinputhpa"].objectMeta,
				componentExt: testInput["invalidinputhpa"].componentExt,
			},
			expected: expectedHPASpecs["predictordefaulthpa"],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createHPAFromLegacyComponentExt(tt.args.objectMeta, tt.args.componentExt)
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("Test %q unexpected hpa (-want +got): %v", tt.name, diff)
			}
		})
	}
}

// TestCreateHPA_IRScaleTarget verifies the OEP-0013 Slice 3 wiring: when the
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

// TestCreateHPA_LegacyShim_Identical pins the byte-identical contract: the
// shim must emit the same HPA as the pre-refactor createHPA. The expected
// object is hand-rolled to encode the legacy generator's exact output for
// componentExt{MinReplicas:3, MaxReplicas:7, ScaleTarget:55, ScaleMetric:cpu}.
func TestCreateHPA_LegacyShim_Identical(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "raw-engine",
		Namespace: "ns",
		Labels:    map[string]string{"app": "raw-engine"},
	}
	cpuResource := v1beta1.MetricCPU
	componentExt := &v1beta1.ComponentExtensionSpec{
		MinReplicas: isvc.GetIntReference(3),
		MaxReplicas: 7,
		ScaleTarget: isvc.GetIntReference(55),
		ScaleMetric: &cpuResource,
	}
	minReplicas := int32(3)
	utilization := int32(55)
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
							Type:               "Utilization",
							AverageUtilization: &utilization,
						},
					},
				},
			},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{},
		},
	}

	got := createHPAFromLegacyComponentExt(componentMeta, componentExt)

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("Legacy shim is not byte-identical (-want +got): %s", diff)
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
