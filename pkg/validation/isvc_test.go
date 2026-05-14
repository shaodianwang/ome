package validation

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func TestValidateInferenceServiceName(t *testing.T) {
	tests := []struct {
		name    string
		isvcName string
		wantErr bool
	}{
		{name: "valid lowercase", isvcName: "my-model", wantErr: false},
		{name: "single char", isvcName: "a", wantErr: false},
		{name: "alphanumeric with hyphens", isvcName: "llama-3-70b", wantErr: false},
		{name: "starts with number", isvcName: "3model", wantErr: true},
		{name: "uppercase", isvcName: "MyModel", wantErr: true},
		{name: "ends with hyphen", isvcName: "model-", wantErr: true},
		{name: "empty string", isvcName: "", wantErr: true},
		{name: "underscore", isvcName: "my_model", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateInferenceServiceName(tc.isvcName)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for name %q, got nil", tc.isvcName)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for name %q: %v", tc.isvcName, err)
			}
		})
	}
}

func TestValidateEngineDecoderConfig(t *testing.T) {
	tests := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{
			name: "no decoder or engine",
			spec: &v1beta1.InferenceServiceSpec{},
		},
		{
			name: "engine without decoder",
			spec: &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		},
		{
			name: "engine with decoder",
			spec: &v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
			},
		},
		{
			name:    "decoder without engine",
			spec:    &v1beta1.InferenceServiceSpec{Decoder: &v1beta1.DecoderSpec{}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEngineDecoderConfig(tc.spec)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateReplicaBounds(t *testing.T) {
	intPtr := func(v int) *int { return &v }

	tests := []struct {
		name    string
		min     *int
		max     *int
		wantErr bool
	}{
		{name: "nil both", min: nil, max: nil},
		{name: "valid range", min: intPtr(1), max: intPtr(3)},
		{name: "min equals max", min: intPtr(2), max: intPtr(2)},
		{name: "min zero", min: intPtr(0), max: intPtr(1)},
		{name: "negative min", min: intPtr(-1), max: intPtr(1), wantErr: true},
		{name: "zero max", min: intPtr(0), max: intPtr(0), wantErr: true},
		{name: "min greater than max", min: intPtr(5), max: intPtr(2), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateReplicaBounds(tc.min, tc.max)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateAutoscalerConfig(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		kedaConfig  *v1beta1.KedaConfig
		wantErr     bool
	}{
		{
			name:        "no autoscaler annotation",
			annotations: map[string]string{},
		},
		{
			name: "valid HPA class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
			},
		},
		{
			name: "valid HPA with cpu metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: "cpu",
			},
		},
		{
			name: "HPA with invalid metric",
			annotations: map[string]string{
				constants.AutoscalerClass:   string(constants.AutoscalerClassHPA),
				constants.AutoscalerMetrics: "bogus",
			},
			wantErr: true,
		},
		{
			name: "unsupported autoscaler class",
			annotations: map[string]string{
				constants.AutoscalerClass: "unsupported",
			},
			wantErr: true,
		},
		{
			name: "valid KEDA with scaling operator",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
			},
			kedaConfig: &v1beta1.KedaConfig{ScalingOperator: "GreaterThan"},
		},
		{
			name: "KEDA with invalid scaling operator",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
			},
			kedaConfig: &v1beta1.KedaConfig{ScalingOperator: "InvalidOp"},
			wantErr:    true,
		},
		{
			name: "KEDA with invalid threshold",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassKEDA),
			},
			kedaConfig: &v1beta1.KedaConfig{ScalingThreshold: "not-a-number"},
			wantErr:    true,
		},
		{
			name: "valid external class",
			annotations: map[string]string{
				constants.AutoscalerClass: string(constants.AutoscalerClassExternal),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
				Spec:       v1beta1.InferenceServiceSpec{KedaConfig: tc.kedaConfig},
			}
			_, err := ValidateAutoscalerConfig(isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateAutoscalerTargetUtilizationPercentage(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{name: "no annotation", annotations: map[string]string{}},
		{name: "valid 50", annotations: map[string]string{constants.TargetUtilizationPercentage: "50"}},
		{name: "valid 1", annotations: map[string]string{constants.TargetUtilizationPercentage: "1"}},
		{name: "valid 100", annotations: map[string]string{constants.TargetUtilizationPercentage: "100"}},
		{name: "zero", annotations: map[string]string{constants.TargetUtilizationPercentage: "0"}, wantErr: true},
		{name: "101", annotations: map[string]string{constants.TargetUtilizationPercentage: "101"}, wantErr: true},
		{name: "not a number", annotations: map[string]string{constants.TargetUtilizationPercentage: "abc"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
			}
			err := ValidateAutoscalerTargetUtilizationPercentage(isvc)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
