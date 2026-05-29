package keda

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

func TestCalculateMinMaxReplicas(t *testing.T) {
	testCases := []struct {
		name               string
		componentExt       *v1beta1.ComponentExtensionSpec
		expectedMinReplica int32
		expectedMaxReplica int32
	}{
		{
			name: "Default min/max replicas",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: nil,
				MaxReplicas: 0,
			},
			expectedMinReplica: 1,
			expectedMaxReplica: 1,
		},
		{
			name: "Custom min/max replicas",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: intPtr(2),
				MaxReplicas: 10,
			},
			expectedMinReplica: 2,
			expectedMaxReplica: 10,
		},
		{
			name: "Max replicas less than min - should use min",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: intPtr(5),
				MaxReplicas: 3,
			},
			expectedMinReplica: 5,
			expectedMaxReplica: 5,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			minReplicas := calculateMinReplicas(tt.componentExt)
			maxReplicas := calculateMaxReplicas(tt.componentExt, minReplicas)

			if minReplicas != tt.expectedMinReplica {
				t.Errorf("Expected minReplicas %d, got %d", tt.expectedMinReplica, minReplicas)
			}
			if maxReplicas != tt.expectedMaxReplica {
				t.Errorf("Expected maxReplicas %d, got %d", tt.expectedMaxReplica, maxReplicas)
			}
		})
	}
}

// TestCreateScaledObject_IRScaleTarget verifies the OEP-0013 Slice 3 wiring:
// when the caller passes an InferenceReplica ScaleTarget and a KedaAutoscaler
// with a single trigger, the generator emits a ScaledObject targeting the IR
// with the trigger forwarded verbatim and the default min/max bounds applied.
func TestCreateScaledObject_IRScaleTarget(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "foo-engine",
		Namespace: "ns",
	}
	scaleTargetRef := kedav1.ScaleTarget{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	kedaSpec := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{
				Type: "prometheus",
				Metadata: map[string]string{
					"serverAddress": "http://prom:9090",
					"metricName":    "tps",
					"query":         "sum(rate(http_requests_total[1m]))",
					"threshold":     "10",
				},
			},
		},
	}

	got := createScaledObject(componentMeta, scaleTargetRef, kedaSpec, 1, 5)

	if got.Name != utils.GetScaledObjectName("foo-engine") {
		t.Errorf("Name mismatch: want %s, got %s", utils.GetScaledObjectName("foo-engine"), got.Name)
	}
	if got.Namespace != "ns" {
		t.Errorf("Namespace mismatch: want ns, got %s", got.Namespace)
	}
	if got.Spec.ScaleTargetRef == nil {
		t.Fatal("Expected ScaleTargetRef to be set, got nil")
	}
	if diff := cmp.Diff(scaleTargetRef, *got.Spec.ScaleTargetRef); diff != "" {
		t.Errorf("ScaleTargetRef mismatch (-want +got): %s", diff)
	}
	if got.Spec.MinReplicaCount == nil || *got.Spec.MinReplicaCount != 1 {
		t.Errorf("MinReplicaCount: want 1, got %v", got.Spec.MinReplicaCount)
	}
	if got.Spec.MaxReplicaCount == nil || *got.Spec.MaxReplicaCount != 5 {
		t.Errorf("MaxReplicaCount: want 5, got %v", got.Spec.MaxReplicaCount)
	}
	if diff := cmp.Diff(kedaSpec.Triggers, got.Spec.Triggers); diff != "" {
		t.Errorf("Triggers not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateScaledObject_AdvancedPassThrough pins the OEP-0013 README 678-706
// verbatim contract for the KEDA Advanced block: the entire AdvancedConfig
// struct (HorizontalPodAutoscalerConfig, RestoreToOriginalReplicaCount) must
// land on the ScaledObject without any field-level filtering.
func TestCreateScaledObject_AdvancedPassThrough(t *testing.T) {
	componentMeta := metav1.ObjectMeta{Name: "foo-engine", Namespace: "ns"}
	scaleTargetRef := kedav1.ScaleTarget{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	restoreOriginal := true
	advanced := &kedav1.AdvancedConfig{
		HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{
			Name: "custom-hpa-name",
		},
		RestoreToOriginalReplicaCount: restoreOriginal,
	}
	kedaSpec := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{Type: "cpu", Metadata: map[string]string{"value": "50"}},
		},
		Advanced: advanced,
	}

	got := createScaledObject(componentMeta, scaleTargetRef, kedaSpec, 1, 3)

	if diff := cmp.Diff(advanced, got.Spec.Advanced); diff != "" {
		t.Errorf("Advanced not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateScaledObject_PollingCooldownFallback pins the OEP-0013 README
// 678-706 verbatim contract for the scalar pass-through fields:
// PollingInterval, CooldownPeriod, IdleReplicaCount, Fallback. Each must
// land on the ScaledObject as the same pointer/struct.
func TestCreateScaledObject_PollingCooldownFallback(t *testing.T) {
	componentMeta := metav1.ObjectMeta{Name: "foo-engine", Namespace: "ns"}
	scaleTargetRef := kedav1.ScaleTarget{
		APIVersion: "ome.io/v1beta1",
		Kind:       "InferenceReplica",
		Name:       "foo-engine",
	}
	polling := int32(15)
	cooldown := int32(120)
	idle := int32(0)
	fallback := &kedav1.Fallback{
		FailureThreshold: 3,
		Replicas:         2,
	}
	kedaSpec := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{Type: "cpu", Metadata: map[string]string{"value": "50"}},
		},
		PollingInterval:  &polling,
		CooldownPeriod:   &cooldown,
		IdleReplicaCount: &idle,
		Fallback:         fallback,
	}

	got := createScaledObject(componentMeta, scaleTargetRef, kedaSpec, 1, 3)

	if got.Spec.PollingInterval == nil || *got.Spec.PollingInterval != polling {
		t.Errorf("PollingInterval: want %d, got %v", polling, got.Spec.PollingInterval)
	}
	if got.Spec.CooldownPeriod == nil || *got.Spec.CooldownPeriod != cooldown {
		t.Errorf("CooldownPeriod: want %d, got %v", cooldown, got.Spec.CooldownPeriod)
	}
	if got.Spec.IdleReplicaCount == nil || *got.Spec.IdleReplicaCount != idle {
		t.Errorf("IdleReplicaCount: want %d, got %v", idle, got.Spec.IdleReplicaCount)
	}
	if diff := cmp.Diff(fallback, got.Spec.Fallback); diff != "" {
		t.Errorf("Fallback not passed verbatim (-want +got): %s", diff)
	}
}

// TestCreateScaledObject_LegacyShim_Identical pins the byte-identical
// contract: the shim must emit the same ScaledObject as the pre-refactor
// createScaledObject for the Raw Deployment dispatch path. The expected
// object is hand-rolled to encode the legacy generator's exact output for a
// componentExt that supplies an Autoscaler.Keda block, including the
// `k8slens-edit-resource-version` label filtering.
func TestCreateScaledObject_LegacyShim_Identical(t *testing.T) {
	componentMeta := metav1.ObjectMeta{
		Name:      "raw-engine",
		Namespace: "ns",
		Labels: map[string]string{
			"app":                           "raw-engine",
			"k8slens-edit-resource-version": "skip-me",
		},
		Annotations: map[string]string{"foo": "bar"},
	}
	polling := int32(30)
	keda := &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{
				Type: "prometheus",
				Metadata: map[string]string{
					"serverAddress": "http://prom:9090",
					"metricName":    "tps",
					"query":         "sum(rate(http_requests_total[1m]))",
					"threshold":     "10",
				},
			},
		},
		PollingInterval: &polling,
	}
	componentExt := &v1beta1.ComponentExtensionSpec{
		MinReplicas: intPtr(2),
		MaxReplicas: 6,
		Autoscaler: &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerKEDA,
			Keda:  keda,
		},
	}

	minReplicas := int32(2)
	maxReplicas := int32(6)
	expected := &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:        utils.GetScaledObjectName("raw-engine"),
			Namespace:   "ns",
			Labels:      map[string]string{"app": "raw-engine"},
			Annotations: map[string]string{"foo": "bar"},
		},
		Spec: kedav1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1.ScaleTarget{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "raw-engine",
			},
			MinReplicaCount: &minReplicas,
			MaxReplicaCount: &maxReplicas,
			Triggers:        keda.Triggers,
			PollingInterval: &polling,
		},
	}

	got := createScaledObjectFromLegacy(componentMeta, componentExt)

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Errorf("Legacy shim is not byte-identical (-want +got): %s", diff)
	}
}

func intPtr(i int) *int {
	return &i
}
