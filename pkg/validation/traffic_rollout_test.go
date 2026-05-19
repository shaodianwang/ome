package validation

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestValidateRollout(t *testing.T) {
	manual := v1beta1.PromotionPolicyManual
	canary10 := int64(10)
	completeSteps := []v1beta1.RolloutStep{
		{
			NewCapacity:   intstr.FromString("10%"),
			OldCapacity:   ioStrPtr(intstr.FromString("100%")),
			TrafficWeight: 10,
		},
		{
			NewCapacity:   intstr.FromString("100%"),
			OldCapacity:   ioStrPtr(intstr.FromString("0%")),
			TrafficWeight: 100,
		},
	}
	tests := []struct {
		name         string
		spec         *v1beta1.InferenceServiceSpec
		wantOK       bool
		wantContains string
	}{
		{
			name:   "nil spec",
			spec:   nil,
			wantOK: true,
		},
		{
			name:   "no traffic",
			spec:   &v1beta1.InferenceServiceSpec{},
			wantOK: true,
		},
		{
			name: "no rollout block",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{},
			},
			wantOK: true,
		},
		{
			name: "complete two-step rollout — happy path",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						PromotionPolicy: &manual,
						Steps:           completeSteps,
					},
				},
			},
			wantOK: true,
		},
		{
			name: "final step trafficWeight != 100",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						Steps: []v1beta1.RolloutStep{
							{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50},
							{NewCapacity: intstr.FromString("75%"), TrafficWeight: 75},
						},
					},
				},
			},
			wantContains: "IncompleteRolloutPlan",
		},
		{
			name: "step capacity > 200%",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						Steps: []v1beta1.RolloutStep{
							{NewCapacity: intstr.FromString("300%"), TrafficWeight: 100},
						},
					},
				},
			},
			wantContains: "InvalidStepCapacity",
		},
		{
			name: "step capacity garbage string (missing %)",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						Steps: []v1beta1.RolloutStep{
							{NewCapacity: intstr.FromString("25"), TrafficWeight: 100},
						},
					},
				},
			},
			wantContains: "InvalidStepCapacity",
		},
		{
			name: "step int capacity negative",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						Steps: []v1beta1.RolloutStep{
							{NewCapacity: intstr.FromInt(-1), TrafficWeight: 100},
						},
					},
				},
			},
			wantContains: "InvalidStepCapacity",
		},
		{
			name: "step int capacity zero is OK (BlueGreenStandby uses 0% / 0 pods)",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						Steps: []v1beta1.RolloutStep{
							{NewCapacity: intstr.FromInt(0), TrafficWeight: 0},
							{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
						},
					},
				},
			},
			wantOK: true,
		},
		{
			name: "step oldCapacity string bad",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						Steps: []v1beta1.RolloutStep{
							{
								NewCapacity:   intstr.FromString("50%"),
								OldCapacity:   ioStrPtr(intstr.FromString("forty-percent")),
								TrafficWeight: 100,
							},
						},
					},
				},
			},
			wantContains: "InvalidStepCapacity",
		},
		{
			name: "mixed forms — rollout AND engine.canaryTrafficPercent",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						CanaryTrafficPercent: &canary10,
					},
				},
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{Steps: completeSteps},
				},
			},
			wantContains: "MixedRolloutForms",
		},
		{
			name: "mixed forms — rollout AND decoder.canaryTrafficPercent",
			spec: &v1beta1.InferenceServiceSpec{
				Decoder: &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						CanaryTrafficPercent: &canary10,
					},
				},
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{Steps: completeSteps},
				},
			},
			wantContains: "MixedRolloutForms",
		},
		{
			name: "mixed forms — rollout AND router.canaryTrafficPercent",
			spec: &v1beta1.InferenceServiceSpec{
				Router: &v1beta1.RouterSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						CanaryTrafficPercent: &canary10,
					},
				},
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{Steps: completeSteps},
				},
			},
			wantContains: "MixedRolloutForms",
		},
		{
			name: "only canaryTrafficPercent (no rollout) is OK",
			spec: &v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						CanaryTrafficPercent: &canary10,
					},
				},
			},
			wantOK: true,
		},
		{
			name: "empty steps is rejected (does not panic)",
			spec: &v1beta1.InferenceServiceSpec{
				Traffic: &v1beta1.TrafficSpec{
					Rollout: &v1beta1.RolloutSpec{
						PromotionPolicy: &manual,
						Steps:           []v1beta1.RolloutStep{},
					},
				},
			},
			wantContains: "EmptyRolloutSteps",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRollout(tc.spec)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected ok, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantContains)
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantContains, err)
			}
		})
	}
}

func ioStrPtr(v intstr.IntOrString) *intstr.IntOrString {
	return &v
}
