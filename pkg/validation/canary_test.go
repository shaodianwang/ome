package validation

import (
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func canarySpec(steps ...v1beta1.RolloutStep) *v1beta1.InferenceServiceSpec {
	return &v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Canary: &v1beta1.CanarySpec{Steps: steps}}}
}

// canarySpecMode builds a canary spec with an engine component at the given
// deployment mode, for the OMENative-gate tests.
func canarySpecMode(mode constants.DeploymentModeType, steps ...v1beta1.RolloutStep) *v1beta1.InferenceServiceSpec {
	s := canarySpec(steps...)
	s.DeploymentMode = &mode
	s.Engine = &v1beta1.EngineSpec{}
	return s
}

func ptrPolicy(p v1beta1.PromotionPolicy) *v1beta1.PromotionPolicy { return &p }

func TestValidateCanary_OK(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("10%"), TrafficWeight: 10},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("valid canary rejected: %v", err)
	}
}

func TestValidateCanary_Empty(t *testing.T) {
	if err := ValidateCanary(canarySpec()); err == nil {
		t.Fatal("empty steps must be rejected")
	}
}

func TestValidateCanary_FinalWeightNot100(t *testing.T) {
	spec := canarySpec(v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 50})
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("final step trafficWeight must be 100")
	}
}

func TestValidateCanary_TrafficToZeroCapacity(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutStep{NewCapacity: intstr.FromInt(0), TrafficWeight: 10},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("trafficWeight>0 with zero NewCapacity must be rejected")
	}
}

func TestValidateCanary_TrafficToZeroPercentCapacity(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("0%"), TrafficWeight: 10},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal(`trafficWeight>0 with "0%" NewCapacity must be rejected`)
	}
}

func TestValidateCanary_MutexWithCoordination(t *testing.T) {
	spec := canarySpec(v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100})
	spec.Rollout.Coordination = &v1beta1.RolloutCoordinationSpec{}
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), "coordination") {
		t.Fatalf("canary + coordination must be rejected, got: %v", err)
	}
}

func TestValidateCanary_Unset(t *testing.T) {
	if err := ValidateCanary(&v1beta1.InferenceServiceSpec{}); err != nil {
		t.Fatalf("unset canary must pass: %v", err)
	}
	var nilSpec *v1beta1.InferenceServiceSpec
	if err := ValidateCanary(nilSpec); err != nil {
		t.Fatalf("nil spec must pass: %v", err)
	}
}

func TestValidateCanary_RequiresOMENative(t *testing.T) {
	final := v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100}
	// Non-OMENative component → rejected.
	spec := canarySpecMode(constants.RawDeployment, final)
	err := ValidateCanary(spec)
	if err == nil || !strings.Contains(err.Error(), ReasonCanaryRequiresOMENative) {
		t.Fatalf("canary on RawDeployment must be rejected, got: %v", err)
	}
	// OMENative component → accepted.
	if err := ValidateCanary(canarySpecMode(constants.OMENative, final)); err != nil {
		t.Fatalf("canary on OMENative must pass: %v", err)
	}
	// Unset mode (empty) → rejected (canary needs explicit OMENative).
	if err := ValidateCanary(canarySpecMode("", final)); err == nil {
		t.Fatal("canary with unset deploymentMode must be rejected")
	}
}

func TestValidateCanary_DecreasingWeight(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("60%"), TrafficWeight: 30},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("decreasing trafficWeight must be rejected")
	}
}

func TestValidateCanary_PolicyRequiredOnPause(t *testing.T) {
	spec := canarySpec(
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50, Pause: &v1beta1.RolloutPause{}},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("a paused step without promotionPolicy must be rejected")
	}
	// With a policy set, the same plan is valid.
	spec.Rollout.Canary.PromotionPolicy = ptrPolicy(v1beta1.PromotionPolicyAuto)
	if err := ValidateCanary(spec); err != nil {
		t.Fatalf("paused step with promotionPolicy must pass: %v", err)
	}
}

func TestValidateCanary_OldCapacitySurgeRejected(t *testing.T) {
	old100 := intstr.FromString("100%")
	// Blue-green "validate at full scale" (100%/100%) implies surge → rejected.
	spec := canarySpec(
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), OldCapacity: &old100, TrafficWeight: 0},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	if err := ValidateCanary(spec); err == nil {
		t.Fatal("oldCapacity implying surge above desired must be rejected")
	}
	// The complement (10%/90%) is accepted.
	old90 := intstr.FromString("90%")
	ok := canarySpec(
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("10%"), OldCapacity: &old90, TrafficWeight: 10},
		v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
	)
	if err := ValidateCanary(ok); err != nil {
		t.Fatalf("complement oldCapacity must pass: %v", err)
	}
}
