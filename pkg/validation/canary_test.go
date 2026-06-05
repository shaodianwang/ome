package validation

import (
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func canarySpec(steps ...v1beta1.RolloutStep) *v1beta1.InferenceServiceSpec {
	return &v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Canary: &v1beta1.CanarySpec{Steps: steps}}}
}

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
