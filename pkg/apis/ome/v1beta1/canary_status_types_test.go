package v1beta1

import "testing"

func TestGetCanary(t *testing.T) {
	var nilSpec *InferenceServiceSpec
	if nilSpec.GetCanary() != nil {
		t.Fatal("nil spec must return nil canary")
	}
	s := &InferenceServiceSpec{}
	if s.GetCanary() != nil {
		t.Fatal("no rollout must return nil canary")
	}
	s.Rollout = &RolloutSpec{Canary: &CanarySpec{}}
	if s.GetCanary() == nil {
		t.Fatal("set canary must be returned")
	}
}
