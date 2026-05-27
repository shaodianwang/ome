package v1beta1

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestRolloutSpec_JSONRoundTrip(t *testing.T) {
	scaleDown := int32(30)
	manual := PromotionPolicyManual
	auto := PromotionPolicyAuto
	pauseDuration := metav1.Duration{Duration: 10 * time.Minute}
	tests := []struct {
		name string
		in   RolloutSpec
	}{
		{
			name: "single step, manual, no scaleDownDelay",
			in: RolloutSpec{
				PromotionPolicy: &manual,
				Steps: []RolloutStep{
					{
						NewCapacity:   intstr.FromString("100%"),
						OldCapacity:   intStrPtr(intstr.FromString("0%")),
						TrafficWeight: 100,
					},
				},
			},
		},
		{
			name: "three step canary with capacity surge and pause",
			in: RolloutSpec{
				PromotionPolicy:       &manual,
				ScaleDownDelaySeconds: &scaleDown,
				Steps: []RolloutStep{
					{
						NewCapacity:   intstr.FromString("10%"),
						OldCapacity:   intStrPtr(intstr.FromString("100%")),
						TrafficWeight: 10,
						Pause:         &RolloutPause{},
					},
					{
						NewCapacity:   intstr.FromString("50%"),
						OldCapacity:   intStrPtr(intstr.FromString("50%")),
						TrafficWeight: 50,
						Pause:         &RolloutPause{Duration: &pauseDuration},
					},
					{
						NewCapacity:   intstr.FromString("100%"),
						OldCapacity:   intStrPtr(intstr.FromString("0%")),
						TrafficWeight: 100,
					},
				},
			},
		},
		{
			name: "blue-green two-step with auto promotion",
			in: RolloutSpec{
				PromotionPolicy: &auto,
				Steps: []RolloutStep{
					{
						// Validate at full capacity, no traffic.
						NewCapacity:   intstr.FromString("100%"),
						OldCapacity:   intStrPtr(intstr.FromString("100%")),
						TrafficWeight: 0,
						Pause:         &RolloutPause{Duration: &pauseDuration},
					},
					{
						// BlueGreen cutover.
						NewCapacity:   intstr.FromString("100%"),
						OldCapacity:   intStrPtr(intstr.FromString("0%")),
						TrafficWeight: 100,
					},
				},
			},
		},
		{
			name: "absolute capacity counts (not percentages)",
			in: RolloutSpec{
				Steps: []RolloutStep{
					{
						NewCapacity:   intstr.FromInt(1),
						TrafficWeight: 0,
					},
					{
						NewCapacity:   intstr.FromInt(3),
						TrafficWeight: 100,
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var out RolloutSpec
			if err := json.Unmarshal(raw, &out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(tc.in, out) {
				t.Fatalf("round-trip mismatch\nin:  %#v\nout: %#v\nraw: %s", tc.in, out, string(raw))
			}
		})
	}
}

func TestPromotionPolicy_Constants(t *testing.T) {
	cases := map[PromotionPolicy]string{
		PromotionPolicyManual: "Manual",
		PromotionPolicyAuto:   "Auto",
	}
	for typ, want := range cases {
		if string(typ) != want {
			t.Errorf("PromotionPolicy %q != %q", string(typ), want)
		}
	}
}

func TestTrafficSpec_WithRollout_RoundTrip(t *testing.T) {
	// The Rollout sub-block in TrafficSpec must round-trip cleanly so
	// operators authoring a step-based plan see what they wrote back.
	alg := LoadBalancingTypeConsistentHash
	scaleDown := int32(30)
	manual := PromotionPolicyManual
	in := TrafficSpec{
		Algorithm: &alg,
		ConsistentHash: &ConsistentHashSpec{
			Type:    HashTypeHeader,
			Headers: []HashHeader{{Name: "X-Session-ID"}},
		},
		Rollout: &RolloutSpec{
			PromotionPolicy:       &manual,
			ScaleDownDelaySeconds: &scaleDown,
			Steps: []RolloutStep{
				{
					NewCapacity:   intstr.FromString("10%"),
					OldCapacity:   intStrPtr(intstr.FromString("100%")),
					TrafficWeight: 10,
					Pause:         &RolloutPause{},
				},
				{
					NewCapacity:   intstr.FromString("100%"),
					OldCapacity:   intStrPtr(intstr.FromString("0%")),
					TrafficWeight: 100,
				},
			},
		},
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out TrafficSpec
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch\nin:  %#v\nout: %#v\nraw: %s", in, out, string(raw))
	}
}

func TestRolloutSpec_DeepCopy(t *testing.T) {
	pauseDuration := metav1.Duration{Duration: 5 * time.Minute}
	scaleDown := int32(30)
	manual := PromotionPolicyManual
	original := &RolloutSpec{
		PromotionPolicy:       &manual,
		ScaleDownDelaySeconds: &scaleDown,
		Steps: []RolloutStep{
			{
				NewCapacity:   intstr.FromString("25%"),
				OldCapacity:   intStrPtr(intstr.FromString("75%")),
				TrafficWeight: 25,
				Pause:         &RolloutPause{Duration: &pauseDuration},
			},
			{
				NewCapacity:   intstr.FromString("100%"),
				OldCapacity:   intStrPtr(intstr.FromString("0%")),
				TrafficWeight: 100,
			},
		},
	}
	clone := original.DeepCopy()
	if !reflect.DeepEqual(original, clone) {
		t.Fatalf("DeepCopy not equal:\norig:  %#v\nclone: %#v", original, clone)
	}

	// Mutate the clone's step list — original must be unaffected.
	clone.Steps[0].TrafficWeight = 99
	if original.Steps[0].TrafficWeight != 25 {
		t.Fatalf("DeepCopy did not deep-copy Steps slice: original mutated")
	}

	// Mutate the clone's Pause duration — original must be unaffected.
	*clone.Steps[0].Pause.Duration = metav1.Duration{Duration: time.Hour}
	if original.Steps[0].Pause.Duration.Duration != 5*time.Minute {
		t.Fatalf("DeepCopy did not deep-copy Pause.Duration: original mutated")
	}
}

func intStrPtr(v intstr.IntOrString) *intstr.IntOrString {
	return &v
}
