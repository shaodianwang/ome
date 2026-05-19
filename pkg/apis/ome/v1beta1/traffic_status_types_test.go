package v1beta1

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTrafficStatus_JSONRoundTrip(t *testing.T) {
	// LastTransitionTime is omitted from the fixtures because
	// metav1.Time round-trips through JSON in local timezone, which
	// does not reflect.DeepEqual with the constructed UTC value.
	// The structural-shape round-trip is what this test cares about.
	tests := []struct {
		name string
		in   TrafficStatus
	}{
		{
			name: "empty",
			in:   TrafficStatus{},
		},
		{
			name: "fully populated, accepted",
			in: TrafficStatus{
				Algorithm: "ConsistentHash",
				BackendPolicyResource: &BackendPolicyRef{
					APIVersion: "gateway.envoyproxy.io/v1alpha1",
					Kind:       "BackendTrafficPolicy",
					Name:       "llama-70b",
				},
				TargetedHTTPRoutes: []string{
					"llama-70b",
					"llama-70b-router",
					"llama-70b-decoder",
					"llama-70b-engine",
				},
				Conditions: []metav1.Condition{
					{
						Type:               TrafficConditionBackendPolicyReady,
						Status:             metav1.ConditionTrue,
						Reason:             TrafficReasonAcceptedByGateway,
						ObservedGeneration: 7,
					},
				},
			},
		},
		{
			name: "deferred to conflicting policy",
			in: TrafficStatus{
				Algorithm: "ConsistentHash",
				Conditions: []metav1.Condition{
					{
						Type:    TrafficConditionBackendPolicyReady,
						Status:  metav1.ConditionFalse,
						Reason:  TrafficReasonConflictingPolicy,
						Message: `BackendTrafficPolicy "llama-70b" already exists`,
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
			var out TrafficStatus
			if err := json.Unmarshal(raw, &out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(tc.in, out) {
				t.Fatalf("round-trip mismatch\nin:  %#v\nout: %#v\nraw: %s", tc.in, out, string(raw))
			}
		})
	}
}

func TestTrafficStatus_LastTransitionTime_Survives(t *testing.T) {
	// Separate test that just exercises the LastTransitionTime
	// serialization path without DeepEqual (timezone normalization
	// in metav1.Time makes DeepEqual unreliable).
	in := metav1.NewTime(time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC))
	cond := metav1.Condition{
		Type:               TrafficConditionBackendPolicyReady,
		Status:             metav1.ConditionTrue,
		Reason:             TrafficReasonAcceptedByGateway,
		LastTransitionTime: in,
	}
	raw, err := json.Marshal(cond)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out metav1.Condition
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !out.LastTransitionTime.Time.Equal(in.Time) {
		t.Fatalf("timestamp not preserved: in=%v out=%v", in.Time, out.LastTransitionTime.Time)
	}
}

func TestRolloutPhase_Constants(t *testing.T) {
	// Constants must match the strings in the kubebuilder enum tag on
	// ComponentStatusSpec.RolloutPhase. Drift causes CRD validation to
	// reject phases the controller writes.
	cases := map[RolloutPhase]string{
		RolloutPhaseStable:           "Stable",
		RolloutPhaseCanarying:        "Canarying",
		RolloutPhaseBlueGreenStandby: "BlueGreenStandby",
		RolloutPhasePending:          "Pending",
		RolloutPhasePaused:           "Paused",
		RolloutPhasePromoting:        "Promoting",
		RolloutPhaseRollingBack:      "RollingBack",
		RolloutPhaseFailed:           "Failed",
	}
	for typ, want := range cases {
		if string(typ) != want {
			t.Errorf("RolloutPhase %q != %q", string(typ), want)
		}
	}
}

func TestTrafficReason_Constants(t *testing.T) {
	cases := map[string]string{
		TrafficReasonAcceptedByGateway:     "AcceptedByGateway",
		TrafficReasonConflictingPolicy:     "ConflictingPolicy",
		TrafficReasonUnsupportedField:      "UnsupportedField",
		TrafficReasonNoTranslatorAvailable: "NoTranslatorAvailable",
		TrafficReasonGatewayRejected:       "GatewayRejected",
		TrafficReasonPending:               "Pending",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("traffic reason %q != %q", got, want)
		}
	}
}

func TestTrafficStatus_DeepCopy(t *testing.T) {
	original := &TrafficStatus{
		Algorithm: "ConsistentHash",
		BackendPolicyResource: &BackendPolicyRef{
			APIVersion: "gateway.envoyproxy.io/v1alpha1",
			Kind:       "BackendTrafficPolicy",
			Name:       "x",
		},
		TargetedHTTPRoutes: []string{"a", "b"},
		Conditions: []metav1.Condition{
			{
				Type:   TrafficConditionBackendPolicyReady,
				Status: metav1.ConditionTrue,
				Reason: TrafficReasonAcceptedByGateway,
			},
		},
	}
	clone := original.DeepCopy()
	if !reflect.DeepEqual(original, clone) {
		t.Fatalf("DeepCopy not equal:\norig:  %#v\nclone: %#v", original, clone)
	}

	// Mutate the clone — original must be unaffected.
	clone.TargetedHTTPRoutes[0] = "z"
	if original.TargetedHTTPRoutes[0] != "a" {
		t.Fatalf("DeepCopy did not deep-copy TargetedHTTPRoutes")
	}
	clone.Conditions[0].Reason = "Different"
	if original.Conditions[0].Reason != TrafficReasonAcceptedByGateway {
		t.Fatalf("DeepCopy did not deep-copy Conditions")
	}
}

func TestComponentStatusSpec_RolloutPhase_OmitemptyShape(t *testing.T) {
	// When RolloutPhase is the empty string, JSON must omit it so
	// existing clients reading ComponentStatusSpec see nothing new.
	zero := ComponentStatusSpec{}
	raw, err := json.Marshal(zero)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(raw) == `{"rolloutPhase":""}` {
		t.Fatalf("empty rolloutPhase should be omitted, got: %s", string(raw))
	}
	// Sanity: setting a phase produces it in JSON.
	withPhase := ComponentStatusSpec{RolloutPhase: RolloutPhaseCanarying}
	raw, err = json.Marshal(withPhase)
	if err != nil {
		t.Fatalf("Marshal with phase: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got["rolloutPhase"] != "Canarying" {
		t.Fatalf("expected rolloutPhase=Canarying in JSON, got: %v", got["rolloutPhase"])
	}
}
