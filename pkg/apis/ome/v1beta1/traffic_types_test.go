package v1beta1

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestTrafficSpec_JSONRoundTrip(t *testing.T) {
	ttlSecs := int64(3600)
	header := LoadBalancingType("ConsistentHash")
	tests := []struct {
		name string
		in   TrafficSpec
	}{
		{
			name: "empty",
			in:   TrafficSpec{},
		},
		{
			name: "algorithm only",
			in: TrafficSpec{
				Algorithm: ptrLBType(LoadBalancingTypeLeastRequest),
			},
		},
		{
			name: "consistent hash on header",
			in: TrafficSpec{
				Algorithm: &header,
				ConsistentHash: &ConsistentHashSpec{
					Type: HashTypeHeader,
					Headers: []HashHeader{
						{Name: "X-Session-ID"},
					},
				},
			},
		},
		{
			name: "consistent hash on cookie with TTL",
			in: TrafficSpec{
				Algorithm: &header,
				ConsistentHash: &ConsistentHashSpec{
					Type:   HashTypeCookie,
					Cookie: &HashCookie{Name: "ome-session", TTLSeconds: &ttlSecs},
				},
			},
		},
		{
			name: "consistent hash on source IP",
			in: TrafficSpec{
				Algorithm:      &header,
				ConsistentHash: &ConsistentHashSpec{Type: HashTypeSourceIP},
			},
		},
		{
			name: "endpoint override on header",
			in: TrafficSpec{
				Algorithm: ptrLBType(LoadBalancingTypeRoundRobin),
				EndpointOverride: &EndpointOverrideSpec{
					Type:    EndpointOverrideTypeHeader,
					Headers: []HashHeader{{Name: "X-Endpoint-HostPort"}},
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
			var out TrafficSpec
			if err := json.Unmarshal(raw, &out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(tc.in, out) {
				t.Fatalf("round-trip mismatch\nin:  %#v\nout: %#v\nraw: %s", tc.in, out, string(raw))
			}
		})
	}
}

func TestTrafficSpec_OmitemptyShape(t *testing.T) {
	// An empty TrafficSpec should serialize to "{}", and a nil pointer
	// should serialize to "null" in its parent — verifying the omitempty
	// + pointer wiring is correct so older clients see nothing on ISVCs
	// that don't opt in.
	empty := TrafficSpec{}
	raw, err := json.Marshal(empty)
	if err != nil {
		t.Fatalf("Marshal empty: %v", err)
	}
	if string(raw) != `{}` {
		t.Fatalf("expected empty TrafficSpec to serialize to {}, got %s", string(raw))
	}

	type holder struct {
		Traffic *TrafficSpec `json:"traffic,omitempty"`
	}
	nilHolder := holder{}
	rawNil, err := json.Marshal(nilHolder)
	if err != nil {
		t.Fatalf("Marshal nil holder: %v", err)
	}
	if string(rawNil) != `{}` {
		t.Fatalf("expected nil Traffic to be omitted, got %s", string(rawNil))
	}
}

func TestLoadBalancingType_Constants(t *testing.T) {
	// Constants must match the strings in the kubebuilder enum tag on
	// TrafficSpec.Algorithm. If these drift, CRD validation will reject
	// values the controller accepts (or vice versa).
	cases := map[LoadBalancingType]string{
		LoadBalancingTypeRoundRobin:     "RoundRobin",
		LoadBalancingTypeLeastRequest:   "LeastRequest",
		LoadBalancingTypeRandom:         "Random",
		LoadBalancingTypeConsistentHash: "ConsistentHash",
	}
	for typ, want := range cases {
		if string(typ) != want {
			t.Errorf("LoadBalancingType %q != %q", string(typ), want)
		}
	}
}

func TestHashType_Constants(t *testing.T) {
	cases := map[HashType]string{
		HashTypeHeader:   "Header",
		HashTypeCookie:   "Cookie",
		HashTypeSourceIP: "SourceIP",
	}
	for typ, want := range cases {
		if string(typ) != want {
			t.Errorf("HashType %q != %q", string(typ), want)
		}
	}
}

func TestEndpointOverrideType_Constants(t *testing.T) {
	cases := map[EndpointOverrideType]string{
		EndpointOverrideTypeHeader:   "Header",
		EndpointOverrideTypeMetadata: "Metadata",
	}
	for typ, want := range cases {
		if string(typ) != want {
			t.Errorf("EndpointOverrideType %q != %q", string(typ), want)
		}
	}
}

func TestTrafficSpec_DeepCopy(t *testing.T) {
	// The generated DeepCopy must produce a value-equal but
	// independently-mutable clone. Modifying the clone must not affect
	// the original.
	ttlSecs := int64(3600)
	alg := LoadBalancingTypeConsistentHash
	original := &TrafficSpec{
		Algorithm: &alg,
		ConsistentHash: &ConsistentHashSpec{
			Type:    HashTypeHeader,
			Headers: []HashHeader{{Name: "X-Session-ID"}, {Name: "X-Tenant"}},
			Cookie:  &HashCookie{Name: "should-not-appear", TTLSeconds: &ttlSecs},
		},
		EndpointOverride: &EndpointOverrideSpec{
			Type:    EndpointOverrideTypeHeader,
			Headers: []HashHeader{{Name: "X-Endpoint-HostPort"}},
		},
	}
	clone := original.DeepCopy()
	if !reflect.DeepEqual(original, clone) {
		t.Fatalf("DeepCopy not equal:\norig:  %#v\nclone: %#v", original, clone)
	}

	// Mutate the clone — original must be unaffected.
	clone.Headers()[0] = HashHeader{Name: "X-Different"}
	if original.ConsistentHash.Headers[0].Name != "X-Session-ID" {
		t.Fatalf("DeepCopy did not deep-copy Headers slice: original mutated")
	}
}

// Headers is a tiny accessor used only by the DeepCopy test above so we
// don't need to assert on slice indexing in test code that might be
// shared.
func (t *TrafficSpec) Headers() []HashHeader {
	if t.ConsistentHash == nil {
		return nil
	}
	return t.ConsistentHash.Headers
}

func ptrLBType(t LoadBalancingType) *LoadBalancingType {
	return &t
}
