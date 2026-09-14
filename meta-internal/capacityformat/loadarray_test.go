//go:build meta_internal

package capacityformat

import (
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
)

func cfg(unitsPerReplica string) routing.CapacityConfig {
	c := routing.CapacityConfig{Format: FormatLoadArray}
	if unitsPerReplica != "" {
		c.Options = map[string]string{optUnitsPerReplica: unitsPerReplica}
	}
	return c
}

func TestDecode_ReadsTheUndampedElement(t *testing.T) {
	// The damped element divides capacity by an autoscaling pressure score --
	// how loaded the home is, not how much it can serve. As a ceiling that
	// would invert the intent: a busy home would lose share, become less busy,
	// and regain it, oscillating against its own autoscaler.
	got, reason := decode([]byte("[3.2, 6.4]"), cfg("1"))
	if reason != "" {
		t.Fatalf("rejected: %s", reason)
	}
	if got.Servable != 6 {
		t.Fatalf("servable = %d, want 6 (floor of the undamped 6.4, not the damped 3.2)", got.Servable)
	}
	// Left unstamped on purpose: the poller stamps at receipt, which is the
	// honest representation of what this format knows about time.
	if !got.ObservedAt.IsZero() {
		t.Fatalf("ObservedAt = %v, want zero so the poller stamps it", got.ObservedAt)
	}
}

func TestDecode_FloorsRatherThanRounds(t *testing.T) {
	// The value becomes a ceiling, so the rounding error has to land on the
	// side of claiming less: rounding 6.9 up to 7 would let a home carry
	// traffic for capacity nobody reported.
	for _, tc := range []struct {
		body string
		want int32
	}{
		{"[0, 6.9]", 6},
		{"[0, 6.4]", 6},
		{"[0, 1.0]", 1},
		{"[0, 0.9]", 0},
		{"[0, 0]", 0},
	} {
		got, reason := decode([]byte(tc.body), cfg("1"))
		if reason != "" {
			t.Fatalf("%s rejected: %s", tc.body, reason)
		}
		if got.Servable != tc.want {
			t.Fatalf("%s -> servable %d, want %d", tc.body, got.Servable, tc.want)
		}
	}
}

func TestDecode_ConvertsWeightedAcceleratorsToReplicas(t *testing.T) {
	// The format reports weighted accelerators; the plan counts replicas.
	// Without the conversion the ceiling would compare unlike units.
	got, reason := decode([]byte("[0, 6.4]"), cfg("4"))
	if reason != "" {
		t.Fatalf("rejected: %s", reason)
	}
	if got.Servable != 1 {
		t.Fatalf("servable = %d, want 1 (floor(6.4/4))", got.Servable)
	}
}

func TestDecode_RejectsAViolatedOrdering(t *testing.T) {
	// damped = undamped / clamp(score, 1, 5) and the clamp floor is 1, so
	// damped <= undamped is an invariant of the format. Violating it means the
	// body is not this contract, and reading an element would yield a plausible
	// but wrong ceiling rather than an error.
	if _, reason := decode([]byte("[6.4, 3.2]"), cfg("1")); reason == "" {
		t.Fatal("reversed array accepted; a wrong ceiling is worse than none")
	}
	// Equal elements are legal: a pressure score of exactly 1 leaves the value
	// undamped, so this must not be over-rejected.
	got, reason := decode([]byte("[6.4, 6.4]"), cfg("1"))
	if reason != "" {
		t.Fatalf("equal elements rejected: %s", reason)
	}
	if got.Servable != 6 {
		t.Fatalf("servable = %d, want 6", got.Servable)
	}
}

func TestDecode_FailsOpenOnBadShapes(t *testing.T) {
	for _, tt := range []struct{ name, body string }{
		{"object instead of array", `{"servable":3}`},
		{"one element", "[6.4]"},
		{"three elements", "[1.0, 6.4, 9.9]"},
		{"empty array", "[]"},
		{"negative undamped", "[0, -1]"},
		{"non-numeric", `["a","b"]`},
		{"not json", "6.4"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := decode([]byte(tt.body), cfg("1"))
			if reason == "" {
				t.Fatalf("body %q accepted as %d, want fail open with a reason", tt.body, got.Servable)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
		wantErr bool
	}{
		{"units supplied", map[string]string{optUnitsPerReplica: "4"}, false},
		// Without it the reading is weighted accelerators while the plan is
		// replicas, so the ceiling would be meaningless.
		{"missing units", nil, true},
		{"non-numeric units", map[string]string{optUnitsPerReplica: "four"}, true},
		{"zero units", map[string]string{optUnitsPerReplica: "0"}, true},
		{"negative units", map[string]string{optUnitsPerReplica: "-2"}, true},
		// A typo in an option name must fail at startup, not be ignored.
		{"unknown option", map[string]string{"unitsPerReplicas": "4"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(routing.CapacityConfig{Format: FormatLoadArray, Options: tt.options})
			if tt.wantErr && err == nil {
				t.Fatalf("validate(%v) = nil, want an error", tt.options)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validate(%v) = %v, want nil", tt.options, err)
			}
		})
	}
}

func TestInit_RegistersWithTheRoutingController(t *testing.T) {
	// Importing this package is the whole integration surface, so assert the
	// registration actually happened rather than trusting the init().
	var found bool
	for _, n := range routing.RegisteredCapacityFormats() {
		if n == string(FormatLoadArray) {
			found = true
		}
	}
	if !found {
		t.Fatalf("format %q not registered; got %v", FormatLoadArray, routing.RegisteredCapacityFormats())
	}
}
