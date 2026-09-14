//go:build meta_internal

// Package capacityformat registers deployment-specific capacity response
// formats with the routing controller.
//
// Everything here is optional. The routing controller ships one built-in
// format; this package adds another for a router we do not control and cannot
// change to emit the built-in shape. Nothing in the repository imports it
// except a build-tagged file in the manager's composition root, so removing
// this directory leaves a working build that has simply lost the format.
package capacityformat

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
)

// FormatLoadArray reads a two-element JSON array of floats: a damped reading
// followed by an undamped one.
//
// Prefer routing.FormatReport for any producer you control. This format is a
// compatibility shim, and it is strictly weaker in two ways that matter:
//
//   - It carries no timestamp, so the reading is stamped at receipt and MaxAge
//     can never fire. A home wedged while still answering keeps its ceiling
//     indefinitely.
//   - Where it counts only prefill-side accelerators it never observes decode,
//     so it reports full capacity during exactly the prefill/decode rollout
//     skew the capacity input was justified by. It covers a different failure:
//     accelerators that are unhealthy or ejected.
const FormatLoadArray routing.CapacityFormat = "LoadArray"

// optUnitsPerReplica converts a reading in weighted accelerators into servable
// replicas. Required: the two are different quantities, and comparing a
// weighted accelerator count against a replica plan would produce a
// meaningless ceiling.
const optUnitsPerReplica = "unitsPerReplica"

func init() {
	if err := routing.RegisterCapacityFormat(FormatLoadArray, routing.CapacityFormatPlugin{
		Decode:   decode,
		Validate: validate,
		// The producer sends no time of its own.
		StampsOwnTime: false,
	}); err != nil {
		panic(fmt.Sprintf("registering the %s capacity format: %v", FormatLoadArray, err))
	}
}

// validate requires the unit conversion and rejects anything else, so a typo in
// an option name fails at startup instead of being silently ignored.
func validate(cfg routing.CapacityConfig) error {
	for k := range cfg.Options {
		if k != optUnitsPerReplica {
			return fmt.Errorf("routing.capacity.options[%q] is not accepted for format %q", k, FormatLoadArray)
		}
	}
	raw, ok := cfg.Options[optUnitsPerReplica]
	if !ok {
		return fmt.Errorf("routing.capacity.options[%q] is required for format %q", optUnitsPerReplica, FormatLoadArray)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fmt.Errorf("routing.capacity.options[%q]: %q is not a number", optUnitsPerReplica, raw)
	}
	if v <= 0 || math.IsInf(v, 0) {
		return fmt.Errorf("routing.capacity.options[%q] must be a positive finite number, got %v", optUnitsPerReplica, v)
	}
	return nil
}

// decode reads the undamped element and converts it into servable replicas.
//
// Only the second element is read. The first divides capacity by an autoscaling
// pressure score, which measures how loaded a home is rather than how much it
// can serve. As a ceiling that would invert the intent: a busy home would
// report less capacity, lose traffic share, become less busy, and regain it —
// an oscillation against the home's own autoscaler, and exactly the reactive
// drift capacity planning exists to remove.
func decode(body []byte, cfg routing.CapacityConfig) (routing.CapacityReport, string) {
	var pair []float64
	if err := json.Unmarshal(body, &pair); err != nil {
		return routing.CapacityReport{}, fmt.Sprintf("capacity response is not a load array: %v", err)
	}
	if len(pair) != 2 {
		// A different arity is a different contract; guessing which element is
		// undamped would be a coin flip on the whole home's traffic.
		return routing.CapacityReport{}, fmt.Sprintf("capacity load array has %d elements, want 2", len(pair))
	}
	damped, undamped := pair[0], pair[1]
	if math.IsNaN(undamped) || math.IsInf(undamped, 0) {
		return routing.CapacityReport{}, fmt.Sprintf("capacity load array undamped value is not finite (%v)", undamped)
	}
	if undamped < 0 {
		return routing.CapacityReport{}, fmt.Sprintf("capacity load array undamped value is negative (%v)", undamped)
	}
	// The damped element is the undamped one divided by a pressure score whose
	// clamp floor is 1, so damped <= undamped is an invariant of the format. A
	// body violating it is not the array we think it is — reversed, or a
	// different contract that happens to hold two numbers — and reading an
	// element from it would yield a plausible but wrong ceiling rather than an
	// error.
	if !math.IsNaN(damped) && damped > undamped {
		return routing.CapacityReport{}, fmt.Sprintf(
			"capacity load array is not [damped, undamped]: first element %v exceeds second %v", damped, undamped)
	}

	// Validate guarantees this parses and is positive.
	unitsPerReplica, _ := strconv.ParseFloat(cfg.Options[optUnitsPerReplica], 64)

	// Floor rather than round: the value becomes a ceiling, so the error has to
	// land on the side of claiming less than the home can serve. Rounding 6.4 up
	// to 7 would let a home carry traffic for capacity nobody reported.
	servable := math.Floor(undamped / unitsPerReplica)
	if servable > math.MaxInt32 {
		// Refuse rather than wrap: a wrapped ceiling could read as 0 and
		// black-hole a healthy home.
		return routing.CapacityReport{}, fmt.Sprintf("capacity load array yields %v servable replicas, beyond int32", servable)
	}
	// ObservedAt is left zero on purpose. The poller stamps it at receipt, which
	// is the honest representation of what this format can tell us about time.
	return routing.CapacityReport{Servable: int32(servable)}, ""
}
