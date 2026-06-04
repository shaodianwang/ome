package coordination

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// GateContext caches the per-(ISVC, Component) prelude work shared by
// every Check*Gate: nil-ISVC and absent-coord-block short-circuits, the
// ResolveGroups + MembershipFor resolution, and the short-circuit
// reason string. Built once via ResolveGateContext and threaded into
// each gate.
//
// The dispatcher pays for the prelude exactly once per per-Instance
// tick instead of once per gate (4×: ratio + surge | unavailability +
// sequential). The existing Check*Gate public entry points are
// preserved as thin wrappers that build a fresh GateContext on every
// call — direct callers (and the large existing test surface) don't
// need to know about the helper.
type GateContext struct {
	// ISVC is the InferenceService the gate stack is reasoning about.
	// Nil when ResolveGateContext was called with a nil ISVC.
	ISVC *v1beta1.InferenceService

	// Component identifies the Component the gate stack is being asked
	// to allow / deny work for.
	Component v1beta1.ComponentType

	// Group is the resolved coordination group that owns Component. Zero
	// value when ShortCircuit is set.
	Group ResolvedGroup

	// ShortCircuit is true when the gate stack should bypass this
	// Component entirely (no coordination block declared, Component not
	// in any group, or the ISVC pointer was nil). Callers that observe
	// ShortCircuit=true should return (allowed=true, ShortReason)
	// without further gate work.
	ShortCircuit bool

	// ShortReason is the operator-facing explanation when ShortCircuit
	// is true. Empty otherwise.
	ShortReason string
}

// ResolveGateContext walks the shared prelude every Check*Gate runs:
//
//   - nil ISVC                → short-circuit with "nil ISVC"
//   - no coordination block   → short-circuit with "no coordination block"
//   - Component in no group   → short-circuit with "component not in any coord group"
//   - otherwise               → return the resolved group, ready for per-gate logic
//
// Returns a GateContext that subsequent CheckRatio / CheckSurge /
// CheckUnavailability / CheckSequential calls thread through. The
// dispatcher calls this once per per-Instance tick so the
// ResolveGroups + MembershipFor work doesn't run 4 times per dispatch.
func ResolveGateContext(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) GateContext {
	if isvc == nil {
		return GateContext{
			Component:    component,
			ShortCircuit: true,
			ShortReason:  "nil ISVC",
		}
	}
	coord := isvc.Spec.GetRolloutCoordination()
	if coord == nil || len(coord.Groups) == 0 {
		return GateContext{
			ISVC:         isvc,
			Component:    component,
			ShortCircuit: true,
			ShortReason:  "no coordination block",
		}
	}
	group, ok := MembershipFor(ResolveGroups(coord), component)
	if !ok {
		return GateContext{
			ISVC:         isvc,
			Component:    component,
			ShortCircuit: true,
			ShortReason:  "component not in any coord group",
		}
	}
	return GateContext{
		ISVC:      isvc,
		Component: component,
		Group:     group,
	}
}
