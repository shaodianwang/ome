package placement

import "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"

// AnyInstanceAdmitted reports whether any component of the (derived) ISVC has at
// least one Instance that has left the Kueue admission gate. Retained as a
// low-level predicate (and for single-component callers); the OEP-0018 race
// winner uses AllComponentsAdmitted, which is correct for multi-component (PD)
// services.
func AnyInstanceAdmitted(isvc *v1beta1.InferenceService) bool {
	for _, cs := range isvc.Status.Components {
		if componentHasAdmittedInstance(cs) {
			return true
		}
	}
	return false
}

// AllComponentsAdmitted reports whether EVERY component the derived ISVC declares
// in its spec has at least one admitted Instance. This is the OEP-0018 race
// signal: a cluster wins only once Kueue has admitted all of the service's
// components there. For a PD service (engine + decoder) this prevents declaring a
// winner while the engine is admitted but the decoder is still gated — which
// would otherwise delete the losers prematurely and strand a half-placed
// service. A service with no declared components is never a winner.
func AllComponentsAdmitted(isvc *v1beta1.InferenceService) bool {
	declared := declaredComponents(isvc)
	if len(declared) == 0 {
		return false
	}
	for _, comp := range declared {
		cs, ok := isvc.Status.Components[comp]
		if !ok || !componentHasAdmittedInstance(cs) {
			return false
		}
	}
	return true
}

// componentHasAdmittedInstance reports whether a component status has at least
// one Instance that has left the Kueue admission gate.
func componentHasAdmittedInstance(cs v1beta1.ComponentStatusSpec) bool {
	if cs.Lifecycle == nil {
		return false
	}
	for _, inst := range cs.Lifecycle.InstanceStatuses {
		if inst.Admitted {
			return true
		}
	}
	return false
}

// declaredComponents returns the components present on the ISVC spec.
func declaredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	if isvc.Spec.Engine != nil {
		out = append(out, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil {
		out = append(out, v1beta1.DecoderComponent)
	}
	if isvc.Spec.Router != nil {
		out = append(out, v1beta1.RouterComponent)
	}
	return out
}
