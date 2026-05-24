package query

import corev1 "k8s.io/api/core/v1"

// Label keys and values OMENative stamps onto every managed Pod /
// Service / ControllerRevision / ConfigMap. Shared here so the
// dispatch code (top-level omenative), the ops/ subpackage, and the
// pod / EndpointSlice watch handlers all read the same constants.
const (
	LabelManagedBy           = "ome.io/managed-by"
	LabelInstanceIdx         = "ome.io/instance-index"
	LabelInstanceIncarnation = "ome.io/instance-incarnation"
	LabelRunner              = "ome.io/runner"
	LabelRevisionHash        = "ome.io/revision-hash"
	ManagedByOMENative       = "OMENative"
)

// RevisionHashFromControllerRevisionName extracts the trailing
// hash segment from a ControllerRevision name of the form
// `<isvc>-<component>-<hash>`. Returns "" when the name doesn't
// match the expected shape. Used by the pod renderer to stamp
// ome.io/revision-hash from a target *appsv1.ControllerRevision and
// by the coordination layer to recover the hash for env-var
// injection.
func RevisionHashFromControllerRevisionName(name string) string {
	for i := len(name) - 1; i > 0; i-- {
		if name[i] == '-' {
			return name[i+1:]
		}
	}
	return ""
}

// ServingConditionType is the controller-owned readiness gate
// OMENative flips during drain, update, and migration. Render appends
// it to every Pod's ReadinessGates; the op layer toggles the
// corresponding PodCondition to gate EndpointSlice readiness.
const ServingConditionType corev1.PodConditionType = "ome.io/serving"
