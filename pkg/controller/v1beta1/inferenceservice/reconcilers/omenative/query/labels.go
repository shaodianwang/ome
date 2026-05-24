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
	ManagedByOMENative       = "OMENative"
)

// ServingConditionType is the controller-owned readiness gate
// OMENative flips during drain, update, and migration. Render appends
// it to every Pod's ReadinessGates; the op layer toggles the
// corresponding PodCondition to gate EndpointSlice readiness.
const ServingConditionType corev1.PodConditionType = "ome.io/serving"
