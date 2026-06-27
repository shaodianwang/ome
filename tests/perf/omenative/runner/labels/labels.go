// Package labels holds the label keys the OMENative perf harness stamps
// on generated InferenceServices and reads back off their produced child
// resources (InferenceReplicas, pods). Kept in its own leaf package so
// the generator, observer, and recorder can share them without an import
// cycle.
//
// These are harness-internal coordination labels, not OME API contract —
// they live under a perf-specific subdomain so they can never collide
// with a real ome.io/* label the controller cares about.
package labels

const (
	// ClassLabel carries the size-class name (single-small / gang-... )
	// the generator assigned to an InferenceService. The observer reads it
	// off the InferenceReplica — the generator stamps it onto the ISVC and
	// the IR projector copies the ISVC's labels onto the IR — to attribute
	// each readiness event to a class so the recorder can bucket
	// time-to-Ready.
	ClassLabel = "perf.ome.io/class"

	// HarnessLabel marks every object the generator creates so cleanup
	// and listing can target exactly the harness's objects. Value is
	// always HarnessLabelValue.
	HarnessLabel      = "perf.ome.io/harness"
	HarnessLabelValue = "omenative-scale"
)
