// Package labels holds the label keys the harness stamps on generated
// InferenceServices and reads back off their produced child resources.
// Kept in its own leaf package so the generator, executor, and recorder
// can share them without an import cycle.
//
// These are harness-internal coordination labels, not OME API contract —
// they live under a perf-specific subdomain so they can never collide
// with a real ome.io/* label the controller cares about.
package labels

const (
	// ClassLabel carries the size-class name (small/medium/large/...) the
	// generator assigned to an InferenceService. The observer reads it off the
	// ISVC to attribute each readiness event to a class so the recorder can
	// bucket time-to-Ready. (The simulated ready delay is carried separately by
	// ReadyAfterMsLabel, which the executor reads off the Deployment.)
	ClassLabel = "perf.ome.io/class"

	// ReadyAfterMsLabel carries the per-InferenceService simulated
	// ready-delay (milliseconds, as a decimal string) so the executor is
	// self-contained: it needs only the resource it is reconciling, not a
	// side lookup into the run config. Stamped on the InferenceService.
	ReadyAfterMsLabel = "perf.ome.io/ready-after-ms"

	// HarnessLabel marks every object the generator creates so cleanup
	// and listing can target exactly the harness's objects. Value is
	// always HarnessLabelValue.
	HarnessLabel      = "perf.ome.io/harness"
	HarnessLabelValue = "isvc-scale"
)
