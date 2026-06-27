// Package labels holds the label keys the multi-cluster placement harness
// stamps on the source InferenceServices it generates. Kept in its own leaf
// package so the generator, admitter, and recorder can share them without an
// import cycle.
//
// These are harness-internal coordination labels, not OME API contract —
// they live under a perf-specific subdomain so they can never collide with a
// real ome.io/* label the placement controller cares about.
package labels

const (
	// ClassLabel carries the size-class name the generator assigned to a
	// source InferenceService. The observer reads it off the source to
	// attribute each placement-latency measurement to a class so the
	// recorder can bucket placement latency.
	ClassLabel = "perf.ome.io/class"

	// HarnessLabel marks every source InferenceService the generator
	// creates so cleanup and listing can target exactly the harness's
	// objects. Value is always HarnessLabelValue. The placement controller
	// copies a derived ISVC's labels through to the member, so this label
	// also rides onto the derived copies — which is what lets the
	// member-side admitter scope its watch to harness-derived objects.
	HarnessLabel      = "perf.ome.io/harness"
	HarnessLabelValue = "multicluster-placement"
)
