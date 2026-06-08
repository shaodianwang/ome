// Update step-constant re-exports for the omenative/ops package.
//
// The per-Instance Update state machine (in-place / recreate / surge
// variants) lives in pkg/controller/v1beta1/workload/ops after the
// workload-extraction refactor; the ISVC dispatch path drives it via
// workload.Reconcile (see dispatch.go), NOT through this package. The
// former ops.Update adapter shim was removed once dispatch stopped
// calling it — it had zero callers (see
// .claude/specs/2026-06-08-omenative-workload-code-review.md §4).
//
// What remains here are the three Update step-name constants that the
// v1beta1-typed timeouts handler (timeouts.go) still reads to recognize
// in-flight Update operations by their Step. They re-export the
// workload-owned source-of-truth so the handler doesn't have to reach
// into workload/ops for the value or hardcode the string.
package ops

import (
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// updateStepInPlace identifies the InPlace step a Type=Update operation
// uses while the in-place image patch is being driven. The omenative
// timeouts handler reads this constant to recognize in-flight in-place
// updates that need to fall back to recreate when their deadline elapses
// — without it the handler would have to either reach into workload/ops
// for the constant or hardcode the string.
const updateStepInPlace = workloadops.UpdateStepInPlace

// updateStepDrain identifies the recreate-drain step a Type=Update
// operation uses while the old pods are being drained ahead of the
// recreate-with-bumped-incarnation pass. Re-exported for the same reason
// as updateStepInPlace.
const updateStepDrain = workloadops.UpdateStepDrain

// updateStepSurge identifies the SurgeThenDrain entry step. Re-exported
// for the timeouts test that asserts a surge deadline escalates Phase
// to Failed (test uses the constant in assertions on the post-escalation
// Operation.Step).
const updateStepSurge = workloadops.UpdateStepSurge
