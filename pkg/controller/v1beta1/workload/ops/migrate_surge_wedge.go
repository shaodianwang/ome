// migrate_surge_wedge.go — terminal kubelet evidence on a migration's
// surge ends the move before the record's Deadline.
//
// A surge pod parked in a waiting reason it cannot get itself out of
// never runs, so the handover it exists for can never happen. Both
// escalation paths skip a Migrate-pinned row on purpose — a serving
// source must not be failed for its replacement's fate — which leaves
// the record as the only consumer of this evidence. The read therefore
// lives in the migration pass, which already holds the surge pods, and
// drives the SAME record close an expiry drives: the record is the
// authority, its Deadline is the backstop.
package ops

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// migrationSourceUnhealthyReason is the LastFailure.Reason a source
// records when its migration is closed and observation finds the source
// itself not runtime-ready. It names the source's own condition, not
// the close that exposed it — the surge's wedge is what the record and
// the event carry.
const migrationSourceUnhealthyReason = "MigrationClosedSourceUnhealthy"

// surgeWedgeBlocker names the first surge pod parked in a terminal
// kubelet waiting reason past the configured stuck-pod grace, in the
// phrasing the record carries as its outcome.
//
// The reason set and the grace are the ones every other reader of this
// evidence uses, so a wedge one pass acts on is a wedge the others
// recognize. An unconfigured grace disables the read exactly as it
// disables the fast escalation. A pod already Terminating is on its way
// out — the recycle or the scale-down pipeline owns it — so it proves
// nothing about the migration.
func surgeWedgeBlocker(input workload.ReconcileInput, surgePods []*corev1.Pod) (string, bool) {
	if input.StuckPodGrace <= 0 {
		return "", false
	}
	now := input.Now()
	for _, pod := range surgePods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		reason, stuck := workload.PodStuckInTerminalWaiting(pod, now, input.StuckPodGrace)
		if !stuck {
			continue
		}
		return fmt.Sprintf("surge pod %s wedged in %s past the stuck-pod grace", pod.Name, reason), true
	}
	return "", false
}

// failMigrationOnWedgedSurge closes the record Failed on blocker through
// the shared record path — surge unpinned for the bounded scale-down
// pipeline, source restored to what observation finds with its drain
// hold released — and emits the one Warning that names the pod and the
// reason. No RetryBlock is charged: the wedge indicts the image or the
// node the surge landed on, not an attempt the source is allowed to make
// on its own revision.
func failMigrationOnWedgedSurge(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	rec *workload.MigrationRecord,
	surgeIdx int32,
	blocker string,
) error {
	if err := failMigrationThroughRecord(ctx, deps, input, plan, rec, blocker,
		migrationSourceUnhealthyReason, blocker+"; source pods not runtime-ready"); err != nil {
		return err
	}
	recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationSurgeWedged,
		"OMENative migration uuid=%s failed: %s; tearing down surge instance=%d",
		rec.RequestUUID, blocker, surgeIdx)
	return nil
}
