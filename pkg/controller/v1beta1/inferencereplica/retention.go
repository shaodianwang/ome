package inferencereplica

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
)

// defaultRevisionRetention is the maximum number of non-live
// ControllerRevisions the IR controller keeps per (parent ISVC,
// Component). 10 matches the retired OMENative direct path
// (omenative/status.go's defaultRevisionRetention, OEP §2.2) and the
// StatefulSet default — enough headroom for rollback inspection without
// unbounded etcd growth. Reusing the same bound keeps the IR-managed
// path byte-equivalent to the path it replaced.
const defaultRevisionRetention = 10

// sweepRevisions trims accumulated ControllerRevisions for this IR down
// to defaultRevisionRetention non-live entries, never deleting a
// revision that is currently live (CurrentRevision / UpdateRevision or
// any InstanceStatus.RunningRevision / TargetRevision).
//
// Best-effort and non-fatal: a retention failure is logged + evented but
// NEVER fails the reconcile. The retired OMENative direct path treated
// retention as a post-commit step too; here we go a step further and
// suppress the error entirely (matching the IR reconciler's headless-
// Service and deferred-status-write suppression pattern) because the
// next reconcile re-runs the sweep — a stale extra CR is harmless, a
// failed reconcile is not.
//
// MUST be called AFTER the status write so the live-name union reads the
// just-committed CurrentRevision / UpdateRevision (the deferred status
// writer mirrors those onto ir.Status before this runs).
func (r *Reconciler) sweepRevisions(ctx context.Context, ir *v1beta1.InferenceReplica) {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	// liveNames union protects in-flight migrations: an Instance whose
	// RunningRevision points at an older CR must keep that CR, or the
	// migration resume path can't find its source payload and bails.
	liveNames := revision.CollectLiveRevisionNames(
		ir.Status.CurrentRevision,
		ir.Status.UpdateRevision,
		v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses),
	)

	if err := revision.RetainControllerRevisions(
		ctx, r.Client, reader, irRevisionKey(ir), defaultRevisionRetention, liveNames...,
	); err != nil {
		r.Log.V(1).Error(err, "ControllerRevision retention sweep failed; non-live revisions may accumulate until the next reconcile",
			"ir", client.ObjectKeyFromObject(ir))
		if r.Recorder != nil {
			r.Recorder.Eventf(ir, corev1.EventTypeWarning, "RevisionRetentionFailed",
				"InferenceReplica %s/%s ControllerRevision retention sweep failed: %v",
				ir.Namespace, ir.Name, err)
		}
	}
}
