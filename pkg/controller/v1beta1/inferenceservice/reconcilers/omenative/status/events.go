package status

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Event reasons emitted by OMENative. Centralized so a `git grep` on
// the constant catches every site that fires the same event class.
// Each reason maps to one or two transition points; see the OBS-1
// table in `.claude/specs/2026-05-21-oep-0007-omenative-review-merged.md`
// for the canonical inventory.
const (
	// Create / scale up lifecycle.
	EventReasonInstanceCreated = "InstanceCreated"
	EventReasonInstanceReady   = "InstanceReady"

	// In-place update lifecycle.
	EventReasonInPlaceUpdateStarted     = "InPlaceUpdateStarted"
	EventReasonInPlaceUpdateCompleted   = "InPlaceUpdateCompleted"
	EventReasonInPlaceUpdateNotPossible = "InPlaceUpdateNotPossible"
	EventReasonInPlaceTimeoutFallback   = "InPlaceTimeoutFallback"

	// Recreate update lifecycle.
	EventReasonRecreateUpdateStarted   = "RecreateUpdateStarted"
	EventReasonRecreateUpdateCompleted = "RecreateUpdateCompleted"

	// Restart-on-pod-loss lifecycle.
	EventReasonRestartTriggered = "RestartTriggered"
	EventReasonRestartCompleted = "RestartCompleted"

	// Operation timeout / stuck.
	EventReasonOperationDeadlineExceeded = "OperationDeadlineExceeded"

	// EventReasonInstanceFailed fires when an Instance has been escalated
	// from a transient phase (Creating / Updating / Restarting / Migrating)
	// to Phase=Failed because its in-flight Operation exceeded its
	// Deadline and the controller has given up trying to converge it.
	// Operator intervention required.
	EventReasonInstanceFailed = "InstanceFailed"

	// Migration request lifecycle.
	EventReasonMigrationRequestAccepted = "MigrationRequestAccepted"
	EventReasonMigrationRequestRejected = "MigrationRequestRejected"
	EventReasonMigrationCompleted       = "MigrationCompleted"
	EventReasonUnsupportedSchemaVersion = "UnsupportedSchemaVersion"

	// Migration capacity / validation rejections.
	EventReasonRateLimited               = "RateLimited"
	EventReasonMigrationFromNodeMismatch = "MigrationFromNodeMismatch"
	EventReasonMigrationSurgeAbandoned   = "MigrationSurgeAbandoned"

	// Surge NodeAffinity would collapse to "hostname=FromNode AND
	// hostname!=FromNode" — rejected before rendering.
	EventReasonMigrationNodeAffinityConflict = "MigrationNodeAffinityConflict"

	// Fired when a pod under the OMENative selector lacks
	// ome.io/instance-incarnation. The controller refuses to delete it.
	EventReasonFoundOrphan = "FoundOrphan"

	// Revision-collision (rare; salted retry succeeded).
	EventReasonRevisionCollision = "RevisionCollision"

	// EventReasonMaybeNoGangScheduler is a soft Warning fired the first
	// time a multi-pod Instance's PodGroup is created under a pod
	// template whose `spec.schedulerName` is unset OR equals the
	// upstream default ("default-scheduler"). The PodGroup itself was
	// created successfully — but a stock kube-scheduler does NOT read
	// `scheduling.x-k8s.io/v1alpha1` PodGroup objects, so the gang
	// contract degrades to per-pod scheduling silently. Operators must
	// either install scheduler-plugins as a secondary scheduler and set
	// `runtime.spec.schedulerName=scheduler-plugins-scheduler`, OR
	// install it as a default-scheduler plugin (in which case the
	// warning is a false positive — the controller cannot detect that
	// arrangement from inside the cluster, so we err on the side of
	// surfacing the heuristic). De-duplicated per (ISVC, Component)
	// per controller process so reconciles do not spam the event log;
	// see gang.go `maybeWarnNoGangScheduler` for the dedup map.
	EventReasonMaybeNoGangScheduler = "MaybeNoGangScheduler"
)

// recordEventf emits a K8s Event against the parent InferenceService.
// nil-safe: when the recorder isn't wired (tests that construct
// ReconcileParams{} directly), this is a no-op. eventType is
// corev1.EventTypeNormal or corev1.EventTypeWarning.
//
// All OMENative-emitted events target the parent InferenceService so
// operators see them via `kubectl describe isvc <name>`. Pod-level
// kubelet events stay on the pods.
func RecordEventf(rec record.EventRecorder, isvc *v1beta1.InferenceService, eventType, reason, messageFmt string, args ...any) {
	if rec == nil || isvc == nil {
		return
	}
	rec.Eventf(isvc, eventType, reason, messageFmt, args...)
}

// recordNormal is sugar for the most common case — a normal event
// against the ISVC tied to the current reconcile.
func RecordNormal(rec record.EventRecorder, isvc *v1beta1.InferenceService, reason, messageFmt string, args ...any) {
	RecordEventf(rec, isvc, corev1.EventTypeNormal, reason, messageFmt, args...)
}

// recordWarning is sugar for the warning case.
func RecordWarning(rec record.EventRecorder, isvc *v1beta1.InferenceService, reason, messageFmt string, args ...any) {
	RecordEventf(rec, isvc, corev1.EventTypeWarning, reason, messageFmt, args...)
}

// instanceKey formats "component=engine instance=2" used in event
// messages. Keeps the format consistent across every emitter so
// operators can grep on a single shape.
func InstanceKey(component v1beta1.ComponentType, idx int32) string {
	return fmt.Sprintf("component=%s instance=%d", component, idx)
}
