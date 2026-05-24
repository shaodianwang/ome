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
