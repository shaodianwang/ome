package omenative

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

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

	// Migration capacity / validation rejections (PR F).
	EventReasonRateLimited               = "RateLimited"
	EventReasonMigrationFromNodeMismatch = "MigrationFromNodeMismatch"
	EventReasonMigrationSurgeAbandoned   = "MigrationSurgeAbandoned"

	// Migration overlay validation rejection (PR G, REV-6).
	EventReasonMigrationNodeAffinityConflict = "MigrationNodeAffinityConflict"

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
func recordEventf(p ReconcileParams, eventType, reason, messageFmt string, args ...any) {
	if p.Recorder == nil || p.ISVC == nil {
		return
	}
	p.Recorder.Eventf(p.ISVC, eventType, reason, messageFmt, args...)
}

// recordNormal is sugar for the most common case — a normal event
// against the ISVC tied to the current reconcile.
func recordNormal(p ReconcileParams, reason, messageFmt string, args ...any) {
	recordEventf(p, corev1.EventTypeNormal, reason, messageFmt, args...)
}

// recordWarning is sugar for the warning case.
func recordWarning(p ReconcileParams, reason, messageFmt string, args ...any) {
	recordEventf(p, corev1.EventTypeWarning, reason, messageFmt, args...)
}

// instanceKey formats "component=engine instance=2" used in event
// messages. Keeps the format consistent across every emitter so
// operators can grep on a single shape.
func instanceKey(component v1beta1.ComponentType, idx int32) string {
	return fmt.Sprintf("component=%s instance=%d", component, idx)
}
