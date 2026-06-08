package status

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Event reasons emitted by the live OMENative-on-ISVC path. The bulk of
// the lifecycle event reasons now live in workload/types/events.go and
// are fired by the workload ops package; only the migration-validation
// reasons below are still fired directly by omenative/ops/migrate.go,
// which records them against the parent ISVC before delegating into the
// workload layer. Centralized so a `git grep` on the constant catches
// every site that fires the same event class.
const (
	// EventReasonMigrationRequestRejected fires when a migration request
	// annotation is rejected at validation (malformed payload, missing
	// required fields) before any workload-side migration is attempted.
	EventReasonMigrationRequestRejected = "MigrationRequestRejected"

	// EventReasonUnsupportedSchemaVersion fires when a migration request
	// carries a schemaVersion the controller does not understand.
	EventReasonUnsupportedSchemaVersion = "UnsupportedSchemaVersion"
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
