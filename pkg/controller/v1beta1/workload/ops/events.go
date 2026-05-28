package ops

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// recordNormal emits a Normal K8s Event against target. nil-safe: when
// the recorder isn't wired (tests that construct workload.Deps{}
// directly) OR the target is nil, this is a no-op so callers don't have
// to plumb a recorder. Mirrors the omenative/status.RecordNormal
// helper byte-for-byte so existing operator dashboards keep matching.
func recordNormal(rec record.EventRecorder, target client.Object, reason, messageFmt string, args ...any) {
	if rec == nil || target == nil {
		return
	}
	rec.Eventf(target, corev1.EventTypeNormal, reason, messageFmt, args...)
}

// eventTarget returns the object emitted events should be stamped
// against — input.EventTarget when set, falling back to OwnerObject.
// Matches the nil-OK semantics documented on ReconcileInput.EventTarget
// so workload code never branches on EventTarget==nil itself.
func eventTarget(input workload.ReconcileInput) client.Object {
	if input.EventTarget != nil {
		return input.EventTarget
	}
	return input.OwnerObject
}

// instanceKey formats "component=engine instance=2" used in event
// messages. Keeps the format consistent across every emitter so
// operators can grep on a single shape.
func instanceKey(component workload.ComponentType, idx int32) string {
	return fmt.Sprintf("component=%s instance=%d", component, idx)
}
