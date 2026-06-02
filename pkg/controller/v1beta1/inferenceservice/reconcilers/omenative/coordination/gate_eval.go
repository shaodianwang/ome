package coordination

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// EvaluateUpdateGate runs the full coordination gate stack for a single
// per-Instance Update decision. It is the SHARED decision site for both
// OMENative reconcile paths:
//
//   - the ISVC-direct path  (omenative/ops.ReconcileComponentOps), and
//   - the IR-managed path   (inferencereplica.Reconciler) — the default.
//
// Both wire it into workload.ReconcileInput.UpdateGate. Wiring it on only
// one path silently disables coordination on the other: the IR-managed
// path historically left UpdateGate nil (see the comment that used to
// live in inferencereplica/convert.go), so on the default deployment
// CheckSequential / CheckRatio / CheckSurge / CheckUnavailability were
// all skipped. Both Components of a Sequential group then recreated
// concurrently, and a group-wide MaxSurge never capped a Component whose
// per-Component budget was larger. Keeping the gate body here — in the
// coordination package both paths already depend on — means there is
// exactly one tested implementation that every caller imports.
//
// Gate order: ratio -> surge | unavailability -> sequential. First denial
// wins; later gates short-circuit. The caller reads only the allowed bit;
// denyReason is for logging/observability.
//
// OEP-0007.3 Bug 1: in-place strategies (InPlaceIfPossible / InPlaceOnly)
// skip CheckRatio. CheckRatio always projects a -1 drain (capacity loss);
// for in-place that drain is paired 1:1 with a same-pod return
// (mark-not-ready -> patch -> mark-ready), so the net capacity change is
// ~0. The -1 projection misrepresents that transient as a permanent loss
// and indefinitely over-blocks the smaller Component of a ratio pair
// (e.g. the decoder in a 4:2 engine:decoder pair with 25% tolerance
// deadlocks because 4:1 = 4.0 sits above the [1.5, 2.5] band).
// CheckUnavailability (called below for non-surge strategies, which
// includes both in-place variants) already governs concurrent disruption
// via the MaxUnavailable budget, so RatioBalanced safety is preserved
// without the structural deadlock. The RatioGateBypassed event + metric
// fire so operators see why the gate did not run.
//
// Reads exclusively from isvc.Spec + isvc.Status (no client / cache hits),
// so it is safe to call from the hot dispatcher loop on either path.
// recorder may be nil (the event is best-effort observability).
func EvaluateUpdateGate(
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	recorder record.EventRecorder,
	strategy workloadtypes.UpdateStrategyType,
	inFlightSurge, inFlightUnavail int32,
) (allowed bool, denyReason string) {
	// ResolveGateContext is paid once so the shared prelude (ResolveGroups +
	// MembershipFor + nil-isvc / no-coord short-circuit) runs once instead of
	// four times -- see the coordination.GateContext docs.
	ctx := ResolveGateContext(isvc, component)

	inPlace := strategy == workloadtypes.UpdateStrategyInPlaceIfPossible ||
		strategy == workloadtypes.UpdateStrategyInPlaceOnly
	if !inPlace {
		if ok, reason := ctx.CheckRatio(inFlightUnavail); !ok {
			return false, reason
		}
	} else {
		RecordRatioGateBypassed(string(component), string(strategy))
		if recorder != nil && isvc != nil {
			recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonRatioGateBypassed,
				"RatioGateBypassed: Component=%s, Strategy=%s. RatioBalanced is bypassed for in-place strategies because the drain is paired 1:1 with a same-pod return. Per-Component pacing (CoordinationPacing.PerComponent) still governs concurrent disruption.",
				component, strategy)
		}
	}

	if strategy == workloadtypes.UpdateStrategySurgeThenDrain || strategy == "" {
		if ok, reason := ctx.CheckSurge(inFlightSurge); !ok {
			return false, reason
		}
	} else {
		if ok, reason := ctx.CheckUnavailability(inFlightUnavail); !ok {
			return false, reason
		}
	}

	if ok, reason := ctx.CheckSequential(); !ok {
		return false, reason
	}
	return true, ""
}
