package status

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Condition types written by OMENative. Component-level conditions
// live on `status.components.<comp>.omenative.conditions`;
// Instance-level conditions live on
// `status.components.<comp>.omenative.instanceStatuses[*].conditions`.
// Type names match OEP §3.3.
const (
	// Component-level.
	ConditionAvailable      = "Available"
	ConditionProgressing    = "Progressing"
	ConditionOperationStuck = "OperationStuck"

	// Instance-level.
	ConditionAllPodsScheduled      = "AllPodsScheduled"
	ConditionAllPodsReady          = "AllPodsReady"
	ConditionAllPodsAvailable      = "AllPodsAvailable"
	ConditionInPlaceUpdateFallback = "InPlaceUpdateFallback"
)

// Standard reason strings — kept short and machine-readable so
// `kubectl get isvc -o jsonpath='...conditions[?(@.type=="Available")].reason'`
// is grep-friendly.
const (
	ReasonReady                     = "Ready"
	ReasonNoReplicas                = "NoReplicas"
	ReasonNotAllAvailable           = "NotAllAvailable"
	ReasonRolloutInProgress         = "RolloutInProgress"
	ReasonNoRollout                 = "NoRollout"
	ReasonOperationDeadlineExceeded = "OperationDeadlineExceeded"
	ReasonNoStuckOperations         = "NoStuckOperations"
	ReasonAllPodsScheduled          = "AllPodsScheduled"
	ReasonAllPodsReady              = "AllPodsReady"
	ReasonAllPodsAvailable          = "AllPodsAvailable"
	ReasonInPlaceTimeoutFallback    = "InPlaceTimeoutFallback"
	ReasonNoInPlaceFallback         = "NoInPlaceFallback"

	// ReasonDeadlineExceeded is stamped on Operation.Reason by the
	// timeouts pass when an in-flight Operation's Deadline elapses.
	// operationStuckCondition lifts it into the Component-level
	// OperationStuck condition.
	ReasonDeadlineExceeded = "DeadlineExceeded"
)

// ComputeComponentConditions derives the Component-level condition
// set from the current Component status. ObservedGeneration is the
// ISVC generation the status reflects; SetStatusCondition writes it
// onto each condition so consumers can correlate.
//
// The returned slice is the new desired condition set; callers merge
// it into the existing list via apimeta.SetStatusCondition so
// LastTransitionTime updates only on real transitions.
func ComputeComponentConditions(cs *v1beta1.OMENativeComponentStatus, generation int64) []metav1.Condition {
	if cs == nil {
		return nil
	}
	now := metav1.Now()
	out := []metav1.Condition{
		availableCondition(cs, generation, now),
		progressingCondition(cs, generation, now),
		operationStuckCondition(cs, generation, now),
	}
	return out
}

// availableCondition reports whether every Instance is fully Available
// (all pods have been Ready continuously for ≥ minReadySeconds). At
// v1 alpha minReadySeconds defaults to 0 so Available collapses onto
// Ready, but the distinction stays in the API for the rollout-pacing
// future.
func availableCondition(cs *v1beta1.OMENativeComponentStatus, generation int64, now metav1.Time) metav1.Condition {
	switch {
	case cs.Replicas == 0:
		return metav1.Condition{
			Type:               ConditionAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             ReasonNoReplicas,
			Message:            "Component has no desired Instances",
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	case cs.AvailableReplicas == cs.Replicas:
		return metav1.Condition{
			Type:               ConditionAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             ReasonReady,
			Message:            "All Instances are Available",
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	default:
		return metav1.Condition{
			Type:               ConditionAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             ReasonNotAllAvailable,
			Message:            availabilityMessage(cs),
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	}
}

// progressingCondition reports whether a rollout is in flight —
// UpdateRevision != CurrentRevision, or any Instance is in a
// transient phase (Creating/Updating/Restarting/Migrating/Deleting).
func progressingCondition(cs *v1beta1.OMENativeComponentStatus, generation int64, now metav1.Time) metav1.Condition {
	if isProgressing(cs) {
		return metav1.Condition{
			Type:               ConditionProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             ReasonRolloutInProgress,
			Message:            progressingMessage(cs),
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	}
	return metav1.Condition{
		Type:               ConditionProgressing,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonNoRollout,
		Message:            "All Instances converged",
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

func isProgressing(cs *v1beta1.OMENativeComponentStatus) bool {
	if cs.UpdateRevision != "" && cs.CurrentRevision != cs.UpdateRevision {
		return true
	}
	for _, s := range cs.InstanceStatuses {
		switch s.Phase {
		case v1beta1.OMENativeInstanceCreating,
			v1beta1.OMENativeInstanceUpdating,
			v1beta1.OMENativeInstanceRestarting,
			v1beta1.OMENativeInstanceMigrating,
			v1beta1.OMENativeInstanceDeleting:
			return true
		}
	}
	return false
}

func progressingMessage(cs *v1beta1.OMENativeComponentStatus) string {
	if cs.UpdateRevision != "" && cs.CurrentRevision != cs.UpdateRevision {
		return "Rolling out " + cs.UpdateRevision + " (" +
			itoa(int(cs.UpdatedReadyReplicas)) + "/" + itoa(int(cs.Replicas)) + " on target)"
	}
	return "One or more Instances are in a transient phase"
}

// operationStuckCondition reports True when any Instance has an
// Operation with Reason=DeadlineExceeded — the breadcrumb timeouts.go
// stamps when a non-recoverable op step times out. Lifts the
// breadcrumb into the operator-facing condition surface per OEP §3.3.
func operationStuckCondition(cs *v1beta1.OMENativeComponentStatus, generation int64, now metav1.Time) metav1.Condition {
	stuckInstances := stuckOperations(cs)
	if len(stuckInstances) == 0 {
		return metav1.Condition{
			Type:               ConditionOperationStuck,
			Status:             metav1.ConditionFalse,
			Reason:             ReasonNoStuckOperations,
			LastTransitionTime: now,
			ObservedGeneration: generation,
		}
	}
	return metav1.Condition{
		Type:               ConditionOperationStuck,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonOperationDeadlineExceeded,
		Message:            "Instances with stuck operations: " + commaJoinInt32(stuckInstances),
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

func stuckOperations(cs *v1beta1.OMENativeComponentStatus) []int32 {
	var out []int32
	for _, s := range cs.InstanceStatuses {
		if s.Operation == nil {
			continue
		}
		if s.Operation.Reason == ReasonDeadlineExceeded {
			out = append(out, s.Index)
		}
	}
	return out
}

// ComputeInstanceConditions derives the per-Instance condition set
// from the live pod counters. Excludes Drained / Failed for v1 alpha
// — those need pod-state tracking we don't carry yet.
//
// InPlaceUpdateFallback is set by fallbackInPlaceToRecreate's caller
// (timeouts.go) and flips back to False once the Instance returns to
// steady-Ready (Phase=Ready, no in-flight Operation) — i.e. the
// recreate triggered by the fallback has converged. Without the
// recovery flip the breadcrumb is sticky-True forever, violating the
// K8s convention that conditions reflect current state, not history.
func ComputeInstanceConditions(s *v1beta1.OMENativeInstanceStatus, generation int64) []metav1.Condition {
	if s == nil {
		return nil
	}
	now := metav1.Now()
	out := []metav1.Condition{
		allPodsScheduledCondition(s, generation, now),
		allPodsReadyCondition(s, generation, now),
		allPodsAvailableCondition(s, generation, now),
	}
	fallback := metav1.Condition{
		Type:               ConditionInPlaceUpdateFallback,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonNoInPlaceFallback,
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
	if existing := apimeta.FindStatusCondition(s.Conditions, ConditionInPlaceUpdateFallback); existing != nil &&
		existing.Status == metav1.ConditionTrue &&
		!(s.Phase == v1beta1.OMENativeInstanceReady && s.Operation == nil) {
		// Still recovering from the fallback — preserve the breadcrumb.
		fallback = *existing
	}
	out = append(out, fallback)
	return out
}

func allPodsScheduledCondition(s *v1beta1.OMENativeInstanceStatus, generation int64, now metav1.Time) metav1.Condition {
	status, reason, message := podCountCondition(s.ScheduledPodCount, s.PodCount, "scheduled")
	return metav1.Condition{
		Type:               ConditionAllPodsScheduled,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

func allPodsReadyCondition(s *v1beta1.OMENativeInstanceStatus, generation int64, now metav1.Time) metav1.Condition {
	status, reason, message := podCountCondition(s.ReadyPodCount, s.PodCount, "ready")
	return metav1.Condition{
		Type:               ConditionAllPodsReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

func allPodsAvailableCondition(s *v1beta1.OMENativeInstanceStatus, generation int64, now metav1.Time) metav1.Condition {
	status, reason, message := podCountCondition(s.AvailablePodCount, s.PodCount, "available")
	return metav1.Condition{
		Type:               ConditionAllPodsAvailable,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}
}

// podCountCondition is the shared helper for the AllPods* conditions
// — same shape, only the predicate verb differs.
func podCountCondition(observed, total int32, verb string) (metav1.ConditionStatus, string, string) {
	if total == 0 {
		return metav1.ConditionFalse, "NoPods", "Instance has no pods yet"
	}
	if observed == total {
		switch verb {
		case "scheduled":
			return metav1.ConditionTrue, ReasonAllPodsScheduled, "All pods scheduled"
		case "ready":
			return metav1.ConditionTrue, ReasonAllPodsReady, "All pods Ready"
		case "available":
			return metav1.ConditionTrue, ReasonAllPodsAvailable, "All pods Available"
		}
	}
	return metav1.ConditionFalse,
		"NotAll" + capitalize(verb),
		itoa(int(observed)) + "/" + itoa(int(total)) + " pods " + verb
}

// ApplyComponentConditions merges the desired condition set onto cs
// using apimeta.SetStatusCondition so LastTransitionTime only updates
// on real status changes.
func ApplyComponentConditions(cs *v1beta1.OMENativeComponentStatus, desired []metav1.Condition) {
	if cs == nil {
		return
	}
	for i := range desired {
		apimeta.SetStatusCondition(&cs.Conditions, desired[i])
	}
}

// ApplyInstanceConditions merges the desired condition set onto s.
func ApplyInstanceConditions(s *v1beta1.OMENativeInstanceStatus, desired []metav1.Condition) {
	if s == nil {
		return
	}
	for i := range desired {
		apimeta.SetStatusCondition(&s.Conditions, desired[i])
	}
}

// availabilityMessage builds a human-readable summary like "2/3
// Instances Available" for the Available condition's message field.
func availabilityMessage(cs *v1beta1.OMENativeComponentStatus) string {
	return itoa(int(cs.AvailableReplicas)) + "/" + itoa(int(cs.Replicas)) + " Instances Available"
}

// itoa avoids strconv import in this small file; OMENative counters
// are int32 and the conversion is trivial.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var digits [11]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}

// commaJoinInt32 formats a small int32 slice as "a,b,c". Used in
// stuckOperations message.
func commaJoinInt32(xs []int32) string {
	if len(xs) == 0 {
		return ""
	}
	out := itoa(int(xs[0]))
	for _, x := range xs[1:] {
		out += "," + itoa(int(x))
	}
	return out
}

// capitalize uppercases the first ASCII rune; used to build reason
// strings like "NotAllReady" from the lowercase verb input.
func capitalize(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'a' && b[0] <= 'z' {
		b[0] -= 'a' - 'A'
	}
	return string(b)
}
