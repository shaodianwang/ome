package coordination

import (
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// BuildGroupStatus produces a RolloutCoordinationGroupStatus snapshot
// from the (resolved group, computed transition, ratio state) tuple.
// Pure compute; the caller diffs against the previously-written status
// before issuing an Update.
func BuildGroupStatus(group ResolvedGroup, tr GroupTransition, ratio *RatioState, now time.Time) v1beta1.RolloutCoordinationGroupStatus {
	out := v1beta1.RolloutCoordinationGroupStatus{
		Name:               group.Name,
		Components:         append([]v1beta1.ComponentType(nil), group.Components...),
		Policy:             group.Policy,
		Phase:              tr.Phase,
		CompositePhase:     tr.CompositePhase,
		CurrentComponent:   tr.CurrentComponent,
		PreviousComponent:  tr.PreviousComponent,
		Message:            tr.Message,
		LastTransitionTime: &metav1.Time{Time: now},
	}
	if len(group.Order) > 0 {
		out.Order = append([]v1beta1.ComponentType(nil), group.Order...)
	}
	if group.Pacing.MaxSurge != nil {
		copy := *group.Pacing.MaxSurge
		out.ObservedSurge = &copy
	}
	if ratio != nil && group.Pacing.Type == v1beta1.CoordinationPacingRatioBalanced {
		out.ObservedRatio = ratioStateToAPI(ratio)
	}
	return out
}

// ratioStateToAPI converts the in-memory snapshot into the
// API-on-disk representation.
func ratioStateToAPI(r *RatioState) *v1beta1.RolloutCoordinationRatio {
	out := &v1beta1.RolloutCoordinationRatio{}
	if len(r.Original) > 0 {
		out.Original = make(map[v1beta1.ComponentType]int32, len(r.Original))
		for k, v := range r.Original {
			out.Original[k] = v
		}
	}
	if len(r.Current) > 0 {
		out.Current = make(map[v1beta1.ComponentType]int32, len(r.Current))
		for k, v := range r.Current {
			out.Current[k] = v
		}
	}
	if len(r.NewPods) > 0 {
		out.NewPods = make(map[v1beta1.ComponentType]int32, len(r.NewPods))
		for k, v := range r.NewPods {
			out.NewPods[k] = v
		}
	}
	return out
}

// MergeGroupStatus merges a freshly-computed group status into the
// previously-observed RolloutCoordinationStatus. The merge preserves
// LastTransitionTime when Phase didn't change, since the status is
// supposed to reflect when the phase last *transitioned*, not when
// the controller last looked at it.
func MergeGroupStatus(prev *v1beta1.RolloutCoordinationStatus, next v1beta1.RolloutCoordinationGroupStatus) *v1beta1.RolloutCoordinationStatus {
	out := &v1beta1.RolloutCoordinationStatus{}
	if prev != nil {
		out.Groups = append(out.Groups, prev.Groups...)
	}
	for i := range out.Groups {
		if out.Groups[i].Name != next.Name {
			continue
		}
		if out.Groups[i].Phase == next.Phase && out.Groups[i].LastTransitionTime != nil {
			next.LastTransitionTime = out.Groups[i].LastTransitionTime
		}
		out.Groups[i] = next
		return out
	}
	out.Groups = append(out.Groups, next)
	return out
}

// SetRolloutCoordinationReady stamps the InferenceServiceStatus
// condition `RolloutCoordinationReady` from the per-group phases.
// True when every group is Idle; False with Reason=InProgress when any
// group is mid-rollout; False with Reason=Failed when any group has
// Failed; Unknown with Reason=Pending when groups are declared but
// none have a phase yet.
func SetRolloutCoordinationReady(isvc *v1beta1.InferenceService, groups []v1beta1.RolloutCoordinationGroupStatus, now time.Time) {
	if isvc == nil || len(groups) == 0 {
		return
	}
	cond := ComputeCoordinationReady(groups)
	if cond == nil {
		return
	}
	isvc.Status.SetCondition(apis.ConditionType(v1beta1.RolloutCoordinationReady), cond)
}

// ComputeCoordinationReady is the pure decision step used by
// SetRolloutCoordinationReady. Returns nil when no condition write is
// warranted (no groups declared).
func ComputeCoordinationReady(groups []v1beta1.RolloutCoordinationGroupStatus) *apis.Condition {
	if len(groups) == 0 {
		return nil
	}
	anyFailed := false
	anyMidRollout := false
	anyPending := false
	failedGroups := make([]string, 0)
	midRolloutGroups := make([]string, 0)
	for _, g := range groups {
		switch g.Phase {
		case v1beta1.CoordinationPhaseFailed:
			anyFailed = true
			failedGroups = append(failedGroups, g.Name)
		case v1beta1.CoordinationPhaseIdle, "":
			if g.Phase == "" {
				anyPending = true
			}
		default:
			anyMidRollout = true
			midRolloutGroups = append(midRolloutGroups, g.Name+":"+string(g.Phase))
		}
	}
	switch {
	case anyFailed:
		return &apis.Condition{
			Type:    apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status:  corev1.ConditionFalse,
			Reason:  "Failed",
			Message: "rollout coordination failed in groups: " + strings.Join(failedGroups, ","),
		}
	case anyMidRollout:
		return &apis.Condition{
			Type:    apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status:  corev1.ConditionFalse,
			Reason:  "InProgress",
			Message: "rollout coordination in flight: " + strings.Join(midRolloutGroups, ","),
		}
	case anyPending:
		return &apis.Condition{
			Type:    apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status:  corev1.ConditionUnknown,
			Reason:  "Pending",
			Message: "rollout coordination not yet observed",
		}
	default:
		return &apis.Condition{
			Type:   apis.ConditionType(v1beta1.RolloutCoordinationReady),
			Status: corev1.ConditionTrue,
			Reason: "Idle",
		}
	}
}

// MaxSurgeBudget converts the group's pacing MaxSurge (IntOrString)
// into the absolute pod count budget for one Component's surge
// step, given its desired replica count. Strings are interpreted as
// percentages of replicas, rounded up.
func MaxSurgeBudget(pacing v1beta1.CoordinationPacing, replicas int32) int32 {
	if pacing.MaxSurge == nil {
		return surgeFromPercent(25, replicas)
	}
	if pacing.MaxSurge.Type == intstr.Int {
		v := int32(pacing.MaxSurge.IntValue())
		if v < 0 {
			return 0
		}
		return v
	}
	pct := strPercent(pacing.MaxSurge.StrVal)
	return surgeFromPercent(pct, replicas)
}

// MaxUnavailableBudget converts the group's pacing MaxUnavailable
// (IntOrString) into the absolute pod count, given replicas. Mirror
// of MaxSurgeBudget.
func MaxUnavailableBudget(pacing v1beta1.CoordinationPacing, replicas int32) int32 {
	if pacing.MaxUnavailable == nil {
		return 0
	}
	if pacing.MaxUnavailable.Type == intstr.Int {
		v := int32(pacing.MaxUnavailable.IntValue())
		if v < 0 {
			return 0
		}
		return v
	}
	pct := strPercent(pacing.MaxUnavailable.StrVal)
	return surgeFromPercent(pct, replicas)
}

// surgeFromPercent computes ceil(replicas * percent / 100). Percent
// values outside [0, 100] are clamped; replicas == 0 returns 0.
func surgeFromPercent(percent int, replicas int32) int32 {
	if percent <= 0 || replicas <= 0 {
		return 0
	}
	if percent > 100 {
		percent = 100
	}
	num := int64(replicas) * int64(percent)
	out := num / 100
	if num%100 != 0 {
		out++
	}
	if out > int64(replicas) {
		out = int64(replicas)
	}
	return int32(out)
}

// strPercent parses "<n>%" to int n. Returns 0 on parse failure;
// callers reading from the validated webhook payload won't see this
// branch in practice.
func strPercent(s string) int {
	if len(s) == 0 || s[len(s)-1] != '%' {
		return 0
	}
	out := 0
	for _, ch := range s[:len(s)-1] {
		if ch < '0' || ch > '9' {
			return 0
		}
		out = out*10 + int(ch-'0')
	}
	return out
}
