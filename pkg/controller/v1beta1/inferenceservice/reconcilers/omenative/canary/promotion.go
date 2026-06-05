package canary

import (
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// isRollbackRequested reports whether the operator set ome.io/rollout-rollback.
func isRollbackRequested(isvc *v1beta1.InferenceService) bool {
	_, ok := isvc.Annotations[constants.RolloutRollbackAnnotation]
	return ok
}

// shouldAdvanceManual reports whether the operator promoted THIS canary revision
// via ome.io/rollout-promote=<canaryHash>. The hash guard prevents a stale
// promote (left over from a prior rollout) from advancing a new one.
func shouldAdvanceManual(isvc *v1beta1.InferenceService, canaryHash string) bool {
	return isvc.Annotations[constants.RolloutPromoteAnnotation] == canaryHash
}

// shouldAdvanceAuto reports whether a paused step's Pause.Duration has elapsed
// since the step was entered. A step with no Pause, or a Pause with no Duration,
// advances immediately.
func shouldAdvanceAuto(cs *v1beta1.CanaryStatus, step v1beta1.RolloutStep, now time.Time) bool {
	if step.Pause == nil || step.Pause.Duration == nil {
		return true
	}
	if cs == nil || cs.StepEnteredTime == nil {
		return false
	}
	return !now.Before(cs.StepEnteredTime.Time.Add(step.Pause.Duration.Duration))
}

// shouldAdvance decides whether a paused step may advance, dispatching on the
// promotion policy (Manual = promote annotation; Auto = timed). Defaults to
// Manual when unset (the validator requires a policy when any step pauses, so
// this is a safety default).
func shouldAdvance(isvc *v1beta1.InferenceService, plan *v1beta1.CanarySpec, cs *v1beta1.CanaryStatus, step v1beta1.RolloutStep, now time.Time) bool {
	if plan.PromotionPolicy != nil && *plan.PromotionPolicy == v1beta1.PromotionPolicyAuto {
		return shouldAdvanceAuto(cs, step, now)
	}
	return shouldAdvanceManual(isvc, cs.CanaryRevisionHash)
}

// advanceStep moves to the next step: increment the index, stamp the entry time
// (Auto timing measures from here), and consume the promote annotation so it is
// not re-applied to the next step.
func advanceStep(isvc *v1beta1.InferenceService, now time.Time) {
	cs := isvc.Status.Canary
	cs.CurrentStep++
	cs.StepEnteredTime = &metav1.Time{Time: now}
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
}
