package canary

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
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
func shouldAdvanceAuto(cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep, now time.Time) bool {
	if step.Pause == nil || step.Pause.Duration == nil {
		return true
	}
	if cs == nil || cs.StepEnteredTime == nil {
		return false
	}
	return !now.Before(cs.StepEnteredTime.Time.Add(step.Pause.Duration.Duration))
}

// stepDecision is the verdict for whether a gated step may move. Manual and Auto
// produce only decHold/decAdvance; Analysis — the only policy that can retreat
// without an operator — can also produce decRollback or decFailed.
type stepDecision int

const (
	decHold     stepDecision = iota // stay on this step (keep baking / paused)
	decAdvance                      // advance to the next step (or, on the final step, complete)
	decRollback                     // abort: roll back to the stable revision
	decFailed                       // analysis could not read health past the stall timeout
)

// stepIsAnalysis reports whether a step opts into metric-gated promotion (its own
// Analysis field is set). The gate is per-step; GroupCanary.Analysis only holds
// shared defaults and gates nothing by itself.
func stepIsAnalysis(step v1beta1.RolloutGroupStep) bool {
	return step.Analysis != nil
}

// stepGated reports whether a step holds before advancing: it is gated when it
// opts into analysis or carries a Pause. A bare step (neither) advances as soon
// as capacity + traffic converge.
func stepGated(step v1beta1.RolloutGroupStep) bool {
	return stepIsAnalysis(step) || step.Pause != nil
}

// stepRequeue is how soon to re-check a held step: the step's analysis Interval
// (so sampling re-runs on cadence) for an analysis step, else the standard requeue.
func stepRequeue(step v1beta1.RolloutGroupStep) time.Duration {
	if step.Analysis != nil && step.Analysis.Interval.Duration > 0 {
		return step.Analysis.Interval.Duration
	}
	return reconcileRequeue
}

// evaluateStep decides whether a gated step may advance, dispatching on the step's
// gate by field presence: Analysis (metric sampling — the only branch that can
// return decRollback/decFailed), a timed Pause (Duration elapsed), else Manual
// (promote annotation). The metrics source for analysis is GroupCanary.Prometheus,
// supplied to the sampler via ReconcileInputs.Prometheus.
func evaluateStep(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep) stepDecision {
	switch {
	case stepIsAnalysis(step):
		return evaluateAnalysisStep(ctx, in, step.Analysis, cs, step)
	case step.Pause != nil && step.Pause.Duration != nil:
		if shouldAdvanceAuto(cs, step, in.Now) {
			return decAdvance
		}
		return decHold
	default:
		if shouldAdvanceManual(in.ISVC, cs.CanaryRevisionHash) {
			return decAdvance
		}
		return decHold
	}
}

// evaluateAnalysisStep runs the metric gate for one reconcile: warm up, throttle
// to one sample per Interval, sample, then decide. A breach accrues toward
// FailureLimit (cumulative per step) and rolls back at the limit; a passing
// sample advances once the bake window (Pause.Duration) elapses; an inconclusive
// sample holds (or, per OnInconclusive, rolls back) and escalates to Failed past
// the stall timeout. A matching promote annotation overrides the gate.
func evaluateAnalysisStep(ctx context.Context, in ReconcileInputs, a *v1beta1.RolloutAnalysis, cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep) stepDecision {
	// Operator override: a matching promote annotation force-advances mid-bake.
	if shouldAdvanceManual(in.ISVC, cs.CanaryRevisionHash) {
		return decAdvance
	}
	// Warm-up: no sampling until InitialDelay after the split first served
	// (StepEnteredTime is re-stamped when the step starts serving).
	if a.InitialDelay != nil && cs.StepEnteredTime != nil &&
		in.Now.Before(cs.StepEnteredTime.Time.Add(a.InitialDelay.Duration)) {
		return decHold
	}
	// Throttle: at most one sample per Interval, so a burst of unrelated reconciles
	// cannot over-count failures into a spurious rollback.
	if cs.LastEvaluationTime != nil && in.Now.Before(cs.LastEvaluationTime.Time.Add(a.Interval.Duration)) {
		return decHold
	}
	if in.Sample == nil {
		// Misconfigured (no sampler wired): hold rather than advance ungated.
		return decHold
	}
	res := in.Sample(ctx, in, a)
	cs.LastEvaluationTime = &metav1.Time{Time: in.Now}
	cs.MetricResults = toStatusMetricResults(res.Metrics, in.Now)
	dec := decHold
	switch res.Outcome {
	case analysis.Fail:
		cs.LastConclusiveEvaluationTime = &metav1.Time{Time: in.Now}
		cs.AnalysisFailedChecks++
		if cs.AnalysisFailedChecks >= a.FailureLimit {
			dec = decRollback
		}
	case analysis.Pass:
		cs.LastConclusiveEvaluationTime = &metav1.Time{Time: in.Now}
		if shouldAdvanceAuto(cs, step, in.Now) { // bake window (Pause.Duration) elapsed
			dec = decAdvance
		}
	default: // analysis.Inconclusive
		if a.OnInconclusive != nil && *a.OnInconclusive == v1beta1.OnInconclusiveRollback {
			dec = decRollback
		} else if analysisStalled(cs, readyTimeoutOrDefault(in.ISVC), in.Now) {
			dec = decFailed
		}
	}
	recordAnalysisSample(in.ISVC, in.Component, res, cs.AnalysisFailedChecks)
	if dec == decRollback {
		recordRollback(in.ISVC, in.Component, "analysis")
	}
	return dec
}

// analysisStalled reports whether analysis has been unable to read health for
// longer than timeout, measured from the last conclusive sample (or step entry
// if none yet). A stall means "can't tell," not "bad": the caller parks Failed,
// it does not roll back (unless OnInconclusive=Rollback handled that earlier).
func analysisStalled(cs *v1beta1.CanaryStatus, timeout time.Duration, now time.Time) bool {
	if timeout <= 0 {
		return false
	}
	anchor := cs.LastConclusiveEvaluationTime
	if anchor == nil {
		anchor = cs.StepEnteredTime
	}
	if anchor == nil {
		return false
	}
	return !now.Before(anchor.Time.Add(timeout))
}

// toStatusMetricResults converts evaluator results into the status shape,
// stamping each with the sample time.
func toStatusMetricResults(mrs []analysis.MetricResult, now time.Time) []v1beta1.AnalysisMetricResult {
	if len(mrs) == 0 {
		return nil
	}
	t := &metav1.Time{Time: now}
	out := make([]v1beta1.AnalysisMetricResult, 0, len(mrs))
	for _, m := range mrs {
		out = append(out, v1beta1.AnalysisMetricResult{
			Name:      m.Name,
			Value:     m.Value,
			Threshold: m.Threshold,
			Operator:  m.Operator,
			Passed:    m.Passed,
			Message:   m.Message,
			Time:      t,
		})
	}
	return out
}

// advanceStep moves to the next step: increment the index, stamp the entry time
// (Auto timing measures from here), and consume the promote annotation so it is
// not re-applied to the next step.
func advanceStep(isvc *v1beta1.InferenceService, now time.Time) {
	cs := isvc.Status.Canary
	cs.CurrentStep++
	cs.StepEnteredTime = &metav1.Time{Time: now}
	// Reset the per-step analysis budget + sampling state: the failure budget is
	// scoped to each step (each traffic level gets its own tolerance), and the
	// next step samples fresh.
	cs.AnalysisFailedChecks = 0
	cs.LastEvaluationTime = nil
	cs.LastConclusiveEvaluationTime = nil
	cs.MetricResults = nil
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
}
