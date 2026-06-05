package canary

import (
	"context"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileRequeue is how soon the controller re-checks an in-progress canary
// (capacity coming up, an Auto pause timer, or a drain window).
const reconcileRequeue = 10 * time.Second

// ReconcileInputs is the subset of state the canary executor needs. It mirrors
// coordination.ReconcileInputs. PerRevisionPods is observed serving pod counts
// keyed by revision hash for the canary's component (populated by the
// controller). DesiredReplicas is the component's desired replica count. Now is
// the reconcile clock (injectable for tests).
type ReconcileInputs struct {
	Client             client.Client
	ISVC               *v1beta1.InferenceService
	Component          v1beta1.ComponentType
	CanaryRevisionHash string
	StableRevisionHash string
	DesiredReplicas    int32
	PerRevisionPods    map[string]int32
	Now                time.Time
}

// Result reports the executor's decision for one reconcile. Active=false means
// no canary is in progress. Partition is the StatefulSet-style
// RollingUpdate.Partition the controller applies to the canary's component
// (instances < Partition are held on the stable revision). RequeueAfter > 0
// asks the controller to re-reconcile (capacity / pause / drain pending).
type Result struct {
	Active       bool
	Complete     bool
	Partition    int32
	RequeueAfter time.Duration
}

// Reconcile advances the canary toward the declared plan, mutating isvc.Status
// in-memory. No-op (Active=false) when spec.rollout.canary is unset. Per step it
// performs capacity → traffic → pause, advancing on promotion; the final step
// (TrafficWeight 100) drains and scales the stable revision down.
func Reconcile(ctx context.Context, in ReconcileInputs) (*Result, error) {
	plan := in.ISVC.Spec.GetCanary()
	if plan == nil || len(plan.Steps) == 0 {
		return &Result{Active: false}, nil
	}

	// Rollback takes precedence over normal stepping.
	if isRollbackRequested(in.ISVC) {
		return reconcileRollback(in), nil
	}

	// Initialize the state machine on first sight of this canary.
	if in.ISVC.Status.Canary == nil {
		in.ISVC.Status.Canary = &v1beta1.CanaryStatus{
			CanaryRevisionHash: in.CanaryRevisionHash,
			CurrentStep:        0,
			StepEnteredTime:    &metav1.Time{Time: in.Now},
		}
	}
	cs := in.ISVC.Status.Canary
	if int(cs.CurrentStep) >= len(plan.Steps) {
		cs.CurrentStep = int32(len(plan.Steps) - 1) // safety clamp
	}
	step := plan.Steps[cs.CurrentStep]

	newCount, _ := resolveStepCapacity(step, in.DesiredReplicas)
	partition := partitionForNewCount(in.DesiredReplicas, newCount)

	// Capacity gate: don't shift traffic onto canary pods that aren't Ready yet.
	if in.PerRevisionPods[in.CanaryRevisionHash] < newCount {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePending)
		return &Result{Active: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
	}

	// Capacity satisfied: program this step's external traffic weight.
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, in.StableRevisionHash, step.TrafficWeight)

	// Final step (TrafficWeight 100): drain the stable revision, then complete.
	if int(cs.CurrentStep) == len(plan.Steps)-1 {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePromoting)
		if drainElapsed(cs, plan, in.Now) {
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			clearCanaryStatus(in.ISVC)
			return &Result{Active: true, Complete: true, Partition: 0}, nil
		}
		return &Result{Active: true, Partition: 0, RequeueAfter: reconcileRequeue}, nil
	}

	// Intermediate step: serving a split. Honor the pause, else advance.
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseCanarying)
	if step.Pause != nil && !shouldAdvance(in.ISVC, plan, cs, step, in.Now) {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
		return &Result{Active: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
	}
	advanceStep(in.ISVC, in.Now)
	return &Result{Active: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
}

// reconcileRollback shifts all traffic back to the stable revision and scales
// the canary revision to zero (Partition = DesiredReplicas holds every instance
// on the stable revision). It completes once the canary pods have drained away.
func reconcileRollback(in ReconcileInputs) *Result {
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, in.StableRevisionHash, 0)
	if in.PerRevisionPods[in.CanaryRevisionHash] > 0 {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseRollingBack)
		return &Result{Active: true, Partition: in.DesiredReplicas, RequeueAfter: reconcileRequeue}
	}
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
	clearCanaryStatus(in.ISVC)
	delete(in.ISVC.Annotations, constants.RolloutRollbackAnnotation)
	return &Result{Active: true, Complete: true, Partition: in.DesiredReplicas}
}

// drainElapsed reports whether the ScaleDownDelaySeconds drain window has passed
// since the final step was entered. A nil/zero delay completes immediately.
func drainElapsed(cs *v1beta1.CanaryStatus, plan *v1beta1.CanarySpec, now time.Time) bool {
	if plan.ScaleDownDelaySeconds == nil || *plan.ScaleDownDelaySeconds <= 0 {
		return true
	}
	if cs.StepEnteredTime == nil {
		return false
	}
	deadline := cs.StepEnteredTime.Time.Add(time.Duration(*plan.ScaleDownDelaySeconds) * time.Second)
	return !now.Before(deadline)
}
