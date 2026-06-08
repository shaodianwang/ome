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

// failedRequeue is the slow heartbeat once a canary step is escalated to Failed —
// the rollout is parked (stable keeps serving) until the operator acts, so there
// is no need to tight-poll.
const failedRequeue = 5 * time.Minute

// defaultReadyTimeout is how long a step's capacity gate may stay unsatisfied
// before the canary is marked Failed, when the operator hasn't overridden it via
// constants.RolloutReadyTimeoutAnnotation. Matches that annotation's documented
// default.
const defaultReadyTimeout = 15 * time.Minute

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
	// SecondaryCapacityReady is true when every NON-primary component's canary
	// pods have reached that component's step newCount (PD). The primary's own
	// capacity is checked via PerRevisionPods. Single-component canaries pass true.
	SecondaryCapacityReady bool
	Now                    time.Time
}

// Result reports the executor's decision for one reconcile. Active=false means
// no canary is in progress. Partition is the StatefulSet-style
// RollingUpdate.Partition the controller applies to the canary's component
// (instances < Partition are held on the stable revision). RequeueAfter > 0
// asks the controller to re-reconcile (capacity / pause / drain pending).
type Result struct {
	Active   bool
	Complete bool
	// Stepped is true on the reconcile that advanced to the next (intermediate)
	// step — the edge the step counter + step Event are recorded on.
	Stepped bool
	// RolledBack is true while the canary is rolling back / held rolled-back; the
	// controller reads it (and status.canary.RolledBackRevisionHash) to point the
	// IR at the stable ControllerRevision.
	RolledBack   bool
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

	cs := in.ISVC.Status.Canary

	// Rollback (ome.io/rollout-rollback): abandon the canary and return the
	// component to the stable revision, then HOLD there rejecting the rolled-back
	// revision. The revert itself is driven by the controller — seeing
	// cs.RolledBackRevisionHash it points the IR at the stable ControllerRevision
	// so every Instance rolls back. Here we record the rejected revision, shift
	// traffic to stable, and re-arm only when a different target appears (clearing
	// the annotation alone never retries the rejected revision).
	if cs != nil {
		if cs.RolledBackRevisionHash != "" {
			// Re-arm to a fresh canary ONLY when a genuinely NEW target appears.
			// During the revert the IR targets the stable revision, so the
			// observed target (in.CanaryRevisionHash) reports stable — that is the
			// revision we're holding on, not a new target. Exclude both the stable
			// revision and the rejected revision itself.
			stableHash := otherRevision(in.PerRevisionPods, cs.RolledBackRevisionHash)
			if in.CanaryRevisionHash != "" && in.CanaryRevisionHash != cs.RolledBackRevisionHash && in.CanaryRevisionHash != stableHash {
				*cs = v1beta1.CanaryStatus{
					CanaryRevisionHash: in.CanaryRevisionHash,
					CurrentStep:        0,
					StepEnteredTime:    &metav1.Time{Time: in.Now},
				}
				delete(in.ISVC.Annotations, constants.RolloutRollbackAnnotation)
				// fall through: start a fresh canary toward the new target.
			} else {
				return reconcileRollback(in, cs), nil
			}
		} else if isRollbackRequested(in.ISVC) && int(cs.CurrentStep) < len(plan.Steps) {
			cs.RolledBackRevisionHash = cs.CanaryRevisionHash
			return reconcileRollback(in, cs), nil
		}
	}

	// Done sentinel: a finished canary KEEPS its status with CurrentStep ==
	// len(steps) rather than clearing it. EffectivePartition maps that to the
	// final 100% step (partition 0, all instances on the canary revision); a nil
	// status would instead re-default to step 0's partition and hold instances
	// back on the old revision after completion. No-op unless a new target
	// appears, in which case start a fresh canary.
	if cs != nil && int(cs.CurrentStep) >= len(plan.Steps) {
		if in.CanaryRevisionHash != "" && in.CanaryRevisionHash != cs.CanaryRevisionHash {
			cs.CurrentStep = 0
			cs.CanaryRevisionHash = in.CanaryRevisionHash
			cs.StepEnteredTime = &metav1.Time{Time: in.Now}
			cs.ObservedTrafficWeight = 0
		} else {
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			return &Result{Active: false}, nil
		}
	}

	// Mid-rollout retarget: a NEW target revision appeared after traffic already
	// shifted (e.g. the operator bumped the image again mid-canary). Restart the
	// staged plan against the new target — otherwise the recorded hash goes stale
	// and, under Manual, the operator's promote of the new hash would never match.
	// The step-0 pre-traffic lag window (UpdateRevision briefly behind) is handled
	// by the hash re-sync below, not here.
	if cs != nil && int(cs.CurrentStep) < len(plan.Steps) &&
		in.CanaryRevisionHash != "" && in.CanaryRevisionHash != cs.CanaryRevisionHash &&
		(cs.CurrentStep > 0 || cs.ObservedTrafficWeight > 0) {
		cs.CurrentStep = 0
		cs.CanaryRevisionHash = in.CanaryRevisionHash
		cs.StepEnteredTime = &metav1.Time{Time: in.Now}
		cs.ObservedTrafficWeight = 0
	}

	// Initialize the state machine on first sight of a NEW canary. Don't start
	// one when the component is already fully converged on the target revision —
	// or when the target isn't known yet — so adding a canary to an
	// already-rolled-out ISVC is a no-op, not a phantom rollout.
	if cs == nil {
		if in.CanaryRevisionHash == "" || in.PerRevisionPods[in.CanaryRevisionHash] >= in.DesiredReplicas {
			if in.CanaryRevisionHash != "" && in.DesiredReplicas > 0 {
				setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			}
			return &Result{Active: false}, nil
		}
		in.ISVC.Status.Canary = &v1beta1.CanaryStatus{
			CanaryRevisionHash: in.CanaryRevisionHash,
			CurrentStep:        0,
			StepEnteredTime:    &metav1.Time{Time: in.Now},
		}
		cs = in.ISVC.Status.Canary
	}
	step := plan.Steps[cs.CurrentStep]

	newCount := resolveStepNewCount(step, in.DesiredReplicas)
	partition := partitionForNewCount(in.DesiredReplicas, newCount)

	// Re-sync the recorded canary hash to the live target while step 0 is still
	// waiting on capacity (before any traffic has shifted). The IR's
	// UpdateRevision can briefly lag the spec bump on the first reconcile, so the
	// hash captured at init may be the stale pre-roll revision; locking that for
	// the life of the rollout would mis-report the canary. Once traffic moves the
	// canary is committed, so this only re-syncs during the initial wait.
	if cs.CurrentStep == 0 && in.CanaryRevisionHash != "" && in.PerRevisionPods[in.CanaryRevisionHash] < newCount {
		cs.CanaryRevisionHash = in.CanaryRevisionHash
	}

	// Capacity gate: don't shift traffic until the canary pods are Ready — the
	// primary's own newCount AND every secondary component's canary capacity (PD,
	// so a Ready router can't advance the step while the engine/decoder canary
	// pods behind it are still coming up). If the gate stays unsatisfied past the
	// ready-timeout, escalate to Failed instead of polling Pending forever — the
	// stable revision keeps serving since no traffic has shifted.
	if in.PerRevisionPods[in.CanaryRevisionHash] < newCount || !in.SecondaryCapacityReady {
		if capacityGateExpired(cs, readyTimeoutOrDefault(in.ISVC), in.Now) {
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseFailed)
			return &Result{Active: true, Partition: partition, RequeueAfter: failedRequeue}, nil
		}
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePending)
		return &Result{Active: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
	}

	// Capacity satisfied: program this step's external traffic weight.
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, in.StableRevisionHash, step.TrafficWeight)

	// Final step (TrafficWeight 100): drain the stable revision, then complete.
	if int(cs.CurrentStep) == len(plan.Steps)-1 {
		// Anchor the drain window to the moment 100% traffic actually shifts
		// (capacity is met here — we are past the gate, applyTraffic just ran),
		// not to step entry. On slow capacity the final step is entered well
		// before traffic moves, so measuring the drain from step entry could
		// consume the whole window before cutover. Re-stamp once, on entering
		// Promoting.
		if in.ISVC.Status.Components[in.Component].RolloutPhase != v1beta1.RolloutPhasePromoting {
			cs.StepEnteredTime = &metav1.Time{Time: in.Now}
		}
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePromoting)
		if drainElapsed(cs, plan, in.Now) {
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			// Mark done (sentinel), don't clear: keeps EffectivePartition at
			// partition 0 so the old revision drains instead of being re-held by
			// a step-0 partition. ObservedTrafficWeight stays 100 (all on canary).
			cs.CurrentStep = int32(len(plan.Steps))
			cs.ObservedTrafficWeight = 100
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
	return &Result{Active: true, Stepped: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
}

// reconcileRollback drives the component back to the stable revision and holds
// there, rejecting cs.RolledBackRevisionHash. Traffic goes 100% to stable; the
// controller reads cs.RolledBackRevisionHash and makes the IR roll every Instance
// back to the stable ControllerRevision, so the rejected-revision pods drain.
// While they drain → RollingBack; once gone → RolledBack (held until a different
// target appears).
func reconcileRollback(in ReconcileInputs, cs *v1beta1.CanaryStatus) *Result {
	stableHash := otherRevision(in.PerRevisionPods, cs.RolledBackRevisionHash)
	applyTraffic(in.ISVC, in.Component, cs.RolledBackRevisionHash, stableHash, 0)
	if in.PerRevisionPods[cs.RolledBackRevisionHash] > 0 {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseRollingBack)
		return &Result{Active: true, RolledBack: true, RequeueAfter: reconcileRequeue}
	}
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseRolledBack)
	return &Result{Active: true, RolledBack: true}
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

// readyTimeoutOrDefault is how long a step's capacity gate may stay unsatisfied
// before the canary is marked Failed. Operators override via
// constants.RolloutReadyTimeoutAnnotation; a missing/malformed value falls back
// to defaultReadyTimeout (admission already rejects malformed values).
func readyTimeoutOrDefault(isvc *v1beta1.InferenceService) time.Duration {
	if v, ok := isvc.Annotations[constants.RolloutReadyTimeoutAnnotation]; ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultReadyTimeout
}

// capacityGateExpired reports whether the current step has waited on capacity
// past the timeout. A nil status / missing step-entered time / non-positive
// timeout is never treated as expired, so a transient nil cannot wrongly fail a
// rollout.
func capacityGateExpired(cs *v1beta1.CanaryStatus, timeout time.Duration, now time.Time) bool {
	if cs == nil || cs.StepEnteredTime == nil || timeout <= 0 {
		return false
	}
	return !now.Before(cs.StepEnteredTime.Time.Add(timeout))
}
