package canary

import (
	"context"
	"testing"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func promPolicy(p v1beta1.PromotionPolicy) *v1beta1.PromotionPolicy { return &p }
func i32(v int32) *int32                                            { return &v }

// canaryISVC builds an ISVC whose rollout canaries the single engine Component
// (v2: one RolloutGroup with a canary progression over [engine]).
func canaryISVC(steps []v1beta1.RolloutGroupStep, policy *v1beta1.PromotionPolicy, scaleDownDelay *int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc"},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary: &v1beta1.GroupCanary{
				Steps:                 steps,
				Promotion:             policy,
				ScaleDownDelaySeconds: scaleDownDelay,
			},
		}}}},
	}
}

func twoStep() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

func phaseOf(isvc *v1beta1.InferenceService) v1beta1.RolloutPhase {
	return isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase
}

func baseInputs(isvc *v1beta1.InferenceService, pods map[string]int32) ReconcileInputs {
	return ReconcileInputs{
		ISVC:                   isvc,
		Component:              v1beta1.EngineComponent,
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		DesiredReplicas:        4,
		PerRevisionPods:        pods,
		SecondaryCapacityReady: true, // single-component tests have no secondaries
		Now:                    time.Unix(1000, 0),
	}
}

func TestReconcile_NoCanaryIsNoop(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	res, err := Reconcile(context.Background(), ReconcileInputs{ISVC: isvc})
	if err != nil || res == nil || res.Active {
		t.Fatalf("no canary must be inactive noop: res=%+v err=%v", res, err)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("no canary status should be written")
	}
}

func TestReconcile_CapacityGatePending(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0}))
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("expected Pending while canary pods not ready, got %q", phaseOf(isvc))
	}
	if res.Partition != 2 { // desired 4 - newCount 2
		t.Fatalf("expected Partition 2, got %d", res.Partition)
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("no traffic should shift while gated")
	}
}

func TestReconcile_CanaryingPaused(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("expected Paused at a manual paused step, got %q", phaseOf(isvc))
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 2 {
		t.Fatalf("expected 50/50 split, got %+v", tr)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("should not advance without promote, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_ManualPromoteAdvances(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("promote should advance to step 1, got %d", isvc.Status.Canary.CurrentStep)
	}
	if _, still := isvc.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("promote annotation should be consumed on advance")
	}
}

func TestReconcile_StalePromoteDoesNotAdvance(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "someoldhash"}
	Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2}))
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("stale promote (wrong hash) must not advance, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_AutoAdvance(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 30 * time.Minute}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc := canaryISVC(steps, promPolicy(v1beta1.PromotionPolicyAuto), nil)
	// The pause is anchored to when the split FIRST serves, not to step entry. Seed
	// StepEnteredTime 40 min in the past (> the 30 min pause) to mimic a slow capacity
	// wait: the step was "entered" long ago, but traffic only shifts now — the soak
	// must still run from now, so the canary must NOT advance immediately.
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: t0.Add(-40 * time.Minute)}}

	in := baseInputs(isvc, map[string]int32{"new": 2})
	in.Now = t0 // capacity met → split serves → pause re-anchored to t0, phase Paused
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("pause must anchor to the split serving, not the 40-min-old step entry; advanced too early, step=%d", isvc.Status.Canary.CurrentStep)
	}
	in.Now = t0.Add(10 * time.Minute) // before the pause elapses (from t0)
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("auto must not advance before the pause elapses, step=%d", isvc.Status.Canary.CurrentStep)
	}
	in.Now = t0.Add(31 * time.Minute) // after the pause elapses (from t0)
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("auto must advance after the pause elapses, step=%d", isvc.Status.Canary.CurrentStep)
	}
}

func TestReconcile_FinalStepCompletes(t *testing.T) {
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}, nil, nil)
	// Mid-rollout at the final step (canary already initialized): pods fully on
	// the target, no drain delay → completes. Starting fresh with pods already
	// converged is the "already done" case, not a completion (see
	// TestReconcile_AlreadyConvergedNoRestart).
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4}))
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("expected Stable on completion, got %q", phaseOf(isvc))
	}
	if !res.Complete || res.Partition != 0 {
		t.Fatalf("expected complete + partition 0, got %+v", res)
	}
	// Status is KEPT at the done sentinel (CurrentStep == len(steps)), not cleared:
	// EffectivePartition maps that to partition 0 so the old revision drains. A
	// cleared (nil) status would re-default EffectivePartition to step 0 and hold
	// instances back on the old revision after completion.
	if isvc.Status.Canary == nil || int(isvc.Status.Canary.CurrentStep) != 1 {
		t.Fatalf("canary status should be marked done (CurrentStep=len(steps)=1), got %+v", isvc.Status.Canary)
	}
	// A subsequent reconcile of the finished canary is a no-op — no re-completion.
	res2, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4}))
	if res2.Active || res2.Complete {
		t.Fatalf("finished canary must be a no-op on re-reconcile, got %+v", res2)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("finished canary should stay Stable, got %q", phaseOf(isvc))
	}
}

// TestReconcile_AlreadyConvergedNoRestart guards completion idempotency: once a
// canary finishes it clears Status.Canary, but the canary spec stays on the
// ISVC. A reconcile that then sees a nil status alongside a component already
// fully on the target revision must NOT re-initialize a fresh canary — otherwise
// the rollout loops forever (verified on KIND). A canary only starts when there
// is an actual revision change still to roll out.
func TestReconcile_AlreadyConvergedNoRestart(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 4}))
	if res.Active {
		t.Fatalf("converged component must not (re)start a canary, got %+v", res)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("no canary status should be initialized when already converged")
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("converged component should report Stable, got %q", phaseOf(isvc))
	}
}

// TestReconcile_UnknownTargetNoStart guards the IR-not-ready window: with no
// known target hash yet (empty), the canary must wait rather than initialize a
// state machine keyed on an empty revision.
func TestReconcile_UnknownTargetNoStart(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash = ""
	res, _ := Reconcile(context.Background(), in)
	if res.Active {
		t.Fatalf("unknown target must not start a canary, got %+v", res)
	}
	if isvc.Status.Canary != nil {
		t.Fatal("no canary status should be initialized before the target is known")
	}
}

func TestReconcile_FinalStepDrainWait(t *testing.T) {
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}, nil, i32(60))
	// StepEnteredTime here is the step-entry time; the drain window is anchored to
	// the moment traffic shifts (the first capacity-met reconcile), NOT to this.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}

	in := baseInputs(isvc, map[string]int32{"new": 4})
	// Capacity met here → traffic shifts → drain window starts NOW (t=2000), even
	// though the step was "entered" at t=1000.
	in.Now = time.Unix(2000, 0)
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("expected Promoting (draining), got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
	in.Now = time.Unix(2000, 0).Add(30 * time.Second) // within drain window (from t=2000)
	res, _ = Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("expected still Promoting within the drain window, got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
	in.Now = time.Unix(2000, 0).Add(61 * time.Second) // drain elapsed (from the traffic-shift moment)
	res, _ = Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable || !res.Complete {
		t.Fatalf("expected Stable + complete after drain, got phase=%q complete=%v", phaseOf(isvc), res.Complete)
	}
}

// TestReconcile_Rollback pins the rollback state machine: record the rejected
// revision, shift traffic to stable, RollingBack while the rejected pods drain →
// RolledBack (held), and re-arm only when a different target appears.
func TestReconcile_Rollback(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, ObservedTrafficWeight: 50}
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}

	// Mid-canary rollback: rejected ("new") pods still present → RollingBack,
	// 100% traffic to stable, the rejected hash recorded.
	res, _ := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 2, "old": 2}))
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("expected RollingBack + RolledBack result, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	if isvc.Status.Canary.RolledBackRevisionHash != "new" {
		t.Fatalf("rejected revision should be recorded, got %+v", isvc.Status.Canary)
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 1 || tr[0].Percent != 100 {
		t.Fatalf("rollback must route 100%% to stable, got %+v", tr)
	}

	// Rejected pods drained → RolledBack (held on stable).
	res, _ = Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"old": 4}))
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("expected RolledBack (held) after drain, got phase=%q res=%+v", phaseOf(isvc), res)
	}

	// Clearing the annotation does NOT retry the rejected revision (still held).
	delete(isvc.Annotations, constants.RolloutRollbackAnnotation)
	in := baseInputs(isvc, map[string]int32{"old": 4})
	res, _ = Reconcile(context.Background(), in)
	if !res.RolledBack || isvc.Status.Canary.RolledBackRevisionHash != "new" {
		t.Fatalf("rejected revision must stay held until a new target appears, got %+v res=%+v", isvc.Status.Canary, res)
	}

	// A NEW target re-arms: fresh canary toward it (rejected hash cleared).
	in = baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash = "v3"
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.RolledBackRevisionHash != "" || isvc.Status.Canary.CanaryRevisionHash != "v3" || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("new target must re-arm a fresh canary toward v3, got %+v", isvc.Status.Canary)
	}
}

// TestReconcile_SecondaryCapacityGate pins the PD gate: even when the primary's
// own canary pods are up, no traffic shifts until every secondary component's
// canary capacity is ready.
func TestReconcile_SecondaryCapacityGate(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	in := baseInputs(isvc, map[string]int32{"new": 2}) // primary capacity met
	in.SecondaryCapacityReady = false                  // a secondary's canary pods are not up
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("primary ready but a secondary not → Pending, got %q", phaseOf(isvc))
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("no traffic should shift until every component's canary capacity is up")
	}
}

// TestReconcile_CapacityGateFailsAfterTimeout pins the Failed escalation: a step
// whose canary pods never become Ready parks in Failed (stable still serving)
// after the ready-timeout instead of polling Pending forever.
func TestReconcile_CapacityGateFailsAfterTimeout(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0).Add(5 * time.Minute) // before the 15m default
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("before ready-timeout → Pending, got %q", phaseOf(isvc))
	}

	in.Now = time.Unix(1000, 0).Add(16 * time.Minute) // past the timeout
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("after ready-timeout → Failed, got %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("Failed should slow-requeue at %v, got %v", failedRequeue, res.RequeueAfter)
	}
	if len(isvc.Status.Components[v1beta1.EngineComponent].Traffic) != 0 {
		t.Fatal("no traffic should have shifted (stable keeps serving while Failed)")
	}
}

// TestReconcile_DoneSentinelRestartsOnNewTarget pins the other half of the
// done-sentinel branch: a finished canary that sees a NEW target hash restarts
// the plan from step 0 against the new revision (a subsequent spec bump).
func TestReconcile_DoneSentinelRestartsOnNewTarget(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	// Finished canary on the previous target (done sentinel: CurrentStep == 2).
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "old-canary", CurrentStep: 2, ObservedTrafficWeight: 100}
	in := baseInputs(isvc, map[string]int32{"v3": 0}) // new target v3, its pods not up yet
	in.CanaryRevisionHash = "v3"
	res, _ := Reconcile(context.Background(), in)
	if !res.Active {
		t.Fatalf("a new target after completion must restart the canary (Active), got %+v", res)
	}
	if isvc.Status.Canary.CurrentStep != 0 || isvc.Status.Canary.CanaryRevisionHash != "v3" {
		t.Fatalf("restart should reset to step 0 + the new hash, got %+v", isvc.Status.Canary)
	}
}

// TestReconcile_ReadyTimeoutAnnotationOverride pins that an operator-shortened
// ready-timeout actually changes when Failed is entered.
func TestReconcile_ReadyTimeoutAnnotationOverride(t *testing.T) {
	isvc := canaryISVC(twoStep(), promPolicy(v1beta1.PromotionPolicyManual), nil)
	isvc.Annotations = map[string]string{constants.RolloutReadyTimeoutAnnotation: "2m"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0).Add(90 * time.Second) // < 2m
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("before the 2m override → Pending, got %q", phaseOf(isvc))
	}
	in.Now = time.Unix(1000, 0).Add(121 * time.Second) // > 2m (well before the 15m default)
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("after the 2m override → Failed, got %q", phaseOf(isvc))
	}
}
