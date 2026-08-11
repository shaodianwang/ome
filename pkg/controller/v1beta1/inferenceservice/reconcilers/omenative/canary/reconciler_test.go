package canary

import (
	"context"
	"testing"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func i32(v int32) *int32 { return &v }

// canaryISVC builds an ISVC whose rollout canaries the single engine Component
// (v2: one RolloutGroup with a canary progression over [engine]). Each step's own
// Pause/Analysis decides its gate (manual = bare pause, timed = pause+duration,
// metric-gated = analysis).
func canaryISVC(steps []v1beta1.RolloutGroupStep, scaleDownDelay *int32) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc"},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary: &v1beta1.GroupCanary{
				Steps:                 steps,
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC(steps, nil)
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
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}, i32(60))
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC(twoStep(), nil)
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
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0) // enter the capacity wait (anchors the timeout)
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("entering the capacity wait → Pending, got %q", phaseOf(isvc))
	}

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
// the plan from step 0 against the new revision (a subsequent spec bump),
// carrying NONE of the finished canary's analysis state.
func TestReconcile_DoneSentinelRestartsOnNewTarget(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	// Finished canary on the previous target (done sentinel: CurrentStep == 2).
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "old-canary",
		CurrentStep:           2,
		ObservedTrafficWeight: 100,
		AnalysisFailedChecks:  2,
		LastEvaluationTime:    &metav1.Time{Time: time.Unix(900, 0)},
		MetricResults:         []v1beta1.AnalysisMetricResult{{Name: "err", Passed: false}},
	}
	in := baseInputs(isvc, map[string]int32{"v3": 0}) // new target v3, its pods not up yet
	in.CanaryRevisionHash = "v3"
	res, _ := Reconcile(context.Background(), in)
	if !res.Active {
		t.Fatalf("a new target after completion must restart the canary (Active), got %+v", res)
	}
	cs := isvc.Status.Canary
	if cs.CurrentStep != 0 || cs.CanaryRevisionHash != "v3" {
		t.Fatalf("restart should reset to step 0 + the new hash, got %+v", cs)
	}
	if cs.AnalysisFailedChecks != 0 || cs.LastEvaluationTime != nil || cs.MetricResults != nil {
		t.Fatalf("restart must not carry the prior canary's analysis state, got %+v", cs)
	}
}

// TestReconcile_RetargetResetsAnalysisState pins the mid-rollout retarget: a new
// target revision restarts the plan with a FULL status reset. Carrying the prior
// canary's failure budget / metric results over would let a stale
// AnalysisFailedChecks roll back the new (healthy) revision on its first breach.
func TestReconcile_RetargetResetsAnalysisState(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:           "new",
		CurrentStep:                  0,
		ObservedTrafficWeight:        50, // traffic shifted → retarget (not the step-0 re-sync)
		StepEnteredTime:              &metav1.Time{Time: t0.Add(-time.Hour)},
		AnalysisFailedChecks:         2,
		LastEvaluationTime:           &metav1.Time{Time: t0.Add(-time.Minute)},
		LastConclusiveEvaluationTime: &metav1.Time{Time: t0.Add(-time.Minute)},
		MetricResults:                []v1beta1.AnalysisMetricResult{{Name: "err", Passed: false}},
	}
	in := baseInputs(isvc, map[string]int32{"v3": 0, "new": 2, "old": 2})
	in.CanaryRevisionHash = "v3"
	in.Now = t0
	Reconcile(context.Background(), in)
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "v3" || cs.CurrentStep != 0 || cs.ObservedTrafficWeight != 0 {
		t.Fatalf("retarget must restart at step 0 toward v3, got %+v", cs)
	}
	if cs.AnalysisFailedChecks != 0 || cs.LastEvaluationTime != nil || cs.LastConclusiveEvaluationTime != nil || cs.MetricResults != nil {
		t.Fatalf("retarget must not carry prior-canary analysis state, got %+v", cs)
	}
}

// TestReconcile_PromoteConsumptionPersisted pins that consuming the promote
// annotation is durable: the controller persists only the status subresource,
// so without an explicit metadata patch the annotation survives on the
// apiserver and one promote advances EVERY subsequent step of the canary.
func TestReconcile_PromoteConsumptionPersisted(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc := canaryISVC(steps, nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Client = c
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("promote should advance one step, got %d", isvc.Status.Canary.CurrentStep)
	}
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutPromoteAnnotation]; still {
		t.Fatal("promote consumption must be persisted to the apiserver, not just in-memory")
	}
	// A fresh pass over the reloaded (apiserver) annotations must hold at step 1.
	isvc.Annotations = stored.Annotations
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Canary.CurrentStep != 1 {
		t.Fatalf("one promote must advance one step only, got %d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_RollbackConsumptionPersistedOnRearm pins the rollback-annotation
// half of durable consumption: a re-arm toward a new target must remove
// ome.io/rollout-rollback on the apiserver — left in place, the reloaded
// annotation marks the fresh canary rolled back on its first pass.
func TestReconcile_RollbackConsumptionPersistedOnRearm(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = "ns"
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", RolledBackRevisionHash: "new", AnalysisFailedChecks: 3}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.Client = c
	in.CanaryRevisionHash = "v3"
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	cs := isvc.Status.Canary
	if cs.CanaryRevisionHash != "v3" || cs.RolledBackRevisionHash != "" || cs.AnalysisFailedChecks != 0 {
		t.Fatalf("re-arm must fully reset toward v3, got %+v", cs)
	}
	stored := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "svc"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, still := stored.Annotations[constants.RolloutRollbackAnnotation]; still {
		t.Fatal("rollback consumption must be persisted to the apiserver, not just in-memory")
	}
}

// TestReconcile_CapacityDipAfterLongBakeIsNotFailed pins the capacity-gate
// anchor: the ready-timeout measures the CURRENT capacity wait, not time since
// step entry. A step that has been serving (baking) longer than the timeout
// must re-enter Pending on a capacity dip — not park a healthy canary Failed
// on the first pass.
func TestReconcile_CapacityDipAfterLongBakeIsNotFailed(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	t0 := time.Unix(100000, 0)
	// Step 0 has been serving (Paused) for 20 min — past the 15 min default timeout.
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "new",
		CurrentStep:           0,
		ObservedTrafficWeight: 50,
		StepEnteredTime:       &metav1.Time{Time: t0.Add(-20 * time.Minute)},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePaused)

	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 2}) // canary pods just dipped
	in.Now = t0
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("capacity dip after a long bake → Pending (fresh wait), got %q", phaseOf(isvc))
	}
	// Still down a full ready-timeout after the dip → Failed.
	in.Now = t0.Add(16 * time.Minute)
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("capacity still down a full timeout after the dip → Failed, got %q", phaseOf(isvc))
	}
}

// TestReconcile_NegativeStepClamped guards the unvalidated status subresource:
// an externally-written negative CurrentStep must clamp to 0, not panic the
// reconcile indexing plan.Steps.
func TestReconcile_NegativeStepClamped(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: -3}
	res, err := Reconcile(context.Background(), baseInputs(isvc, map[string]int32{"new": 0}))
	if err != nil || res == nil {
		t.Fatalf("negative step must not panic: res=%+v err=%v", res, err)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("negative step should clamp to 0, got %d", isvc.Status.Canary.CurrentStep)
	}
}

// TestReconcile_ReadyTimeoutAnnotationOverride pins that an operator-shortened
// ready-timeout actually changes when Failed is entered.
func TestReconcile_ReadyTimeoutAnnotationOverride(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Annotations = map[string]string{constants.RolloutReadyTimeoutAnnotation: "2m"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0) // enter the capacity wait (anchors the timeout)
	Reconcile(context.Background(), in)
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

// TestReconcile_AnalysisFailedIsSticky pins the parked-terminal contract for the
// analysis path: once a step escalates to Failed (analysis stalled inconclusive
// past the ready-timeout), it stays Failed across reconciles — it does NOT
// oscillate back to Paused. The hold must not re-stamp StepEnteredTime (that
// would reset the stall clock the next pass reads as "not stalled," flipping to
// Paused, then re-failing a timeout later) nor re-sample. It re-arms only when a
// genuinely new target revision appears.
func TestReconcile_AnalysisFailedIsSticky(t *testing.T) {
	analysisStep := v1beta1.RolloutGroupStep{
		Capacity: intstr.FromString("50%"), Traffic: 50,
		Analysis: &v1beta1.RolloutAnalysis{
			Interval:     metav1.Duration{Duration: time.Minute},
			FailureLimit: 3,
			Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
		},
	}
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{analysisStep, {Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)

	t0 := time.Unix(100000, 0)
	// Seed the step already Canarying with both anchors 20 min in the past (> the
	// 15 min default ready-timeout) so the next inconclusive sample is past the
	// stall and escalates to Failed on this reconcile. Phase Canarying makes the
	// intermediate-step anchor skip its re-stamp, so the past anchors survive.
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:           "new",
		CurrentStep:                  0,
		StepEnteredTime:              &metav1.Time{Time: t0.Add(-20 * time.Minute)},
		LastConclusiveEvaluationTime: &metav1.Time{Time: t0.Add(-20 * time.Minute)},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)

	// First reconcile: inconclusive sample past the stall → Failed.
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Now = t0
	inconclusive := &countingSample{outcome: analysis.Inconclusive, at: t0}
	in.Sampler = inconclusive
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("analysis stalled past the ready-timeout must escalate to Failed, got %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("Failed should slow-requeue at %v, got %v", failedRequeue, res.RequeueAfter)
	}
	enteredAtFailure := isvc.Status.Canary.StepEnteredTime.Time

	// Second reconcile: same (unchanged) target, a later clock, and a sampler that
	// would PASS — under the oscillation bug the re-stamp + re-evaluate would flip
	// this to Paused (or advance). Sticky Failed must hold without re-sampling and
	// without re-stamping StepEnteredTime.
	in.Now = t0.Add(13 * time.Minute) // ready-timeout + stall would fire again on their cadence
	passing := &countingSample{outcome: analysis.Pass, at: in.Now}
	in.Sampler = passing
	res, _ = Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("Failed must be sticky, not oscillate to %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("held Failed should keep the slow heartbeat %v, got %v", failedRequeue, res.RequeueAfter)
	}
	if !isvc.Status.Canary.StepEnteredTime.Time.Equal(enteredAtFailure) {
		t.Fatalf("StepEnteredTime must not be re-stamped while held Failed: was %v, now %v", enteredAtFailure, isvc.Status.Canary.StepEnteredTime.Time)
	}
	if passing.calls != 0 {
		t.Fatalf("held Failed must not re-sample analysis, got %d calls", passing.calls)
	}
	if isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("held Failed must not advance the step, got %d", isvc.Status.Canary.CurrentStep)
	}

	// A NEW target re-arms a fresh canary (step 0, new hash) and leaves the parked
	// Failed phase behind.
	in.Now = t0.Add(20 * time.Minute)
	in.CanaryRevisionHash = "v3"
	in.PerRevisionPods = map[string]int32{"old": 4} // new target's pods not up yet
	in.Sampler = &countingSample{outcome: analysis.Pass, at: in.Now}
	Reconcile(context.Background(), in)
	if isvc.Status.Canary.CanaryRevisionHash != "v3" || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("a new target must re-arm a fresh canary toward v3, got %+v", isvc.Status.Canary)
	}
	if phaseOf(isvc) == v1beta1.RolloutPhaseFailed {
		t.Fatal("re-armed canary must leave the parked Failed phase, still Failed")
	}
}

// TestReconcile_CapacityGateFailedIsSticky pins the same parked-terminal contract
// for the capacity-gate Failed path: a step whose canary pods never become Ready
// escalates to Failed after the ready-timeout and then STAYS Failed. While held
// Failed the hold returns before the Pending anchor, so StepEnteredTime is never
// re-stamped — this guards the sticky hold (same slow heartbeat, no re-stamp, no
// re-arm) rather than regressing it.
func TestReconcile_CapacityGateFailedIsSticky(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", CurrentStep: 0, StepEnteredTime: &metav1.Time{Time: time.Unix(1000, 0)}}
	in := baseInputs(isvc, map[string]int32{"new": 0}) // canary pods never come up

	in.Now = time.Unix(1000, 0) // enter the capacity wait (anchors the timeout)
	Reconcile(context.Background(), in)
	in.Now = time.Unix(1000, 0).Add(16 * time.Minute) // past the 15m default
	Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("capacity gate past the ready-timeout → Failed, got %q", phaseOf(isvc))
	}
	enteredAtFailure := isvc.Status.Canary.StepEnteredTime.Time

	// Later reconcile, same target, pods still down: must stay Failed, not flip to
	// Pending, and must not re-stamp the entry time.
	in.Now = time.Unix(1000, 0).Add(40 * time.Minute)
	res, _ := Reconcile(context.Background(), in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("capacity-gate Failed must be sticky, not oscillate to %q", phaseOf(isvc))
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("held Failed should keep the slow heartbeat %v, got %v", failedRequeue, res.RequeueAfter)
	}
	if !isvc.Status.Canary.StepEnteredTime.Time.Equal(enteredAtFailure) {
		t.Fatalf("StepEnteredTime must not be re-stamped while held Failed: was %v, now %v", enteredAtFailure, isvc.Status.Canary.StepEnteredTime.Time)
	}
}
