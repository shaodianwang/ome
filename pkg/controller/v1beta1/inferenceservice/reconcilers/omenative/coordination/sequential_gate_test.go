package coordination

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// --- activeSequentialComponent (pure helper shared with state machine) ---

func TestActiveSequentialComponent_PicksFirstInFlight(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: true},
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: false},
	}
	got, ok := activeSequentialComponent(order, components)
	if !ok {
		t.Fatal("expected ok=true when first in Order is rolling")
	}
	if got != v1beta1.DecoderComponent {
		t.Errorf("got %q want decoder", got)
	}
}

func TestActiveSequentialComponent_SkipsCompletedToFindRolling(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: false},
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: true},
	}
	got, ok := activeSequentialComponent(order, components)
	if !ok {
		t.Fatal("expected ok=true when later-in-Order is rolling")
	}
	if got != v1beta1.EngineComponent {
		t.Errorf("got %q want engine", got)
	}
}

func TestActiveSequentialComponent_NoneInFlightReturnsFalse(t *testing.T) {
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	components := map[v1beta1.ComponentType]ComponentObservation{
		v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: false},
		v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: false},
	}
	if _, ok := activeSequentialComponent(order, components); ok {
		t.Errorf("expected ok=false when no Component is rolling")
	}
}

func TestActiveSequentialComponent_RespectsOrderNotMapIteration(t *testing.T) {
	// Both Components rolling — should pick decoder because it's first
	// in Order, regardless of map iteration order.
	order := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
	for i := 0; i < 100; i++ {
		components := map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {Component: v1beta1.DecoderComponent, RolloutInFlight: true},
			v1beta1.EngineComponent:  {Component: v1beta1.EngineComponent, RolloutInFlight: true},
		}
		got, ok := activeSequentialComponent(order, components)
		if !ok || got != v1beta1.DecoderComponent {
			t.Fatalf("iteration %d: got=%q ok=%v want decoder/true", i, got, ok)
		}
	}
}

// --- CheckSequentialGate ---

// mkSequentialFixture returns an ISVC declared with one Sequential
// coordination group [decoder, engine]. Caller can then mutate
// Status.Components and Status.RolloutCoordination to model the
// scenario under test.
func mkSequentialFixture(name string, soak time.Duration) *v1beta1.InferenceService {
	// v2: Sequential-across-Components is a RUN of single-Component
	// blueGreen groups, one per Component, in list order. ResolveGroups
	// collapses that run back into a single Sequential ResolvedGroup whose
	// Order is the Components in list order (and whose Soak is the max of
	// the per-group soaks).
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
	if soak > 0 {
		// Inter-Component soak rides on the group(s); collapseSequential
		// takes the max across the run.
		isvc.Spec.Rollout.Groups[0].Soak = &metav1.Duration{Duration: soak}
	}
	isvc.Name = name
	return isvc
}

// setComponentRevisions stamps Status.Components[c].Lifecycle with
// ControllerRevision names that produce the desired RolloutInFlight
// answer (target!=current ⇒ in flight). Revision-name shape mirrors
// what coordination.revisionHashFromName parses:
// <isvcName>-<component>-<hash>.
func setComponentRevisions(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, currentHash, targetHash string) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.CurrentRevision = isvc.Name + "-" + string(c) + "-" + currentHash
	cs.Lifecycle.UpdateRevision = isvc.Name + "-" + string(c) + "-" + targetHash
	isvc.Status.Components[c] = cs
}

// setComponentObservedGeneration stamps
// Status.Components[c].Lifecycle.ObservedGeneration with `gen`. The
// AggregateAndWriteStatus deferred write copies isvc.Generation onto
// every Component's ObservedGeneration at the end of each reconcile,
// so simulating a moment-of-bump means setting Status.* to N while
// leaving isvc.Generation at N+1.
func setComponentObservedGeneration(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, gen int64) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.ObservedGeneration = gen
	isvc.Status.Components[c] = cs
}

// setComponentAvailableAt stamps Status.Components[c].Lifecycle with
// an Available=True condition whose LastTransitionTime is `at` — the
// timestamp shape the Sequential soak gate now consumes. Mirrors what
// the OMENative status reconciler writes via
// apimeta.SetStatusCondition when AvailableReplicas==Replicas.
func setComponentAvailableAt(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, at time.Time) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	cs.Lifecycle.Conditions = append(cs.Lifecycle.Conditions, metav1.Condition{
		Type:               conditionAvailable,
		Status:             metav1.ConditionTrue,
		Reason:             "Ready",
		LastTransitionTime: metav1.NewTime(at),
	})
	isvc.Status.Components[c] = cs
}

func TestCheckSequentialGate_NilISVC(t *testing.T) {
	allowed, _ := CheckSequentialGate(nil, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("nil ISVC should bypass the gate")
	}
}

func TestCheckSequentialGate_NoCoordBlock(t *testing.T) {
	allowed, _ := CheckSequentialGate(&v1beta1.InferenceService{}, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("no coord block should bypass the gate")
	}
}

func TestCheckSequentialGate_ComponentNotInAnyGroup(t *testing.T) {
	isvc := mkSequentialFixture("seq-isvc", 0)
	// Router isn't in the [decoder, engine] group.
	allowed, reason := CheckSequentialGate(isvc, v1beta1.RouterComponent)
	if !allowed {
		t.Errorf("component outside any coord group must bypass: %s", reason)
	}
}

func TestCheckSequentialGate_NonSequentialGroupBypasses(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("BlueGreen group must bypass Sequential gate: %s", reason)
	}
	if !strings.Contains(reason, "Sequential") {
		t.Errorf("reason should mention Sequential when bypassing: %s", reason)
	}
}

func TestCheckSequentialGate_IdleGroupBypasses(t *testing.T) {
	// Spec declares Sequential, but no Component has a target revision
	// — no rollout in flight, so the gate must bypass (the dispatcher
	// won't even call it if there's no work, but if it does, this is
	// the safe answer).
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev1")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("idle group must bypass: %s", reason)
	}
}

func TestCheckSequentialGate_ActiveComponentAllowed(t *testing.T) {
	// Decoder is rolling (target!=current); engine is idle.
	// Decoder IS the active Component — gate must allow.
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	allowed, reason := CheckSequentialGate(isvc, v1beta1.DecoderComponent)
	if !allowed {
		t.Errorf("decoder is active; gate must allow: %s", reason)
	}
}

func TestCheckSequentialGate_NonActiveComponentDenied(t *testing.T) {
	// Decoder is rolling; engine is also bumped but Sequential says
	// engine must wait. The gate must deny for engine and name the
	// active Component in the reason.
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must wait while decoder rolls: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name the active Component: %s", reason)
	}
	if !strings.Contains(reason, "Sequential waiting") {
		t.Errorf("denial reason should say 'Sequential waiting': %s", reason)
	}
}

func TestCheckSequentialGate_LaterComponentActiveDeniesEarlier(t *testing.T) {
	// Decoder is already on rev2 (completed). Engine is rolling
	// (rev1→rev2). Engine is the active Component now. If the
	// dispatcher asks about decoder again (e.g., scale-up adds another
	// decoder Instance), the gate must... wait. Decoder isn't rolling
	// per the revision-hash check, so the active selector returns
	// engine. Asked about decoder, the gate denies (engine has the
	// turn). This mirrors the state machine: once Sequential walks
	// past decoder, decoder cannot disrupt engine's rollout window.
	isvc := mkSequentialFixture("seq-isvc", 0)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	allowed, reason := CheckSequentialGate(isvc, v1beta1.DecoderComponent)
	if allowed {
		t.Errorf("decoder must defer to active engine: %s", reason)
	}
	if !strings.Contains(reason, "engine") {
		t.Errorf("denial reason should name engine: %s", reason)
	}
}

func TestCheckSequentialGate_SoakHoldsActiveComponent(t *testing.T) {
	// Decoder completed at T=now-1s (recorded in its Available
	// condition's LastTransitionTime); engine is now active (rolling).
	// Soak is 15s → gate must deny engine for the remaining ~14s.
	const soak = 15 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	setComponentAvailableAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-1*time.Second))
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("soak window must hold engine: %s", reason)
	}
	if !strings.Contains(reason, "Sequential.Soak") {
		t.Errorf("denial reason should mention Sequential.Soak: %s", reason)
	}
}

func TestCheckSequentialGate_SoakReleasesAfterDuration(t *testing.T) {
	// Same shape as the hold case but decoder's Available transition
	// is well past the soak window — gate allows.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	setComponentAvailableAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-soak-1*time.Second))
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("soak elapsed; gate must allow: %s", reason)
	}
}

func TestCheckSequentialGate_SoakNotArmedForFirstComponent(t *testing.T) {
	// Decoder (first in Order) is the active Component. No prior
	// Component completed → no soak to honor. Gate must allow even
	// with a configured soak.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	allowed, reason := CheckSequentialGate(isvc, v1beta1.DecoderComponent)
	if !allowed {
		t.Errorf("first Component must not be soak-held: %s", reason)
	}
}

func TestCheckSequentialGate_SoakWithoutPriorAvailableBypasses(t *testing.T) {
	// active==component AND a previous Component is recorded as
	// completed, BUT no Available condition has been written yet —
	// soak gate has no clock, must default to allow.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	// No Available condition on decoder.
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("missing prior Available must bypass soak: %s", reason)
	}
}

func TestCheckSequentialGate_SoakWithPriorAvailableFalseBypasses(t *testing.T) {
	// Decoder's Available condition is present but False (no rollout
	// has finished yet on the previous Component). The gate must NOT
	// fall back to time.Now() — return zero time so the soak doesn't
	// fire on a bogus clock.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	// Available=False on decoder.
	cs := isvc.Status.Components[v1beta1.DecoderComponent]
	cs.Lifecycle.Conditions = []metav1.Condition{{
		Type:               conditionAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             "NotAllAvailable",
		LastTransitionTime: metav1.Now(),
	}}
	isvc.Status.Components[v1beta1.DecoderComponent] = cs
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("Available=False must not arm soak clock: %s", reason)
	}
}

func TestCheckSequentialGate_PostDecoderConvergence_AllowsEngine(t *testing.T) {
	// End-to-end post-convergence reproduction of the dev-cluster stall
	// (image bf0641b9, /tmp/policy-validate-v2.sh seq):
	//
	//   - decoder rolled first and completed (revisions converged, the
	//     OMENative status reconciler promoted CurrentRevision and stamped
	//     Available=True). decoder is the previously-active Sequential
	//     Component and has been past its Available transition for well
	//     longer than the configured soak.
	//   - engine's per-reconcile AggregateAndWriteStatus deferred write
	//     advanced UpdateRevision to NEW, AND ObservedGeneration to
	//     isvc.Generation. CurrentRevision is still OLD because no engine
	//     pod has flipped onto the new revision yet (the gate has been
	//     denying it the whole time decoder was rolling).
	//
	// The gate's revision-skew signal pins engine as RolloutInFlight=true
	// (UpdateRev != CurrentRev). decoder shows RolloutInFlight=false
	// (revisions converged + ObservedGeneration caught up). active selector
	// returns engine; soak window has elapsed; the gate MUST allow engine
	// to fire its first per-Instance Update. If it doesn't, the dispatcher
	// returns RequeueAfter on the gate denial and the rollout stalls
	// indefinitely until an unrelated watch event happens to wake the
	// reconciler.
	const soak = 10 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	isvc.Generation = 2 // operator bumped both Components one generation ago

	// Decoder: fully converged. Revisions match, ObservedGeneration
	// caught up, Available=True well past the soak window.
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "NEW", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 2)
	setComponentAvailableAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-(soak + 5*time.Second)))

	// Engine: CurrentRevision still OLD (no pod yet on the new
	// revision), UpdateRevision already advanced to NEW by the deferred
	// status write, ObservedGeneration caught up.
	setComponentRevisions(isvc, v1beta1.EngineComponent, "OLD", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 2)

	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("post-decoder-convergence + soak elapsed: engine MUST be allowed, got denied: %s", reason)
	}
	if !strings.Contains(reason, "Sequential active Component") {
		t.Errorf("post-decoder allowance reason should signal active-Component path: %s", reason)
	}
}

func TestCheckSequentialGate_SoakWindowRespected(t *testing.T) {
	// Companion to PostDecoderConvergence_AllowsEngine: SAME ISVC shape
	// (decoder converged, engine pending), but the previous Component's
	// Available transition is WITHIN the soak window. The gate must deny
	// engine until soak elapses; the dispatcher's RequeueAfter cycles the
	// reconcile every few seconds and the gate flips to allow as soon as
	// time.Since(decoderAvailable) >= group.Soak.
	const soak = 30 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	isvc.Generation = 2

	setComponentRevisions(isvc, v1beta1.DecoderComponent, "NEW", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 2)
	// Decoder just became Available 2s ago — well inside the 30s soak.
	setComponentAvailableAt(isvc, v1beta1.DecoderComponent, time.Now().Add(-2*time.Second))

	setComponentRevisions(isvc, v1beta1.EngineComponent, "OLD", "NEW")
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 2)

	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("within-soak-window: engine MUST be denied, got allowed: %s", reason)
	}
	if !strings.Contains(reason, "Sequential.Soak") {
		t.Errorf("within-soak denial reason should mention 'Sequential.Soak': %s", reason)
	}
}

func TestCheckSequentialGate_MomentOfBump(t *testing.T) {
	// Reproduces the bug observed on the dev cluster (image fb0bb7e2,
	// gap=0s between decoder/engine creationTimestamps when both spec
	// annotations are bumped in the same operator action):
	//
	// At the moment of bump, isvc.Generation has incremented (N → N+1)
	// but no Component's deferred AggregateAndWriteStatus has run yet,
	// so:
	//   - UpdateRevision still names the OLD ControllerRevision for
	//     BOTH Components (the new target.Name lives only in the
	//     reconciler's local `target` variable, not in status).
	//   - ObservedGeneration is still N on every Component's status
	//     block.
	//
	// Pre-fix: revision-hash skew is silent (UpdateRevision ==
	// CurrentRevision == OLD) for both Components → activeSequentialComponent
	// sees nothing rolling → gate falls through to "Sequential group
	// idle" → both Components dispatch concurrently, blowing the
	// Sequential invariant.
	//
	// Post-fix: generation skew (ObservedGeneration=N <
	// isvc.Generation=N+1) flips RolloutInFlight=true on every
	// Component whose status hasn't caught up. activeSequentialComponent
	// picks decoder (first in Order); engine is correctly denied.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 2 // operator's spec bump just landed
	// Both Components carry stale status: UpdateRevision points at the
	// pre-bump CR, ObservedGeneration is one behind isvc.Generation.
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev1")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 1)
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 1)

	// Engine must wait — decoder is first in Order and the gen-skew
	// signal pins decoder as the active Sequential Component.
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must be gated at moment-of-bump: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name the active Component (decoder): %s", reason)
	}
	if !strings.Contains(reason, "Sequential waiting") {
		t.Errorf("denial reason should say 'Sequential waiting': %s", reason)
	}

	// Decoder is the active Sequential Component — gate must allow.
	if allowed, reason := CheckSequentialGate(isvc, v1beta1.DecoderComponent); !allowed {
		t.Errorf("decoder is the active Component at moment-of-bump: %s", reason)
	}
}

func TestCheckSequentialGate_GenerationSkewOnlyOneComponent(t *testing.T) {
	// Defensive: only decoder's status is stale (engine already
	// caught up in a prior reconcile, e.g. a partial bump on a
	// previously-converged group). Decoder's gen-skew alone is enough
	// to pin it as the active Sequential Component; engine must defer.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 5
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev1", "rev1")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev1")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 4) // stale
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 5)  // caught up

	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("engine must wait while decoder has gen-skew: %s", reason)
	}
	if !strings.Contains(reason, "decoder") {
		t.Errorf("denial reason should name decoder: %s", reason)
	}
}

func TestCheckSequentialGate_GenerationCaughtUpAndIdleBypasses(t *testing.T) {
	// Once every Component's ObservedGeneration matches isvc.Generation
	// AND revisions are converged, the gate must bypass (no rollout in
	// flight anywhere). This is the steady-state read; without it the
	// fix would wedge the gate on a converged ISVC.
	isvc := mkSequentialFixture("seq-isvc", 0)
	isvc.Generation = 7
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev2", "rev2")
	setComponentObservedGeneration(isvc, v1beta1.DecoderComponent, 7)
	setComponentObservedGeneration(isvc, v1beta1.EngineComponent, 7)

	if allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent); !allowed {
		t.Errorf("converged group must bypass gate: %s", reason)
	}
	if allowed, reason := CheckSequentialGate(isvc, v1beta1.DecoderComponent); !allowed {
		t.Errorf("converged group must bypass gate: %s", reason)
	}
}

func TestCheckSequentialGate_NoOrderBypasses(t *testing.T) {
	// Defensive: a 2-Component group with no Order set resolves to a
	// (non-Sequential) blueGreen group — the v2 spec can no longer express
	// "Sequential without Order" (Sequential is a run of single-Component
	// groups the controller collapses with a derived Order). The gate must
	// still bypass cleanly without panic / wedge.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
					// Order intentionally empty.
				}},
			},
		},
	}
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("order-less group must not wedge dispatcher: %s", reason)
	}
}

// emitSequentialDeferredEvent: when an operator bumps a later-in-Order
// Component's spec while an earlier Component is still rolling,
// Sequential semantics defer the later rollout and the controller
// must emit SequentialNextSpecBumpDeferred so the operator understands
// why the spec change isn't visible yet.

func TestEmitSequentialDeferred_NotSequentialPolicy(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicyBlueGreen,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("BlueGreen policy must not emit Sequential events; got %v", got)
	}
}

func TestEmitSequentialDeferred_NoComponentRolling(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: false},
			v1beta1.EngineComponent:  {RolloutInFlight: false},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("no active component → no deferred event; got %v", got)
	}
}

func TestEmitSequentialDeferred_ActiveButNoLaterBump(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: false, CurrentRevisionHash: "rev1"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("active decoder + idle engine → no deferred event; got %v", got)
	}
}

func TestEmitSequentialDeferred_FiresWhenLaterComponentAlsoBumped(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	// Decoder is active (RolloutInFlight). Engine was ALSO bumped
	// while decoder is rolling → Sequential defers engine. Expect one
	// deferred event for engine.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	got := drainEvents(rec)
	if len(got) != 1 {
		t.Fatalf("want exactly 1 deferred event; got %d: %v", len(got), got)
	}
	if !contains(got[0], EventReasonSequentialNextSpecBumpDeferred) {
		t.Errorf("event reason mismatch: %s", got[0])
	}
	if !contains(got[0], "engine") {
		t.Errorf("event should name later Component (engine): %s", got[0])
	}
	if !contains(got[0], "decoder") {
		t.Errorf("event should name active Component (decoder): %s", got[0])
	}
}

func TestEmitSequentialDeferred_MultipleLaterComponentsFireMultipleEvents(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent},
	}
	// Decoder active, both engine + router also bumped → 2 deferred events.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.RouterComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	emitSequentialDeferredEvent(rec, isvc, g, obs, "")
	got := drainEvents(rec)
	if len(got) != 2 {
		t.Fatalf("want 2 deferred events (engine + router); got %d: %v", len(got), got)
	}
}

func TestEmitSequentialDeferred_NilRecorder(t *testing.T) {
	// Must not panic on nil recorder.
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true},
			v1beta1.EngineComponent:  {RolloutInFlight: true},
		},
	}
	emitSequentialDeferredEvent(nil, &v1beta1.InferenceService{}, g, obs, "")
}

func TestEmitSequentialDeferred_EmptyOrder(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  nil,
	}
	emitSequentialDeferredEvent(rec, isvc, g, GroupObservation{Group: g}, "")
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("empty Order → no events; got %v", got)
	}
}

// =============================================================================
// Test helpers
// =============================================================================

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	n := len(sub)
	for i := 0; i+n <= len(s); i++ {
		if s[i:i+n] == sub {
			return i
		}
	}
	return -1
}

func TestEmitSequentialDeferred_DedupWhenActiveUnchanged(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
	}
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	// prevActive=decoder matches current activeComponent → dedup suppresses.
	emitSequentialDeferredEvent(rec, isvc, g, obs, v1beta1.DecoderComponent)
	if got := drainEvents(rec); len(got) != 0 {
		t.Errorf("dedup: same active Component must suppress re-emit; got %v", got)
	}
}

func TestEmitSequentialDeferred_FiresWhenActiveAdvances(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Name:   "0",
		Policy: v1beta1.CoordinationPolicySequential,
		Order:  []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent, v1beta1.RouterComponent},
	}
	// Decoder done; engine now active; router deferred.
	obs := GroupObservation{
		Group: g,
		Components: map[v1beta1.ComponentType]ComponentObservation{
			v1beta1.DecoderComponent: {RolloutInFlight: false, CurrentRevisionHash: "rev2"},
			v1beta1.EngineComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
			v1beta1.RouterComponent:  {RolloutInFlight: true, TargetRevisionHash: "rev2"},
		},
	}
	// prevActive=decoder; now activeComponent=engine → dedup clears, emit.
	emitSequentialDeferredEvent(rec, isvc, g, obs, v1beta1.DecoderComponent)
	got := drainEvents(rec)
	if len(got) != 1 {
		t.Fatalf("active advanced (decoder→engine) → expect re-emit for router; got %d: %v", len(got), got)
	}
	if !contains(got[0], "router") {
		t.Errorf("event should name router as deferred: %s", got[0])
	}
}

// Force corev1 import (used indirectly via record).
var _ = corev1.EventTypeWarning

// ResolveGateContext is the once-per-tick prelude helper backing all
// Check* gates. These tests pin the four short-circuit paths and the
// resolved-group success path; the per-gate behavior is already covered
// by the existing ratio_test.go / sequential_gate_test.go suites that
// continue to exercise the public CheckRatioGate / CheckSurgeGate /
// CheckUnavailabilityGate / CheckSequentialGate wrappers.

func TestResolveGateContext_NilISVC(t *testing.T) {
	ctx := ResolveGateContext(nil, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("nil ISVC: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "nil ISVC" {
		t.Errorf("nil ISVC: want ShortReason=nil ISVC, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("no coord block: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "no coordination groups" {
		t.Errorf("no coord block: want ShortReason=no coordination groups, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_ComponentNotInAnyGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("not in any group: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "component not in any coord group" {
		t.Errorf("not in any group: want ShortReason=component not in any coord group, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_ResolvesGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
						BlueGreen:  &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)
	if ctx.ShortCircuit {
		t.Fatalf("resolved group: want ShortCircuit=false, got %+v", ctx)
	}
	if ctx.Group.Policy != v1beta1.CoordinationPolicyBlueGreen {
		t.Errorf("resolved group: want Policy=BlueGreen, got %q", ctx.Group.Policy)
	}
	if len(ctx.Group.Components) != 2 {
		t.Errorf("resolved group: want 2 Components, got %d", len(ctx.Group.Components))
	}
	if ctx.ISVC != isvc {
		t.Errorf("resolved group: ISVC pointer not threaded through")
	}
	if ctx.Component != v1beta1.EngineComponent {
		t.Errorf("resolved group: Component not threaded through, got %q", ctx.Component)
	}
}

// TestGateContextCheckRatio_ShortCircuitsAllowed pins the
// short-circuit path on the GateContext-aware variant — the dispatcher
// reads the (allowed=true, reason) tuple even when the ISVC has no
// coord block. Behavior parity with CheckRatioGate's "nil ISVC" /
// "no coordination block" / "component not in any coord group"
// branches is what makes the wrapper safe to keep as a 1-liner.
func TestGateContextCheckRatio_ShortCircuitsAllowed(t *testing.T) {
	ctx := GateContext{
		ShortCircuit: true,
		ShortReason:  "no coordination block",
	}
	if allowed, reason := ctx.CheckRatio(0, -1); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit: got (allowed=%v, reason=%q), want (true, no coordination block)", allowed, reason)
	}
	if allowed, reason := ctx.CheckSurge(0); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit surge: got (allowed=%v, reason=%q)", allowed, reason)
	}
	if allowed, reason := ctx.CheckUnavailability(0); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit unavail: got (allowed=%v, reason=%q)", allowed, reason)
	}
	if allowed, reason := ctx.CheckSequential(); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit sequential: got (allowed=%v, reason=%q)", allowed, reason)
	}
}
