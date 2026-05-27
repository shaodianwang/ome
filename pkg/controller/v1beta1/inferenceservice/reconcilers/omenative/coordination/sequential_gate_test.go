package coordination

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicySequential,
					Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				}},
			},
		},
	}
	if soak > 0 {
		isvc.Spec.RolloutCoordination.Groups[0].Soak = &metav1.Duration{Duration: soak}
	}
	isvc.Name = name
	return isvc
}

// setComponentRevisions stamps Status.Components[c].OMENative with
// ControllerRevision names that produce the desired RolloutInFlight
// answer (target!=current ⇒ in flight). Revision-name shape mirrors
// what coordination.revisionHashFromName parses:
// <isvcName>-<component>-<hash>.
func setComponentRevisions(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, currentHash, targetHash string) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[c]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}
	cs.OMENative.CurrentRevision = isvc.Name + "-" + string(c) + "-" + currentHash
	cs.OMENative.UpdateRevision = isvc.Name + "-" + string(c) + "-" + targetHash
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
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
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
	// Decoder completed at T=now-2s (recorded in
	// LastTransitionTime), engine is now active (rolling). Soak is 5s
	// → gate must deny engine for the remaining ~3s.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	twoAgo := metav1.NewTime(time.Now().Add(-2 * time.Second))
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name:               "0", // ResolveGroups names index-only groups "0", "1", ...
			LastTransitionTime: &twoAgo,
		}},
	}
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if allowed {
		t.Errorf("soak window must hold engine: %s", reason)
	}
	if !strings.Contains(reason, "Sequential.Soak") {
		t.Errorf("denial reason should mention Sequential.Soak: %s", reason)
	}
}

func TestCheckSequentialGate_SoakReleasesAfterDuration(t *testing.T) {
	// Same shape as the hold case but LastTransitionTime is 10s ago
	// for a 5s soak — soak has elapsed, gate allows.
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	tenAgo := metav1.NewTime(time.Now().Add(-10 * time.Second))
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name:               "0",
			LastTransitionTime: &tenAgo,
		}},
	}
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
	now := metav1.Now()
	isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{{
			Name:               "0",
			LastTransitionTime: &now,
		}},
	}
	allowed, reason := CheckSequentialGate(isvc, v1beta1.DecoderComponent)
	if !allowed {
		t.Errorf("first Component must not be soak-held: %s", reason)
	}
}

func TestCheckSequentialGate_SoakWithoutPriorStatusBypasses(t *testing.T) {
	// active==component AND a previous Component is recorded as
	// completed, BUT no prior status was written yet — soak gate has
	// no clock, must default to allow (matches the state machine's
	// "PreviousPhaseEnteredAt.IsZero() => no hold" branch).
	const soak = 5 * time.Second
	isvc := mkSequentialFixture("seq-isvc", soak)
	setComponentRevisions(isvc, v1beta1.DecoderComponent, "rev2", "rev2")
	setComponentRevisions(isvc, v1beta1.EngineComponent, "rev1", "rev2")
	// No Status.RolloutCoordination written.
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("missing prior status must bypass soak: %s", reason)
	}
}

func TestCheckSequentialGate_NoOrderBypasses(t *testing.T) {
	// Defensive: Sequential without an Order is shape-invalid (the
	// validator catches it), but the gate should not panic / wedge —
	// bypass with a noisy reason.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicySequential,
					// Order intentionally empty.
				}},
			},
		},
	}
	allowed, reason := CheckSequentialGate(isvc, v1beta1.EngineComponent)
	if !allowed {
		t.Errorf("Sequential without Order must not wedge dispatcher: %s", reason)
	}
}
