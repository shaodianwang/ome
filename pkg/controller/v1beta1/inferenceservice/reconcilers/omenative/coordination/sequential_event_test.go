package coordination

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// emitSequentialDeferredEvent: when an operator bumps a later-in-Order
// Component's spec while an earlier Component is still rolling,
// Sequential semantics defer the later rollout and the controller
// must emit SequentialNextSpecBumpDeferred so the operator understands
// why the spec change isn't visible yet.

func TestEmitSequentialDeferred_NotSequentialPolicy(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	isvc := &v1beta1.InferenceService{}
	g := ResolvedGroup{
		Policy: v1beta1.CoordinationPolicyAtomic,
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
		t.Errorf("Atomic policy must not emit Sequential events; got %v", got)
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
