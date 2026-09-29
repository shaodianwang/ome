package canary

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func statusWithRecord(cs *v1beta1.CanaryStatus, phase v1beta1.RolloutPhase) *v1beta1.InferenceServiceStatus {
	return &v1beta1.InferenceServiceStatus{
		Canary: cs.DeepCopy(),
		Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Canary: cs.DeepCopy(), RolloutPhase: phase},
		},
	}
}

func engineRecord(s *v1beta1.InferenceServiceStatus) *v1beta1.CanaryStatus {
	return s.Components[v1beta1.EngineComponent].Canary
}

// A pass that read the current record writes its decision.
func TestBaseKeepsADecisionMadeFromTheCurrentRecord(t *testing.T) {
	read := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePaused)
	base := NewBase(read)
	desired := read.DeepCopy()
	engineRecord(desired).RolledBackRevisionHash = "new"
	live := read.DeepCopy()

	if base.PreserveFresh(desired, live) || base.Stale() {
		t.Fatal("a decision made from the current record was treated as stale")
	}
	if engineRecord(desired).RolledBackRevisionHash != "new" {
		t.Fatalf("the pass's decision was dropped: %+v", engineRecord(desired))
	}
}

// A pass that read a stale copy keeps the live record, phase and traffic,
// and reports it so the pass decides again from the current record.
func TestBaseKeepsTheLiveRecordOverAStalePass(t *testing.T) {
	stale := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePaused)
	base := NewBase(stale)
	live := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new", RolledBackRevisionHash: "new"}, v1beta1.RolloutPhaseRollingBack)
	entry := live.Components[v1beta1.EngineComponent]
	entry.Traffic = []v1beta1.ComponentTrafficTarget{{RevisionName: "stable", Percent: 100}}
	live.Components[v1beta1.EngineComponent] = entry
	desired := stale.DeepCopy()
	// The stale pass stepped the canary it believed was still live.
	engineRecord(desired).CurrentStep = 1
	desired.Canary.CurrentStep = 1
	entry = desired.Components[v1beta1.EngineComponent]
	entry.RolloutPhase = v1beta1.RolloutPhasePending
	entry.Lifecycle = &v1beta1.LifecycleStatus{ReadyReplicas: 3}
	desired.Components[v1beta1.EngineComponent] = entry

	if !base.PreserveFresh(desired, live) || !base.Stale() {
		t.Fatal("a decision made from a stale record was not detected")
	}
	got := desired.Components[v1beta1.EngineComponent]
	if got.Canary.RolledBackRevisionHash != "new" || got.Canary.CurrentStep != 0 || got.RolloutPhase != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the live record was not kept: %+v phase=%q", got.Canary, got.RolloutPhase)
	}
	if len(got.Traffic) != 1 || got.Traffic[0].Percent != 100 {
		t.Fatalf("the live traffic was not kept: %+v", got.Traffic)
	}
	if desired.Canary.RolledBackRevisionHash != "new" || desired.Canary.CurrentStep != 0 {
		t.Fatalf("the live alias was not kept: %+v", desired.Canary)
	}
	if got.Lifecycle == nil || got.Lifecycle.ReadyReplicas != 3 {
		t.Fatalf("state the executor does not own was touched: %+v", got.Lifecycle)
	}
}

// A live record that appeared after the pass read no record is newer too.
func TestBaseKeepsARecordThePassNeverRead(t *testing.T) {
	base := NewBase(&v1beta1.InferenceServiceStatus{})
	live := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePending)
	desired := &v1beta1.InferenceServiceStatus{}

	if !base.PreserveFresh(desired, live) {
		t.Fatal("a record the pass never read was overwritten")
	}
	if engineRecord(desired) == nil || engineRecord(desired).CanaryRevisionHash != "new" || desired.Canary == nil {
		t.Fatalf("the live record was not kept: %+v", desired)
	}
}

// The state a write persisted is the base of the pass's later decisions.
func TestBaseAdvancesToTheWrittenState(t *testing.T) {
	read := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "old", RolledBackRevisionHash: "old"}, v1beta1.RolloutPhaseRolledBack)
	base := NewBase(read)
	written := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePending)
	base.Advance(written)
	desired := written.DeepCopy()
	engineRecord(desired).CurrentStep = 1

	if base.PreserveFresh(desired, written.DeepCopy()) || base.Stale() {
		t.Fatal("a decision made from the state this pass wrote was treated as stale")
	}
	if engineRecord(desired).CurrentStep != 1 {
		t.Fatalf("the pass's decision was dropped: %+v", engineRecord(desired))
	}
}

// Without a base nothing is guarded.
func TestNilBaseGuardsNothing(t *testing.T) {
	var base *Base
	desired := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new"}, v1beta1.RolloutPhasePaused)
	live := statusWithRecord(&v1beta1.CanaryStatus{CanaryRevisionHash: "new", RolledBackRevisionHash: "new"}, v1beta1.RolloutPhaseRollingBack)
	if base.PreserveFresh(desired, live) || base.Stale() {
		t.Fatal("a nil base must not guard")
	}
	if engineRecord(desired).RolledBackRevisionHash != "" {
		t.Fatal("a nil base must not touch the status")
	}
}
