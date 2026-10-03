package rolloutrun

import (
	"context"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

func readyOn(index int32, running string) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{Index: index, Phase: workloadtypes.InstancePhaseReady, RunningRevision: running}
}

func patchingTo(index int32, running, pinned string) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{
		Index:           index,
		Phase:           workloadtypes.InstancePhaseUpdating,
		RunningRevision: running,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           workloadtypes.UpdateStepInPlace,
			TargetRevision: pinned,
		},
	}
}

// TestRevertAtTheLastInPlaceInstanceClosesTheRunSuperseded: a rolling
// group rolls four Instances in place. With three Ready on the new
// revision and the last mid-patch, the spec returns to the old revision.
// The Component's current revision is withdrawn while Instances still run
// the superseded one, so the run that targeted it closes as superseded
// and a fresh run pinned at the old revision carries the group back; once
// every Instance is Ready on the old revision that run closes Completed
// and nothing reopens.
func TestRevertAtTheLastInPlaceInstanceClosesTheRunSuperseded(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{},
	})
	// current mirrors the IR controller's rollup: it follows
	// CurrentRevisionFor over the rows the pass publishes.
	current := oldRev
	pass := func(t *testing.T, target string, rows []workloadtypes.InstanceStatus) {
		t.Helper()
		current = workloadstatus.CurrentRevisionFor(rows, current, target)
		local := isvc.DeepCopy()
		local.ObjectMeta.ResourceVersion = ""
		if _, err := Reconcile(context.Background(), testInputs(t, local, irPublished(t, current, target, rows))); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		isvc.Status = local.Status
	}
	active := func() *v1beta1.RolloutRun {
		if isvc.Status.Rollout == nil {
			return nil
		}
		return isvc.Status.Rollout.ActiveRun
	}
	lastOutcome := func() v1beta1.RolloutRunOutcome {
		if isvc.Status.Rollout == nil || isvc.Status.Rollout.LastRun == nil {
			return ""
		}
		return isvc.Status.Rollout.LastRun.Outcome
	}
	runID := func(t *testing.T, step string) string {
		t.Helper()
		if active() == nil {
			t.Fatalf("%s: no run open", step)
		}
		return active().RunID
	}

	pass(t, oldRev, []workloadtypes.InstanceStatus{readyOn(0, oldRev), readyOn(1, oldRev), readyOn(2, oldRev), readyOn(3, oldRev)})
	if active() != nil {
		t.Fatalf("converged workload opened a run: %+v", active())
	}

	pass(t, newRev, []workloadtypes.InstanceStatus{patchingTo(0, oldRev, newRev), readyOn(1, oldRev), readyOn(2, oldRev), readyOn(3, oldRev)})
	rolling := runID(t, "roll to the new revision")
	if got := active().TargetRevisions[0].Revision; got != query.RevisionFromName(newRev).Hash() {
		t.Fatalf("roll pinned %q, want the new revision %q", got, query.RevisionFromName(newRev).Hash())
	}

	pass(t, newRev, []workloadtypes.InstanceStatus{readyOn(0, newRev), readyOn(1, newRev), readyOn(2, newRev), patchingTo(3, oldRev, newRev)})
	if got := runID(t, "at the last Instance"); got != rolling {
		t.Fatalf("the roll changed runs before any retarget: got %s want %s", got, rolling)
	}

	// The revert lands with the last Instance mid-patch: its attempt is
	// re-aimed at the old revision while the other three still run the
	// new one.
	pass(t, oldRev, []workloadtypes.InstanceStatus{readyOn(0, newRev), readyOn(1, newRev), readyOn(2, newRev), patchingTo(3, oldRev, oldRev)})
	if got := lastOutcome(); got != v1beta1.RolloutRunSuperseded {
		t.Fatalf("the run that targeted the new revision closed %q, want %q", got, v1beta1.RolloutRunSuperseded)
	}
	reverting := runID(t, "revert at the last Instance")
	if reverting == rolling {
		t.Fatalf("the revert kept the run that targeted the new revision open: %s", reverting)
	}
	if got := active().TargetRevisions[0].Revision; got != query.RevisionFromName(oldRev).Hash() {
		t.Fatalf("the fresh run pinned %q, want the old revision %q", got, query.RevisionFromName(oldRev).Hash())
	}

	// The group comes back one Instance at a time under the fresh run.
	pass(t, oldRev, []workloadtypes.InstanceStatus{patchingTo(0, newRev, oldRev), readyOn(1, newRev), readyOn(2, newRev), readyOn(3, oldRev)})
	pass(t, oldRev, []workloadtypes.InstanceStatus{readyOn(0, oldRev), patchingTo(1, newRev, oldRev), readyOn(2, newRev), readyOn(3, oldRev)})
	pass(t, oldRev, []workloadtypes.InstanceStatus{readyOn(0, oldRev), readyOn(1, oldRev), patchingTo(2, newRev, oldRev), readyOn(3, oldRev)})
	if got := runID(t, "rolling back"); got != reverting {
		t.Fatalf("the rollback changed runs mid-way: got %s want %s", got, reverting)
	}

	for i := 0; i < 3; i++ {
		pass(t, oldRev, []workloadtypes.InstanceStatus{readyOn(0, oldRev), readyOn(1, oldRev), readyOn(2, oldRev), readyOn(3, oldRev)})
		if active() != nil {
			t.Fatalf("pass %d: a converged rollback keeps a run open: %+v", i, active())
		}
		if got := lastOutcome(); got != v1beta1.RolloutRunCompleted {
			t.Fatalf("pass %d: the fresh run closed %q, want %q", i, got, v1beta1.RolloutRunCompleted)
		}
	}
}
