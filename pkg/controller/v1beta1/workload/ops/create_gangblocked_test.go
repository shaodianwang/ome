package ops

// Every pod of a multi-pod Instance is stamped with its gang's
// deterministic PodGroup name. When that name is held by another
// controller, or by an object still being collected, a member created
// against it would join a group this owner does not control — so the
// create choke point withholds the members and leaves the row to the
// escalation pass instead of failing the whole reconcile.

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// blockedGangFixture is the gang-surge fixture with its rejected member
// removed, so the pass reaches the create step, and the surge index
// classified as unusable.
func blockedGangFixture(t *testing.T, state workload.GangState) *gangRejectionFixture {
	t.Helper()
	legacyResetExpectations(t)
	f := newGangRejectionFixture(t, "OutOfmemory")
	if err := f.client.Delete(context.Background(), f.dead); err != nil {
		t.Fatalf("clear the pre-existing surge pod: %v", err)
	}
	f.input.Gangs = workload.NewGangObservations()
	f.input.Gangs.Record(f.surgeIndex, workload.GangObservation{
		Name:    query.PodGroupName(f.isvcName, workload.ComponentEngine, f.surgeIndex),
		State:   state,
		Message: "PodGroup is not usable by this owner",
	})
	return f
}

func gangPodCount(t *testing.T, f *gangRejectionFixture) int {
	t.Helper()
	pods := &corev1.PodList{}
	if err := f.client.List(context.Background(), pods, client.InNamespace(f.namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return len(pods.Items)
}

// TestGangSurgeCreate_BlockedPodGroupNameWithholdsMembers: with no
// PodGroup callback wired, the create choke point itself is the gate —
// the replacement gang gets no members and the pass reports no progress
// rather than an error.
//
// Cells: instance/Update/GangSurgeTarget/gang.podGroup[OwnershipConflict]/before
func TestGangSurgeCreate_BlockedPodGroupNameWithholdsMembers(t *testing.T) {
	f := blockedGangFixture(t, workload.GangStateOwnershipConflict)

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while its PodGroup name was unusable")
	}
	if got := gangPodCount(t, f); got != 0 {
		t.Fatalf("created %d members against a PodGroup name this owner cannot write", got)
	}
}

// TestGangSurgeCreate_BlockedPodGroupEnsureDefersToEscalation: the
// inline surge ensure reports the same collision as an error. That is
// classified evidence about one row, so the pass defers instead of
// taking every other Instance down with it.
//
// Cells: instance/Update/GangSurgeTarget/gang.podGroup[Terminating]/before
func TestGangSurgeCreate_BlockedPodGroupEnsureDefersToEscalation(t *testing.T) {
	f := blockedGangFixture(t, workload.GangStateTerminating)
	f.deps.EnsureGangPodGroup = func(context.Context, workload.ReconcileInput, workload.ComponentPlan, workload.InstancePlan) (string, error) {
		return "", fmt.Errorf("%w: PodGroup is terminating", workload.ErrGangNameUnusable)
	}

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: got %v want the blocked name deferred, not an error", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while its PodGroup name was still occupied")
	}
	if got := gangPodCount(t, f); got != 0 {
		t.Fatalf("created %d members while the PodGroup name was still occupied", got)
	}
}

// TestGangSurgeCreate_UnblockedGangStillCreates: the gate is scoped to
// the states that make a name unusable. A group this owner controls and
// can write gets its members, so the gate cannot quietly stall a healthy
// surge.
//
// Cells: instance/Update/GangSurgeTarget/gang.podGroup[Deleted]/before
func TestGangSurgeCreate_UnblockedGangStillCreates(t *testing.T) {
	f := blockedGangFixture(t, workload.GangStateNone)

	if _, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target); err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if got := gangPodCount(t, f); got == 0 {
		t.Fatal("a usable gang name must not withhold members")
	}
}
