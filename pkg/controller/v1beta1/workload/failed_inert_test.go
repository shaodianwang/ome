package workload_test

// A row parked at Failed holds no attempt, so most of what an operator
// can change mid-flight has no reader on it. These tests pin that: the
// row, its RetryBlock and the cluster come out of the pass exactly as
// they went in, whichever knob moved.

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
)

// failedRowTarget is the revision the parked row is held on.
const failedRowTarget = "llama-70b-engine-target01"

// failedRowFixture is a disposed fresh start: one Instance stamped
// Failed with no operation, held on its target revision by a Held
// RetryBlock, and no pods. Without the block the Create pass would
// re-materialize it, so the block is what keeps the row parked for the
// length of a pass.
func failedRowFixture(t *testing.T) (workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *testAtomicMutationStore, *[]string) {
	t.Helper()
	in := minimalInput(t)
	in.ObservedState.UpdateRevision = failedRowTarget
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseFailed, PodCount: 1},
	}
	in.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: failedRowTarget, State: workload.RetryBlockHeld},
	}
	removed := recordRetryBlockRemovals(&in)
	store := installTestAtomicMutationStore(&in)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: failedRowTarget}}
	return in, minimalPlan(), target, store, removed
}

// runFailedRowPass reconciles the fixture once and asserts the row came
// out untouched: no status write, still Failed with no operation, no
// pod materialized, and its block intact.
func runFailedRowPass(t *testing.T, in workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, store *testAtomicMutationStore, removed *[]string) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	if _, err := workload.Reconcile(context.Background(), workload.Deps{Client: c}, in, plan, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if store.writes != 0 {
		t.Errorf("status writes: got %d want 0", store.writes)
	}
	got := store.status(0)
	if got == nil || got.Phase != workload.InstancePhaseFailed || got.Operation != nil {
		t.Errorf("row: got %+v want Failed with no operation", got)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(in.Key.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("pods: got %d want none; the Held block denies the fresh start", len(pods.Items))
	}
	if len(*removed) != 0 {
		t.Errorf("RetryBlock removals: got %v want none; the row's block is for a live revision", *removed)
	}
}

// TestFailedRow_OperatorConfigChangesLeaveItParked: the requeue cadence,
// the gang schedule clamp and the migration audit caps are all consulted
// by work in flight — a wake-up to schedule, a PodGroup to build, a
// migration to admit. A Failed row has none of those, so changing any of
// them reaches it with no reader.
//
// Cells: instance/Failed/config.changed[requeue]/before, instance/Failed/config.changed[gangScheduleTimeout]/before, instance/Failed/config.changed[audit]/before
func TestFailedRow_OperatorConfigChangesLeaveItParked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*workload.ReconcileInput, *workload.ComponentPlan)
	}{
		{
			name: "requeue cadence",
			change: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.Requeue = workload.RequeueIntervals{Operation: 90 * time.Second, Gate: 45 * time.Second}
			},
		},
		{
			name: "gang schedule clamp",
			change: func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.GangScheduleTimeout = &workload.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
			},
		},
		{
			name: "migration audit caps",
			change: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.MigrationAudit = &workload.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 2, Window: time.Hour}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, plan, target, store, removed := failedRowFixture(t)
			tc.change(&in, &plan)
			runFailedRowPass(t, in, plan, target, store, removed)
		})
	}
}

// TestFailedRow_ExcludedAnnotationEditDoesNotRedriveIt: an edit to an
// annotation the revision payload filters out leaves the hash where it
// was, so the row's target does not move and the pass has nothing new to
// act on. The hash check is half the claim — without it, a pass that
// wrote nothing would only mean the edit never reached the renderer.
//
// Cells: instance/Failed/spec.annotationOnly/before
func TestFailedRow_ExcludedAnnotationEditDoesNotRedriveIt(t *testing.T) {
	in, plan, target, store, removed := failedRowFixture(t)
	before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
	after := &metav1.ObjectMeta{Annotations: map[string]string{
		"ome.io/base-model-name":                   "llama-7b",
		constants.ReleaseHeldRevisionAnnotationKey: failedRowTarget,
	}}
	hBefore, _, err := revision.Hash(in.DesiredSpec.PodSpec, before, nil, "")
	if err != nil {
		t.Fatalf("hash before the edit: %v", err)
	}
	hAfter, _, err := revision.Hash(in.DesiredSpec.PodSpec, after, nil, "")
	if err != nil {
		t.Fatalf("hash after the edit: %v", err)
	}
	if hBefore != hAfter {
		t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
	}

	in.DesiredSpec.PodTemplateObjectMeta = after
	runFailedRowPass(t, in, plan, target, store, removed)
}

// TestFailedRow_SupersedePruneKeepsTheBlockItWaitsOn: the end-of-pass
// prune is what bounds persisted RetryBlock history, and it is scoped to
// revisions nothing targets anymore. However much history it drops, the
// block holding this row — which is for a live revision — is never a
// candidate, and the row itself is not written either way.
//
// Cells: instance/Failed/config.changed[retryBlockHistoryLimit]/before
func TestFailedRow_SupersedePruneKeepsTheBlockItWaitsOn(t *testing.T) {
	in, plan, target, store, removed := failedRowFixture(t)
	in.ObservedState.RetryBlocks = append(in.ObservedState.RetryBlocks,
		workload.RetryBlock{TargetRevision: "llama-70b-engine-stale001", State: workload.RetryBlockHeld},
		workload.RetryBlock{TargetRevision: "llama-70b-engine-stale002", State: workload.RetryBlockBackoff},
	)

	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	if _, err := workload.Reconcile(context.Background(), workload.Deps{Client: c}, in, plan, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	want := []string{"llama-70b-engine-stale001", "llama-70b-engine-stale002"}
	if len(*removed) != len(want) {
		t.Fatalf("pruned: got %v want %v", *removed, want)
	}
	for i, rev := range want {
		if (*removed)[i] != rev {
			t.Fatalf("pruned: got %v want %v", *removed, want)
		}
	}
	if store.writes != 0 {
		t.Errorf("status writes: got %d want 0", store.writes)
	}
	if got := store.status(0); got == nil || got.Phase != workload.InstancePhaseFailed || got.Operation != nil {
		t.Errorf("row: got %+v want Failed with no operation", got)
	}
}
