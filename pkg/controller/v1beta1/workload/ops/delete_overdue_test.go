package ops

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// overdueDrainStatus builds a Delete-owned row whose drain deadline has
// already elapsed at overdueDrainNow.
func overdueDrainStatus(index int32, deadline time.Time) workload.InstanceStatus {
	status := deleteOwnedStatus(index, deadline.Add(-time.Minute))
	status.Operation.Deadline = metav1.NewTime(deadline)
	return status
}

// overdueDrainTerminatingPod is a pod of index already on its way out.
func overdueDrainTerminatingPod(index int32, deletedAt time.Time) *corev1.Pod {
	pod := deleteBatchPod(index)
	stamp := metav1.NewTime(deletedAt)
	pod.DeletionTimestamp = &stamp
	pod.Finalizers = []string{"ome.io/test-hold"}
	return pod
}

// overdueDrainInput wires a delete pass whose clock sits at now.
func overdueDrainInput(statuses []workload.InstanceStatus, now time.Time) workload.ReconcileInput {
	input := deleteBatchInput(deleteBatchOwner(), statuses)
	input.Clock = clocktesting.NewFakeClock(now)
	return input
}

func overdueDrainEvents(rec *record.FakeRecorder) []string {
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

// An elapsed drain deadline announces itself once and changes nothing
// else: the row keeps its Deleting phase, the delete operation and the
// index, and the pod is not force-deleted. The second pass sees the
// record and stays silent.
//
// Cells: instance/Delete/Drain/timer.operationDeadline/before
func TestDeleteBatchOverdueDrainAnnouncesOncePerEpisode(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	deadline := t0.Add(time.Minute)
	now := deadline.Add(3 * time.Minute)
	statuses := []workload.InstanceStatus{overdueDrainStatus(1, deadline)}
	pod := overdueDrainTerminatingPod(1, t0)
	pods := map[int32][]*corev1.Pod{1: {pod}}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
	store := newDeleteMutationStore(deleteBatchOwner(), statuses)
	rec := record.NewFakeRecorder(16)
	expectations := workload.NewExpectations()

	var events []string
	for pass := 0; pass < 2; pass++ {
		input := overdueDrainInput(statuses, now)
		input.ApplyInstanceMutationsWithRetryBlock = store.apply
		result, err := DeleteBatch(context.Background(),
			workload.Deps{Client: c, Recorder: rec, Expectations: expectations},
			input, deleteBatchPlan(), nil, pods)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if !result.InProgress {
			t.Fatalf("pass %d: the wave must keep the index: %+v", pass, result)
		}
		events = append(events, overdueDrainEvents(rec)...)
	}

	overdue := 0
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonDrainOverdue)) {
			overdue++
			if !strings.Contains(e, "instance=1") || !strings.Contains(e, "3m0s") ||
				!strings.Contains(e, pod.Name) || !strings.Contains(e, "still Terminating") {
				t.Errorf("event does not name the Instance, the pods and how long overdue: %q", e)
			}
		}
	}
	if overdue != 1 {
		t.Fatalf("DrainOverdue events = %d, want 1 (events=%v)", overdue, events)
	}
	row := store.statuses[1]
	if row.Phase != workload.InstancePhaseDeleting || !deleteOwned(row) {
		t.Fatalf("the row must stay Deleting under its delete operation: %+v", row)
	}
	if row.LastFailure == nil || row.LastFailure.Reason != workload.DrainOverdueReason ||
		!row.LastFailure.Time.Equal(&row.Operation.Deadline) || row.LastFailure.PodName != pod.Name {
		t.Fatalf("announcement record = %+v", row.LastFailure)
	}
	if store.writes != 1 {
		t.Fatalf("status writes = %d, want 1 (the second pass must write nothing)", store.writes)
	}
	live := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("the announcement must not delete the pod: %v", err)
	}
}

// A drain inside its deadline says nothing.
//
// Cells: instance/Delete/Drain/timer.operationDeadline/before
func TestDeleteBatchDrainWithinDeadlineStaysSilent(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	deadline := t0.Add(time.Hour)
	statuses := []workload.InstanceStatus{overdueDrainStatus(1, deadline)}
	pod := overdueDrainTerminatingPod(1, t0)
	pods := map[int32][]*corev1.Pod{1: {pod}}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
	store := newDeleteMutationStore(deleteBatchOwner(), statuses)
	rec := record.NewFakeRecorder(16)
	input := overdueDrainInput(statuses, t0.Add(time.Minute))
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	if _, err := DeleteBatch(context.Background(),
		workload.Deps{Client: c, Recorder: rec, Expectations: workload.NewExpectations()},
		input, deleteBatchPlan(), nil, pods); err != nil {
		t.Fatal(err)
	}
	if events := overdueDrainEvents(rec); len(events) != 0 {
		t.Fatalf("events = %v, want none", events)
	}
	if store.statuses[1].LastFailure != nil {
		t.Fatalf("record = %+v, want none", store.statuses[1].LastFailure)
	}
}

// A fresh delete operation is a fresh episode: a row readmitted under a
// new deadline announces again rather than reading the stale record as
// "already told".
//
// Cells: instance/Delete/Drain/timer.operationDeadline/before
func TestOverdueDrainReannouncesOnANewDeadline(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	first := metav1.NewTime(t0)
	second := metav1.NewTime(t0.Add(time.Hour))
	status := overdueDrainStatus(1, second.Time)
	status.LastFailure = &workload.InstanceTermination{Reason: workload.DrainOverdueReason, Time: first}
	pods := []*corev1.Pod{overdueDrainTerminatingPod(1, t0)}

	if _, ok := overdueDrain(status, pods, second.Add(time.Minute)); !ok {
		t.Fatal("a new deadline must open a new episode")
	}
	status.LastFailure.Time = second
	if _, ok := overdueDrain(status, pods, second.Add(time.Minute)); ok {
		t.Fatal("the announced episode must not repeat")
	}
}

// The pass ignores rows it does not own: a row with no pods left is
// completing, and a non-Delete operation is another pass's business.
//
// Cells: instance/Delete/Drain/timer.operationDeadline/before
func TestOverdueDrainIgnoresRowsItDoesNotOwn(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	now := t0.Add(time.Hour)
	pods := []*corev1.Pod{overdueDrainTerminatingPod(1, t0)}

	if _, ok := overdueDrain(overdueDrainStatus(1, t0), nil, now); ok {
		t.Error("a row with no pods left is completing, not overdue")
	}
	updating := overdueDrainStatus(1, t0)
	updating.Phase = workload.InstancePhaseUpdating
	updating.Operation.Type = workload.InstanceOperationUpdate
	if _, ok := overdueDrain(updating, pods, now); ok {
		t.Error("a non-Delete operation belongs to the escalation pass")
	}
	undated := overdueDrainStatus(1, t0)
	undated.Operation.Deadline = metav1.Time{}
	if _, ok := overdueDrain(undated, pods, now); ok {
		t.Error("an operation with no deadline never expires")
	}
}

// The pod budget bounds how much deleting runs at once, not what the
// operator is told: a delete-owned row deferred out of this pass's wave
// is announced past its deadline like any other.
//
// Cells: instance/Delete/Drain/timer.operationDeadline/before
func TestDeleteBatchOverdueDrainAnnouncesDeferredRows(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	deadline := t0.Add(time.Minute)
	now := deadline.Add(2 * time.Minute)
	statuses := []workload.InstanceStatus{overdueDrainStatus(1, deadline), overdueDrainStatus(2, deadline)}
	// The oldest row is selected first, so index 2 is the deferred one.
	statuses[0].Operation.StartedAt = metav1.NewTime(t0)
	statuses[1].Operation.StartedAt = metav1.NewTime(t0.Add(time.Second))
	first, second := overdueDrainTerminatingPod(1, t0), overdueDrainTerminatingPod(2, t0)
	pods := map[int32][]*corev1.Pod{1: {first}, 2: {second}}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(first, second).Build()
	store := newDeleteMutationStore(deleteBatchOwner(), statuses)
	rec := record.NewFakeRecorder(16)
	budget := int32(1)
	input := overdueDrainInput(statuses, now)
	input.ScaleDownPodBatchSize = &budget
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(),
		workload.Deps{Client: c, Recorder: rec, Expectations: workload.NewExpectations()},
		input, deleteBatchPlan(), nil, pods)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deferred != 1 {
		t.Fatalf("deferred = %d, want 1 — the fixture must defer a row out of the wave", result.Deferred)
	}
	for _, index := range []int32{1, 2} {
		row := store.statuses[index]
		if row.LastFailure == nil || row.LastFailure.Reason != workload.DrainOverdueReason {
			t.Errorf("instance %d record = %+v, want the overdue announcement", index, row.LastFailure)
		}
	}
	overdue := 0
	for _, e := range overdueDrainEvents(rec) {
		if strings.Contains(e, string(workload.EventReasonDrainOverdue)) {
			overdue++
		}
	}
	if overdue != 2 {
		t.Fatalf("DrainOverdue events = %d, want 2 (the driven row and the deferred one)", overdue)
	}
}
