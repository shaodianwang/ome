package workload_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// crashLoopRepairRevision is the revision the wedged pods below carry;
// its hash matches the label enginePod stamps.
const crashLoopRepairRevision = "llama-70b-engine-priorrev"

// wedgedEnginePod is an engine pod parked in a terminal kubelet waiting
// reason for longer than any grace these fixtures configure.
func wedgedEnginePod(idx int32) *corev1.Pod {
	pod := enginePod("llama-70b", "prod", idx)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  constants.MainContainerName,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	}}
	return pod
}

// crashLoopRepairFixture wires n Ready, operation-free rows whose pods
// are all wedged on the current revision — the shape one bad argument
// or one bad image produces across a whole Component.
func crashLoopRepairFixture(t *testing.T, n int32, maxUnavailable *intstr.IntOrString) (workload.ReconcileInput, workload.ComponentPlan, []client.Object) {
	t.Helper()
	in := minimalInput(t)
	in.StuckPodGrace = time.Minute
	in.ObservedState.CurrentRevision = crashLoopRepairRevision
	in.DesiredSpec.Replicas = n
	plan := workload.ComponentPlan{
		Component:     workload.ComponentEngine,
		Replicas:      n,
		RestartPolicy: workload.RestartPolicyNone,
		UpdateStrategy: workload.UpdateStrategy{
			Type:          workload.UpdateStrategyRecreatePod,
			RollingUpdate: &workload.RollingUpdate{MaxUnavailable: maxUnavailable},
		},
	}
	var pods []client.Object
	for idx := int32(0); idx < n; idx++ {
		in.ObservedState.InstanceStatuses = append(in.ObservedState.InstanceStatuses, workload.InstanceStatus{
			Index: idx, Incarnation: 1, Phase: workload.InstancePhaseReady,
			PodCount: 1, RunningRevision: crashLoopRepairRevision,
		})
		plan.Instances = append(plan.Instances, workload.InstancePlan{
			Index: idx, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		})
		pods = append(pods, wedgedEnginePod(idx))
	}
	return in, plan, pods
}

// installRepairStore routes every status adapter the restart pass uses
// into one store, so a pass's repairs are observable as committed rows.
func installRepairStore(in *workload.ReconcileInput) *testAtomicMutationStore {
	store := installTestAtomicMutationStore(in)
	in.ApplyInstanceMutations = func(ctx context.Context, muts []workload.InstanceMutation) error {
		return store.apply(ctx, muts, "", nil)
	}
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		return store.apply(ctx, []workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	return store
}

// countRestarting reports how many rows the store holds in Restarting.
func countRestarting(store *testAtomicMutationStore, n int32) int {
	restarting := 0
	for idx := int32(0); idx < n; idx++ {
		if s := store.status(idx); s != nil && s.Phase == workload.InstancePhaseRestarting {
			restarting++
		}
	}
	return restarting
}

// A wedge is a property of the revision, so every Ready Instance
// qualifies for the crash-loop repair in the same pass. The
// unavailability budget paces it: one repair opens per pass, and the
// repair already in flight keeps the next pass from opening a second.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestReconcile_CrashLoopRepair_PacedByUnavailabilityBudget(t *testing.T) {
	ctx := context.Background()
	one := intstr.FromInt32(1)
	in, plan, pods := crashLoopRepairFixture(t, 3, &one)
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	deps := workload.Deps{Client: c, APIReader: c, Expectations: workload.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, updateTarget()); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if got := countRestarting(store, 3); got != 1 {
		t.Fatalf("repairs opened = %d, want 1 under a MaxUnavailable of 1", got)
	}

	// The admitted repair anchors the projection while it is in flight,
	// so the next pass opens none of its peers — whether or not the
	// first one finishes in that pass.
	store.sync(&in)
	if _, err := workload.Reconcile(ctx, deps, in, plan, updateTarget()); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	for _, idx := range []int32{1, 2} {
		s := store.status(idx)
		if s == nil || s.Phase != workload.InstancePhaseReady || s.Operation != nil {
			t.Fatalf("instance %d = %+v, want an untouched Ready row behind the in-flight repair", idx, s)
		}
	}
}

// An uncapped Component keeps the unpaced behavior: with no budget
// configured nothing bounds the repair.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestReconcile_CrashLoopRepair_UncappedRepairsEveryWedge(t *testing.T) {
	ctx := context.Background()
	in, plan, pods := crashLoopRepairFixture(t, 3, nil)
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	deps := workload.Deps{Client: c, APIReader: c, Expectations: workload.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, updateTarget()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 3); got != 3 {
		t.Fatalf("repairs opened = %d, want 3 with no budget configured", got)
	}
}

// The cross-Component coordination gate is the second layer: a denial
// there holds the repair even where the per-Component budget allows it.
// A rebuild never surges, so the gate is consulted on the arm that
// bounds pods taken offline.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestReconcile_CrashLoopRepair_HeldByCoordinationGate(t *testing.T) {
	ctx := context.Background()
	in, plan, pods := crashLoopRepairFixture(t, 2, nil)
	consults := 0
	in.UpdateGate = func(strategy workload.UpdateStrategyType, surge, unavail int32) (bool, workload.RolloutHoldGate, string) {
		consults++
		if strategy != workload.UpdateStrategyRecreatePod {
			t.Errorf("a repair rebuilds pods in place; gate consulted with strategy %q", strategy)
		}
		return false, workload.RolloutHoldGateRatio, "peer Component is behind"
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workload.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, updateTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 2); got != 0 {
		t.Fatalf("repairs opened = %d, want 0 while the gate denies", got)
	}
	if consults == 0 {
		t.Fatal("the coordination gate must be consulted before a repair opens")
	}
	// Nothing in the cluster changes while the gate denies, so the pass
	// must carry its own wake-up and say why it is waiting.
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue: no watch event will re-consult the gate", result)
	}
	assertRepairHeld(t, rec, "peer Component is behind")
}

// assertRepairHeld drains the recorder and requires exactly one
// RepairHeld warning carrying detail.
func assertRepairHeld(t *testing.T, rec *record.FakeRecorder, detail string) {
	t.Helper()
	held := 0
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
			if strings.Contains(e, string(workload.EventReasonRepairHeld)) {
				held++
				if !strings.Contains(e, detail) {
					t.Errorf("RepairHeld must name the layer holding the repair; got %q", e)
				}
			}
			continue
		default:
		}
		break
	}
	if held != 1 {
		t.Fatalf("RepairHeld events = %d, want 1 (events=%v)", held, events)
	}
}

// With the budget already spent by an in-flight update, every wedged
// row is denied and the pass opens nothing at all. That is the state
// with no watch event coming: the Component would sit wedged until an
// unrelated wake-up, so the pass requeues itself and reports the hold.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/before
func TestReconcile_CrashLoopRepair_AllDeniedRequeuesAndReports(t *testing.T) {
	ctx := context.Background()
	one := intstr.FromInt32(1)
	in, plan, pods := crashLoopRepairFixture(t, 3, &one)
	// Index 0 is mid-update rather than wedged, so it spends the budget
	// without being a restart selection of its own.
	in.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseUpdating
	in.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		ID: "update-0", Type: workload.InstanceOperationUpdate, Step: "InPlace",
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workload.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, updateTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, idx := range []int32{1, 2} {
		if s := store.status(idx); s == nil || s.Phase != workload.InstancePhaseReady {
			t.Fatalf("instance %d = %+v, want an untouched Ready row: the budget is spent", idx, s)
		}
	}
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue: nothing else will wake the wedged Component", result)
	}
	assertRepairHeld(t, rec, "unavailability budget")
}
