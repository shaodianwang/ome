package workload_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/escalation"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestReconcile_RestartPass_OneLiveListForManyInstances pins the restart
// pass's read cost: it does a SINGLE live pod List for the
// whole Component and buckets by Instance index, not one live List per
// Instance. With three healthy Ready instances under the gang-default
// RecreateInstance policy, the live APIReader must see exactly one
// PodList call from the restart pass (the update / create passes read
// the cached Client, not the live reader).
func TestReconcile_RestartPass_OneLiveListForManyInstances(t *testing.T) {
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
	}
	pods := []client.Object{
		isvc,
		enginePod(isvc.Name, isvc.Namespace, 0),
		enginePod(isvc.Name, isvc.Namespace, 1),
		enginePod(isvc.Name, isvc.Namespace, 2),
	}
	// The restart pass reads the LIVE reader (APIReader), which has no Pod
	// field index — liveBucketPods calls ListOMENativePodsByName with
	// useIndex=false, so it goes straight to the label selector (ONE List).
	// No index registration here: the live path never probes MatchingFields,
	// so the assertion measures the per-Component-vs-per-Instance list count
	// without any index-fallback noise.
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(pods...).Build()
	counter := &listCountingReader{Reader: c}
	deps := workloadtypes.Deps{Client: c, APIReader: counter}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	plan := workloadtypes.ComponentPlan{
		Component:     workloadtypes.ComponentEngine,
		Replicas:      3,
		RestartPolicy: workloadtypes.RestartPolicyRecreateInstance,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	if _, err := workload.Reconcile(context.Background(), deps, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// The live reader is used ONLY by the restart pass here (no restart
	// fires, so no destructive live ops run). It must list once, not once
	// per Instance.
	if counter.podListCalls != 1 {
		t.Errorf("restart pass must issue exactly 1 live pod List for 3 instances, got %d", counter.podListCalls)
	}
}

// TestReconcile_RestartPass_PerInstanceSemanticsPreserved proves the
// single List + per-Instance bucketing keeps exact per-Instance restart
// semantics: a Failed pod in index 1's bucket triggers a restart (status
// flips to Restarting on index 1) while healthy indices 0 and 2 are left
// untouched.
func TestReconcile_RestartPass_PerInstanceSemanticsPreserved(t *testing.T) {
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
	}
	objs := []client.Object{
		isvc,
		enginePod(isvc.Name, isvc.Namespace, 0),
		failedEnginePod(isvc.Name, isvc.Namespace, 1), // the restart trigger
		enginePod(isvc.Name, isvc.Namespace, 2),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	in.MutateInstance = roundTripMutateInstance(c, isvc, workloadtypes.ComponentEngine)
	plan := workloadtypes.ComponentPlan{
		Component:     workloadtypes.ComponentEngine,
		Replicas:      3,
		RestartPolicy: workloadtypes.RestartPolicyRecreateInstance,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("a restart in flight must requeue; got %+v", result)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s1 := instanceStatusByIndex(c, fresh, v1beta1.EngineComponent, 1)
	if s1 == nil || s1.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("index 1 (Failed pod) must flip to Restarting; got %+v", s1)
	}
	for _, idx := range []int32{0, 2} {
		s := instanceStatusByIndex(c, fresh, v1beta1.EngineComponent, idx)
		// Healthy indices must not be dragged into a restart. The dispatcher
		// short-circuits on the first restarting Instance, so their status
		// stays at its observed Ready (or is simply never written).
		if s != nil && s.Phase == v1beta1.OMENativeInstanceRestarting {
			t.Errorf("healthy index %d must NOT be restarting; got %+v", idx, s)
		}
	}
}

// An in-flight Restart owns its existing Instance, but it must not prevent
// unrelated absent indices from beginning their Create lifecycle.
func TestReconcile_ScaleUpDuringRestart_CreatesFreshIndices(t *testing.T) {
	name := "scale-during-restart"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 3, false, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workloadtypes.InstancePhaseRestarting,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "restart-0", Type: workloadtypes.InstanceOperationRestart, Step: "Drain",
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 1}})

	result, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("an in-flight restart must schedule another pass; got %+v", result)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	for idx := int32(0); idx < 3; idx++ {
		name := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, idx, "default", 0)
		if !names[name] {
			t.Errorf("expected pod %q to be materialized in the restart pass; got %v", name, names)
		}
	}
	s0 := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if s0 == nil || s0.Phase != v1beta1.OMENativeInstanceRestarting || s0.Operation == nil ||
		s0.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Errorf("restart-owned index 0 must remain Restarting with its Restart operation; got %+v", s0)
	}
	for _, idx := range []int32{1, 2} {
		s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, idx)
		if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil ||
			s.Operation.Type != v1beta1.InstanceOperationCreate {
			t.Errorf("fresh index %d must begin a Create operation; got %+v", idx, s)
		}
	}
}

// A committed partial gang is eligible for Restart even though the reconcile's
// observation still describes it as Create-owned. Once Restart takes ownership,
// the fresh-index Create pass must exclude that index while creating an
// unrelated absent index.
func TestReconcile_ScaleUpDuringRestart_ExcludesRestartSelectedCreateOwner(t *testing.T) {
	name := "scale-partial-gang"
	targetName := name + "-engine-target"
	leader := enginePod(name, "prod", 0)
	leader.Name = query.PodName(name, workloadtypes.ComponentEngine, 0, "leader", 0)
	leader.Labels[query.LabelRunner] = "leader"
	f := newRestartScaleTestFixture(t, name, 2, false, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
		RunningRevision: targetName, TargetRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
			TargetRevision: targetName,
		},
	}, []workloadtypes.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}, leader)

	result, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("the partial-gang restart must schedule another pass; got %+v", result)
	}

	s0 := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if s0 == nil || s0.Phase != v1beta1.OMENativeInstanceRestarting || s0.Incarnation != 2 ||
		s0.Operation == nil || s0.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("index 0 must remain owned by Restart at incarnation 2; got %+v", s0)
	}
	s1 := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1)
	if s1 == nil || s1.Phase != v1beta1.OMENativeInstanceCreating || s1.Operation == nil ||
		s1.Operation.Type != v1beta1.InstanceOperationCreate {
		t.Errorf("unrelated absent index 1 must begin a Create operation; got %+v", s1)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	for _, runner := range []string{"leader", "worker"} {
		freshName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 1, runner, 0)
		if !names[freshName] {
			t.Errorf("expected fresh-index pod %q; got %v", freshName, names)
		}
		restartName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, runner, 0)
		if names[restartName] {
			t.Errorf("restart-selected index 0 must not be recreated from the stale Create observation; found %q in %v", restartName, names)
		}
	}
}

// A standard pause permits an existing Restart to advance, but it continues to
// prohibit Create work for absent desired indices.
func TestReconcile_PausedRestart_DoesNotCreateFreshIndices(t *testing.T) {
	name := "paused-restart"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 2, true, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workloadtypes.InstancePhaseRestarting,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "restart-0", Type: workloadtypes.InstanceOperationRestart, Step: "Drain",
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 1}})

	result, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("the permitted restart must schedule another pass; got %+v", result)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	restartName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, "default", 0)
	if !names[restartName] {
		t.Errorf("paused reconcile must still advance the existing restart; got %v", names)
	}
	freshName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 1, "default", 0)
	if names[freshName] {
		t.Errorf("paused reconcile must not materialize absent index 1; got %v", names)
	}
	if s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1); s != nil {
		t.Errorf("paused reconcile must not allocate status for absent index 1; got %+v", s)
	}
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
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if got := countRestarting(store, 3); got != 1 {
		t.Fatalf("repairs opened = %d, want 1 under a MaxUnavailable of 1", got)
	}

	// The admitted repair anchors the projection while it is in flight,
	// so the next pass opens none of its peers — whether or not the
	// first one finishes in that pass.
	store.sync(&in)
	if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	for _, idx := range []int32{1, 2} {
		s := store.status(idx)
		if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil {
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
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
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
	in.UpdateGate = func(strategy workloadtypes.UpdateStrategyType, surge, unavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
		consults++
		if strategy != workloadtypes.UpdateStrategyRecreatePod {
			t.Errorf("a repair rebuilds pods in place; gate consulted with strategy %q", strategy)
		}
		return false, workloadtypes.RolloutHoldGateRatio, "peer Component is behind"
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget())
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
	in.ObservedState.InstanceStatuses[0].Phase = workloadtypes.InstancePhaseUpdating
	in.ObservedState.InstanceStatuses[0].Operation = &workloadtypes.InstanceOperation{
		ID: "update-0", Type: workloadtypes.InstanceOperationUpdate, Step: "InPlace",
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, idx := range []int32{1, 2} {
		if s := store.status(idx); s == nil || s.Phase != workloadtypes.InstancePhaseReady {
			t.Fatalf("instance %d = %+v, want an untouched Ready row: the budget is spent", idx, s)
		}
	}
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue: nothing else will wake the wedged Component", result)
	}
	assertRepairHeld(t, rec, "unavailability budget")
}

// TestRestartPass_IsolatesInstanceStatusWriteFailure: instance 0's status
// writes are rejected on every pass. Instance 1, selected in the same pass,
// must start and finish its restart regardless; each failing pass returns an
// error naming instance 0; once instance 0's writes succeed both converge.
func TestRestartPass_IsolatesInstanceStatusWriteFailure(t *testing.T) {
	h := newRestartHarness(t, 2)
	rejected := errors.New("status update rejected")
	h.mutateErr = map[int32]error{0: rejected}
	h.losePods(0, 1)

	_, err := h.stepResult()
	if err == nil {
		t.Fatal("the pass must fail while instance 0's status write is rejected")
	}
	if !errors.Is(err, rejected) {
		t.Fatalf("the returned error must wrap the rejected write; got %v", err)
	}
	if !strings.Contains(err.Error(), "restart instance 0") {
		t.Fatalf("the returned error must name instance 0; got %v", err)
	}
	if strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("instance 1 did not fail and must not be reported; got %v", err)
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Incarnation != 1 || s.Operation != nil {
		t.Fatalf("instance 0 must be untouched while its writes are rejected; got %+v", s)
	}
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("instance 0 must not be recreated before its Restarting write lands; got %d pods", len(pods))
	}
	if s := h.instance(1); s == nil || s.Phase != workloadtypes.InstancePhaseRestarting ||
		s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationRestart {
		t.Fatalf("instance 1 must start its restart in the same pass; got %+v", s)
	}
	if !h.atIncarnation(1, 2) {
		h.dumpState("after first failing pass")
		t.Fatalf("instance 1 must have its replacement pod at incarnation 2")
	}

	// Two more passes for instance 1's promote: one writes the serving gate
	// on the replacement, the next observes kubelet folding it into PodReady.
	for pass := 0; pass < 2; pass++ {
		_, err = h.stepResult()
		if err == nil || !strings.Contains(err.Error(), "restart instance 0") {
			t.Fatalf("the pass must keep failing on instance 0; got %v", err)
		}
	}
	if s := h.instance(1); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || !h.atIncarnation(1, 2) {
		h.dumpState("after the failing passes")
		t.Fatalf("instance 1 must complete its restart while instance 0 keeps failing; got %+v", s)
	}
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("instance 0 must still be untouched; got %d pods", len(pods))
	}

	h.mutateErr = nil
	converged := h.run(10, func() bool {
		return h.allReady(2) && h.atIncarnation(0, 2) && h.atIncarnation(1, 2)
	})
	if !converged {
		h.dumpState("after instance 0's writes were accepted")
		t.Fatalf("both instances must converge once instance 0's writes succeed")
	}
}

// TestRestartPass_NoErrors_RequeuesAndCreatesFreshIndices pins the
// error-free contract of a multi-Instance restart pass: every selected
// Instance advances, the pass requeues at the restart interval, and a
// surge-free index added by a concurrent scale-up is materialized in the
// same pass instead of waiting behind the in-flight restarts.
func TestRestartPass_NoErrors_RequeuesAndCreatesFreshIndices(t *testing.T) {
	h := newRestartHarness(t, 2)
	h.losePods(0, 1)
	h.replicas = 3
	h.setTarget(h.revV1, goodImage)

	res, err := h.stepResult()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != testRequeueIntervals.Operation { //nolint:staticcheck // a configured cadence must not also set the bare flag
		t.Fatalf("the restart pass must requeue at the restart interval; got %+v", res)
	}

	// The same pass with no configured cadence still carries a wake-up:
	// a restart the pass opened has nothing in the cluster to announce
	// its next step, and a fresh create that asks for no wake of its own
	// must not erase it.
	unconfigured := newRestartHarness(t, 2)
	unconfigured.requeue = workloadtypes.RequeueIntervals{}
	unconfigured.losePods(0, 1)
	unconfigured.replicas = 3
	unconfigured.setTarget(unconfigured.revV1, goodImage)
	res, err = unconfigured.stepResult()
	if err != nil {
		t.Fatalf("Reconcile with no configured cadence: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unconfigured cadence: got RequeueAfter %v want 0", res.RequeueAfter)
	}
	if !res.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
		t.Fatalf("a restart pass with no cadence must still wake; got %+v", res)
	}
	for _, idx := range []int32{0, 1} {
		if s := h.instance(idx); s == nil || s.Phase != workloadtypes.InstancePhaseRestarting ||
			s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationRestart {
			t.Fatalf("instance %d must be restarting; got %+v", idx, s)
		}
		if !h.atIncarnation(idx, 2) {
			h.dumpState("restart pass")
			t.Fatalf("instance %d must have its replacement pod at incarnation 2", idx)
		}
	}
	if s := h.instance(2); s == nil || s.Phase != workloadtypes.InstancePhaseCreating ||
		s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationCreate {
		t.Fatalf("fresh index 2 must begin its Create in the restart pass; got %+v", s)
	}
	if pods := h.podsOf(2); len(pods) != 1 {
		t.Fatalf("fresh index 2 must be materialized in the restart pass; got %d pods", len(pods))
	}

	if !h.run(10, func() bool { return h.allReady(3) }) {
		h.dumpState("after scale-up during restart")
		t.Fatalf("restarted and fresh instances must all converge")
	}
}

func newRestartScaleTestFixture(
	t *testing.T,
	name string,
	replicas int32,
	paused bool,
	status workloadtypes.InstanceStatus,
	runners []workloadtypes.RunnerPlan,
	pods ...client.Object,
) *restartScaleTestFixture {
	t.Helper()
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: types.UID("uid-" + name)},
	}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"},
		Status: v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			v1beta1convert.InstanceStatusFromWorkload(status),
		}},
	}
	input := minimalInput(t)
	objects := []client.Object{isvc, ir}
	if status.RunningRevision != "" {
		// The row names the revision it runs; the rebuild renders that
		// revision's stored template.
		objects = append(objects, revisionWithPodSpec(t, status.RunningRevision, isvc.Namespace, input.DesiredSpec.PodSpec))
	}
	objects = append(objects, pods...)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objects...).Build()

	input.OwnerObject = isvc
	input.EventTarget = isvc
	input.Key.OwnerName = isvc.Name
	input.DesiredSpec.Replicas = replicas
	input.DesiredSpec.MultiPod = len(runners) > 1
	input.DesiredSpec.Paused = paused
	input.ObservedState.UpdateRevision = status.RunningRevision
	input.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{cloneTestInstanceStatus(status)}
	input.MutateInstance = roundTripMutateInstance(c, isvc, workloadtypes.ComponentEngine)

	instances := make([]workloadtypes.InstancePlan, 0, replicas)
	for idx := int32(0); idx < replicas; idx++ {
		incarnation := int64(1)
		if idx == status.Index {
			incarnation = status.Incarnation
		}
		instances = append(instances, workloadtypes.InstancePlan{
			Index: idx, Incarnation: incarnation,
			Runners: append([]workloadtypes.RunnerPlan(nil), runners...),
		})
	}
	plan := workloadtypes.ComponentPlan{
		Component:            workloadtypes.ComponentEngine,
		Replicas:             replicas,
		RestartPolicy:        workloadtypes.RestartPolicyRecreateInstance,
		InstanceReadyTimeout: time.Minute,
		Paused:               paused,
		Instances:            instances,
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: status.RunningRevision, Namespace: isvc.Namespace},
	}
	return &restartScaleTestFixture{isvc: isvc, client: c, input: input, plan: plan, target: target}
}

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

// crashLoopRepairTarget is the roll target the repair tests reconcile
// toward: the revision the wedged rows already run. A repair is selected
// only while that revision is still the target; a Component wedged on a
// revision that is not the target is the update pass's.
func crashLoopRepairTarget() *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: crashLoopRepairRevision, Namespace: "prod"},
	}
}

// crashLoopRepairFixture wires n Ready, operation-free rows whose pods
// are all wedged on the current revision — the shape one bad argument
// or one bad image produces across a whole Component.
func crashLoopRepairFixture(t *testing.T, n int32, maxUnavailable *intstr.IntOrString) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, []client.Object) {
	t.Helper()
	in := minimalInput(t)
	in.StuckPodGrace = time.Minute
	in.ObservedState.CurrentRevision = crashLoopRepairRevision
	in.DesiredSpec.Replicas = n
	plan := workloadtypes.ComponentPlan{
		Component:     workloadtypes.ComponentEngine,
		Replicas:      n,
		RestartPolicy: workloadtypes.RestartPolicyNone,
		UpdateStrategy: workloadtypes.UpdateStrategy{
			Type:          workloadtypes.UpdateStrategyRecreatePod,
			RollingUpdate: &workloadtypes.RollingUpdate{MaxUnavailable: maxUnavailable},
		},
	}
	var pods []client.Object
	for idx := int32(0); idx < n; idx++ {
		in.ObservedState.InstanceStatuses = append(in.ObservedState.InstanceStatuses, workloadtypes.InstanceStatus{
			Index: idx, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady,
			PodCount: 1, RunningRevision: crashLoopRepairRevision,
		})
		plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
			Index: idx, Incarnation: 1,
			Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
		})
		pods = append(pods, wedgedEnginePod(idx))
	}
	return in, plan, pods
}

// installRepairStore routes every status adapter the restart pass uses
// into one store, so a pass's repairs are observable as committed rows.
func installRepairStore(in *workloadtypes.ReconcileInput) *testAtomicMutationStore {
	store := installTestAtomicMutationStore(in)
	in.ApplyInstanceMutations = func(ctx context.Context, muts []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, muts, "", nil)
	}
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	return store
}

// countRestarting reports how many rows the store holds in Restarting.
func countRestarting(store *testAtomicMutationStore, n int32) int {
	restarting := 0
	for idx := int32(0); idx < n; idx++ {
		if s := store.status(idx); s != nil && s.Phase == workloadtypes.InstancePhaseRestarting {
			restarting++
		}
	}
	return restarting
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
			if strings.Contains(e, string(workloadtypes.EventReasonRepairHeld)) {
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

// newRestartHarness is the recovery harness with n single-pod Instances
// under RestartPolicy=RecreateInstance, converged Ready on v1.
func newRestartHarness(t *testing.T, replicas int32) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle = workloadtypes.Lifecycle{RestartPolicy: &policy}
	h.replicas = replicas
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.allReady(replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1 for %d instances", replicas)
	}
	return h
}

// crashForm names how the old-revision Instance breaks: its pod dies at
// every start and is never Ready again, its pod dies after it is up and
// keeps coming back between crashes, or a gang loses a member outright.
type crashForm string

const (
	crashAtStart   crashForm = "crash-at-start"
	crashAfterUp   crashForm = "crash-after-ready"
	gangMemberLost crashForm = "member-lost"
)

// offTargetStrategies is every update strategy with the budget the closed
// loop runs it under: one surge, or one Instance offline at a time.
var offTargetStrategies = []struct {
	strategy       workloadtypes.UpdateStrategyType
	surge, unavail int32
}{
	{workloadtypes.UpdateStrategySurgeThenDrain, 1, 0},
	{workloadtypes.UpdateStrategyRecreatePod, 0, 1},
	{workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 1},
	{workloadtypes.UpdateStrategyInPlaceOnly, 0, 1},
}

// offTargetCrashHarness converges replicas Instances on v1 under the
// named strategy, budget and restart policy, with the runner container
// named as production names it so container-restart evidence is read.
func offTargetCrashHarness(t *testing.T, gang bool, strategy workloadtypes.UpdateStrategyType, surge, unavail int32, policy workloadtypes.RestartPolicy, replicas int32) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarnessFor(t, gang, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(64)
	h.replicas = replicas
	maxSurge := intstr.FromInt32(surge)
	maxUnavailable := intstr.FromInt32(unavail)
	h.lifecycle = workloadtypes.Lifecycle{
		RestartPolicy: &policy,
		UpdateStrategy: &workloadtypes.UpdateStrategy{
			Type:          strategy,
			RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
		},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}
	return h
}

// breakInstance applies form to the Instance at idx on the running
// revision: the leader (or single) pod is pointed at the crash image, or
// the gang's worker is deleted.
func breakInstance(t *testing.T, h *recoveryHarness, idx int32, gang bool, form crashForm) {
	t.Helper()
	runner := ""
	if gang {
		runner = "leader"
	}
	switch form {
	case crashAtStart:
		h.crashPod(idx, runner, crashStartImage)
	case crashAfterUp:
		h.crashPod(idx, runner, crashLoadImage)
	case gangMemberLost:
		for _, pod := range h.podsOf(idx) {
			if pod.Labels[query.LabelRunner] != "worker" {
				continue
			}
			if err := h.c.Delete(h.ctx, pod); err != nil {
				t.Fatalf("delete worker %s: %v", pod.Name, err)
			}
			return
		}
		t.Fatalf("instance %d has no worker to lose", idx)
	}
}

// TestReconcile_OffTargetCrash_RollsForwardAcrossTheMatrix is the
// closed-loop lock for the ownership rule, over every shape the engine
// alone can express: single-pod and leader+worker Instances, every update
// strategy, both restart policies, and the two forms a persistent crash
// takes plus a lost gang member. One Instance on v1 breaks while the
// target moves to the fixed revision. Whatever evidence the restart
// trigger holds — a crash loop, a runner restart after Ready, a member
// short — the fix lands on the broken Instance through the update pass:
// no repair is recorded, the roll stays inside its budget, and every
// Instance ends Ready on the fixed revision with pods on the fixed image.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-surge, instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-inplace, instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-recreate, instance/Ready/pod.containerRestart/boundary@T-ready-surge, instance/Ready/pod.containerRestart/boundary@T-ready-inplace, instance/Ready/pod.containerRestart/boundary@T-ready-recreate, instance/Ready/pod.deleted[source]/boundary@T-ready-surge, instance/Ready/pod.deleted[source]/boundary@T-ready-recreate, instance/Ready/spec.revision/boundary@T-ready-crashloop-restart, instance/Ready/spec.revision/boundary@T-ready-restart-crash, instance/Ready/spec.revision/boundary@T-ready-restart
func TestReconcile_OffTargetCrash_RollsForwardAcrossTheMatrix(t *testing.T) {
	const replicas = 3
	for _, gang := range []bool{false, true} {
		shape := "single"
		forms := []crashForm{crashAtStart, crashAfterUp}
		if gang {
			shape = "gang"
			forms = append(forms, gangMemberLost)
		}
		for _, sc := range offTargetStrategies {
			for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
				for _, form := range forms {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", shape, sc.strategy, policy, form), func(t *testing.T) {
						h := offTargetCrashHarness(t, gang, sc.strategy, sc.surge, sc.unavail, policy, replicas)
						// The last Instance breaks: the roll reaches it last, so
						// the crash is observed on a Ready row with no operation
						// for as long as the roll takes to get there.
						victim := int32(replicas - 1)
						breakInstance(t, h, victim, gang, form)
						h.events = nil
						h.setTarget(h.revFixed, fixedImage)

						withinBudget := func() {
							if h.repairInFlight() {
								h.dumpState("repair opened")
								t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
							}
							sts := h.irStatuses()
							if sc.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
								if n := escalation.CurrentSurgeInFlight(sts); n > sc.surge {
									t.Fatalf("%d surges in flight, budget %d", n, sc.surge)
								}
							} else if n := escalation.CurrentUnavailableInFlight(sts); n > sc.unavail {
								t.Fatalf("%d Instances offline for the roll, budget %d", n, sc.unavail)
							}
						}
						if !h.runWithInvariant(80, func() bool { return h.settledOn(h.revFixed, replicas) }, withinBudget) {
							h.dumpState("after the bump")
							t.Fatalf("the Component never settled on the fixed revision with the broken Instance healed")
						}
						if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
							t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
						}
					})
				}
			}
		}
	}
}

// TestReconcile_OffTargetCrash_StartIsAdmittedLikeAnyOther pins the
// admission of the broken Instance's roll: it is a fresh start like its
// peers', paced by the same per-Component budget, and the coordination
// gate is consulted for it exactly as for a dark Failed row — not on a
// non-surge strategy, where the crash-looping pod set serves nothing and
// the gate already counts the outage, and once on a surge strategy, where
// the replacement genuinely adds a pod. The same gate stands in for a
// coordination group: every start that opens is admitted through it.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-recreate, instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-surge
func TestReconcile_OffTargetCrash_StartIsAdmittedLikeAnyOther(t *testing.T) {
	const replicas = 3
	for _, sc := range offTargetStrategies[:2] {
		t.Run(string(sc.strategy), func(t *testing.T) {
			h := offTargetCrashHarness(t, false, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
			consults := 0
			h.gate = func(strategy workloadtypes.UpdateStrategyType, _, _ int32) (bool, workloadtypes.RolloutHoldGate, string) {
				if strategy != sc.strategy {
					t.Errorf("gate consulted with mechanism %q, want %q", strategy, sc.strategy)
				}
				consults++
				return true, "", ""
			}
			breakInstance(t, h, replicas-1, false, crashAtStart)
			h.setTarget(h.revFixed, fixedImage)
			if !h.run(80, func() bool { return h.settledOn(h.revFixed, replicas) }) {
				h.dumpState("after the bump")
				t.Fatalf("the Component never settled on the fixed revision")
			}
			want := replicas
			if sc.strategy != workloadtypes.UpdateStrategySurgeThenDrain {
				// The broken Instance serves nothing: its recreate opens no
				// new unavailability and skips the consult.
				want = replicas - 1
			}
			if consults != want {
				t.Fatalf("gate consults = %d, want %d", consults, want)
			}
		})
	}
}

// servingInstances counts the Instances whose full pod set is in rotation
// (ContainersReady with the serving gate), which is how the Component's
// published ServingReplicas counts a gang: one member out of rotation
// takes the whole Instance out of the count.
func (h *recoveryHarness) servingInstances() int32 {
	perInstance := len(h.desired.Runners)
	serving := map[int32]int{}
	for _, pod := range h.livePods() {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || !podreadiness.IsContainersReady(pod) || !podreadiness.IsServing(pod) {
			continue
		}
		serving[idx]++
	}
	var n int32
	for _, count := range serving {
		if count >= perInstance {
			n++
		}
	}
	return n
}

// coordinationUnavailabilityGate stands in for a coordination group's
// unavailability gate, read the way the group gate reads the Component's
// published counters: the Instances not serving are the unavailability
// already open, and a drain-first start is admitted while that count, plus
// the starts this pass has opened, plus this one, stays within
// maxUnavailable. A surge start never reaches it.
func coordinationUnavailabilityGate(h *recoveryHarness, replicas, maxUnavailable int32, consults *int) func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
	return func(strategy workloadtypes.UpdateStrategyType, _, inFlightUnavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
		h.t.Helper()
		*consults++
		if strategy == workloadtypes.UpdateStrategySurgeThenDrain {
			h.t.Fatalf("a drain-first roll consulted the gate as a surge start")
		}
		unavailable := replicas - h.servingInstances()
		if unavailable < 0 {
			unavailable = 0
		}
		projected := unavailable + inFlightUnavail + 1
		if projected > maxUnavailable {
			return false, workloadtypes.RolloutHoldGateBudget, fmt.Sprintf(
				"unavailable budget %d exhausted (current %d, in-flight %d, would become %d)",
				maxUnavailable, unavailable, inFlightUnavail, projected)
		}
		return true, "", ""
	}
}

// TestReconcile_OffTargetCrash_GangStartIsExemptFromTheCoordinationGate
// pins the gate exemption on the gang shape. A gang serves through its
// leader: with the leader wedged the Instance is already out of the
// coordination gate's serving count, whatever the workers report, so the
// roll's drain-first start on it opens no new unavailability and must skip
// the gate consult. Consulting it would count the Instance's own outage a
// second time, and under maxUnavailable=1 the gate would deny the only
// start that can end the outage on every pass.
//
// The healthy Instance rolls first, so the wedged one is reached with the
// roll's own budget free and the gate as the only layer left to answer to.
//
// Cells: instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-recreate, instance/Ready/pod.waiting[CrashLoopBackOff]/boundary@T-ready-inplace
func TestReconcile_OffTargetCrash_GangStartIsExemptFromTheCoordinationGate(t *testing.T) {
	const replicas = 2
	for _, sc := range offTargetStrategies[1:] {
		for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
			t.Run(fmt.Sprintf("%s/%s", sc.strategy, policy), func(t *testing.T) {
				h := offTargetCrashHarness(t, true, sc.strategy, sc.surge, sc.unavail, policy, replicas)
				consults := 0
				h.gate = coordinationUnavailabilityGate(h, replicas, sc.unavail, &consults)
				h.events = nil
				h.setTarget(h.revFixed, fixedImage)
				h.step()
				if first := h.instance(0); first == nil || first.Phase != workloadtypes.InstancePhaseUpdating {
					h.dumpState("after the bump")
					t.Fatalf("instance 0 = %+v, want the healthy Instance rolling first", first)
				}
				victim := int32(replicas - 1)
				breakInstance(t, h, victim, true, crashAtStart)

				withinBudget := func() {
					if h.repairInFlight() {
						h.dumpState("repair opened")
						t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
					}
					if n := escalation.CurrentUnavailableInFlight(h.irStatuses()); n > sc.unavail {
						t.Fatalf("%d Instances offline for the roll, budget %d", n, sc.unavail)
					}
				}
				if !h.runWithInvariant(80, func() bool { return h.settledOn(h.revFixed, replicas) }, withinBudget) {
					h.dumpState("after the bump")
					t.Fatalf("the Component never settled on the fixed revision: the wedged gang was held behind its own outage")
				}
				if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
					t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
				}
				// Only the healthy Instance's start was anything the gate
				// had to pace.
				if consults != replicas-1 {
					t.Fatalf("gate consults = %d, want %d: the wedged gang serves nothing and skips the consult", consults, replicas-1)
				}
			})
		}
	}
}

// A repair that parked at Failed — its rebuilt pods wedged past the
// stuck-pod grace or the attempt deadline — has two exits of its own, and
// the stories below drive each of them end to end through Reconcile: the
// set coming up on its own resumes the promote, and a set that stays
// wedged is rebuilt again on the operator's retry ladder until it heals
// or the ladder is spent.

// retryLadder is the seconds-scale lifecycle.updateRetry the stories run
// under, read here for a parked repair's re-arms; the fake clock steps at
// least 30s per pass.
func retryLadder(maxAttempts int32, initial time.Duration) *workloadtypes.RetryPolicy {
	return &workloadtypes.RetryPolicy{MaxAttempts: maxAttempts, InitialDelay: initial, MaxDelay: 10 * time.Minute, Multiplier: 2}
}

// parkedRepair reports whether the Instance is Failed with its Restart
// operation preserved.
func parkedRepair(h *recoveryHarness, idx int32) bool {
	s := h.instance(idx)
	return s != nil && s.Phase == workloadtypes.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationRestart
}

// servingReady reports whether the Instance is Ready with no operation
// and exactly n live pods, all in rotation.
func servingReady(h *recoveryHarness, idx int32, n int) bool {
	s := h.instance(idx)
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil {
		return false
	}
	serving := 0
	for _, pod := range h.podsOf(idx) {
		if podreadiness.IsServing(pod) {
			serving++
		}
	}
	return len(h.podsOf(idx)) == n && serving == n
}

// parkRepair drives a converged Instance into the parked shape: the pod
// set is lost, the rebuild wedges in reason, and the escalation fails the
// attempt with its Restart preserved.
func parkRepair(t *testing.T, h *recoveryHarness, reason string, sticky bool) {
	t.Helper()
	h.armWedge(reason, sticky)
	h.losePods(0)
	if !h.run(40, func() bool { return parkedRepair(h, 0) }) {
		h.dumpState("park")
		t.Fatalf("the rebuild never parked at Failed with its Restart preserved")
	}
}

// eventCount is how many of the events recorded so far carry reason.
func eventCount(h *recoveryHarness, reason string) int {
	n := 0
	for _, e := range h.events {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}

// The rebuilt set comes up on its own — the kubelet's retry succeeds once
// the missing ConfigMap key is back — and the parked repair finishes with
// its promote: serving gate written, Ready on the running revision, the
// incarnation the pods already carry, no operator action. The ladder is
// not consulted, so an unconfigured one changes nothing here.
//
// Cells: instance/Failed/pod.containersReady[source]/before, instance/Restart/Drain/pod.containersReady[source]/after@T-restart-stuck, instance/Restart/Drain/pod.containersReady[source]/after@T-restart-stuckgrace
func TestSpentRepair_RebuiltSetComesUp_ResumesThePromote(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ladder *workloadtypes.RetryPolicy
	}{
		{"ladder configured", retryLadder(3, 2*time.Minute)},
		{"ladder unconfigured", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, 1)
			h.retryPolicy = tc.ladder
			parkRepair(t, h, "CreateContainerConfigError", false)
			parked := h.instance(0)

			h.clearWedge()
			if !h.run(20, func() bool { return servingReady(h, 0, 1) }) {
				h.dumpState("resume")
				t.Fatalf("the healed set was never promoted")
			}
			got := h.instance(0)
			if got.Incarnation != parked.Incarnation {
				t.Errorf("incarnation = %d, want %d: a set that came up is promoted, not rebuilt", got.Incarnation, parked.Incarnation)
			}
			if got.RunningRevision != h.revV1.Name {
				t.Errorf("running revision = %q, want %q", got.RunningRevision, h.revV1.Name)
			}
		})
	}
}

// The rebuilt set stays wedged even after the cause is gone — the pods
// cannot recover in place — so the parked repair re-arms on the ladder:
// not before the configured delay from the recorded failure, then as a
// whole rebuild at the next incarnation, which heals.
//
// Cells: instance/Failed/timer.repairRetry/before
func TestSpentRepair_WedgedSet_ReArmsOnTheLadderAndHeals(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = retryLadder(3, 2*time.Minute)
	parkRepair(t, h, "CreateContainerConfigError", true)
	parked := h.instance(0)
	anchor := parked.LastFailure.Time.Time
	notBefore := anchor.Add(2 * time.Minute)

	// The cause clears at once; the parked pod stays wedged regardless.
	h.clearWedge()
	healed := h.runWithInvariant(40,
		func() bool { return servingReady(h, 0, 1) },
		func() {
			if h.clk.Now().Before(notBefore) && !parkedRepair(h, 0) {
				h.dumpState("early re-arm")
				t.Fatalf("the repair re-armed at %v, before the ladder's first delay elapsed at %v", h.clk.Now(), notBefore)
			}
		})
	if !healed {
		h.dumpState("re-arm")
		t.Fatalf("the re-armed repair never healed")
	}
	got := h.instance(0)
	if got.Incarnation != parked.Incarnation+1 {
		t.Errorf("incarnation = %d, want %d: one rebuild on the ladder", got.Incarnation, parked.Incarnation+1)
	}
}

// A gang whose members are all alive but wedged has no member to lose, so
// only the ladder rebuilds it; the rebuild is the whole gang.
//
// Cells: instance/Failed/timer.repairRetry/before
func TestSpentRepair_Gang_AllMembersWedged_ReArmsOnTheLadder(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.retryPolicy = retryLadder(3, time.Minute)
	h.driveToReadyOnV1()
	parkRepair(t, h, "CreateContainerConfigError", true)
	parked := h.instance(0)
	if pods := h.podsOf(0); len(pods) != 2 {
		t.Fatalf("parked gang has %d pods, want both members present and wedged", len(pods))
	}

	h.clearWedge()
	if !h.run(40, func() bool { return servingReady(h, 0, 2) }) {
		h.dumpState("gang re-arm")
		t.Fatalf("the re-armed gang never healed")
	}
	if got := h.instance(0); got.Incarnation != parked.Incarnation+1 {
		t.Errorf("incarnation = %d, want %d", got.Incarnation, parked.Incarnation+1)
	}
}

// Under RestartPolicy=None the only repair is the crash-loop one, and its
// parked attempt re-arms the same way: the policy governs whether a repair
// starts, not whether one that started may try again.
//
// Cells: instance/Failed/timer.repairRetry/before
func TestSpentRepair_CrashLoopRepairUnderPolicyNone_ReArmsOnTheLadder(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.retryPolicy = retryLadder(3, time.Minute)
	h.driveToReadyOnV1()

	// The running pod crash-loops, and so does every rebuild while the
	// cause stands.
	h.armWedge("CrashLoopBackOff", true)
	h.wedgeLivePods("CrashLoopBackOff")
	if !h.run(40, func() bool { return parkedRepair(h, 0) }) {
		h.dumpState("crash-loop park")
		t.Fatalf("the crash-loop repair never parked at Failed")
	}
	parked := h.instance(0)

	h.clearWedge()
	if !h.run(40, func() bool { return servingReady(h, 0, 1) }) {
		h.dumpState("crash-loop re-arm")
		t.Fatalf("the re-armed crash-loop repair never healed")
	}
	if got := h.instance(0); got.Incarnation != parked.Incarnation+1 {
		t.Errorf("incarnation = %d, want %d", got.Incarnation, parked.Incarnation+1)
	}
}

// A cause that never clears spends the ladder: each re-arm rebuilds once
// more, and after maxAttempts the row parks for good — no further
// incarnation, one RepairRetriesExhausted warning — until an operator
// resets it or publishes a corrected revision.
//
// Cells: instance/Failed/timer.repairRetry/before
func TestSpentRepair_NeverHeals_ParksAfterTheLadderIsSpent(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = retryLadder(2, 30*time.Second)
	h.recorder = record.NewFakeRecorder(64)
	parkRepair(t, h, "CreateContainerConfigError", true)
	first := h.instance(0).Incarnation

	spent := func() bool {
		s := h.instance(0)
		return parkedRepair(h, 0) && s.Operation.RetryCount == 2
	}
	if !h.run(60, spent) {
		h.dumpState("ladder")
		t.Fatalf("the ladder was never spent")
	}
	final := h.instance(0)
	if final.Incarnation != first+2 {
		t.Errorf("incarnation = %d, want %d: one rebuild per re-arm", final.Incarnation, first+2)
	}
	h.settle(20)
	if got := h.instance(0); !parkedRepair(h, 0) || got.Incarnation != final.Incarnation {
		h.dumpState("parked for good")
		t.Fatalf("the spent ladder re-armed again: %+v", got)
	}
	if n := eventCount(h, string(workloadtypes.EventReasonRepairRetriesExhausted)); n != 1 {
		t.Errorf("RepairRetriesExhausted events = %d, want exactly 1", n)
	}
}

// No ladder configured is the fail-safe: a wedged set stays parked, with
// no rebuild and no attempt counted, until an operator acts.
//
// Cells: instance/Failed/timer.repairRetry/before
func TestSpentRepair_NoLadder_StaysParked(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = nil
	h.recorder = record.NewFakeRecorder(64)
	parkRepair(t, h, "CreateContainerConfigError", true)
	parked := h.instance(0)

	h.settle(40)
	got := h.instance(0)
	if !parkedRepair(h, 0) || got.Incarnation != parked.Incarnation || got.Operation.RetryCount != 0 {
		h.dumpState("unconfigured")
		t.Fatalf("an unconfigured ladder must leave the repair parked as it was: %+v", got)
	}
	if n := eventCount(h, string(workloadtypes.EventReasonRepairRetriesExhausted)); n != 0 {
		t.Errorf("RepairRetriesExhausted events = %d, want none: there is no ladder to exhaust", n)
	}
}

// The reset mailbox stays the immediate escape from a parked repair: with
// the pods deleted and the spent operation cleared, the Create pass
// rebuilds the Instance on the next pass, ladder or no ladder.
//
// Cells: instance/Failed/spec.resetInstances[all]/before
func TestSpentRepair_ResetMailboxStillRebuilds(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = retryLadder(1, 30*time.Second)
	parkRepair(t, h, "CreateContainerConfigError", true)
	if !h.run(40, func() bool { return parkedRepair(h, 0) && h.instance(0).Operation.RetryCount == 1 }) {
		h.dumpState("ladder")
		t.Fatalf("the ladder was never spent")
	}

	// What the mailbox does: the pods go, the operation is cleared.
	h.clearWedge()
	h.losePods(0)
	cleared, err := status.ClearFailedInstanceOperation(h.ctx, h.mutateInstance(), 0)
	if err != nil || !cleared {
		t.Fatalf("clear the parked operation: cleared=%v err=%v", cleared, err)
	}
	if !h.run(30, func() bool { return servingReady(h, 0, 1) }) {
		h.dumpState("reset")
		t.Fatalf("the reset Instance was never rebuilt")
	}
}

// A revision Held after its retry budget is spent stays Held when the
// cause of its failures clears: only a new revision or the release
// mailbox admits another attempt at it. The Instance keeps serving the
// revision it ran.
//
// Cells: instance/Failed/gate.deny[Held]/before
func TestHeldRevision_StaysHeldWhenItsCauseHeals(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(3)

	// The wreckage of the last attempt may still be parked on the bad
	// revision; what the hold forbids is a new attempt.
	badPodsBefore := len(h.badPods())
	h.badImageHeals = true
	h.settle(12)
	if b := h.findBlock(h.revBad.Name); b == nil || b.State != workloadtypes.RetryBlockHeld {
		t.Fatalf("block = %+v, want Held: a healed cause releases nothing on its own", b)
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != h.revV1.Name {
		h.dumpState("held")
		t.Fatalf("the Instance must keep serving v1 while the revision is Held: %+v", s)
	}
	if got := len(h.badPods()); got > badPodsBefore {
		h.dumpState("held")
		t.Fatalf("bad-revision pods grew from %d to %d: no attempt at a Held revision may open", badPodsBefore, got)
	}

	// The release mailbox's effect: the block is gone, and the next pass
	// attempts the revision again, which now converges.
	h.blocks = nil
	if !h.run(40, func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == h.revBad.Name
	}) {
		h.dumpState("released")
		t.Fatalf("the released revision never converged")
	}
}
