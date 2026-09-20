package ops

// The two non-surge update mechanisms — the in-place image patch and the
// recreate roll — share one shape: a single step converging on the target
// revision, with everything else that can happen to the row while it runs
// being either inert or a plain wait. These tests pin that half of the
// contract: what an observation or an operator edit does NOT move.

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// nonSurgeFixture is one Instance with a roll in flight and one pod. The
// caller decorates the pod, edits the input or the plan, then runs a pass
// and reads the row back.
type nonSurgeFixture struct {
	isvc       *v1beta1.InferenceService
	c          client.Client
	targetCR   *appsv1.ControllerRevision
	targetSpec *corev1.PodSpec
	pod        *corev1.Pod
	strategy   workload.UpdateStrategyType
	// blocks records every RetryBlock the pass asked to write.
	blocks *[]string
}

// inPlaceInFlightFixture seeds an in-place patch already stamped on the
// row: Phase=Updating, Op{Update, InPlace} at the target, and the pod it
// is patching still on the running revision's image.
func inPlaceInFlightFixture(t *testing.T) *nonSurgeFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir, _ := inPlaceRollWithoutPods(t)
	ir.Status.InstanceStatuses[0].Operation.Strategy = string(v1beta1.UpdateStrategyInPlaceIfPossible)
	targetSpec := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	return &nonSurgeFixture{
		isvc: isvc, c: c, targetCR: tcr, targetSpec: targetSpec, pod: pod,
		strategy: workload.UpdateStrategyInPlaceIfPossible,
	}
}

// recreateDrainInFlightFixture seeds a recreate already stamped on the
// row: Phase=Updating, Op{Update, Drain} at the bumped Incarnation. The
// pod the caller supplies decides which phase of the step the pass lands
// in — an old-incarnation pod keeps it in Phase A, a bumped one in C.
func recreateDrainInFlightFixture(t *testing.T) *nonSurgeFixture {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 2)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)

	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	live.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	live.Status.InstanceStatuses[0].Incarnation = 2
	live.Status.InstanceStatuses[0].TargetRevision = tcr.Name
	live.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		ID:             "update-0-1",
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           updateStepDrain,
		TargetRevision: tcr.Name,
		Strategy:       string(v1beta1.UpdateStrategyRecreatePod),
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Minute)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Minute)),
		Deadline:       metav1.NewTime(time.Now().Add(29 * time.Minute)),
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("seed the in-flight recreate: %v", err)
	}
	return &nonSurgeFixture{
		isvc: isvc, c: c, targetCR: tcr, targetSpec: targetSpec,
		strategy: workload.UpdateStrategyRecreatePod,
	}
}

// input builds a fresh ReconcileInput and wires the RetryBlock recorder.
func (f *nonSurgeFixture) input() workload.ReconcileInput {
	in := legacyTestInput(f.isvc, f.c, workload.ComponentEngine)
	in.ObservedState.UpdateRevision = f.targetCR.Name
	blocks := &[]string{}
	f.blocks = blocks
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}
	return in
}

// nonSurgeOutcome is everything a pass over an in-flight roll may move.
type nonSurgeOutcome struct {
	done        bool
	phase       v1beta1.OMENativeInstancePhase
	step        string
	incarnation int64
	failed      bool
	blocks      int
	podSurvives bool
	// podImage is the image on the observed pod after the pass, so an
	// edit that silently changed what was written to it is visible.
	podImage string
}

// run drives one pass with pods and reports what moved. tweak may edit
// the input and the plan the way an operator config change would.
func (f *nonSurgeFixture) run(t *testing.T, pods []*corev1.Pod, tweak func(*workload.ReconcileInput, *workload.ComponentPlan)) nonSurgeOutcome {
	t.Helper()
	in := f.input()
	plan := legacyComponentPlan(f.strategy, nil)
	if tweak != nil {
		tweak(&in, &plan)
	}
	deps := workload.Deps{Client: f.c, Recorder: record.NewFakeRecorder(32)}
	done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0], f.targetCR, f.targetSpec, pods)
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	s := legacyInstanceStatusesOnIR(f.c, f.isvc, workload.ComponentEngine)[0]
	out := nonSurgeOutcome{
		done:        done,
		phase:       s.Phase,
		incarnation: s.Incarnation,
		failed:      s.LastFailure != nil,
		blocks:      len(*f.blocks),
		podSurvives: true,
	}
	if s.Operation != nil {
		out.step = s.Operation.Step
	}
	if len(pods) > 0 {
		fresh := &corev1.Pod{}
		err := f.c.Get(context.Background(), client.ObjectKeyFromObject(pods[0]), fresh)
		out.podSurvives = err == nil
		if err == nil && len(fresh.Spec.Containers) > 0 {
			out.podImage = fresh.Spec.Containers[0].Image
		}
	}
	return out
}

// terminatingPod deletes pod through a finalizer so the fake client keeps
// the object with a deletionTimestamp — what the live List hands a pass
// over a pod the apiserver has not collected yet.
func terminatingPod(t *testing.T, c client.Client, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	live := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("re-read pod %s: %v", pod.Name, err)
	}
	live.Finalizers = append(live.Finalizers, "ome.io/test-hold")
	if err := c.Update(ctx, live); err != nil {
		t.Fatalf("pin pod %s with a finalizer: %v", pod.Name, err)
	}
	if err := c.Delete(ctx, live); err != nil {
		t.Fatalf("delete pod %s: %v", pod.Name, err)
	}
	out := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), out); err != nil {
		t.Fatalf("re-read terminating pod %s: %v", pod.Name, err)
	}
	if out.DeletionTimestamp == nil {
		t.Fatalf("pod %s did not become Terminating", pod.Name)
	}
	return out
}

// TestUpdateInPlace_PodObservationsThatDecideNothing: the in-place step
// promotes on one thing only — every changed image confirmed from
// container status on a pod that is PodReady past its availability
// window. Everything else the kubelet can report about that pod either
// is the patch taking effect or is a wait: none of them ends the attempt,
// bumps the Incarnation, or moves the step off the patch. The roll is
// bounded by its operation deadline, not by any of these readings.
//
// Cells: instance/Update/InPlace/pod.containerRestart/before, instance/Update/InPlace/pod.stuckTerminating/before, instance/Update/InPlace/pod.nodeLost[NodeGone]/before, instance/Update/InPlace/pod.evicted/before, instance/Update/InPlace/pod.phaseTerminal[Failed]/before, instance/Update/InPlace/pod.phaseTerminal[Succeeded]/before
func TestUpdateInPlace_PodObservationsThatDecideNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		observe func(t *testing.T, f *nonSurgeFixture) *corev1.Pod
	}{
		{
			name: "a container restarting under the patch",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name: "main", Image: "llama:v1", RestartCount: 3,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}}
				return pod
			},
		},
		{
			name: "the pod wedged Terminating from outside",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				return terminatingPod(t, f.c, f.pod)
			},
		},
		{
			name: "the node hosting the pod gone",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Spec.NodeName = "node-that-no-longer-exists"
				return pod
			},
		},
		{
			name: "the pod evicted",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.Phase = corev1.PodFailed
				pod.Status.Reason = "Evicted"
				return pod
			},
		},
		{
			name: "the pod in a terminal Failed phase",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.Phase = corev1.PodFailed
				return pod
			},
		},
		{
			name: "the pod in a terminal Succeeded phase",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.Phase = corev1.PodSucceeded
				return pod
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := inPlaceInFlightFixture(t)
			pod := tc.observe(t, f)
			got := f.run(t, []*corev1.Pod{pod}, nil)

			if got.done {
				t.Errorf("done: got true want false (none of these is the promote signal)")
			}
			if got.phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating", got.phase)
			}
			if got.step != updateStepInPlace {
				t.Errorf("Operation.Step: got %q want %q", got.step, updateStepInPlace)
			}
			if got.incarnation != 1 {
				t.Errorf("Incarnation: got %d want 1 (in-place keeps the materialization)", got.incarnation)
			}
			if got.failed {
				t.Errorf("LastFailure: got one want none; only the operation deadline ends this roll")
			}
			if got.blocks != 0 {
				t.Errorf("RetryBlock writes: got %d want none", got.blocks)
			}
		})
	}
}

// TestUpdateInPlace_OperatorConfigChangesDoNotChangeThePatch: every knob
// below is re-read per reconcile, and each one has its reader somewhere
// else — the admission gate, the scale-down pacing, the PodGroup build,
// the migration admission, the end-of-pass prune, the wake-up cadence. A
// patch already converging reads none of them, so editing any of them
// leaves the row and its pod exactly as the unedited pass does.
//
// Cells: instance/Update/InPlace/config.changed[updateRetry]/before, instance/Update/InPlace/config.changed[forceDelete]/before, instance/Update/InPlace/config.changed[scaleDownInterval]/before, instance/Update/InPlace/config.changed[requeue]/before, instance/Update/InPlace/config.changed[gangScheduleTimeout]/before, instance/Update/InPlace/config.changed[retryBlockHistoryLimit]/before, instance/Update/InPlace/config.changed[audit]/before
func TestUpdateInPlace_OperatorConfigChangesDoNotChangeThePatch(t *testing.T) {
	baseline := inPlaceInFlightFixture(t)
	want := baseline.run(t, []*corev1.Pod{baseline.pod}, nil)

	for _, tc := range configChangeCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := inPlaceInFlightFixture(t)
			got := f.run(t, []*corev1.Pod{f.pod}, tc.tweak)
			if got != want {
				t.Errorf("the %s edit moved the roll: got %+v want the unedited %+v", tc.name, got, want)
			}
		})
	}
}

// configChange is one operator-config edit and the name the machine
// tables give it.
type configChange struct {
	name  string
	tweak func(*workload.ReconcileInput, *workload.ComponentPlan)
}

// configChangeCases is the set of knobs whose cells claim no reader on a
// roll in flight. retryBlockHistoryLimit has no field here on purpose:
// it is consumed by the status writer's end-of-pass prune, so what an
// update pass owes it is to write no RetryBlock at all — which every
// case here asserts through the outcome's block count.
func configChangeCases() []configChange {
	return []configChange{
		{
			name: "update retry policy",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.UpdateRetryPolicy = &workload.RetryPolicy{
					MaxAttempts: 7, InitialDelay: time.Minute, MaxDelay: time.Hour, Multiplier: 2,
				}
			},
		},
		{
			name: "force-delete policy",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.ForceDelete = fdPolicy()
			},
		},
		{
			name: "scale-down cadence",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.ScaleDownRequeueInterval = 90 * time.Second
			},
		},
		{
			name: "requeue cadence",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.Requeue = workload.RequeueIntervals{Operation: 11 * time.Minute, Gate: 7 * time.Minute}
			},
		},
		{
			name: "gang schedule clamp",
			tweak: func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.GangScheduleTimeout = &workload.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
			},
		},
		{
			name: "migration audit caps",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.MigrationAudit = &workload.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 2, Window: time.Hour}
			},
		},
		{
			name: "superseded RetryBlock history",
			tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.ObservedState.RetryBlocks = []workload.RetryBlock{
					{TargetRevision: "llama-70b-engine-stale001", State: workload.RetryBlockHeld},
					{TargetRevision: "llama-70b-engine-stale002", State: workload.RetryBlockBackoff},
				}
			},
		},
	}
}

// TestUpdateWithPods_InPlaceRollLosingItsPodTwiceReDerivesTheSameRecreate:
// the re-resolution is a step restamp on one operation, not a new
// attempt, so re-observing the same empty pod set commits nothing
// further. The second pass lands on the recreate, which owns the empty
// set from there and rebuilds under the same name at the same
// Incarnation.
//
// Cells: instance/Update/InPlace/pod.deleted[source]/boundary@T-inplace-podlost-recreate, instance/Update/InPlace/pod.deleted[source]/after@T-inplace-podlost-recreate
func TestUpdateWithPods_InPlaceRollLosingItsPodTwiceReDerivesTheSameRecreate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	targetSpec := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	recorder := record.NewFakeRecorder(32)
	deps := workload.Deps{Client: c, Recorder: recorder}

	// The boundary pass: the loss is observed and the step is restamped
	// to the recreate, which renders the replacement.
	if _, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, targetSpec, nil); err != nil {
		t.Fatalf("UpdateWithPods (boundary): %v", err)
	}
	first := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if first.Operation == nil || first.Operation.Step != updateStepDrain {
		t.Fatalf("boundary pass: got %+v want the step restamped to %s", first.Operation, updateStepDrain)
	}
	events := len(recorder.Events)

	// Delete what it rendered and hand the next pass the same empty set.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	for i := range pods.Items {
		if err := c.Delete(context.Background(), &pods.Items[i]); err != nil {
			t.Fatalf("delete the replacement: %v", err)
		}
	}
	legacyResetExpectations(t)
	if _, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, targetSpec, nil); err != nil {
		t.Fatalf("UpdateWithPods (after): %v", err)
	}

	second := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if second.Operation == nil || second.Operation.Step != updateStepDrain {
		t.Fatalf("after pass: got %+v want the recreate still owning the row", second.Operation)
	}
	if second.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (one attempt across both losses)", second.Operation.ID, op.ID)
	}
	if second.Incarnation != first.Incarnation {
		t.Errorf("Incarnation: got %d want %d (a re-observed loss re-materializes nothing new)",
			second.Incarnation, first.Incarnation)
	}
	if len(recorder.Events) != events {
		t.Errorf("events: got %d want %d; the re-derived write announces nothing", len(recorder.Events), events)
	}
}

// TestUpdateWithPods_InPlaceReadinessAndRetargetAfterThePodLoss: once the
// pod loss has moved the row to the recreate, the in-place promote is not
// what finishes the roll — the readiness of a pod the patch was aimed at
// cannot commit it, and a further revision move is decided by the
// recreate's own stamp rather than by the patch's.
//
// Cells: instance/Update/InPlace/pod.containersReady[source]/boundary@T-inplace-podlost-recreate, instance/Update/InPlace/pod.containersReady[source]/after@T-inplace-podlost-recreate, instance/Update/InPlace/spec.revision/boundary@T-inplace-podlost-recreate, instance/Update/InPlace/spec.revision/after@T-inplace-podlost-recreate
func TestUpdateWithPods_InPlaceReadinessAndRetargetAfterThePodLoss(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, _ := inPlaceRollWithoutPods(t)
	targetSpec := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(32)}

	// Boundary: the empty set re-resolves the roll and renders the
	// replacement. The pod that reported ready is gone, so nothing it
	// said can promote the patch being retired.
	done, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, targetSpec, nil)
	if err != nil {
		t.Fatalf("UpdateWithPods (boundary): %v", err)
	}
	if done {
		t.Fatal("the re-resolution pass must not promote")
	}
	bumped := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0].Incarnation

	// After: the row is on the recreate. The replacement reporting
	// ContainersReady is judged by the recreate's own bar, and a second
	// revision move falls through the recreate stamp's idempotency guard
	// — another Incarnation bump, not an in-place retarget.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected one replacement, got %d", len(pods.Items))
	}
	replacement := &pods.Items[0]
	replacement.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), replacement); err != nil {
		t.Fatalf("mark the replacement ContainersReady: %v", err)
	}
	nextSpec := legacyTargetSpecImage("llama:v3")
	nextCR := legacyEnsureTargetCR(t, c, isvc, nextSpec)

	legacyResetExpectations(t)
	next := legacyTestInput(isvc, c, workload.ComponentEngine)
	next.ObservedState.UpdateRevision = nextCR.Name
	done, err = UpdateWithPods(context.Background(), deps, next, plan, plan.Instances[0], nextCR, nextSpec,
		[]*corev1.Pod{replacement})
	if err != nil {
		t.Fatalf("UpdateWithPods (after): %v", err)
	}
	if done {
		t.Fatal("a retarget must not complete the roll")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != updateStepDrain {
		t.Fatalf("Operation: got %+v want the recreate still owning the row", s.Operation)
	}
	if s.Operation.TargetRevision != nextCR.Name {
		t.Errorf("Operation.TargetRevision: got %q want %q", s.Operation.TargetRevision, nextCR.Name)
	}
	if s.Incarnation <= bumped {
		t.Errorf("Incarnation: got %d want a further bump past %d (the recreate restarts Phase A)", s.Incarnation, bumped)
	}
}

// TestRecreateDrain_ReadyOldIncarnationPodIsNotThePromoteSignal: Phase A
// still owns a pod of the superseded Incarnation. That pod being PodReady
// and serving is what makes it worth draining, not evidence the roll is
// done — so the pass drains and deletes it and promotes nothing.
//
// Cells: instance/Update/Drain/pod.ready[source]/before
func TestRecreateDrain_ReadyOldIncarnationPodIsNotThePromoteSignal(t *testing.T) {
	f := recreateDrainInFlightFixture(t)
	old := legacyPodAtIncarnation(f.isvc, 0, 1, true /* ready */, true /* serving */)
	old.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	if err := f.c.Create(context.Background(), old); err != nil {
		t.Fatalf("seed the old-incarnation pod: %v", err)
	}
	f.pod = old

	got := f.run(t, []*corev1.Pod{old}, nil)
	if got.done {
		t.Errorf("done: got true want false (Phase A is not finished)")
	}
	if got.phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", got.phase)
	}
	if got.step != updateStepDrain {
		t.Errorf("Operation.Step: got %q want %q", got.step, updateStepDrain)
	}
	if got.podSurvives {
		t.Errorf("the ready old-incarnation pod survived Phase A; readiness does not exempt it from the drain")
	}
}

// TestRecreateDrain_PodObservationsThatDecideNothing: the recreate
// promotes over one bar — the bumped set complete, serving and PodReady
// past its availability window — and re-derives it every wake-up. A pod
// wedged Terminating, one whose node is gone, and one in a terminal phase
// all fail that bar in the same way: the roll waits, and only its
// operation deadline ends it.
//
// Cells: instance/Update/Drain/pod.stuckTerminating/before, instance/Update/Drain/pod.nodeLost[NodeGone]/before, instance/Update/Drain/pod.phaseTerminal[Failed]/before, instance/Update/Drain/pod.phaseTerminal[Succeeded]/before
func TestRecreateDrain_PodObservationsThatDecideNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		// incarnation the observed pod carries: 1 is a Phase A leftover,
		// 2 is a rebuilt member Phase C is waiting on.
		incarnation int64
		observe     func(t *testing.T, f *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod
	}{
		{
			name:        "an old-incarnation pod wedged Terminating",
			incarnation: 1,
			observe: func(t *testing.T, f *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				return terminatingPod(t, f.c, pod)
			},
		},
		{
			name:        "a rebuilt pod whose node is gone",
			incarnation: 2,
			observe: func(_ *testing.T, _ *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				out := pod.DeepCopy()
				out.Spec.NodeName = "node-that-no-longer-exists"
				return out
			},
		},
		{
			name:        "a rebuilt pod in a terminal Failed phase",
			incarnation: 2,
			observe: func(_ *testing.T, _ *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				out := pod.DeepCopy()
				out.Status.Phase = corev1.PodFailed
				return out
			},
		},
		{
			name:        "a rebuilt pod in a terminal Succeeded phase",
			incarnation: 2,
			observe: func(_ *testing.T, _ *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				out := pod.DeepCopy()
				out.Status.Phase = corev1.PodSucceeded
				return out
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := recreateDrainInFlightFixture(t)
			pod := legacyPodAtIncarnation(f.isvc, 0, tc.incarnation, true /* ContainersReady */, false /* not serving */)
			pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
			if err := f.c.Create(context.Background(), pod); err != nil {
				t.Fatalf("seed the observed pod: %v", err)
			}
			f.pod = pod

			got := f.run(t, []*corev1.Pod{tc.observe(t, f, pod)}, nil)
			if got.done {
				t.Errorf("done: got true want false (none of these clears the promote bar)")
			}
			if got.phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating", got.phase)
			}
			if got.step != updateStepDrain {
				t.Errorf("Operation.Step: got %q want %q", got.step, updateStepDrain)
			}
			if got.failed {
				t.Errorf("LastFailure: got one want none; only the operation deadline ends this roll")
			}
			if got.blocks != 0 {
				t.Errorf("RetryBlock writes: got %d want none", got.blocks)
			}
		})
	}
}

// TestRecreateDrain_OperatorConfigChangesDoNotChangeWhatItDecides: same
// claim as the in-place half — the recreate consults none of these knobs
// while it converges, so the row, the pod set and the pass's answer come
// out identical to the unedited pass.
//
// Cells: instance/Update/Drain/config.changed[updateRetry]/before, instance/Update/Drain/config.changed[forceDelete]/before, instance/Update/Drain/config.changed[scaleDownInterval]/before, instance/Update/Drain/config.changed[requeue]/before, instance/Update/Drain/config.changed[gangScheduleTimeout]/before, instance/Update/Drain/config.changed[retryBlockHistoryLimit]/before, instance/Update/Drain/config.changed[audit]/before
func TestRecreateDrain_OperatorConfigChangesDoNotChangeWhatItDecides(t *testing.T) {
	// A Phase C row: the rebuilt pod is ContainersReady but not yet
	// PodReady, so the pass has real work (the serving flip) and a real
	// wait to re-derive.
	seed := func(t *testing.T) (*nonSurgeFixture, *corev1.Pod) {
		t.Helper()
		f := recreateDrainInFlightFixture(t)
		pod := legacyPodAtIncarnation(f.isvc, 0, 2, true /* ContainersReady */, false /* not serving */)
		pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
		if err := f.c.Create(context.Background(), pod); err != nil {
			t.Fatalf("seed the rebuilt pod: %v", err)
		}
		f.pod = pod
		return f, pod
	}

	baseline, basePod := seed(t)
	want := baseline.run(t, []*corev1.Pod{basePod}, nil)

	for _, tc := range configChangeCases() {
		t.Run(tc.name, func(t *testing.T) {
			f, pod := seed(t)
			got := f.run(t, []*corev1.Pod{pod}, tc.tweak)
			if got != want {
				t.Errorf("the %s edit moved the roll: got %+v want the unedited %+v", tc.name, got, want)
			}
		})
	}
}

// TestRecreateDrain_ConflictOnTheServingFlipRetriesFromAFreshRead: Phase
// C's serving flip pins the resourceVersion of the read it was computed
// from, so a concurrent writer on the same pod rejects the patch. The
// row is not a casualty of that: it keeps its phase, its operation and
// its Incarnation, and the pass simply has to run again.
//
// Cells: instance/Update/Drain/api.conflict/before
func TestRecreateDrain_ConflictOnTheServingFlipRetriesFromAFreshRead(t *testing.T) {
	f := recreateDrainInFlightFixture(t)
	pod := legacyPodAtIncarnation(f.isvc, 0, 2, true /* ContainersReady */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	if err := f.c.Create(context.Background(), pod); err != nil {
		t.Fatalf("seed the rebuilt pod: %v", err)
	}

	base, ok := f.c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", f.c)
	}
	conflicting := interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return apierrors.NewConflict(corev1.Resource("pods"), obj.GetName(),
					errors.New("the object has been modified; please apply your changes to the latest version"))
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})

	in := f.input()
	plan := legacyComponentPlan(f.strategy, nil)
	deps := workload.Deps{Client: conflicting, Recorder: record.NewFakeRecorder(16)}
	done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0], f.targetCR, f.targetSpec,
		[]*corev1.Pod{pod})
	if done {
		t.Errorf("done: got true want false (the serving flip never landed)")
	}
	if err == nil {
		t.Errorf("error: got nil want the conflict surfaced so the pass runs again")
	}

	s := legacyInstanceStatusesOnIR(f.c, f.isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating (a conflict costs a pass, not the attempt)", s.Phase)
	}
	if s.Operation == nil || s.Operation.Step != updateStepDrain {
		t.Errorf("Operation: got %+v want the recreate intact", s.Operation)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2", s.Incarnation)
	}
	if len(*f.blocks) != 0 {
		t.Errorf("RetryBlock writes: got %v want none (a conflict blames no revision)", *f.blocks)
	}
}
