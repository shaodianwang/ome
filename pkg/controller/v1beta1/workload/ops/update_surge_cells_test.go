package ops

// The SurgeThenDrain cycle runs two ordinal slots on one Instance index,
// so almost everything that happens to it while it is in flight has to be
// answered without moving the step: the source is capacity until the
// drain says otherwise, and the replacement is the only route back to it.
// These tests pin that half of the contract for both surge states — what
// an observation, an operator edit or an apiserver refusal does NOT move.

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// surgeOutcome is everything one pass over a surge in flight may move.
type surgeOutcome struct {
	done          bool
	phase         v1beta1.OMENativeInstancePhase
	step          string
	activeOrdinal int32
	waiting       string
	failed        bool
	blocks        int
	sourceAlive   bool
	sourceServing bool
	targetAlive   bool
	targetServing bool
}

// runSurgePass drives one UpdateWithPods pass over the fixture's live pod
// set and reports what moved. tweak may edit the input and the plan the
// way an operator config change would.
func runSurgePass(t *testing.T, f *midSurgeFixture, tweak func(*workload.ReconcileInput, *workload.ComponentPlan)) surgeOutcome {
	t.Helper()
	in := legacyTestInput(f.isvc, f.client, workload.ComponentEngine)
	in.ObservedState.UpdateRevision = f.targetCR.Name
	blocks := &[]string{}
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	if tweak != nil {
		tweak(&in, &plan)
	}
	done, err := UpdateWithPods(context.Background(), legacyTestDeps(f.client), in, plan,
		plan.Instances[0], f.targetCR, f.targetSpec, f.survivingPods(t))
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	out := surgeOutcome{
		done:          done,
		phase:         s.Phase,
		activeOrdinal: s.ActiveOrdinal,
		failed:        s.LastFailure != nil,
		blocks:        len(*blocks),
	}
	if s.Operation != nil {
		out.step, out.waiting = s.Operation.Step, s.Operation.Waiting
	}
	if pod, alive := f.livePod(t, f.sourcePod); alive {
		out.sourceAlive, out.sourceServing = true, podreadiness.IsServing(pod)
	}
	if pod, alive := f.livePod(t, f.surgePod); alive {
		out.targetAlive, out.targetServing = true, podreadiness.IsServing(pod)
	}
	return out
}

// surgeConfigChangeCases is the operator-config edit set the surge cells
// claim no reader for. It is the shared set plus the relocation budget,
// which the surge rows name and the non-surge rows do not.
//
// Two knobs in those cells have no field to set here. teardownDeadline
// lives on the owner's teardown clock and retryBlockHistoryLimit on the
// status writer's end-of-pass prune; what a surge pass owes either is to
// write nothing of its own, which the outcome comparison asserts.
func surgeConfigChangeCases() []configChange {
	return append(configChangeCases(), configChange{
		name: "auto-migration budget",
		tweak: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
			in.Disposition.AutoMigrateMaxAttempts = 3
		},
	})
}

// TestSurge_OperatorConfigChangesDoNotChangeWhatItDecides: none of these
// knobs is read where a surge decides its next action. The retry policy
// and the relocation budget are consulted only once an escalation fires,
// the teardown and scale-down knobs belong to their own passes, the gang
// clamp to the PodGroup build, the audit caps to migration admission, and
// the requeue cadence only paces how soon the row is looked at again. An
// edited pass must therefore leave the row and both ordinal slots exactly
// where the unedited one does.
//
// Cells: instance/Update/Surge/config.changed[updateRetry]/before, instance/Update/Surge/config.changed[forceDelete]/before, instance/Update/Surge/config.changed[teardownDeadline]/before, instance/Update/Surge/config.changed[scaleDownInterval]/before, instance/Update/Surge/config.changed[autoMigrateBudget]/before, instance/Update/Surge/config.changed[requeue]/before, instance/Update/Surge/config.changed[gangScheduleTimeout]/before, instance/Update/Surge/config.changed[audit]/before, instance/Update/Surge/config.changed[retryBlockHistoryLimit]/before, instance/Update/SurgeDrain/config.changed[updateRetry]/before, instance/Update/SurgeDrain/config.changed[forceDelete]/before, instance/Update/SurgeDrain/config.changed[teardownDeadline]/before, instance/Update/SurgeDrain/config.changed[scaleDownInterval]/before, instance/Update/SurgeDrain/config.changed[autoMigrateBudget]/before, instance/Update/SurgeDrain/config.changed[requeue]/before, instance/Update/SurgeDrain/config.changed[gangScheduleTimeout]/before, instance/Update/SurgeDrain/config.changed[audit]/before, instance/Update/SurgeDrain/config.changed[retryBlockHistoryLimit]/before
func TestSurge_OperatorConfigChangesDoNotChangeWhatItDecides(t *testing.T) {
	for _, step := range []struct {
		name          string
		step          string
		surgeReady    bool
		sourceServing bool
	}{
		{"surge", updateStepSurge, false, true},
		{"drain", updateStepSurgeDrain, true, false},
	} {
		t.Run(step.name, func(t *testing.T) {
			want := runSurgePass(t, newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing), nil)
			for _, tc := range surgeConfigChangeCases() {
				t.Run(tc.name, func(t *testing.T) {
					f := newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing)
					if got := runSurgePass(t, f, tc.tweak); got != want {
						t.Errorf("the %s edit moved the cycle: got %+v want the unedited %+v", tc.name, got, want)
					}
				})
			}
		})
	}
}

// TestSurge_PodObservationsThatHoldTheStep: the surge advances on one
// thing — the replacement clearing the promote bar — and the drain on
// one other — the source's pods disappearing. Everything else the
// kubelet can report about either slot is a wait: none of them ends the
// attempt, recreates the occupied slot, or moves the step. The operation
// deadline is what bounds them.
//
// Cells: instance/Update/Surge/pod.containerRestart/before, instance/Update/Surge/pod.stuckTerminating/before, instance/Update/Surge/pod.phaseTerminal[Failed]/before, instance/Update/SurgeDrain/pod.stuckTerminating/before, instance/Update/SurgeDrain/pod.nodeLost[NodeGone]/before
func TestSurge_PodObservationsThatHoldTheStep(t *testing.T) {
	for _, tc := range []struct {
		name          string
		step          string
		surgeReady    bool
		sourceServing bool
		// observe decorates the fixture in place, in the cluster, so the
		// pass's own live read sees it.
		observe func(t *testing.T, f *midSurgeFixture)
	}{
		{
			name: "a container of the replacement restarting",
			step: updateStepSurge, sourceServing: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				patchPodStatus(t, f, f.surgePod, func(pod *corev1.Pod) {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
						Name: "main", RestartCount: 4,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
					}}
				})
			},
		},
		{
			name: "the replacement wedged Terminating at the surge ordinal",
			step: updateStepSurge, sourceServing: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				terminatingPod(t, f.client, f.surgePod)
			},
		},
		{
			name: "the replacement in a terminal Failed phase",
			step: updateStepSurge, sourceServing: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				patchPodStatus(t, f, f.surgePod, func(pod *corev1.Pod) {
					pod.Status.Phase = corev1.PodFailed
				})
			},
		},
		{
			name: "the draining source wedged Terminating",
			step: updateStepSurgeDrain, surgeReady: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				terminatingPod(t, f.client, f.sourcePod)
			},
		},
		{
			name: "the node under the draining source gone",
			step: updateStepSurgeDrain, surgeReady: true,
			observe: func(t *testing.T, f *midSurgeFixture) {
				terminatingPod(t, f.client, f.sourcePod)
				patchPodStatus(t, f, f.sourcePod, func(pod *corev1.Pod) {
					pod.Status.Reason = "NodeLost"
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, tc.step, tc.surgeReady, tc.sourceServing)
			tc.observe(t, f)

			got := runSurgePass(t, f, nil)

			if got.done {
				t.Errorf("done: got true want false (none of these completes the cycle)")
			}
			if got.phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating", got.phase)
			}
			if got.step != tc.step {
				t.Errorf("Operation.Step: got %q want %q", got.step, tc.step)
			}
			if got.activeOrdinal != 0 {
				t.Errorf("ActiveOrdinal: got %d want 0 (no promote)", got.activeOrdinal)
			}
			if got.failed {
				t.Errorf("LastFailure: got one want none; only the deadline ends this cycle")
			}
			if got.blocks != 0 {
				t.Errorf("RetryBlock writes: got %d want none", got.blocks)
			}
			if !got.sourceAlive {
				t.Errorf("the source was deleted; nothing here converges a drain")
			}
			if !got.targetAlive {
				t.Errorf("the occupied surge slot was recreated or collected")
			}
		})
	}
}

// patchPodStatus applies mutate to the live copy of pod and writes the
// status back, so the pass's own read observes it.
func patchPodStatus(t *testing.T, f *midSurgeFixture, pod *corev1.Pod, mutate func(*corev1.Pod)) {
	t.Helper()
	live, alive := f.livePod(t, pod)
	if !alive {
		t.Fatalf("pod %s is gone", pod.Name)
	}
	mutate(live)
	if err := f.client.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("patch pod %s status: %v", pod.Name, err)
	}
}

// TestSurgeDrain_RepeatedUnrouteAfterTheDeleteWritesNothing: the drain
// takes the source out of rotation itself and then deletes it. Once that
// delete is issued, the source being reported out of rotation again — by
// its gate or by its endpoint — is a restatement of what the step already
// acted on, so the pass re-derives the same wait and commits nothing.
//
// The delete is held open with a finalizer, which is what keeps the row
// on SurgeDrain for the second observation instead of promoting past it.
//
// Cells: instance/Update/SurgeDrain/pod.servingOff[source]/boundary@T-surgedrain-promote, instance/Update/SurgeDrain/pod.servingOff[source]/after@T-surgedrain-promote, instance/Update/SurgeDrain/endpoint.rotationOut[source]/boundary@T-surgedrain-promote, instance/Update/SurgeDrain/endpoint.rotationOut[source]/after@T-surgedrain-promote
func TestSurgeDrain_RepeatedUnrouteAfterTheDeleteWritesNothing(t *testing.T) {
	f := newMidSurgeFixture(t, updateStepSurgeDrain, true /* surgeReady */, false /* sourceServing */)
	terminatingPod(t, f.client, f.sourcePod)

	first := runSurgePass(t, f, nil)
	if first.step != updateStepSurgeDrain || first.done {
		t.Fatalf("first pass: got %+v want the drain still waiting on its source", first)
	}
	rv := readRV(t, f.client, f.isvc)

	// The same two observations again, on a source whose delete is
	// already in flight: the gate is off and no endpoint is left.
	second := runSurgePass(t, f, nil)

	if second != first {
		t.Errorf("re-observed unroute moved the row: got %+v want the unchanged %+v", second, first)
	}
	if got := readRV(t, f.client, f.isvc); got != rv {
		t.Errorf("owner resourceVersion: got %s want %s (a repeated unroute writes nothing)", got, rv)
	}
}

// TestSurgeUpdate_SourceLostBeforeTheDrainPromotesTheReplacement: a
// single-pod source that disappears before the drain step skips the whole
// drain block — there is nothing left to take out of rotation — but not
// the promote bar. The replacement is promoted onto the pinned revision
// with ActiveOrdinal advanced to its slot, and the restart trigger never
// claims the loss because the row is not Ready.
//
// Cells: instance/Update/Surge/pod.deleted[source]/before
func TestSurgeUpdate_SourceLostBeforeTheDrainPromotesTheReplacement(t *testing.T) {
	f := newMidSurgeFixture(t, updateStepSurge, true /* surgeReady */, true /* sourceServing */)
	if err := f.client.Delete(context.Background(), f.sourcePod); err != nil {
		t.Fatalf("lose the source: %v", err)
	}

	got := runSurgePass(t, f, nil)

	if !got.done {
		t.Fatalf("the promote must complete the cycle once the source is gone: %+v", got)
	}
	if got.phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", got.phase)
	}
	if got.activeOrdinal != 1 {
		t.Errorf("ActiveOrdinal: got %d want 1 (the replacement's slot)", got.activeOrdinal)
	}
	if !got.targetAlive {
		t.Errorf("the replacement must be the Instance's pod, not collected with the source")
	}
}

// TestSurgeUpdate_LateReadyReplacementDoesNotUnfailAStuckSurge: the
// stuck-pod escalator has already ended this attempt, and a replacement
// that reaches Ready afterwards is not a retraction. The surge entry
// stamp refuses to resurrect a Failed row at the same target, so the row
// stays Failed and recovery stays where it belongs — a corrective
// revision or the retry gate.
//
// Cells: instance/Update/Surge/pod.ready[target]/after@T-surge-stuck
func TestSurgeUpdate_LateReadyReplacementDoesNotUnfailAStuckSurge(t *testing.T) {
	f := newMidSurgeFixture(t, updateStepSurge, true /* surgeReady */, true /* sourceServing */)
	failInstanceRow(t, f, "StuckPod")

	got := runSurgePass(t, f, nil)

	if got.phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("Phase: got %q want Failed (a late readiness observation does not un-fail the row)", got.phase)
	}
	if got.activeOrdinal != 0 {
		t.Errorf("ActiveOrdinal: got %d want 0 (nothing is promoted out of Failed)", got.activeOrdinal)
	}
	if got.done {
		t.Errorf("done: got true want false")
	}
}

// failInstanceRow stamps the row Failed with reason, the shape the
// escalation pass leaves behind.
func failInstanceRow(t *testing.T, f *midSurgeFixture, reason string) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: f.isvc.Namespace, Name: legacyIRName(f.isvc, workload.ComponentEngine)}
	if err := f.client.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed
	ir.Status.InstanceStatuses[0].LastFailure = &v1beta1.InstanceTermination{
		Reason: reason, Time: metav1.Now(),
	}
	if err := f.client.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("stamp the row Failed: %v", err)
	}
}

// TestSurge_ExcludedAnnotationEditLeavesTheCycleAlone: annotations and
// labels the revision payload filters out are not part of the hash, so
// the pinned target does not move and neither surge state has anything
// new to act on. The hash check is half the claim — without it a pass
// that decided the same thing would only mean the edit never reached the
// renderer.
//
// Cells: instance/Update/Surge/spec.annotationOnly/before, instance/Update/SurgeDrain/spec.annotationOnly/before
func TestSurge_ExcludedAnnotationEditLeavesTheCycleAlone(t *testing.T) {
	before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
	after := &metav1.ObjectMeta{Annotations: map[string]string{
		"ome.io/base-model-name":                   "llama-7b",
		constants.ReleaseHeldRevisionAnnotationKey: "llama-70b-engine-old00001",
	}}

	for _, step := range []struct {
		name          string
		step          string
		surgeReady    bool
		sourceServing bool
	}{
		{"surge", updateStepSurge, false, true},
		{"drain", updateStepSurgeDrain, true, false},
	} {
		t.Run(step.name, func(t *testing.T) {
			// One fixture per pass: they share the process-wide
			// expectations cache, which every build resets.
			baseline := newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing)
			hBefore, _, err := revision.Hash(baseline.targetSpec, before, nil, "")
			if err != nil {
				t.Fatalf("hash before the edit: %v", err)
			}
			hAfter, _, err := revision.Hash(baseline.targetSpec, after, nil, "")
			if err != nil {
				t.Fatalf("hash after the edit: %v", err)
			}
			if hBefore != hAfter {
				t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
			}
			want := runSurgePass(t, baseline, nil)

			f := newMidSurgeFixture(t, step.step, step.surgeReady, step.sourceServing)
			got := runSurgePass(t, f, func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.DesiredSpec.PodTemplateObjectMeta = after
			})
			if got != want {
				t.Errorf("the annotation edit moved the cycle: got %+v want the unedited %+v", got, want)
			}
		})
	}
}

// TestSurge_StrategyEditBesideAnotherWriteChangesNothing: the mechanism
// of an attempt is pinned on its operation, so an edit that lands in the
// same window as the step stamp changes neither the mode this attempt
// finishes under nor the write in flight. The pass that advances the step
// under an edited strategy is the same pass, ordinal for ordinal, as the
// one that advances it under the pinned one.
//
// Cells: instance/Update/Surge/spec.strategy/boundary@T-surge-target-ready, instance/Update/Surge/spec.strategy/after@T-surge-target-ready
func TestSurge_StrategyEditBesideAnotherWriteChangesNothing(t *testing.T) {
	// The fixture sits exactly at the step boundary: the replacement has
	// cleared the promote bar, so this pass writes SurgeDrain.
	want := runSurgePass(t, newMidSurgeFixture(t, updateStepSurge, true, true), nil)
	if want.step != updateStepSurgeDrain {
		t.Fatalf("fixture: got step %q want the pass to stamp %q", want.step, updateStepSurgeDrain)
	}

	for _, strategy := range []workload.UpdateStrategyType{
		workload.UpdateStrategyInPlaceIfPossible,
		workload.UpdateStrategyInPlaceOnly,
		workload.UpdateStrategyRecreatePod,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			f := newMidSurgeFixture(t, updateStepSurge, true, true)
			boundary := runSurgePass(t, f, func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.UpdateStrategy.Type = strategy
			})
			if boundary != want {
				t.Errorf("boundary pass under %s: got %+v want the pinned %+v", strategy, boundary, want)
			}
			// And the pass after the stamp finishes the cycle as a
			// surge: the drained source is gone and the replacement is
			// promoted onto its own ordinal, which is a shape none of
			// the edited-to strategies has.
			after := runSurgePass(t, f, func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.UpdateStrategy.Type = strategy
			})
			if !after.done || after.phase != v1beta1.OMENativeInstanceReady || after.activeOrdinal != 1 {
				t.Errorf("after the stamp under %s: got %+v want the surge cycle completing on ordinal 1", strategy, after)
			}
		})
	}
}

// TestSurgeUpdate_ConflictOnTheServingFlipCostsAPassNotTheAttempt: both
// surge states write a serving gate — the surge puts the replacement in
// rotation, the drain takes the source out — and both writes pin the
// resourceVersion of the read they were computed from. A concurrent
// writer on the same pod therefore rejects the patch. The row is not a
// casualty of that: it keeps its phase, its step and its pin, no revision
// is blamed, and the pass simply has to run again.
//
// Cells: instance/Update/Surge/api.conflict/before, instance/Update/SurgeDrain/api.conflict/before
func TestSurgeUpdate_ConflictOnTheServingFlipCostsAPassNotTheAttempt(t *testing.T) {
	for _, tc := range []struct {
		name string
		step string
	}{
		{name: "surge marks the replacement serving", step: updateStepSurge},
		{name: "drain takes the source out of rotation", step: updateStepSurgeDrain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, tc.step, true /* surgeReady */, true /* sourceServing */)
			// The replacement is runtime-ready but not yet gated, so the
			// surge step's own write is the one that races.
			clearServingGate(t, f, f.surgePod)

			in := legacyTestInput(f.isvc, f.client, workload.ComponentEngine)
			in.ObservedState.UpdateRevision = f.targetCR.Name
			blocks := &[]string{}
			in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
			deps := legacyTestDeps(conflictOnPodStatusPatch(t, f.client))

			done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0],
				f.targetCR, f.targetSpec, f.survivingPods(t))
			if done {
				t.Errorf("done: got true want false (the gate write never landed)")
			}
			if err == nil {
				t.Errorf("error: got nil want the conflict surfaced so the pass runs again")
			}

			s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
			if s.Phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating (a conflict costs a pass, not the attempt)", s.Phase)
			}
			if s.Operation == nil || s.Operation.TargetRevision != f.targetCR.Name {
				t.Errorf("Operation: got %+v want the surge still pinned to %s", s.Operation, f.targetCR.Name)
			}
			if s.ActiveOrdinal != 0 {
				t.Errorf("ActiveOrdinal: got %d want 0 (nothing was promoted)", s.ActiveOrdinal)
			}
			if len(*blocks) != 0 {
				t.Errorf("RetryBlock writes: got %v want none (a conflict blames no revision)", *blocks)
			}
		})
	}
}

// clearServingGate drops the lifecycle serving condition from pod, the
// state a replacement is in between ContainersReady and the surge's own
// gate write.
func clearServingGate(t *testing.T, f *midSurgeFixture, pod *corev1.Pod) {
	t.Helper()
	patchPodStatus(t, f, pod, func(live *corev1.Pod) {
		kept := live.Status.Conditions[:0]
		for _, cond := range live.Status.Conditions {
			if cond.Type == query.ServingConditionType {
				continue
			}
			kept = append(kept, cond)
		}
		live.Status.Conditions = kept
	})
}

// conflictOnPodStatusPatch wraps c so every pod status patch loses the
// race, the shape a concurrent writer on the same object produces.
func conflictOnPodStatusPatch(t *testing.T, c client.Client) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return apierrors.NewConflict(corev1.Resource("pods"), obj.GetName(),
					errors.New("the object has been modified; please apply your changes to the latest version"))
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// TestSurgeDrainRecreate_RejectionDispositions: past the surge step the
// source is already out of rotation, so the create that rebuilds a lost
// replacement is the row's only way back to capacity. It meets the same
// classified rejections a fresh create does and is disposed the same way:
// a 422 is permanent and blames the pinned revision, a terminating
// namespace is permanent and blames nothing, a quota refusal is a wait
// that parks the clock, and a 429 is pacing that writes nothing at all.
//
// Cells: instance/Update/SurgeDrain/api.invalid/before, instance/Update/SurgeDrain/api.namespaceTerminating/before, instance/Update/SurgeDrain/api.quotaDenied/before, instance/Update/SurgeDrain/api.throttled[TooManyRequests]/before
func TestSurgeDrainRecreate_RejectionDispositions(t *testing.T) {
	for _, tc := range surgeRejectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, updateStepSurgeDrain, true /* surgeReady */, false /* sourceServing */)
			// The replacement is lost, so this pass's work is the rebuild
			// create — the only write a rejection can land on here.
			if err := f.client.Delete(context.Background(), f.surgePod); err != nil {
				t.Fatalf("lose the replacement: %v", err)
			}
			assertSurgeRejection(t, f, tc)
		})
	}
}

// TestSurgeCreate_RejectionDispositions: the surge's Phase 1 create meets
// the same classified rejections, and the source keeps serving through
// every one of them. A 422 ends the attempt with the pinned revision
// blamed, a terminating namespace ends it with nothing blamed, a quota
// refusal waits with the clock parked, and a 429 writes nothing.
//
// Cells: instance/Update/Surge/api.invalid/before, instance/Update/Surge/api.namespaceTerminating/before, instance/Update/Surge/api.throttled[TooManyRequests]/before
func TestSurgeCreate_RejectionDispositions(t *testing.T) {
	for _, tc := range surgeRejectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, updateStepSurge, false /* surgeReady */, true /* sourceServing */)
			if err := f.client.Delete(context.Background(), f.surgePod); err != nil {
				t.Fatalf("clear the surge slot: %v", err)
			}
			assertSurgeRejection(t, f, tc)
		})
	}
}

// surgeRejection is one classified apiserver refusal and what the surge
// owes it.
type surgeRejection struct {
	name       string
	reject     error
	wantFailed bool
	wantReason string
	wantBlocks int
	wantWait   bool
	wantPacing time.Duration
}

func surgeRejectionCases() []surgeRejection {
	return []surgeRejection{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantFailed: true,
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     siteNamespaceTerminatingError("engine-pod"),
			wantFailed: true,
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
		{
			name:     "quota exceeded",
			reject:   siteQuotaError(),
			wantWait: true,
		},
		{
			name:       "throttled",
			reject:     apierrors.NewTooManyRequests("apiserver is shedding load", 7),
			wantPacing: 7 * time.Second,
		},
	}
}

// assertSurgeRejection drives one pass whose pod creates are refused with
// tc.reject and checks the row against what the disposition owes.
func assertSurgeRejection(t *testing.T, f *midSurgeFixture, tc surgeRejection) {
	t.Helper()
	base := f.client
	in := legacyTestInput(f.isvc, base, workload.ComponentEngine)
	in.ObservedState.UpdateRevision = f.targetCR.Name
	in.Pacing = &workload.APIPacing{}
	blocks := &[]string{}
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	deps := legacyTestDeps(rejectPodCreates(t, base, tc.reject))

	done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0],
		f.targetCR, f.targetSpec, f.survivingPods(t))
	if err != nil {
		t.Fatalf("UpdateWithPods: %v (a classified rejection is disposed, not returned)", err)
	}
	if done {
		t.Fatal("a refused create must not complete the cycle")
	}

	s := legacyInstanceStatusesOnIR(base, f.isvc, workload.ComponentEngine)[0]
	switch {
	case tc.wantFailed:
		if s.Phase != v1beta1.OMENativeInstanceFailed {
			t.Fatalf("Phase: got %q want Failed (the rejection is permanent)", s.Phase)
		}
		if s.Operation != nil {
			t.Errorf("Operation: got %+v want cleared with the disposal", s.Operation)
		}
		if s.LastFailure == nil || s.LastFailure.Reason != tc.wantReason {
			t.Errorf("LastFailure: got %+v want reason %s", s.LastFailure, tc.wantReason)
		}
	default:
		if s.Phase != v1beta1.OMENativeInstanceUpdating {
			t.Errorf("Phase: got %q want Updating (the cycle is still in flight)", s.Phase)
		}
		capacityBlocked := s.Operation != nil && workload.OperationCapacityBlocked(legacyFromV1beta1Op(s.Operation))
		if capacityBlocked != tc.wantWait {
			t.Errorf("Operation: got %+v want capacity-blocked=%v", s.Operation, tc.wantWait)
		}
	}
	if len(*blocks) != tc.wantBlocks {
		t.Errorf("RetryBlock writes: got %v want %d (only a bad pod spec blames the revision)", *blocks, tc.wantBlocks)
	}
	if got := in.Pacing.Throttle(); got != tc.wantPacing {
		t.Errorf("pass pacing: got %v want %v", got, tc.wantPacing)
	}
}

// TestGangSurge_ReplacementPodsLostAroundThePromote: the gang handoff is
// one write that retires the source row and makes the replacement index
// the Instance, and it is preconditioned on the replacement still being
// the whole gang it claimed to be. A replacement whose pods vanished
// before that write is therefore not promoted — the pass rebuilds the
// missing members and the source row stays exactly where it was. Once
// the write has landed the question belongs to the other row: the source
// is gone, the replacement carries no operation, and nothing in the
// surge machine is left to claim a pod it loses.
//
// Cells: instance/Update/SurgeDrain/pod.deleted[target]/boundary@T-surgedrain-gang-complete, instance/Update/SurgeDrain/pod.deleted[target]/after@T-surgedrain-gang-complete
func TestGangSurge_ReplacementPodsLostAroundThePromote(t *testing.T) {
	const isvcName, ns, targetRev = "gang-handoff", "test-ns", "gang-handoff-engine-newrev"
	surgeIdx := int32(2)

	// build returns the drain-step pair and the client holding whatever
	// replacement pods the caller asked for. The source has no pods left,
	// so the promote is the only work the pass has.
	build := func(t *testing.T, withPods bool) (workload.ReconcileInput, *workload.InstanceStatus, *[]int32, workload.ComponentPlan, *appsv1.ControllerRevision, client.Client) {
		t.Helper()
		legacyResetExpectations(t)
		var objs []client.Object
		if withPods {
			for _, runner := range []string{"leader", "worker"} {
				pod := gangSurgePod(isvcName, ns, surgeIdx, runner, "newrev")
				pod.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
				}
				objs = append(objs, pod)
			}
		}
		c := legacyNewFakeClient(t, objs...)
		src := &workload.InstanceStatus{
			Index:           0,
			Phase:           workload.InstancePhaseUpdating,
			RunningRevision: "gang-handoff-engine-oldrev",
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           updateStepSurgeDrain,
				SurgeIndex:     &surgeIdx,
				TargetRevision: targetRev,
			},
		}
		removed := &[]int32{}
		in := gangAbandonInput(isvcName, ns, src, removed)
		isvc := legacyMinimalISVC(isvcName, ns, 1)
		in.EventTarget, in.OwnerObject = isvc, isvc
		in.OwnerGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")
		in.DesiredSpec = workload.WorkloadDesiredSpec{
			PodSpec:       legacyTargetSpecImage("example.com/app:v2"),
			WorkerPodSpec: legacyTargetSpecImage("example.com/app:v2"),
		}
		in.ObservedState.InstanceStatuses = []workload.InstanceStatus{*src, {
			Index:          surgeIdx,
			Incarnation:    1,
			Phase:          workload.InstancePhaseCreating,
			TargetRevision: targetRev,
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepGangSurgeTarget,
				TargetRevision: targetRev,
			},
		}}
		return in, src, removed, legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain),
			&appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRev}}, c
	}

	t.Run("the replacement's pods vanish before the write", func(t *testing.T) {
		in, src, removed, plan, target, c := build(t, false /* no replacement pods */)

		done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), in, plan, plan.Instances[0], target)
		if err != nil {
			t.Fatalf("gangSurgeUpdate: %v", err)
		}
		if done {
			t.Fatalf("the handoff must not complete with the replacement gang missing")
		}
		if len(*removed) != 0 {
			t.Errorf("RemoveInstance: got %v want none (the source row stays)", *removed)
		}
		if src.Operation == nil || src.Operation.Step != updateStepSurgeDrain {
			t.Errorf("source Operation: got %+v want the drain still open at %s", src.Operation, updateStepSurgeDrain)
		}
		if src.Phase != workload.InstancePhaseUpdating {
			t.Errorf("source Phase: got %q want Updating", src.Phase)
		}
	})

	t.Run("the write has landed", func(t *testing.T) {
		in, src, removed, plan, target, c := build(t, true /* replacement gang serving */)

		done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), in, plan, plan.Instances[0], target)
		if err != nil {
			t.Fatalf("gangSurgeUpdate: %v", err)
		}
		if !done {
			t.Fatalf("the handoff must complete with the replacement gang serving: src=%+v", src)
		}
		if len(*removed) != 1 || (*removed)[0] != 0 {
			t.Fatalf("RemoveInstance: got %v want the source row retired", *removed)
		}
		// Nothing of the surge is left on the replacement index, so a pod
		// it loses from here is the create and restart passes' business.
		for _, s := range in.ObservedState.InstanceStatuses {
			if s.Index != surgeIdx {
				continue
			}
			if s.Operation != nil && s.Operation.SurgeIndex != nil {
				t.Errorf("replacement row: got %+v want no surge pin left to claim a lost pod", s.Operation)
			}
		}
	})
}
