package workload_test

// The end-of-pass block (escalation, then the RetryBlock supersede-prune)
// is per-Instance and re-derives its own evidence, so it must survive an
// op pass that errored for one Instance. These tests drive
// workload.Execute with an op pass that fails and assert what the pass
// still does — and, just as importantly, what it must leave alone.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// rejectedStatusWrite is the 422 an apiserver returns for a status write
// it will never accept, whatever the pass retries.
func rejectedStatusWrite() error {
	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ome.io", Kind: "InferenceReplica"},
		"llama-70b-engine",
		field.ErrorList{field.Invalid(field.NewPath("status", "instances"), "", "rejected")},
	)
}

// restartOnlyDecision selects exactly one Instance for the restart pass
// and leaves the end-of-pass block enabled.
func restartOnlyDecision(inst workload.InstancePlan) workload.Decision {
	return workload.Decision{
		Actions: []workload.PlannedAction{{
			Kind:     workload.ActionRestart,
			Restarts: []workload.RestartSelection{{Instance: inst, Reason: "pod count 0 below desired 1"}},
		}},
		Escalate: true,
	}
}

// threeInstancePlan covers indices 0..2 with one pod each.
func threeInstancePlan() workload.ComponentPlan {
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, Replicas: 3}
	for idx := int32(0); idx < 3; idx++ {
		plan.Instances = append(plan.Instances, workload.InstancePlan{
			Index: idx, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		})
	}
	return plan
}

// TestExecuteErroredOpPassStillEscalatesExpiredDeadline: the restart pass
// fails on instance 1 (its status write is rejected 422 and the
// classifier has no attempt to dispose, so the error propagates), and the
// pass still escalates instance 0's elapsed operation deadline and prunes
// the superseded RetryBlock. Instance 2 is parked on a quota wait: the
// pass must leave its Waiting token and its zeroed deadline exactly as
// they were, because escalation only reads holds and re-derives them from
// live evidence.
//
// Cells: instance/Ready/api.invalid/before, instance/Update/Surge/timer.operationDeadline/before
func TestExecuteErroredOpPassStillEscalatesExpiredDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	surgeIdx := int32(10)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{
				ID:             "update-0",
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepSurge,
				SurgeIndex:     &surgeIdx,
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workload.InstancePhaseReady},
		{
			Index: 2, Incarnation: 1, PodCount: 1, Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				ID:             "create-2",
				Type:           workload.InstanceOperationCreate,
				Step:           "CreatePods",
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Waiting:        workload.RejectionReasonQuotaExceeded,
			},
		},
	}
	in.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: "superseded"}}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	rejected := rejectedStatusWrite()
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		if idx == 1 {
			return rejected
		}
		return store.apply(ctx, []workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	plan := threeInstancePlan()
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	deps := workload.Deps{Client: c, APIReader: c, Expectations: workload.NewExpectations()}
	snapshot := workload.SnapshotWithPodsForTest(in, nil)

	result, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))

	if err == nil {
		t.Fatal("the pass must still report the restart failure")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("returned error must carry the apiserver rejection; got %v", err)
	}
	if !strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("returned error must name the failed Instance; got %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %+v, want the errored pass's empty result", result)
	}
	if got := store.status(0); got == nil || got.Phase != workload.InstancePhaseFailed ||
		got.LastFailure == nil || got.LastFailure.Reason != workload.DeadlineExceededReason || got.Operation == nil {
		t.Fatalf("expired Instance was not escalated: %+v", got)
	}
	if got := store.status(1); got == nil || got.Phase != workload.InstancePhaseReady || got.Operation != nil {
		t.Fatalf("instance 1 must be untouched while its writes are rejected: %+v", got)
	}
	held := store.status(2)
	if held == nil || held.Phase != workload.InstancePhaseCreating || held.Operation == nil {
		t.Fatalf("held Instance changed phase: %+v", held)
	}
	if held.Operation.Waiting != workload.RejectionReasonQuotaExceeded {
		t.Fatalf("held Instance lost its Waiting token: %q", held.Operation.Waiting)
	}
	if !held.Operation.Deadline.IsZero() {
		t.Fatalf("held Instance's parked deadline was re-armed: %v", held.Operation.Deadline)
	}
	if warnings != 1 || prunes != 1 {
		t.Fatalf("end-of-pass effects: warnings=%d prunes=%d, want 1/1", warnings, prunes)
	}
}

// TestExecuteErroredPodReadDoesNotFabricateStuckEvidence: the restart
// pass fails because the Pod list failed, so the escalation pass reading
// the same memoized source fails too. The evidence it would have used is
// deadline-only — a read failure never invents a stuck pod — nothing is
// stamped Failed, and the joined error keeps the op error first.
//
// Cells: instance/Ready/api.throttled[ServiceUnavailable]/before, instance/Update/Surge/timer.operationDeadline/before
func TestExecuteErroredPodReadDoesNotFabricateStuckEvidence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	surgeIdx := int32(10)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.StuckPodGrace = time.Minute
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workload.InstancePhaseUpdating,
			Operation: &workload.InstanceOperation{
				ID:             "update-0",
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepSurge,
				SurgeIndex:     &surgeIdx,
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workload.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		return store.apply(ctx, []workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }

	shed := apierrors.NewServiceUnavailable("the apiserver shed the list")
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return shed
			},
		}).Build()
	deps := workload.Deps{Client: c, APIReader: c, Expectations: workload.NewExpectations()}
	snapshot := workload.NewObservedSnapshot(deps, in, workload.ComponentEngine, in.ObservedState.InstanceStatuses)

	ev := snapshot.EvidenceFor(ctx, 0, now, in.StuckPodGrace)
	if !ev.DeadlinePassed {
		t.Fatalf("evidence must still report the elapsed deadline: %+v", ev)
	}
	if ev.StuckPod != nil || ev.StuckReason != "" || ev.Unschedulable != nil {
		t.Fatalf("a failed pod read must not fabricate pod evidence: %+v", ev)
	}

	plan := minimalPlan()
	plan.Instances = append(plan.Instances, workload.InstancePlan{
		Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
	})
	_, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))

	if err == nil {
		t.Fatal("the pass must report both the restart failure and the escalation read failure")
	}
	opFirst := strings.Index(err.Error(), "restart instance 1")
	escalationSecond := strings.Index(err.Error(), "list pods for escalation pass")
	if opFirst < 0 || escalationSecond < 0 {
		t.Fatalf("joined error must name both failures; got %v", err)
	}
	if opFirst > escalationSecond {
		t.Fatalf("the op error must be reported first; got %v", err)
	}
	if got := store.status(0); got != nil && got.Phase == workload.InstancePhaseFailed {
		t.Fatalf("a failed pod read must not stamp Failed: %+v", got)
	}
	if warnings != 0 {
		t.Fatalf("WarnInstanceFailed fired %d times on a failed read", warnings)
	}
}

// TestExecuteErroredSiblingKeepsRelocationFirstTry: which
// DisposeExpiredAttempt branch runs is decided by the blamed pod set, not
// by whether a sibling's operation errored this pass. A single-pod Create
// parked Running-but-unready on one node, under Auto migration policy
// with budget, still takes the relocation branch on its first expiry
// while the restart pass fails for another Instance — a terminal
// AutoRecover directive naming the suspect node, no RetryBlock, and the
// born-terminal status mirror.
//
// Cells: instance/Ready/api.invalid/before, instance/Create/CreatePods/timer.operationDeadline/before
func TestExecuteErroredSiblingKeepsRelocationFirstTry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	const suspectNode = "node-a"
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.UpdateRevision = "llama-70b-engine-abcde"
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				ID:             "create-0",
				Type:           workload.InstanceOperationCreate,
				Step:           "CreatePods",
				TargetRevision: "llama-70b-engine-abcde",
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workload.InstancePhaseReady},
	}
	in.Disposition = workload.DispositionDeps{AutoMigrateMaxAttempts: 3}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	rejected := rejectedStatusWrite()
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		if idx == 1 {
			return rejected
		}
		return store.apply(ctx, []workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	blocks := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		blocks++
		return nil
	}
	var mirrored []workload.MigrationRecord
	in.AppendMigration = func(_ context.Context, rec workload.MigrationRecord) error {
		mirrored = append(mirrored, rec)
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workload.MigrationModeAuto
	plan.Instances = append(plan.Instances, workload.InstancePlan{
		Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
	})
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	deps := workload.Deps{Client: c, APIReader: c, Expectations: workload.NewExpectations()}
	limbo := nodeScopedLimboPod("llama-70b-engine-0-default-0", in.Key.Namespace, suspectNode, "abcde", now.Add(-2*time.Minute))
	snapshot := workload.SnapshotWithPodsForTest(in, map[int32][]*corev1.Pod{0: {limbo}})

	_, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))
	if err == nil || !strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("the sibling restart must still fail the pass; got %v", err)
	}

	ledger, lerr := audit.LoadLedgerForOwner(ctx, c, in.OwnerObject)
	if lerr != nil {
		t.Fatalf("load ledger: %v", lerr)
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("ledger entries: got %d want the first relocation directive", len(ledger.Entries))
	}
	e := ledger.Entries[0]
	if e.Phase != audit.PhaseCompleted || e.Reason != audit.ReasonAutoRecover || e.Outcome != audit.OutcomeRelocateRecreate {
		t.Errorf("entry: got phase=%q reason=%q outcome=%q want a terminal AutoRecover relocate-recreate directive",
			e.Phase, e.Reason, e.Outcome)
	}
	if e.FromNode != suspectNode || e.SourceInstance != 0 || e.SurgeInstance != 0 {
		t.Errorf("entry identity: got fromNode=%q source=%d surge=%d want %s/0/0 (a directive allocates no surge)",
			e.FromNode, e.SourceInstance, e.SurgeInstance, suspectNode)
	}
	if len(mirrored) != 1 || mirrored[0].Trigger != workload.MigrationTriggerAuto ||
		mirrored[0].Phase != workload.MigrationPhaseRelocated || mirrored[0].Attempt != 1 {
		t.Errorf("status mirror: got %+v want one born-terminal Auto record on attempt 1", mirrored)
	}
	if blocks != 0 {
		t.Errorf("MutateRetryBlock calls: got %d want none (relocation never holds a revision)", blocks)
	}
	if got := store.status(0); got == nil || got.Phase != workload.InstancePhaseFailed || got.Operation != nil {
		t.Fatalf("relocated Instance: got %+v want Failed with the operation cleared", got)
	}
}

// nodeScopedLimboPod is the readiness-limbo shape pinned to one node and
// one revision — the node-scoped fault the relocation branch is for.
func nodeScopedLimboPod(name, namespace, node, revisionHash string, since time.Time) *corev1.Pod {
	pod := runningNotReadyPod(name, since)
	pod.Namespace = namespace
	pod.Spec.NodeName = node
	pod.Labels = map[string]string{query.LabelRevisionHash: revisionHash}
	return pod
}
