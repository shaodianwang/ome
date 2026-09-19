package ops_test

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// supersededFixture is one Instance whose Create attempt is pinned to a
// revision the target has since moved off, with the attempt half built:
// some of its pods exist, carrying the pinned revision's hash.
type supersededFixture struct {
	isvc     *v1beta1.InferenceService
	client   client.Client
	target   *appsv1.ControllerRevision
	plan     workload.ComponentPlan
	recorder *record.FakeRecorder
	priorOp  v1beta1.InstanceOperation
	// blocks seeds ObservedState.RetryBlocks for the pass.
	blocks []workload.RetryBlock
}

const supersededPriorRevision = "llama-70b-engine-bad0bad0"

// newSupersededFixture builds the half-built attempt. gang selects a
// two-Runner Instance whose worker is still missing; otherwise the Instance
// is single-pod with its one pod already created and not yet Ready.
func newSupersededFixture(t *testing.T, gang bool) *supersededFixture {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	priorOp := v1beta1.InstanceOperation{
		ID:             "create-0-1",
		Type:           v1beta1.InstanceOperationCreate,
		Step:           "CreatePods",
		TargetRevision: supersededPriorRevision,
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Hour)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Hour)),
		Deadline:       metav1.NewTime(time.Now().Add(-30 * time.Minute)),
	}
	op := priorOp
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		PodCount:    1,
		Operation:   &op,
	})
	var pod *corev1.Pod
	plan := buildPlanSinglePodEngine(1)
	if gang {
		pod = gangPod(isvc, 0, "leader", 0, 1, false, false)
		plan = buildPlanGangEngine(workload.RestartPolicyNone)
		plan.Instances[0].Incarnation = 1
	} else {
		pod = podForInstance(isvc, 0, false, false)
	}
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(supersededPriorRevision)
	c := newFakeClient(t, isvc, ir, pod)
	return &supersededFixture{
		isvc:     isvc,
		client:   c,
		target:   &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-good0bad", Namespace: "prod"}},
		plan:     plan,
		recorder: record.NewFakeRecorder(8),
		priorOp:  priorOp,
	}
}

// run drives one Create pass against freshly observed status, the way the
// dispatcher does.
func (f *supersededFixture) run(t *testing.T) (writes int) {
	t.Helper()
	input := buildTestInput(f.isvc, f.client, workload.ComponentEngine)
	input.ObservedState.RetryBlocks = f.blocks
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		writes++
		return inner(ctx, idx, mutate)
	}
	if _, err := ops.Create(context.Background(), workload.Deps{Client: f.client, Recorder: f.recorder},
		input, f.plan, f.target); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return writes
}

// requireRetiredIntoFreshAttempt asserts the shape a superseded Create
// attempt is left in: still Creating under a new Create operation pinned to
// the current target, with nothing recorded against the revision it was
// building. retiredID is the identity of the attempt that was replaced.
func requireRetiredIntoFreshAttempt(t *testing.T, s *v1beta1.OMENativeInstanceStatus, retiredID, targetName string) {
	t.Helper()
	if s == nil {
		t.Fatalf("InstanceStatus missing")
	}
	if s.Phase != v1beta1.OMENativeInstanceCreating || s.LastFailure != nil {
		t.Fatalf("retired attempt: got {phase:%s lastFailure:%+v}, want Creating with no failure",
			s.Phase, s.LastFailure)
	}
	op := s.Operation
	if op == nil || op.Type != v1beta1.InstanceOperationCreate {
		t.Fatalf("retired attempt must be replaced by a fresh Create operation; got %+v", op)
	}
	if op.ID == retiredID {
		t.Errorf("Operation.ID = %q, want a new attempt identity", op.ID)
	}
	if op.TargetRevision != targetName {
		t.Errorf("Operation.TargetRevision = %q, want %q", op.TargetRevision, targetName)
	}
}

// Cells: instance/Create/CreatePods/spec.revision/before
func TestCreate_SupersededAttempt_ReplacedByFreshAttempt(t *testing.T) {
	for _, gang := range []bool{false, true} {
		name := "single pod"
		if gang {
			name = "gang"
		}
		t.Run(name, func(t *testing.T) {
			f := newSupersededFixture(t, gang)

			if writes := f.run(t); writes != 1 {
				t.Fatalf("status writes in the retiring pass: got %d, want exactly 1", writes)
			}

			s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
			if s == nil {
				t.Fatalf("InstanceStatus disappeared")
			}
			if s.Phase != v1beta1.OMENativeInstanceCreating {
				t.Errorf("Phase = %q, want Creating: a superseded spec is not a failure of the retired revision", s.Phase)
			}
			if s.LastFailure != nil {
				t.Errorf("LastFailure = %+v, want nil", s.LastFailure)
			}
			op := s.Operation
			if op == nil {
				t.Fatalf("Operation cleared; the attempt must be replaced by a fresh one")
			}
			if op.Type != v1beta1.InstanceOperationCreate || op.Step != "CreatePods" {
				t.Errorf("Operation = {type:%s step:%s}, want {Create CreatePods}", op.Type, op.Step)
			}
			if op.ID == f.priorOp.ID {
				t.Errorf("Operation.ID = %q: the replacement must be a new attempt, not the retired one", op.ID)
			}
			if op.TargetRevision != f.target.Name {
				t.Errorf("Operation.TargetRevision = %q, want %q", op.TargetRevision, f.target.Name)
			}
			if !op.Deadline.After(f.priorOp.Deadline.Time) {
				t.Errorf("Operation.Deadline = %s, want re-armed past the retired attempt's %s",
					op.Deadline, f.priorOp.Deadline)
			}

			events := drainEvents(f.recorder)
			if len(events) != 1 {
				t.Fatalf("events: got %v, want exactly one", events)
			}
			if !strings.HasPrefix(events[0], "Normal ") {
				t.Errorf("event type: got %q, want Normal — a retired revision did not fail", events[0])
			}
			if !strings.Contains(events[0], supersededPriorRevision) || !strings.Contains(events[0], f.target.Name) {
				t.Errorf("event %q must name both the superseded and the new revision", events[0])
			}

			// A second pass observes the replacement it already wrote, so
			// the retirement neither re-fires nor re-announces itself.
			before := *op
			if writes := f.run(t); writes != 0 {
				t.Errorf("status writes in the second pass: got %d, want 0", writes)
			}
			for _, e := range drainEvents(f.recorder) {
				if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
					t.Errorf("second pass re-announced the retirement: %q", e)
				}
			}
			after := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0).Operation
			if after == nil || after.ID != before.ID || after.TargetRevision != before.TargetRevision {
				t.Errorf("second pass replaced the attempt again: %+v", after)
			}
		})
	}
}

// Cells: instance/Create/CreatePods/spec.revision/before
func TestCreate_FreshAttemptRecreatesRetiredPodsAtItsRevision(t *testing.T) {
	f := newSupersededFixture(t, true /* gang */)
	f.run(t)

	// The pass after the retirement owns the retired attempt's pods: they
	// carry a revision the pinned attempt is not converging to.
	f.run(t)
	pods := &corev1.PodList{}
	if err := f.client.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	targetHash := query.RevisionHashFromControllerRevisionName(f.target.Name)
	for i := range pods.Items {
		if got := pods.Items[i].Labels[query.LabelRevisionHash]; got == targetHash {
			continue
		}
		if pods.Items[i].DeletionTimestamp != nil {
			continue
		}
		t.Errorf("pod %s survived on revision hash %q; the fresh attempt must recreate it at %q",
			pods.Items[i].Name, pods.Items[i].Labels[query.LabelRevisionHash], targetHash)
	}
}

// onlyPod re-reads the fixture's single pod from the fake client.
func (f *supersededFixture) onlyPod(t *testing.T) *corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := f.client.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("fixture pods: got %d, want 1", len(pods.Items))
	}
	return &pods.Items[0]
}

// Cells: instance/Create/CreatePods/spec.revision/before
func TestCreate_RuntimeReadyAttemptPromotesRatherThanRetiring(t *testing.T) {
	// A fully materialized, runtime-ready attempt is one promote away from
	// serving. Retiring it drops pods that are already routed and throws away
	// a warmed accelerator; the update machinery rolls the Instance to the new
	// target afterwards, with a surge and without a traffic gap.
	for _, tc := range []struct {
		name           string
		minReadyWindow int32
		wantPhase      v1beta1.OMENativeInstancePhase
	}{
		{"inside the availability window", 3600, v1beta1.OMENativeInstanceCreating},
		{"window elapsed", 0, v1beta1.OMENativeInstanceReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSupersededFixture(t, false /* gang */)
			f.plan.MinReadySeconds = tc.minReadyWindow
			pod := f.onlyPod(t)
			pod.Status = promotablePodForInstance(f.isvc, 0).Status
			if err := f.client.Status().Update(context.Background(), pod); err != nil {
				t.Fatalf("make the attempt's pod runtime-ready: %v", err)
			}

			f.run(t)

			s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
			if s.Phase != tc.wantPhase {
				t.Errorf("Phase = %q, want %q", s.Phase, tc.wantPhase)
			}
			if s.Operation != nil && s.Operation.TargetRevision == f.target.Name {
				t.Errorf("the attempt was retired one promote short of serving: %+v", s.Operation)
			}
			for _, e := range drainEvents(f.recorder) {
				if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
					t.Errorf("retirement announced for a runtime-ready attempt: %q", e)
				}
			}
			if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Errorf("the runtime-ready pod was deleted (%v)", err)
			}
		})
	}
}

// Cells: instance/Create/CreatePods/spec.revision/before
func TestCreate_RetargetOntoBlockedRevisionDefersRetirement(t *testing.T) {
	// Replacing an attempt starts a fresh one, so the new target's RetryBlock
	// gates it exactly as it gates a first attempt. Retiring first and
	// discovering the block afterwards would tear down the old attempt and
	// then refuse to build anything.
	for _, tc := range []struct {
		name  string
		block workload.RetryBlock
	}{
		{"held", workload.RetryBlock{State: workload.RetryBlockHeld}},
		{"backing off", workload.RetryBlock{State: workload.RetryBlockBackoff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSupersededFixture(t, false /* gang */)
			block := tc.block
			block.TargetRevision = f.target.Name
			if block.State == workload.RetryBlockBackoff {
				next := metav1.NewTime(time.Now().Add(time.Hour))
				block.NextRetryAt = &next
			}
			f.blocks = []workload.RetryBlock{block}

			if writes := f.run(t); writes != 0 {
				t.Errorf("status writes: got %d, want 0 while the new target is denied", writes)
			}
			s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
			if s.Operation == nil || s.Operation.TargetRevision != supersededPriorRevision {
				t.Errorf("the old attempt was retired onto a denied target: %+v", s.Operation)
			}
			if events := drainEvents(f.recorder); len(events) != 0 {
				t.Errorf("events: got %v, want none — nothing happened", events)
			}
			pods := &corev1.PodList{}
			if err := f.client.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			targetHash := query.RevisionHashFromControllerRevisionName(f.target.Name)
			for i := range pods.Items {
				if pods.Items[i].Labels[query.LabelRevisionHash] == targetHash {
					t.Errorf("pod %s was built at the denied revision", pods.Items[i].Name)
				}
			}
		})
	}
}

// Cells: instance/Create/CreatePods/spec.revision/before
func TestCreate_PromotedInstanceRepairIsNotRetired(t *testing.T) {
	// A promoted Instance whose pod is being rebuilt runs a Create attempt on
	// a row that records a RunningRevision. Its pods belong to a serving
	// materialization, so rolling them to a new target is the update
	// machinery's business — and the Create pass's own cleanup declines them
	// forever, which would leave the retirement announced and unfinished.
	f := newSupersededFixture(t, false /* gang */)
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: "prod", Name: irName(f.isvc, workload.ComponentEngine)}
	if err := f.client.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses[0].RunningRevision = supersededPriorRevision
	if err := f.client.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed the running revision: %v", err)
	}

	f.run(t)

	s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
	if s.Operation == nil || s.Operation.TargetRevision != supersededPriorRevision {
		t.Errorf("a promoted Instance's repair was retired by the Create pass: %+v", s.Operation)
	}
	for _, e := range drainEvents(f.recorder) {
		if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
			t.Errorf("retirement announced for a row the cleanup declines: %q", e)
		}
	}
}

// Cells: instance/Create/CreatePods/spec.revision/before
func TestCreate_RetargetDefersWhileAnotherAttemptRetries(t *testing.T) {
	// A RetryInProgress block admits one attempt at a time. An in-flight
	// Create is allowed to carry no TargetRevision at all, so a
	// revision-scoped reading of "is anyone attempting this" answers no and
	// lets a second attempt start beside the first.
	f := newSupersededFixture(t, false /* gang */)
	f.plan.Replicas = 2
	f.plan.Instances = append(f.plan.Instances, workload.InstancePlan{
		Index:       1,
		Incarnation: 1,
		Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
	})
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: "prod", Name: irName(f.isvc, workload.ComponentEngine)}
	if err := f.client.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
		Index:       1,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			ID:   "create-1-1",
			Type: v1beta1.InstanceOperationCreate,
			Step: "CreatePods",
		},
	})
	if err := f.client.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed the sibling attempt: %v", err)
	}
	f.blocks = []workload.RetryBlock{{
		TargetRevision: f.target.Name,
		State:          workload.RetryBlockRetryInProgress,
	}}

	f.run(t)

	s := findInstanceStatusOnIR(f.client, f.isvc, workload.ComponentEngine, 0)
	if s.Operation == nil || s.Operation.TargetRevision != supersededPriorRevision {
		t.Errorf("retired onto a revision another attempt already holds: %+v", s.Operation)
	}
	for _, e := range drainEvents(f.recorder) {
		if strings.Contains(e, string(workload.EventReasonCreateAttemptSuperseded)) {
			t.Errorf("retirement announced while the revision is one-at-a-time: %q", e)
		}
	}
}
