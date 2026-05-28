package ops

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// createRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up.
const createRequeueInterval = 5 * time.Second

// Event reasons emitted by the workload Create state machine.
const (
	// EventReasonInstanceCreated fires the first time an Instance's
	// pods are materialized (first transition into Phase=Creating).
	EventReasonInstanceCreated = "InstanceCreated"

	// EventReasonInstanceReady fires when an Instance transitions from
	// any non-Ready phase to Phase=Ready (every pod runtime-ready and
	// ome.io/serving=True).
	EventReasonInstanceReady = "InstanceReady"

	// Migration request lifecycle.
	EventReasonMigrationRequestAccepted      = "MigrationRequestAccepted"
	EventReasonMigrationRequestRejected      = "MigrationRequestRejected"
	EventReasonMigrationCompleted            = "MigrationCompleted"
	EventReasonRateLimited                   = "RateLimited"
	EventReasonMigrationFromNodeMismatch     = "MigrationFromNodeMismatch"
	EventReasonMigrationNodeAffinityConflict = "MigrationNodeAffinityConflict"
)

// Create drives Create / Scale-Up toward each desired Instance. Multi-pass:
//
//  1. Render missing pods with ome.io/serving=False; patch InstanceStatus
//     to Phase=Creating with the Create operation stamped.
//  2. Wait for runtime Ready on every pod, flip ome.io/serving=True,
//     promote InstanceStatus to Phase=Ready (Operation=nil).
//
// Caller restricts to single-pod Instances via workload.BuildPlan;
// EndpointSlice convergence is owned by drain.go.
//
// When target != nil the Ready promote records RunningRevision=target.Name
// so detectUpdateTrigger's fast-path (s.RunningRevision == target.Name) can
// short-circuit. Without it, per-pod podMatchesTarget runs and false-positives
// against the post-Render pod mutations, triggering a spurious recreate.
//
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types. The ISVC-OMENative dispatcher delegates
// via internalsource.BuildReconcileInput; the IR controller will call
// the same entry point with its own ReconcileInput.
func Create(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("Create: nil client")
	}

	existing, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("Create: list pods: %w", err)
	}
	byInstance := query.BucketPodsByInstanceIdx(existing)

	allReady := true
	for _, inst := range plan.Instances {
		ready, err := reconcileInstance(ctx, deps, input, plan, inst, byInstance[inst.Index], target)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !ready {
			allReady = false
		}
	}

	if !allReady {
		return ctrl.Result{RequeueAfter: createRequeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

// reconcileInstance drives one Instance toward Ready. Returns ready=true
// only when every pod is runtime-ready and ome.io/serving=True. target has
// the same nil-OK semantics as Create.
func reconcileInstance(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, existing []*corev1.Pod, target *appsv1.ControllerRevision) (bool, error) {
	desired := expectedPodNamesForInstance(input, plan, inst)
	existingByName := query.IndexPodsByName(existing)

	missing := make([]podTarget, 0)
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}

	// Only patch Creating when there's something to create — otherwise the
	// next pass would oscillate status back to Creating after Ready.
	if len(missing) > 0 {
		// Capture before patch — findInstanceStatus reads ObservedState,
		// so the patch below doesn't backfill this view.
		isFirstCreate := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index) == nil
		if err := patchInstanceStatusCreating(ctx, input, inst.Index, inst.Incarnation, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("patch status Creating (instance=%d): %w", inst.Index, err)
		}
		if isFirstCreate {
			recordNormal(deps.Recorder, eventTarget(input), EventReasonInstanceCreated,
				"OMENative %s materialized; %d pod(s) requested",
				instanceKey(input.Key.Component, inst.Index), len(missing))
		}

		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}

		if _, err := createMissingPods(ctx, deps, input, plan, inst, inst.Index, missing, revisionHashFromTarget(target)); err != nil {
			return false, err
		}
		return false, nil
	}

	// All desired pods exist. Are they runtime-Ready?
	if !query.AllPodsRuntimeReady(existing) {
		return false, nil
	}

	// Flip ome.io/serving=True. For freshly-created pods the condition
	// doesn't exist yet; the Lifecycle writer creates it on first flip.
	for _, pod := range existing {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// Promote to Ready. Fire InstanceReady only on transition.
	wasReady := false
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil && s.Phase == workload.InstancePhaseReady {
		wasReady = true
	}
	var promoteErr error
	if target != nil {
		promoteErr = patchInstanceStatusReadyOnRevision(ctx, input, inst.Index, target.Name)
	} else {
		promoteErr = patchInstanceStatusReady(ctx, input, inst.Index)
	}
	if promoteErr != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, promoteErr)
	}
	if !wasReady {
		recordNormal(deps.Recorder, eventTarget(input), EventReasonInstanceReady,
			"OMENative %s is Ready (%d pod(s) serving)",
			instanceKey(input.Key.Component, inst.Index), len(existing))
	}
	return true, nil
}

// podTarget describes one pod to create within an Instance.
type podTarget struct {
	Name    string
	Runner  workload.RunnerPlan
	Ordinal int32
}

// expectedPodNamesForInstance enumerates every (Runner, ordinal) tuple
// the Instance plan asks for and returns the targets in stable order.
//
// For single-pod Runners (Size == 1) the ordinal slot is read from the
// Instance's recorded ActiveOrdinal in status — SurgeThenDrain
// alternates between slots 0 and 1 across rollouts and the new slot
// becomes canonical on promote. Without this lookup the next
// steady-state Create pass after a surge cycle would hard-code
// ordinal 0, see the canonical pod (now at ordinal 1) as "missing
// ordinal 0", and create a duplicate pod — leaving the Instance with
// 2 alive pods and no drain convergence.
//
// Multi-pod Runners (Size > 1) keep enumerating every 0..Size-1
// ordinal; per-gang-member surge allocation is not yet plumbed, so
// the ActiveOrdinal bit applies only to the single-pod branch.
//
// Instances without a status entry (initial create) default to
// ordinal 0 — matches findInstanceStatus returning nil and preserves
// the pre-fix behavior of every Create-from-empty path.
func expectedPodNamesForInstance(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) []podTarget {
	var targets []podTarget
	for _, runner := range inst.Runners {
		if runner.Size == 1 {
			ordinal := activeOrdinalForInstance(input.ObservedState.InstanceStatuses, inst.Index)
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, ordinal),
				Runner:  runner,
				Ordinal: ordinal,
			})
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, o),
				Runner:  runner,
				Ordinal: o,
			})
		}
	}
	return targets
}

// activeOrdinalForInstance returns the InstanceStatus.ActiveOrdinal
// for idx, or 0 when no status exists yet. Single-pod
// Create / Restart / recreateUpdate paths consult this to address the
// post-surge canonical slot (which may be 1, not 0).
func activeOrdinalForInstance(observed []workload.InstanceStatus, idx int32) int32 {
	if s := findInstanceStatus(observed, idx); s != nil {
		return s.ActiveOrdinal
	}
	return 0
}

// findInstanceStatus returns a pointer to the matching InstanceStatus
// in the observed slice, or nil if absent. Callers must NOT mutate
// the returned pointer — it aliases ObservedState.
func findInstanceStatus(observed []workload.InstanceStatus, idx int32) *workload.InstanceStatus {
	for i := range observed {
		if observed[i].Index == idx {
			return &observed[i]
		}
	}
	return nil
}

// createMissingPods renders and creates targets, owning the per-pod
// ExpectCreates bookkeeping. idx is the expectations bucket — pass
// inst.Index for steady-state Create / Restart Phase B. Callers must
// NOT call ExpectCreates themselves. revisionHash, when non-empty,
// stamps ome.io/revision-hash on every created pod so per-revision
// Services can select it.
func createMissingPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, idx int32, targets []podTarget, revisionHash string) (int, error) {
	created := 0
	for _, t := range targets {
		template := input.DesiredSpec.PodSpec
		if t.Runner.Name == "worker" && input.DesiredSpec.WorkerPodSpec != nil {
			template = input.DesiredSpec.WorkerPodSpec
		}

		pod, err := RenderWithRevision(
			input.OwnerObject,
			input.OwnerGVK,
			renderKeyFromInput(input),
			template,
			input.DesiredSpec.PodTemplateObjectMeta,
			plan,
			inst,
			t.Runner,
			t.Ordinal,
			revisionHash,
			deps.RenderHook,
		)
		if err != nil {
			return created, fmt.Errorf("render pod %s: %w", t.Name, err)
		}

		// EXPECT-ORDER: expectation before RPC, rollback via ObservedCreate
		// on error — a failed Create fires no watch event to decrement it.
		deps.ExpectationsCache().ExpectCreates(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx, 1)
		if err := deps.Client.Create(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedCreate(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)
			if apierrors.IsAlreadyExists(err) {
				// Prior reconcile created it; cache hasn't caught up yet.
				continue
			}
			return created, fmt.Errorf("create pod %s: %w", t.Name, err)
		}
		created++
	}
	return created, nil
}

// renderKeyFromInput projects the workload.ReconcileInput's identity
// into the workload.WorkloadKey shape ops/render.go consumes.
// WorkloadKey.Name is "<owner>-<component>" — the pod-naming
// convention every adapter shares so selectors line up.
func renderKeyFromInput(input workload.ReconcileInput) workload.WorkloadKey {
	return workload.WorkloadKey{
		Namespace: input.Key.Namespace,
		Name:      input.Key.OwnerName + "-" + string(input.Key.Component),
		Component: input.Key.Component,
		Labels:    input.Key.SelectorLabels,
	}
}

// revisionHashFromTarget extracts the hash suffix from a target
// ControllerRevision. Returns "" when target is nil so callers can
// pass-through to RenderWithRevision; an empty hash skips the
// ome.io/revision-hash label.
func revisionHashFromTarget(target *appsv1.ControllerRevision) string {
	if target == nil {
		return ""
	}
	return query.RevisionHashFromControllerRevisionName(target.Name)
}

// patchInstanceStatusCreating idempotently stamps Phase=Creating with the
// Create operation and a Deadline of StartedAt+timeout. incarnation is
// written on first-touch; existing nonzero values are preserved so a
// post-restart reconcile doesn't downgrade the lifecycle token.
func patchInstanceStatusCreating(ctx context.Context, input workload.ReconcileInput, idx int32, incarnation int64, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseCreating &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationCreate &&
			s.Incarnation > 0 {
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = incarnation
		}
		s.Phase = workload.InstancePhaseCreating
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationCreate,
			Step:           "CreatePods",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusReady idempotently moves to Phase=Ready, clears Operation.
func patchInstanceStatusReady(ctx context.Context, input workload.ReconcileInput, idx int32) error {
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseReady && s.Operation == nil {
			return false
		}
		s.Phase = workload.InstancePhaseReady
		s.Operation = nil
		return true
	})
}

// patchInstanceStatusReadyOnRevision idempotently moves to Phase=Ready
// with RunningRevision=rev; clears Operation + TargetRevision. The
// post-promote anchor that lets detectUpdateTrigger's fast-path
// short-circuit on revision-equality before per-pod diffs.
func patchInstanceStatusReadyOnRevision(ctx context.Context, input workload.ReconcileInput, idx int32, rev string) error {
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" {
			return false
		}
		s.Phase = workload.InstancePhaseReady
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		return true
	})
}
