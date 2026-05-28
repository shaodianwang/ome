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

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// createRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up.
const createRequeueInterval = 5 * time.Second

// Create drives Create / Scale-Up toward each desired Instance. Multi-pass:
//
//  1. Render missing pods with ome.io/serving=False; patch InstanceStatus
//     to Phase=Creating with the Create operation stamped.
//  2. Wait for runtime Ready on every pod, flip ome.io/serving=True,
//     promote InstanceStatus to Phase=Ready (Operation=nil).
//
// Caller restricts to single-pod Instances via core.BuildPlan; EndpointSlice
// convergence is owned by drain.go.
//
// When target != nil the Ready promote records RunningRevision=target.Name
// so detectUpdateTrigger's fast-path (s.RunningRevision == target.Name) can
// short-circuit. Without it, per-pod podMatchesTarget runs and false-positives
// against the post-Render pod mutations, triggering a spurious recreate.
func Create(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if p.Client == nil {
		return ctrl.Result{}, fmt.Errorf("Create: nil client")
	}

	existing, err := query.ListOMENativePods(ctx, p.Client, p.ISVC, plan.Component)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("Create: list pods: %w", err)
	}
	byInstance := query.BucketPodsByInstanceIdx(existing)

	allReady := true
	for _, inst := range plan.Instances {
		ready, err := reconcileInstance(ctx, p, plan, inst, byInstance[inst.Index], target)
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
func reconcileInstance(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, existing []*corev1.Pod, target *appsv1.ControllerRevision) (bool, error) {
	desired := expectedPodNamesForInstance(p.ISVC, plan.Component, inst)
	existingByName := query.IndexPodsByName(existing)

	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}

	// Only patch Creating when there's something to create — otherwise the
	// next pass would oscillate status back to Creating after Ready.
	if len(missing) > 0 {
		// Capture before patch — findInstanceStatus uses the cached ISVC,
		// so the patch below doesn't backfill this view.
		isFirstCreate := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index) == nil
		if err := patchInstanceStatusCreating(ctx, p, plan.Component, inst.Index, inst.Incarnation, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("patch status Creating (instance=%d): %w", inst.Index, err)
		}
		if isFirstCreate {
			status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonInstanceCreated,
				"OMENative %s materialized; %d pod(s) requested",
				status.InstanceKey(plan.Component, inst.Index), len(missing))
		}

		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index) {
			return false, nil
		}

		if _, err := createMissingPods(ctx, p, plan, inst, inst.Index, missing, revisionHashFromTarget(target)); err != nil {
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
		if err := podreadiness.MarkPodServing(ctx, p.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// Promote to Ready. Fire InstanceReady only on transition.
	wasReady := false
	if s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil && s.Phase == v1beta1.OMENativeInstanceReady {
		wasReady = true
	}
	var promoteErr error
	if target != nil {
		promoteErr = patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name)
	} else {
		promoteErr = patchInstanceStatusReady(ctx, p, plan.Component, inst.Index)
	}
	if promoteErr != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, promoteErr)
	}
	if !wasReady {
		status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonInstanceReady,
			"OMENative %s is Ready (%d pod(s) serving)",
			status.InstanceKey(plan.Component, inst.Index), len(existing))
	}
	return true, nil
}

// podTarget describes one pod to create within an Instance.
type podTarget struct {
	Name    string
	Runner  core.RunnerPlan
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
// ordinal 0 — matches FindInstanceStatus returning nil and preserves
// the pre-fix behavior of every Create-from-empty path.
func expectedPodNamesForInstance(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inst core.InstancePlan) []podTarget {
	if isvc == nil {
		return nil
	}
	var targets []podTarget
	for _, runner := range inst.Runners {
		if runner.Size == 1 {
			ordinal := activeOrdinalForInstance(isvc, component, inst.Index)
			targets = append(targets, podTarget{
				Name:    query.PodName(isvc.Name, component, inst.Index, runner.Name, ordinal),
				Runner:  runner,
				Ordinal: ordinal,
			})
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			targets = append(targets, podTarget{
				Name:    query.PodName(isvc.Name, component, inst.Index, runner.Name, o),
				Runner:  runner,
				Ordinal: o,
			})
		}
	}
	return targets
}

// activeOrdinalForInstance returns the InstanceStatus.ActiveOrdinal
// for (component, idx), or 0 when no status exists yet. Single-pod
// Create / Restart / recreateUpdate paths consult this to address the
// post-surge canonical slot (which may be 1, not 0).
func activeOrdinalForInstance(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) int32 {
	if s := query.FindInstanceStatus(isvc, component, idx); s != nil {
		return s.ActiveOrdinal
	}
	return 0
}

// createMissingPods renders and creates targets, owning the per-pod
// ExpectCreates bookkeeping. idx is the expectations bucket — pass
// inst.Index for steady-state Create / Restart Phase B; surgeIdx for
// Migrate's surge create. Callers must NOT call ExpectCreates themselves.
// revisionHash, when non-empty, stamps ome.io/revision-hash on every
// created pod so per-revision Services can select it.
func createMissingPods(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, idx int32, targets []podTarget, revisionHash string) (int, error) {
	created := 0
	for _, t := range targets {
		template := p.PodSpec
		if t.Runner.Name == "worker" && p.WorkerPodSpec != nil {
			template = p.WorkerPodSpec
		}

		pod, err := core.RenderWithRevisionForISVC(p, template, plan, inst, t.Runner, t.Ordinal, revisionHash)
		if err != nil {
			return created, fmt.Errorf("render pod %s: %w", t.Name, err)
		}

		// EXPECT-ORDER: expectation before RPC, rollback via ObservedCreate
		// on error — a failed Create fires no watch event to decrement it.
		p.ExpectationsCache().ExpectCreates(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), idx, 1)
		if err := p.Client.Create(ctx, pod); err != nil {
			p.ExpectationsCache().ObservedCreate(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), idx)
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

// revisionHashForInstance returns the revision hash to stamp on pods
// being created for one Instance, given the optional target
// ControllerRevision and the Instance's current status. Used by
// callers (Restart, Migrate) that don't carry a target *CR but still
// need the per-revision label to be correct — the Instance is staying
// on its existing revision, so we read RunningRevision from status.
func revisionHashForInstance(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32, target *appsv1.ControllerRevision) string {
	if hash := revisionHashFromTarget(target); hash != "" {
		return hash
	}
	if isvc == nil {
		return ""
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return ""
	}
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Index == idx && s.RunningRevision != "" {
			return query.RevisionHashFromControllerRevisionName(s.RunningRevision)
		}
	}
	// Fall back to the Component-level UpdateRevision when no
	// per-Instance running revision is recorded — first-create paths
	// hit this branch.
	if cs.OMENative.UpdateRevision != "" {
		return query.RevisionHashFromControllerRevisionName(cs.OMENative.UpdateRevision)
	}
	return query.RevisionHashFromControllerRevisionName(cs.OMENative.CurrentRevision)
}

// patchInstanceStatusCreating idempotently stamps Phase=Creating with the
// Create operation and a Deadline of StartedAt+timeout. incarnation is
// written on first-touch; existing nonzero values are preserved so a
// post-restart reconcile doesn't downgrade the lifecycle token.
func patchInstanceStatusCreating(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, incarnation int64, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceCreating &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationCreate &&
			s.Incarnation > 0 {
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = incarnation
		}
		s.Phase = v1beta1.OMENativeInstanceCreating
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationCreate,
			Step:           "CreatePods",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusReady idempotently moves to Phase=Ready, clears Operation.
func patchInstanceStatusReady(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceReady && s.Operation == nil {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.Operation = nil
		return true
	})
}
