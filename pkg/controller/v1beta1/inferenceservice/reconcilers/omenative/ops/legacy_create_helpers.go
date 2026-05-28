package ops

// Helpers carved out of create.go (part 2 slice 4b of N) and delete.go
// (part 2 slice 4c of N) when those per-Instance state machines moved
// to pkg/controller/v1beta1/workload/ops. The callers below still live
// in this package and read the v1beta1.OMENativeInstanceStatus shape
// directly — keeping the helpers here as v1beta1-typed functions keeps
// the peer ops (update, restart, migrate, timeouts) byte-for-byte
// unchanged. The dispatcher (dispatch.go) also still reads from this
// file (extraInstanceIndices, deleteRequeueInterval).
//
// Each helper shrinks out as the matching peer op moves to workload/ops
// in a follow-up slice.

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// deleteRequeueInterval is the wait between scale-down passes; the
// dispatcher requeues after this when at least one extra Instance is
// still draining. Mirrors the workload-side
// workload/ops.deleteRequeueInterval byte-for-byte (5s); kept here
// because dispatch.go reads it and dispatch.go has not yet migrated
// to workload/ops.
const deleteRequeueInterval = 5 * time.Second

// podTarget describes one pod to create within an Instance. Now
// call-site-orphaned: the peer ops (restart, migrate, update) that
// once consumed it have migrated to workload/ops. Retained until the
// omenative/ops package fully empties; remove in a follow-up sweep
// along with the helpers that take or return it.
//
//nolint:unused
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
//
//nolint:unused
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
//
//nolint:unused
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
//
//nolint:unused
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

// extraInstanceIndices returns InstanceStatus indices the plan no longer
// covers — these are scale-down targets. Set-difference framing handles
// sparse indices from surge migration: index 7 is "extra" only when no
// core.InstancePlan covers it, regardless of replica count. Mid-migration
// Instances are excluded so the surge pair (source + surge) isn't
// scale-down-deleted out from under Migrate.
//
// Read by the omenative dispatcher (ReconcileComponentOps in dispatch.go)
// before driving Delete on each extra index. Stays here as a v1beta1-typed
// helper because the dispatcher still reads ISVC.Status directly; it will
// migrate to workload/ops when the dispatcher itself does.
func extraInstanceIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, plan core.ComponentPlan) []int32 {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range cs.OMENative.InstanceStatuses {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		if s.Phase == v1beta1.OMENativeInstanceMigrating {
			continue
		}
		if s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationMigrate {
			continue
		}
		extras = append(extras, s.Index)
	}
	return extras
}

// removeInstanceStatus drops the InstanceStatus entry for (component, idx).
// hadEntry=true when a real removal happened, false when already cleared.
// Wrapped in retry.RetryOnConflict; re-checks existence on each attempt so
// a parallel clear becomes a no-op.
//
// Mirrors the buildRemoveInstance closure in internalsource (which wraps
// the same operation as a workload.ReconcileInput.RemoveInstance callback).
// Kept here as a v1beta1-typed function so peer ops still in this package
// (migrate.go, timeouts.go) call it byte-for-byte unchanged. Migrates to
// workload/ops alongside its callers in follow-up slices.
func removeInstanceStatus(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) (bool, error) {
	var hadEntry bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		hadEntry = false
		fresh := &v1beta1.InferenceService{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// ISVC deleted during reconcile — the InstanceStatus
				// entry is gone with it. Treat as a successful no-op.
				return nil
			}
			return fmt.Errorf("re-read ISVC: %w", err)
		}
		if fresh.Status.Components == nil {
			return nil
		}
		cs, ok := fresh.Status.Components[component]
		if !ok || cs.OMENative == nil {
			return nil
		}
		insts := cs.OMENative.InstanceStatuses
		pos := -1
		for i, s := range insts {
			if s.Index == idx {
				pos = i
				break
			}
		}
		if pos == -1 {
			return nil
		}
		cs.OMENative.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
		fresh.Status.Components[component] = cs

		if err := p.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("update ISVC status: %w", err)
		}
		hadEntry = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if hadEntry {
		// Forget so a later index reuse doesn't inherit stale counters.
		p.ExpectationsCache().Forget(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(component), idx)
	}
	return hadEntry, nil
}
