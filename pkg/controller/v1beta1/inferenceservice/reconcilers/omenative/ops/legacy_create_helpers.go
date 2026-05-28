package ops

// Helpers carved out of create.go when the per-Instance Create state
// machine moved to pkg/controller/v1beta1/workload/ops (part 2 slice 4b
// of N). The callers below still live in this package and read the
// v1beta1.OMENativeInstanceStatus shape directly — keeping the helpers
// here as v1beta1-typed functions keeps the peer migrations (delete,
// update, restart, migrate, timeouts) byte-for-byte unchanged.
//
// Each helper shrinks out as the matching peer op moves to workload/ops
// in a follow-up slice.

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// podTarget describes one pod to create within an Instance. Kept in
// the omenative/ops package because the peer ops (restart, migrate,
// update) construct and consume this shape directly.
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
