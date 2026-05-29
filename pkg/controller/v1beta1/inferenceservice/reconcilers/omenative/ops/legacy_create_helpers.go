package ops

// Helpers carved out of create.go (part 2 slice 4b of N) and delete.go
// (part 2 slice 4c of N) when those per-Instance state machines moved
// to pkg/controller/v1beta1/workload/ops. The helpers below remain in
// this package because the dispatcher (dispatch.go) and the peer ops
// still in this package (timeouts.go, the revision-hash test) call
// them directly and read the v1beta1.OMENativeInstanceStatus shape.
//
// Each helper shrinks out as the matching caller migrates to
// workload/ops in a follow-up slice.

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
