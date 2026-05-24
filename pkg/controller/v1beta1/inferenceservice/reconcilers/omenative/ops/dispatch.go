// Package ops implements the per-Component CRUD operations the OMENative
// reconciler dispatches once a ComponentPlan has been built and a target
// ControllerRevision is in hand. Each lifecycle operation
// (Create / Update / Restart / Delete / Migrate) lives in its own file and
// is driven from ReconcileComponentOps below; the file split mirrors the
// state-machine boundaries in OEP §6.
//
// The op layer is a leaf: it imports core/ for the carrier types
// (ReconcileParams, ComponentPlan, ...) and query/ for the read-side
// helpers (pod listing, naming, label keys), but does NOT import the
// top-level omenative package — that direction would close a cycle since
// the top-level reconciler dispatches into this package.
package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
)

// ReconcileComponentOps drives the five lifecycle operations for one
// Component on one reconcile pass. Lifted out of the top-level
// ReconcileComponent so the deferred status write in the caller runs
// unconditionally, even on early returns from this path.
//
// Order matters:
//  1. Expire timeouts (any in-flight Operation past its Deadline).
//  2. Scale-down (Delete extras above MinReplicas).
//  3. Restart pass (per-Instance recreate when RestartPolicy fires).
//  4. Migration pass (per-Component, surge migration toward FromNode-off).
//  5. Update pass (per-Instance in-place or recreate update).
//  6. Create pass (materialize any missing Instances).
//
// Restart / Migration / Update return early on partial progress so the
// next reconcile sees the post-mutation status before driving Create.
func ReconcileComponentOps(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if err := ExpireOperationsIfDeadlinePassed(ctx, p, plan); err != nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: expire timeouts (component=%s): %w", plan.Component, err)
	}

	// Scale-down first. An ISVC that drops MinReplicas leaves
	// InstanceStatus entries above the new replica count; Delete drives
	// each to gone.
	extras := extraInstanceIndices(p.ISVC, plan.Component, plan)
	allDeleted := true
	for _, idx := range extras {
		done, derr := Delete(ctx, p, plan, idx)
		if derr != nil {
			return ctrl.Result{}, fmt.Errorf("OMENative reconciler: delete instance %d: %w", idx, derr)
		}
		if !done {
			allDeleted = false
		}
	}
	if !allDeleted {
		return ctrl.Result{RequeueAfter: deleteRequeueInterval}, nil
	}

	// Per-Instance restart pass.
	anyRestarting := false
	if plan.RestartPolicy == v1beta1.OMENativeRestartPolicyRecreateInstance {
		for _, inst := range plan.Instances {
			needsRestart, reason, derr := detectRestartTrigger(ctx, p, plan, inst)
			if derr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: detect restart trigger (instance=%d): %w", inst.Index, derr)
			}
			if !needsRestart {
				continue
			}
			done, rerr := Restart(ctx, p, plan, inst, reason)
			if rerr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: restart instance %d: %w", inst.Index, rerr)
			}
			if !done {
				anyRestarting = true
			}
		}
	}
	if anyRestarting {
		return ctrl.Result{RequeueAfter: restartRequeueInterval}, nil
	}

	// Per-Component migration pass.
	if plan.MigrationMode != v1beta1.OMENativeMigrationPolicyModeNever {
		uuid, req, sourceIdx, found, mderr := detectMigrationRequest(ctx, p, plan.Component)
		if mderr != nil {
			return ctrl.Result{}, fmt.Errorf("OMENative reconciler: detect migration request (component=%s): %w", plan.Component, mderr)
		}
		if found {
			done, merr := Migrate(ctx, p, plan, sourceIdx, uuid, req)
			if merr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: migrate instance %d: %w", sourceIdx, merr)
			}
			if !done {
				return ctrl.Result{RequeueAfter: migrateRequeueInterval}, nil
			}
			// Migrate just removed the source InstanceStatus and promoted
			// the surge to Ready. plan was computed with the stale
			// pre-migration view (both source and surge present), so the
			// Update + Create passes below would fall through to recreate
			// the source-side index. Re-queue so the next reconcile
			// rebuilds plan from the post-migration status.
			return ctrl.Result{Requeue: true}, nil
		}
	}

	// Per-Instance update pass.
	if target != nil {
		anyUpdating := false
		for _, inst := range plan.Instances {
			needs, derr := detectUpdateTrigger(ctx, p, plan, inst, target, p.PodSpec)
			if derr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: detect update trigger (instance=%d): %w", inst.Index, derr)
			}
			if !needs {
				continue
			}
			done, uerr := Update(ctx, p, plan, inst, target, p.PodSpec)
			if uerr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: update instance %d: %w", inst.Index, uerr)
			}
			if !done {
				anyUpdating = true
			}
		}
		if anyUpdating {
			return ctrl.Result{RequeueAfter: updateRequeueInterval}, nil
		}
	}

	return Create(ctx, p, plan, target)
}
