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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// ratioGateRequeueInterval is the wait between Update passes when
// RatioBalanced pacing has gated this Component's surge. Short enough
// that a peer Component catching up next reconcile unblocks this one
// promptly; long enough not to spam the apiserver.
const ratioGateRequeueInterval = 3 * time.Second

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
		anyGated := false
		// inFlightUnavail tracks pods this wake-up has already flipped
		// into a draining/recreating state from a previously-Ready
		// baseline. Closes the within-wake-up stale-snapshot hole that
		// otherwise lets the dispatcher fire every instance in one shot
		// (the mass-outage bug at scale).
		//
		// We only count instances STARTING a new update this wake-up.
		// Instances already in Phase=Updating from a prior wake-up are
		// already reflected in status.ServingReplicas — counting them
		// would double-charge the budget and deadlock the in-flight pod
		// (it'd be denied permission to continue its own update).
		var inFlightUnavail int32
		var inFlightSurge int32
		// SurgeThenDrain doesn't take pods offline — it adds an extra
		// pod per Instance. Gate selection at the dispatcher level
		// branches on the strategy in plan.UpdateStrategy.Type so each
		// strategy is throttled by the budget that actually applies.
		isSurgeStrategy := plan.UpdateStrategy.Type == v1beta1.OMENativeUpdateStrategySurgeThenDrain ||
			plan.UpdateStrategy.Type == ""
		for _, inst := range plan.Instances {
			needs, derr := detectUpdateTrigger(ctx, p, plan, inst, target, p.PodSpec)
			if derr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: detect update trigger (instance=%d): %w", inst.Index, derr)
			}
			if !needs {
				continue
			}
			s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index)
			startingFresh := s == nil || s.Phase != v1beta1.OMENativeInstanceUpdating
			if startingFresh {
				// RatioBalanced gate: ask coordination whether the peer
				// Components' new-revision pod counts allow this Component
				// to bump one more. Pass inFlightUnavail so the gate
				// projects against post-drain serving counts, not the
				// stale snapshot (same fix the unavailability gate uses).
				if allowed, _ := coordination.CheckRatioGate(p.ISVC, plan.Component, inFlightUnavail); !allowed {
					anyGated = true
					continue
				}
				if isSurgeStrategy {
					// MaxSurge gate: surge adds an extra pod, doesn't
					// drop one. Throttle concurrent surges across
					// Instances so alive never exceeds desired+MaxSurge.
					if allowed, _ := coordination.CheckSurgeGate(p.ISVC, plan.Component, inFlightSurge); !allowed {
						anyGated = true
						continue
					}
				} else {
					// MaxUnavailable gate: don't take another pod offline
					// if doing so would push the Component above its
					// budget. Skipped for SurgeThenDrain — surge never
					// dips serving.
					if allowed, _ := coordination.CheckUnavailabilityGate(p.ISVC, plan.Component, inFlightUnavail); !allowed {
						anyGated = true
						continue
					}
				}
				// Sequential gate: in a Sequential group, only the
				// active Component (first in Order with RolloutInFlight)
				// is allowed to fire a fresh Update this reconcile. The
				// state machine emits decoder.* / engine.* composite
				// phases in lock-step; without this gate, the dispatcher
				// would fire Update on every Instance whose update
				// trigger is true, recreating both Components
				// concurrently and skipping the Sequential.Awaiting
				// composite phase entirely.
				if allowed, _ := coordination.CheckSequentialGate(p.ISVC, plan.Component); !allowed {
					anyGated = true
					continue
				}
			}
			done, uerr := Update(ctx, p, plan, inst, target, p.PodSpec)
			if uerr != nil {
				return ctrl.Result{}, fmt.Errorf("OMENative reconciler: update instance %d: %w", inst.Index, uerr)
			}
			if startingFresh {
				// Charge the wake-up budget only for fresh starts. The
				// pod has just been pulled from rotation (or is about
				// to be); subsequent gate checks in this wake-up must
				// account for it. Strategy decides which budget applies.
				if isSurgeStrategy {
					inFlightSurge++
				} else {
					inFlightUnavail++
				}
			}
			if !done {
				anyUpdating = true
			}
		}
		if anyUpdating {
			return ctrl.Result{RequeueAfter: updateRequeueInterval}, nil
		}
		if anyGated {
			return ctrl.Result{RequeueAfter: ratioGateRequeueInterval}, nil
		}
	}

	return Create(ctx, p, plan, target)
}
