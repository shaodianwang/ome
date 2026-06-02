// Workload-side per-Component dispatcher. Reconcile is the single entry
// point the ISVC OMENative dispatcher and the InferenceReplica
// controller call into; it drives one logical workload (one Component
// of one owner) through the Create / Update / Restart / Migrate /
// Delete pipeline by way of the workload/ops state machines.
//
// What this dispatcher does NOT own (the caller runs these around
// Reconcile):
//   - ExpireOperationsIfDeadlinePassed — v1beta1-typed; reads
//     ISVC.Status.Components.Lifecycle.InstanceStatuses.Operation
//     fields directly.
//   - PodMonitor — handled by the top-level podmonitor reconciler.
//   - ensureRevisionWithCollisionRetry — ISVC-shape collision-counter
//     bookkeeping; the caller computes the target ControllerRevision
//     and passes it in.
//   - AggregateAndWriteStatus — ISVC-shape counters + top-level
//     EngineReady / DecoderReady / RouterReady condition.
//   - workload.ReconcileHeadlessService — invoked by the caller before
//     Reconcile so both the ISVC adapter and the IR adapter can share
//     the Service renderer.
//
// Cross-Component coordination gates reach the dispatcher via the
// UpdateGate callback on ReconcileInput; the workload package itself
// never imports `inferenceservice/reconcilers/omenative/coordination/`.
package workload

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// ratioGateRequeueInterval is short enough that a peer Component
// catching up next reconcile unblocks this one promptly; long
// enough not to spam the apiserver. Unlike the per-op intervals
// (workloadops.{Create,Delete,Restart,Migrate,Update}RequeueInterval)
// this pacing is dispatcher-owned, not op-state-machine-owned.
const ratioGateRequeueInterval = 3 * time.Second

// Reconcile drives one workload (one Component of one owner) toward its
// desired state. The caller constructs a fully populated ReconcileInput
// + Deps + plan + target and calls Reconcile once per per-Component
// reconcile pass.
//
// The op chain:
//
//  1. Scale-down — Delete for any InstanceStatus index the plan no
//     longer asks for. Completes before scale-up so excess pods don't
//     run alongside in-flight drains.
//  2. Restart — per-Instance pod-loss / pod-Failed triggers; dispatched
//     when the Component's restart policy allows it. One pass per
//     reconcile.
//  3. Migration — input.DetectMigrationRequest then Migrate. Returns
//     Requeue=true after completion so the next reconcile rebuilds
//     plan from the post-migration status.
//  4. Update — DetectUpdateTrigger then Update. Tracks within-pass
//     in-flight counters and consults input.UpdateGate so
//     cross-Component coordination can throttle the rollout.
//  5. Create — materialize any missing Instances.
//
// target may be nil when DesiredSpec.PodSpec is nil (MinReplicas=0).
// Restart / Update passes short-circuit on nil target; Create returns
// immediately.
func Reconcile(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("workload.Reconcile: nil client (component=%s)", plan.Component)
	}

	// 1. Scale-down.
	extras := ExtraInstanceIndices(input.ObservedState.InstanceStatuses, plan)
	allDeleted := true
	for _, idx := range extras {
		done, derr := workloadops.Delete(ctx, deps, input, plan, idx)
		if derr != nil {
			return ctrl.Result{}, fmt.Errorf("workload.Reconcile: delete instance %d: %w", idx, derr)
		}
		if !done {
			allDeleted = false
		}
	}
	if !allDeleted {
		return ctrl.Result{RequeueAfter: workloadops.DeleteRequeueInterval}, nil
	}

	// 2. Per-Instance restart pass.
	anyRestarting := false
	if plan.RestartPolicy == RestartPolicyRecreateInstance {
		for _, inst := range plan.Instances {
			needsRestart, reason, derr := workloadops.DetectRestartTrigger(ctx, deps, input, plan, inst)
			if derr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: detect restart trigger (instance=%d): %w", inst.Index, derr)
			}
			if !needsRestart {
				continue
			}
			done, rerr := workloadops.Restart(ctx, deps, input, plan, inst, reason)
			if rerr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: restart instance %d: %w", inst.Index, rerr)
			}
			if !done {
				anyRestarting = true
			}
		}
	}
	if anyRestarting {
		return ctrl.Result{RequeueAfter: workloadops.RestartRequeueInterval}, nil
	}

	// 3. Per-Component migration pass. Skip when mode is Never or the
	// adapter didn't wire a detector.
	//
	// Migrate's third return (accepted) distinguishes two done=false
	// modes:
	//   - accepted=true: migration is mid-flight (status stamped, ledger
	//     Started, or fresh request that just stamped). Requeue at the
	//     Migrate interval; do NOT fall through (DetectUpdateTrigger
	//     already suppresses Migrate-owned status, so falling through
	//     would be a no-op anyway, but the explicit requeue is cleaner).
	//   - accepted=false: migration deferred without taking ownership
	//     (fresh request, source not yet steady-Ready because of an
	//     in-flight Update/Restart/Create). Fall through to Update/Create
	//     so the in-flight op converges. Without fall-through the
	//     dispatcher loops indefinitely at the MigrateRequeueInterval and
	//     the in-flight op never runs — the affinity-trigger-not-detected
	//     deadlock.
	if plan.MigrationMode != MigrationModeNever && input.DetectMigrationRequest != nil {
		uuid, req, sourceIdx, found, mderr := input.DetectMigrationRequest(ctx)
		if mderr != nil {
			return ctrl.Result{}, fmt.Errorf("workload.Reconcile: detect migration request (component=%s): %w", plan.Component, mderr)
		}
		if found {
			done, accepted, merr := workloadops.Migrate(ctx, deps, input, plan, sourceIdx, uuid, req)
			if merr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: migrate instance %d: %w", sourceIdx, merr)
			}
			if !done && accepted {
				// Mid-flight: requeue at the per-op interval.
				return ctrl.Result{RequeueAfter: workloadops.MigrateRequeueInterval}, nil
			}
			if done {
				// Migrate just removed the source InstanceStatus and
				// promoted the surge to Ready (or wrote a terminal
				// Failed). plan was computed with the stale pre-migration
				// view (both source and surge present, or pre-failure
				// state), so the Update + Create passes below would fall
				// through to recreate the source-side index. Re-queue so
				// the next reconcile rebuilds plan from the post-
				// migration status.
				return ctrl.Result{Requeue: true}, nil
			}
			// !done && !accepted: fresh-request defer (e.g., source
			// Phase=Updating from an in-flight spec edit). Fall through
			// to Update/Create so the in-flight op converges; the next
			// reconcile re-detects the same annotation against a steady-
			// Ready source.
		}
	}

	// 4. Per-Instance update pass.
	if target != nil {
		anyUpdating := false
		anyGated := false
		// anyUpdateRan tracks whether ANY Update call fired this wake-up
		// — including ones that returned done=true (e.g., a surge that
		// just promoted Phase to Ready and stamped RunningRevision). Even
		// a "done" Update mutates InstanceStatus; the subsequent Create
		// pass would observe the pre-Update ObservedState snapshot, which
		// is stale wrt ActiveOrdinal / RunningRevision / Phase. Letting
		// Create run on stale state leads to two bugs:
		//
		//   1. Create's Ready-promote stamps RunningRevision=target.Name
		//      even when the existing pods carry a different revision
		//      (the X-2 bump-during-bump corruption mode where status
		//      says vN but pods are still on vN-1). Bug #58.
		//   2. Create's scale-up reads activeOrdinalForInstance from
		//      stale ObservedState, sees a "missing" pod at the
		//      pre-promote ordinal slot, and creates a duplicate
		//      alongside the post-surge canonical pod.
		//
		// Skipping Create + requeue when Update fired forces the next
		// reconcile to read fresh ObservedState that reflects the just-
		// committed mutations, so Create's per-pod decisions are made
		// against the current cluster state.
		anyUpdateRan := false
		// Track only instances STARTING a new update this wake-up.
		// Instances already in Phase=Updating from a prior wake-up are
		// already reflected in status.ServingReplicas — counting them
		// would double-charge the budget and deadlock the in-flight pod.
		// Closes the within-wake-up stale-snapshot hole that otherwise
		// lets the dispatcher fire every instance in one shot (mass
		// outage at scale).
		var inFlightUnavail int32
		var inFlightSurge int32
		// SurgeThenDrain doesn't take pods offline; gate selection
		// branches on strategy so each is throttled by the budget that
		// actually applies. Empty Type defaults to SurgeThenDrain —
		// matches workload.BuildPlan's defaulting.
		strategy := plan.UpdateStrategy.Type
		if strategy == "" {
			strategy = UpdateStrategySurgeThenDrain
		}
		isSurgeStrategy := strategy == UpdateStrategySurgeThenDrain
		// Per-Component within-Component budget — distinct layer from
		// the cross-Component coordination-group gate that input.UpdateGate
		// enforces. Both layers must allow before an Instance starts a
		// fresh update; first denial stops the start. See
		// budget.go's package comment for the composition rule:
		//   effective_cap = min(group_cap, per_component_cap)
		// nil RollingUpdate or nil MaxSurge / MaxUnavailable returns
		// BudgetNoLimit → this layer doesn't cap, the group layer is the
		// only constraint.
		var rollingUpdate *RollingUpdate
		if plan.UpdateStrategy.RollingUpdate != nil {
			rollingUpdate = plan.UpdateStrategy.RollingUpdate
		}
		perCompSurgeBudget := PerComponentMaxSurgeBudget(rollingUpdate, plan.Replicas)
		perCompUnavailBudget := PerComponentMaxUnavailableBudget(rollingUpdate, plan.Replicas)
		// Snapshot the prior-pass in-flight counts ONCE — same anchor the
		// per-wake-up counters layer on top of. Without this anchor the
		// per-Component check would re-count every iteration and double-
		// charge the budget against the same Instance.
		priorSurgeInFlight := CurrentSurgeInFlight(input.ObservedState.InstanceStatuses)
		priorUnavailInFlight := CurrentUnavailableInFlight(input.ObservedState.InstanceStatuses)
		for _, inst := range plan.Instances {
			needs, derr := workloadops.DetectUpdateTrigger(ctx, deps, input, plan, inst, target, input.DesiredSpec.PodSpec)
			if derr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: detect update trigger (instance=%d): %w", inst.Index, derr)
			}
			if !needs {
				continue
			}
			s := findObservedInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
			startingFresh := s == nil || s.Phase != InstancePhaseUpdating
			if startingFresh {
				// Per-Component within-Component cap. Independent from
				// the coordination-group cap below: each is its own
				// capacity, both must allow. We project (prior + this-
				// wake-up + 1) against the layer's budget the same way
				// coordination/ratio.go's CheckSurge / CheckUnavailability
				// project against the group budget.
				if isSurgeStrategy {
					if perCompSurgeBudget != BudgetNoLimit {
						projected := priorSurgeInFlight + inFlightSurge + 1
						if projected > perCompSurgeBudget {
							anyGated = true
							continue
						}
					}
				} else {
					if perCompUnavailBudget != BudgetNoLimit {
						projected := priorUnavailInFlight + inFlightUnavail + 1
						if projected > perCompUnavailBudget {
							anyGated = true
							continue
						}
					}
				}
				if input.UpdateGate != nil {
					// Pass in-flight counters so the gate can project
					// against the post-this-pass shape. Independent
					// layer from the per-Component check above.
					if allowed, _ := input.UpdateGate(strategy, inFlightSurge, inFlightUnavail); !allowed {
						anyGated = true
						continue
					}
				}
			}
			done, uerr := workloadops.Update(ctx, deps, input, plan, inst, target, input.DesiredSpec.PodSpec)
			if uerr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: update instance %d: %w", inst.Index, uerr)
			}
			anyUpdateRan = true
			if startingFresh {
				// Charge the wake-up budget only for fresh starts —
				// subsequent gate checks must account for this pod.
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
			return ctrl.Result{RequeueAfter: workloadops.UpdateRequeueInterval}, nil
		}
		if anyGated {
			return ctrl.Result{RequeueAfter: ratioGateRequeueInterval}, nil
		}
		// All Updates that ran returned done=true (steady-state for those
		// instances). Requeue without running Create so the next pass
		// sees fresh ObservedState — see anyUpdateRan comment above for
		// the X-2 (bump-during-bump) corruption modes this guards against.
		if anyUpdateRan {
			return ctrl.Result{Requeue: true}, nil
		}
	}

	// 5. Create pass.
	return workloadops.Create(ctx, deps, input, plan, target)
}

// ExtraInstanceIndices returns InstanceStatus indices the plan no
// longer covers — scale-down targets. Set-difference framing handles
// sparse indices from surge migration (index 7 is extra only when no
// InstancePlan covers it, regardless of replica count). Mid-migration
// Instances are excluded so the surge pair isn't scale-down-deleted
// out from under Migrate.
func ExtraInstanceIndices(observed []InstanceStatus, plan ComponentPlan) []int32 {
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range observed {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		if s.Phase == InstancePhaseMigrating {
			continue
		}
		if s.Operation != nil && s.Operation.Type == InstanceOperationMigrate {
			continue
		}
		extras = append(extras, s.Index)
	}
	return extras
}

// findObservedInstanceStatus returns the matching InstanceStatus
// pointer, or nil if absent.
func findObservedInstanceStatus(observed []InstanceStatus, idx int32) *InstanceStatus {
	for i := range observed {
		if observed[i].Index == idx {
			return &observed[i]
		}
	}
	return nil
}
