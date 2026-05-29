// Workload-side per-Component dispatcher. Reconcile is the single entry
// point the ISVC OMENative dispatcher (today) and the future
// InferenceReplica controller call into; it drives one logical workload
// (one Component of one owner) through the Create / Update / Restart /
// Migrate / Delete pipeline by way of the workload/ops state machines.
//
// SCOPE NOTE: this file collapses the ~225 LOC of per-op dispatch
// boilerplate that lived in omenative/ops/dispatch.go
// (ReconcileComponentOps) down to one workload.Reconcile call from
// the omenative caller. The op bodies in workload/ops/ are unchanged —
// Reconcile is pure dispatch.
//
// IMPORT-CYCLE DESIGN (Option C). workload (parent) imports
// workload/ops to call Create/Update/Delete/Restart/Migrate and the
// trigger detectors by name. workload/ops in turn does NOT import the
// parent workload package — instead, every type ops needs (Deps,
// ReconcileInput, ComponentPlan, InstancePlan, RunnerPlan, ...) lives
// in workload/types. Splitting the types out broke the cycle
// structurally; this entry point dispatches with plain Go function
// calls (no init-time function-variable registration, no Register*
// setters, no DI struct).
//
// Coordination boundary preserved: workload code never imports
// inferenceservice/reconcilers/omenative/coordination/. Cross-Component
// gate decisions reach the dispatcher via the UpdateGate callback on
// ReconcileInput; the ISVC adapter wires that closure to the
// coordination.Check{Ratio,Surge,Unavailability,Sequential}Gate stack
// and workload.Reconcile reads only the (allowed, denyReason) tuple.
//
// What this dispatcher does NOT own:
//   - ExpireOperationsIfDeadlinePassed (v1beta1-typed; reads
//     ISVC.Status.Components.OMENative.InstanceStatuses.Operation
//     fields directly; the caller runs it before invoking Reconcile).
//   - ensureHeadlessService / EnsurePodMonitor (ISVC-shape supporting
//     resources owned by the omenative reconciler).
//   - ensureRevisionWithCollisionRetry (ISVC-shape collision-counter
//     bookkeeping; the caller computes the target ControllerRevision
//     and passes it in).
//   - AggregateAndWriteStatus (ISVC-shape counters + top-level
//     EngineReady / DecoderReady / RouterReady condition; the caller
//     runs it after Reconcile returns).
//
// Those stay on the ISVC adapter; this dispatcher only owns the per-
// Instance dispatch loop.
package workload

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// Per-op requeue intervals when an op returns done=false. Mirror the
// constants in workload/ops byte-for-byte so dispatcher pacing tracks
// the per-op state machines; the cross-import would close the cycle
// the types-split avoids, so they're duplicated explicitly here. Any
// change to one MUST be mirrored to the other.
const (
	// deleteRequeueInterval is the wait between Delete passes when at
	// least one Delete reported done=false. Matches
	// workload/ops.DeleteRequeueInterval byte-for-byte.
	deleteRequeueInterval = 5 * time.Second
	// restartRequeueInterval is the wait between Restart passes.
	// Matches workload/ops.RestartRequeueInterval.
	restartRequeueInterval = 5 * time.Second
	// migrateRequeueInterval is the wait between Migrate passes.
	// Matches workload/ops.MigrateRequeueInterval.
	migrateRequeueInterval = 5 * time.Second
	// updateRequeueInterval is the wait between Update passes when at
	// least one Update reported done=false. Matches
	// workload/ops.UpdateRequeueInterval.
	updateRequeueInterval = 5 * time.Second
	// ratioGateRequeueInterval is the wait between Update passes when
	// the adapter-side UpdateGate denied at least one Instance. Short
	// enough that a peer Component catching up next reconcile unblocks
	// this one promptly; long enough not to spam the apiserver. Same
	// constant the pre-split omenative/ops/dispatch.go used.
	ratioGateRequeueInterval = 3 * time.Second
)

// Reconcile drives one workload (one Component of one owner) toward its
// desired state. The caller (the ISVC OMENative dispatcher today, the
// future InferenceReplica controller tomorrow) constructs a fully
// populated ReconcileInput + Deps + plan + target and calls Reconcile
// once per per-Component reconcile pass.
//
// The op chain (matches the previous omenative/ops/ReconcileComponentOps
// dispatch order byte-for-byte):
//
//  1. Scale-down: drive Delete for any InstanceStatus index that the
//     plan no longer asks for. Scale-down completes before scale-up so
//     excess pods don't run alongside in-flight drains.
//
//  2. Per-Instance restart pass: detect pod loss / pod-Failed triggers
//     and dispatch Restart when the Component's restart policy allows
//     it. Held to one pass per reconcile to keep the state machine
//     simple.
//
//  3. Per-Component migration pass: detect any in-flight or annotation-
//     driven migration request via input.DetectMigrationRequest, then
//     dispatch Migrate. Returns Requeue=true after a Migrate completion
//     so the next reconcile rebuilds plan from the post-migration
//     status.
//
//  4. Per-Instance update pass: compute the target diff via
//     workload/ops.DetectUpdateTrigger and dispatch Update on any
//     Instance that needs it. The dispatcher tracks within-pass
//     in-flight counters (inFlightSurge, inFlightUnavail) and consults
//     input.UpdateGate before starting a fresh Update so cross-
//     Component coordination (the ISVC ratio / surge / unavailability /
//     sequential gates) can throttle the rollout.
//
//  5. Create pass: materialize any missing Instances toward the plan.
//
// All bug-fix logic from PRs #243 / #246 / #256 / #260 / #261 / #262 is
// preserved — the underlying op state machines in workload/ops are
// unchanged, and the dispatch order + gate-counting semantics match the
// pre-extraction omenative/ops/ReconcileComponentOps byte-for-byte.
//
// target may be nil when DesiredSpec.PodSpec is nil (MinReplicas=0 —
// nothing to render). Restart / Update passes short-circuit on nil
// target; Create returns immediately.
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
		return ctrl.Result{RequeueAfter: deleteRequeueInterval}, nil
	}

	// 2. Per-Instance restart pass.
	//
	// plan.RestartPolicy is a v1beta1-typed string (residual coupling
	// documented on ComponentPlan.Component in plan.go). Compare via
	// the workload-side mirror constant's string value so reconcile.go
	// itself doesn't take on a v1beta1 import — same audit-grep boundary
	// the rest of workload upholds (closing PR #264).
	anyRestarting := false
	if string(plan.RestartPolicy) == string(RestartPolicyRecreateInstance) {
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
		return ctrl.Result{RequeueAfter: restartRequeueInterval}, nil
	}

	// 3. Per-Component migration pass.
	//
	// plan.MigrationMode is a v1beta1-typed string (residual coupling).
	// Compare via the workload-side mirror constant so reconcile.go
	// itself doesn't take on a v1beta1 import. The dispatcher skips
	// the pass when the mode is Never OR when the adapter didn't wire
	// a detector (the IR adapter, when it lands, may leave it nil).
	if string(plan.MigrationMode) != string(MigrationModeNever) && input.DetectMigrationRequest != nil {
		uuid, req, sourceIdx, found, mderr := input.DetectMigrationRequest(ctx)
		if mderr != nil {
			return ctrl.Result{}, fmt.Errorf("workload.Reconcile: detect migration request (component=%s): %w", plan.Component, mderr)
		}
		if found {
			done, merr := workloadops.Migrate(ctx, deps, input, plan, sourceIdx, uuid, req)
			if merr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: migrate instance %d: %w", sourceIdx, merr)
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

	// 4. Per-Instance update pass.
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
		// would double-charge the budget and deadlock the in-flight pod.
		var inFlightUnavail int32
		var inFlightSurge int32
		// SurgeThenDrain doesn't take pods offline — it adds an extra
		// pod per Instance. Gate selection at the dispatcher level
		// branches on the strategy in plan.UpdateStrategy.Type so each
		// strategy is throttled by the budget that actually applies.
		//
		// Compare via the workload-side UpdateStrategyType so reconcile.go
		// itself doesn't take on a v1beta1 import (plan.UpdateStrategy.Type
		// is v1beta1-typed residual coupling). Empty Type defaults to
		// SurgeThenDrain — matches workload.BuildPlan's defaulting.
		strategy := UpdateStrategyType(plan.UpdateStrategy.Type)
		if strategy == "" {
			strategy = UpdateStrategySurgeThenDrain
		}
		isSurgeStrategy := strategy == UpdateStrategySurgeThenDrain
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
			if startingFresh && input.UpdateGate != nil {
				// Pass the dispatcher's in-flight counters so the gate
				// can project against the post-this-pass shape. Strategy
				// decides which budget applies on the adapter side.
				if allowed, _ := input.UpdateGate(strategy, inFlightSurge, inFlightUnavail); !allowed {
					anyGated = true
					continue
				}
			}
			done, uerr := workloadops.Update(ctx, deps, input, plan, inst, target, input.DesiredSpec.PodSpec)
			if uerr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: update instance %d: %w", inst.Index, uerr)
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

	// 5. Create pass.
	return workloadops.Create(ctx, deps, input, plan, target)
}

// ExtraInstanceIndices returns InstanceStatus indices the plan no longer
// covers — these are scale-down targets. Set-difference framing handles
// sparse indices from surge migration: index 7 is "extra" only when no
// InstancePlan covers it, regardless of replica count. Mid-migration
// Instances are excluded so the surge pair (source + surge) isn't
// scale-down-deleted out from under Migrate.
//
// Mirrors the v1beta1-typed extraInstanceIndices that previously lived
// in omenative/ops/legacy_create_helpers.go byte-for-byte; the
// workload-typed signature lets the dispatcher reach the same logic
// without the ISVC adapter projection.
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

// findObservedInstanceStatus mirrors findInstanceStatus from
// workload/ops/ for use inside the dispatcher itself. Kept private to
// workload — callers that already imported workload/ops use the
// package-local helper there.
func findObservedInstanceStatus(observed []InstanceStatus, idx int32) *InstanceStatus {
	for i := range observed {
		if observed[i].Index == idx {
			return &observed[i]
		}
	}
	return nil
}
