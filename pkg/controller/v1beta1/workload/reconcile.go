// Workload-side per-Component dispatcher. Reconcile is the single entry
// point the ISVC OMENative dispatcher and the InferenceReplica
// controller call into; it drives one logical workload (one Component
// of one owner) through the Create / Update / Restart / Migrate /
// Delete pipeline by way of the workload/ops state machines.
//
// What this dispatcher does NOT own (the caller runs these around
// Reconcile):
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
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
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
// Shape: observe → plan → execute. One ObservedSnapshot is built up
// front; Plan (plan_decision.go) evaluates every pass trigger against
// it and produces the Decision; Execute applies the Decision through
// the workload/ops state machines and runs the escalation pass.
//
// The op chain:
//
//  1. Scale-down — Delete for any InstanceStatus index the plan no
//     longer asks for. Completes before scale-up so excess pods don't
//     run alongside in-flight drains.
//  2. Restart — per-Instance pod-loss / pod-Failed triggers; dispatched
//     when the Component's restart policy allows it. One pass per
//     reconcile.
//  3. Migration expiry — Manual records past their Deadline are
//     consumed (ops.ExpireMigrations): record closed, pair unpinned,
//     source restored from observation. Runs BEFORE the drive pass so
//     an expired record can never be driven (re-stamped) again, and
//     regardless of MigrationMode so a mode flip to Never cannot
//     strand a non-terminal record.
//  4. Migration — the oldest non-terminal Manual record from
//     ObservedState.Migrations drives Migrate, one per pass. Returns
//     Requeue=true after completion so the next reconcile rebuilds
//     plan from the post-migration status.
//  5. Update — DetectUpdateTrigger then Update. Tracks within-pass
//     in-flight counters and consults input.UpdateGate so
//     cross-Component coordination can throttle the rollout.
//  6. Create — materialize any missing Instances.
//  7. Escalation — the terminal-failure pass (escalation.go): stuck-pod
//     and elapsed-deadline evidence from the snapshot decides the
//     Phase=Failed transition through the disposition classification.
//     Runs after the op passes on every non-teardown, non-paused,
//     non-error return — including the in-flight early-requeue paths, so
//     a wedged surge or gang is escalated while its op keeps requeueing.
//
// target may be nil when DesiredSpec.PodSpec is nil (MinReplicas=0).
// Restart / Update passes short-circuit on nil target; Create returns
// immediately.
func Reconcile(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("workload.Reconcile: nil client (component=%s)", plan.Component)
	}

	// Teardown mode: the owner is being deleted. The planned index set is
	// treated as empty so every observed Instance is a scale-down extra
	// and runs the ordinary Delete pipeline; nothing else runs — not the
	// Paused gate, not Restart / Migrate / Update / Create, not the
	// escalation pass (the Delete pipeline owns wedge escalation via
	// lifecycle.forceDelete). The caller owns completion detection and
	// finalizer decisions.
	//
	// includeMigrating=true: a mid-migration source and its surge are
	// deleted like everything else. The normal-path exclusion protects an
	// in-flight Migrate from the scale-down pass, but under teardown there
	// is no Migrate to protect — excluding the pair would leave its pods
	// with no Delete op and wedge the teardown forever.
	if input.Teardown {
		emptied := plan
		emptied.Instances = nil
		extras := ExtraInstanceIndices(input.ObservedState.InstanceStatuses, emptied, true)
		allDeleted, err := deleteExtraInstances(ctx, deps, input, plan, extras)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !allDeleted {
			return ctrl.Result{RequeueAfter: workloadops.DeleteRequeueInterval}, nil
		}
		return ctrl.Result{}, nil
	}

	// Single observation for this reconcile: pod reads are lazy + memoized
	// per source (live for destructive planning, cached otherwise), so each
	// source is Listed at most once and only when a pass needs it.
	snapshot := NewObservedSnapshot(deps, input, plan.Component, input.ObservedState.InstanceStatuses)

	decision, perr := Plan(ctx, input, plan, target, snapshot)
	if perr != nil {
		return ctrl.Result{}, perr
	}
	return Execute(ctx, deps, input, plan, target, snapshot, decision)
}

// Execute applies the Decision: the op-pass action loop, then the
// escalation pass when Decision.Escalate allows it. Escalation runs on
// every non-error action-loop return — including the in-flight
// early-requeue returns — so a wedged surge or gang is escalated while
// its op keeps requeueing; an op-pass error requeues with backoff and
// the next reconcile escalates from fresh evidence.
func Execute(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, d Decision) (ctrl.Result, error) {
	res, err := executeActions(ctx, deps, input, plan, target, snapshot, d)
	if err != nil || !d.Escalate {
		return res, err
	}
	if eerr := escalateFromEvidence(ctx, deps, input, plan, target, snapshot); eerr != nil {
		return ctrl.Result{}, eerr
	}
	return res, nil
}

// executeActions runs the op-pass pipeline (steps 1-6 of the chain
// documented on Reconcile) for the Decision's selected actions, in
// order. Early returns are in-flight requeues; the ops decide them —
// the Decision selects which op advances, the op's outcome decides
// whether the pipeline stops here.
func executeActions(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, d Decision) (ctrl.Result, error) {
	for _, action := range d.Actions {
		switch action.Kind {
		// 1. Scale-down.
		case ActionScaleDown:
			allDeleted, err := deleteExtraInstances(ctx, deps, input, plan, action.Extras)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !allDeleted {
				return ctrl.Result{RequeueAfter: workloadops.DeleteRequeueInterval}, nil
			}

		// 2. Per-Instance restart pass.
		case ActionRestart:
			anyRestarting := false
			for _, sel := range action.Restarts {
				done, rerr := workloadops.Restart(ctx, deps, input, plan, sel.Instance, sel.Reason)
				if rerr != nil {
					return ctrl.Result{}, fmt.Errorf("workload.Reconcile: restart instance %d: %w", sel.Instance.Index, rerr)
				}
				if !done {
					anyRestarting = true
				}
			}
			if anyRestarting {
				return ctrl.Result{RequeueAfter: workloadops.RestartRequeueInterval}, nil
			}

		// 3. Migration expiry pass. When anything expired, requeue
		// immediately: ObservedState and plan are now stale (record
		// terminal, pair ops cleared), and the next pass's rebuilt plan
		// drops the unpinned surge index so the ordinary step-1 Delete
		// pipeline tears it down.
		case ActionMigrateExpiry:
			if expiredCount, eerr := workloadops.ExpireMigrations(ctx, deps, input, plan); eerr != nil {
				return ctrl.Result{}, fmt.Errorf("workload.Reconcile: expire migrations: %w", eerr)
			} else if expiredCount > 0 {
				return ctrl.Result{Requeue: true}, nil
			}

		// 4. Per-Component migration pass (dispatch pacing unchanged:
		// one record per pass).
		//
		// Migrate's third return (accepted) distinguishes two done=false
		// modes:
		//   - accepted=true: migration is mid-flight (record carries the
		//     surge index, statuses stamped). Requeue at the Migrate
		//     interval; do NOT fall through (DetectUpdateTrigger already
		//     suppresses Migrate-owned status, so falling through would be
		//     a no-op anyway, but the explicit requeue is cleaner).
		//   - accepted=false: migration deferred without taking ownership
		//     (fresh record, source not yet steady-Ready because of an
		//     in-flight Update/Restart/Create). Fall through to Update/Create
		//     so the in-flight op converges. Without fall-through the
		//     dispatcher loops indefinitely at the MigrateRequeueInterval and
		//     the in-flight op never runs — the affinity-trigger-not-detected
		//     deadlock.
		case ActionMigrate:
			rec := action.Migration.Record
			// Reconstruct the executor's request view from the record —
			// the annotation was consumed at accept time.
			req := &audit.MigrationRequest{
				SchemaVersion:   audit.SchemaV1,
				Component:       string(plan.Component),
				Instance:        rec.SourceInstance,
				FromNode:        rec.FromNode,
				HintTargetNodes: append([]string(nil), rec.HintTargetNodes...),
				Reason:          rec.Reason,
			}
			sourceIdx := rec.SourceInstance
			done, accepted, merr := workloadops.Migrate(ctx, deps, input, plan, sourceIdx, rec.RequestUUID, req)
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
			// !done && !accepted: fresh-record defer (e.g., source
			// Phase=Updating from an in-flight spec edit). Fall through
			// to Update/Create so the in-flight op converges; the next
			// reconcile re-picks the same record against a steady-Ready
			// source.

		// 5. Per-Instance update pass.
		case ActionUpdate:
			res, stop, uerr := executeUpdatePass(ctx, deps, input, plan, target, snapshot, action.Update, d.RequeueAfter)
			if uerr != nil {
				return ctrl.Result{}, uerr
			}
			if stop {
				return res, nil
			}

		// 6. Create pass.
		case ActionCreate:
			res, cerr := workloadops.Create(ctx, deps, input, plan, target)
			if cerr != nil {
				return res, cerr
			}
			return foldRetryAfter(res, d.RequeueAfter), nil
		}
	}

	// Only a paused Decision ends without a Create action: scale-down
	// (if any) has run, nothing else may.
	return ctrl.Result{}, nil
}

// executeUpdatePass runs the Update op for the Decision's selected
// Instances, throttled by the within-pass counters, the per-Component
// budget, and the coordination UpdateGate. The gate consult lives HERE,
// not in Plan: it reads live peer/self state, so it must run at the
// update pass's position — after earlier passes' effects (a restart
// completion, a finished scale-down) have landed — to observe the same
// state the pass pipeline showed it. stop=true means the pass consumed
// the reconcile (the caller returns res without running Create).
func executeUpdatePass(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, sel *UpdateSelection, retryBlockWait time.Duration) (res ctrl.Result, stop bool, err error) {
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
	//      says vN but pods are still on vN-1).
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
	// Count only Instances STARTING a new update this wake-up
	// (item.StartingFresh). Instances already in Phase=Updating from a
	// prior wake-up are anchored in the selection's Prior* counts —
	// counting them again would double-charge the budget and deadlock
	// the in-flight pod. Closes the within-wake-up stale-snapshot hole
	// that otherwise lets the dispatcher fire every instance in one
	// shot (mass outage at scale).
	var inFlightUnavail int32
	var inFlightSurge int32
	// gateUnavail is the coordination gate's within-pass delta: fresh
	// starts that pull a SERVING pod from rotation this wake-up. It
	// diverges from inFlightUnavail (the per-Component, op-based
	// counter) on CoordGateExempt starts — a Failed zero-serving
	// Instance's recreate takes nothing additional offline, and its
	// outage is already inside the gate's serving-based count, so
	// charging it here would over-project every later consult in the
	// same pass.
	var gateUnavail int32
	isSurgeStrategy := sel.Strategy == UpdateStrategySurgeThenDrain
	// Same memoized cached read Plan selected from — the pods handed to
	// the Update op match the selection's evidence.
	updateByInstance, lerr := snapshot.CachedPods(ctx)
	if lerr != nil {
		return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: list pods for update pass (component=%s): %w", plan.Component, lerr)
	}
	for _, item := range sel.Items {
		if item.AdoptRevision {
			if berr := workloadops.BackfillRunningRevision(ctx, input, item.Instance.Index, target.Name); berr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: detect update trigger (instance=%d): %w", item.Instance.Index, berr)
			}
			continue
		}
		if item.CleanupOnly {
			// Superseded-revision wreckage: abandon toward the current
			// desired state. Never budget-charged and never gated —
			// cleanup only deletes dead pods / resets a stranded
			// continuation, freeing capacity rather than consuming it.
			done, cerr := workloadops.CleanupWreckage(ctx, deps, input, plan, item.Instance, target, updateByInstance[item.Instance.Index])
			if cerr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: cleanup wreckage (instance=%d): %w", item.Instance.Index, cerr)
			}
			if !done {
				anyUpdating = true
				anyUpdateRan = true
			}
			continue
		}
		if item.StartingFresh {
			// Per-Component within-Component cap. Independent from
			// the coordination-group cap below: each is its own
			// capacity, both must allow; first denial stops the start.
			// We project (prior + this-wake-up + 1) against the layer's
			// budget the same way coordination/ratio.go's CheckSurge /
			// CheckUnavailability project against the group budget.
			if isSurgeStrategy {
				if sel.SurgeBudget != BudgetNoLimit {
					projected := sel.PriorSurgeInFlight + inFlightSurge + 1
					if projected > sel.SurgeBudget {
						anyGated = true
						continue
					}
				}
			} else {
				if sel.UnavailBudget != BudgetNoLimit {
					projected := sel.PriorUnavailInFlight + inFlightUnavail + 1
					if projected > sel.UnavailBudget {
						anyGated = true
						continue
					}
				}
			}
			if input.UpdateGate != nil && !item.CoordGateExempt {
				// Pass in-flight counters so the gate can project
				// against the post-this-pass shape. Independent
				// layer from the per-Component check above.
				// CoordGateExempt starts skip the consult: the gate
				// already counts their outage in its serving-based
				// unavailability, so gating their own recreate is a
				// double count that starves the recovery (see
				// UpdateItem.CoordGateExempt).
				if allowed, _ := input.UpdateGate(sel.Strategy, inFlightSurge, gateUnavail); !allowed {
					anyGated = true
					continue
				}
			}
		}
		done, uerr := workloadops.UpdateWithPods(ctx, deps, input, plan, item.Instance, target, input.DesiredSpec.PodSpec, updateByInstance[item.Instance.Index])
		if uerr != nil {
			return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: update instance %d: %w", item.Instance.Index, uerr)
		}
		anyUpdateRan = true
		if item.StartingFresh {
			// Charge the wake-up budget only for fresh starts —
			// subsequent gate checks must account for this pod.
			if isSurgeStrategy {
				inFlightSurge++
			} else {
				inFlightUnavail++
				if !item.CoordGateExempt {
					gateUnavail++
				}
			}
		}
		if !done {
			anyUpdating = true
		}
	}
	if anyUpdating || anyGated || anyUpdateRan {
		// About to requeue without the full Create pass. Brand-new
		// (surge-free) indices legitimately bypass the skip-Create
		// gate above: they have a genuine ActiveOrdinal=0 and no
		// RunningRevision to mis-stamp, so neither X-2 corruption
		// mode applies — see ops.CreateFreshIndices. Materialize them
		// now so a concurrent scale-up isn't starved behind the
		// in-flight rollout. The full Create pass still owns
		// surge-sensitive (touched) indices once the rollout drains.
		if _, ferr := workloadops.CreateFreshIndices(ctx, deps, input, plan, target); ferr != nil {
			return ctrl.Result{}, false, ferr
		}
	}
	if anyUpdating {
		return foldRetryAfter(ctrl.Result{RequeueAfter: workloadops.UpdateRequeueInterval}, retryBlockWait), true, nil
	}
	if anyGated {
		return foldRetryAfter(ctrl.Result{RequeueAfter: ratioGateRequeueInterval}, retryBlockWait), true, nil
	}
	// All Updates that ran returned done=true (steady-state for those
	// instances). Requeue without running Create so the next pass
	// sees fresh ObservedState — see anyUpdateRan above for the X-2
	// (bump-during-bump) corruption modes this guards against.
	// Immediate requeue is already sooner than any retryBlockWait.
	if anyUpdateRan {
		return ctrl.Result{Requeue: true}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// deleteExtraInstances dispatches workloadops.Delete for every index in
// extras. Shared verbatim by the scale-down step and the teardown branch
// so Delete-of-extras semantics cannot drift between them. Returns
// allDeleted=false while any Delete is still in flight.
func deleteExtraInstances(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, extras []int32) (bool, error) {
	allDeleted := true
	for _, idx := range extras {
		done, derr := workloadops.Delete(ctx, deps, input, plan, idx)
		if derr != nil {
			return false, fmt.Errorf("workload.Reconcile: delete instance %d: %w", idx, derr)
		}
		if !done {
			allDeleted = false
		}
	}
	return allDeleted, nil
}

// foldRetryAfter folds a RetryBlock re-evaluation wake-up into res.
// Additive only: an immediate requeue or an already-earlier
// RequeueAfter wins; otherwise take the min. Never removes a wake-up.
func foldRetryAfter(res ctrl.Result, retryAfter time.Duration) ctrl.Result {
	if retryAfter <= 0 {
		return res
	}
	if res.Requeue && res.RequeueAfter == 0 {
		// Immediate requeue is sooner than any positive retryAfter.
		return res
	}
	if res.RequeueAfter == 0 || retryAfter < res.RequeueAfter {
		res.RequeueAfter = retryAfter
	}
	return res
}

// liveBucketPods does ONE live List of the Component's pods and buckets
// them by Instance index. The restart pass uses the live reader because
// restart is destructive and must not act on a stale cache — the same
// read source DetectRestartTrigger used per-Instance, hoisted to once per
// reconcile.
func liveBucketPods(ctx context.Context, deps Deps, input ReconcileInput, component ComponentType) (map[int32][]*corev1.Pod, error) {
	// Live reader (APIReader) has no Pod field index — useIndex=false skips
	// the doomed MatchingFields probe so this is exactly ONE live List.
	pods, err := query.ListOMENativePodsByName(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, component, false)
	if err != nil {
		return nil, err
	}
	return query.BucketPodsByInstanceIdx(pods), nil
}

// cachedBucketPods does ONE cached List of the Component's pods and
// buckets them by Instance index. The update pass uses the cached reader —
// the same non-destructive read source DetectUpdateTrigger / Update used
// per-Instance, hoisted to once per reconcile.
func cachedBucketPods(ctx context.Context, deps Deps, input ReconcileInput, component ComponentType) (map[int32][]*corev1.Pod, error) {
	// Cached client has the OMENative Pod field index — useIndex=true takes
	// the index fast path instead of scanning every cached pod.
	pods, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, component, true)
	if err != nil {
		return nil, err
	}
	return query.BucketPodsByInstanceIdx(pods), nil
}

// ExtraInstanceIndices returns InstanceStatus indices the plan no
// longer covers — scale-down targets. Set-difference framing handles
// sparse indices from surge migration (index 7 is extra only when no
// InstancePlan covers it, regardless of replica count).
//
// includeMigrating=false (the normal scale-down pass) excludes
// mid-migration Instances — Phase=Migrating sources and
// Operation.Type=Migrate surges — so the pair isn't scale-down-deleted
// out from under Migrate. Teardown passes includeMigrating=true:
// everything must die, and since source and surge each carry their own
// InstanceStatus entry, both run the full Delete pipeline
// (patchInstanceStatusDeleting stamps Deleting over any phase).
func ExtraInstanceIndices(observed []InstanceStatus, plan ComponentPlan, includeMigrating bool) []int32 {
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range observed {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		if !includeMigrating {
			if s.Phase == InstancePhaseMigrating {
				continue
			}
			if s.Operation != nil && s.Operation.Type == InstanceOperationMigrate {
				continue
			}
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
