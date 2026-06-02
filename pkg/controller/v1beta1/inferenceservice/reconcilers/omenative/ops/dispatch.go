// Package ops carries the ISVC-OMENative per-Component dispatch entry
// point ReconcileComponentOps. As of the workload.Reconcile slice the
// per-Instance dispatch loop itself lives in
// pkg/controller/v1beta1/workload; ReconcileComponentOps is the thin
// ISVC-adapter shim that:
//
//  1. Runs ExpireOperationsIfDeadlinePassed (v1beta1-typed, reads
//     ISVC.Status.Components.Lifecycle.InstanceStatuses.Operation
//     directly).
//
//  2. Builds workload.Deps + workload.ReconcileInput from the legacy
//     core.ReconcileParams via internalsource.BuildReconcileInput,
//     wiring the adapter-specific callbacks the workload dispatcher
//     calls back into:
//
//     - DetectMigrationRequest: the v1beta1-typed annotation +
//     audit-ledger scan that lives in migrate.go;
//     - UpdateGate: the omenative.coordination
//     CheckRatioGate / CheckSurgeGate / CheckUnavailabilityGate /
//     CheckSequentialGate stack.
//
//  3. Delegates the per-Instance dispatch loop to workload.Reconcile.
//
// All the per-op state machines (Create / Update / Delete / Restart /
// Migrate) live in workload/ops; this file is purely adapter glue.
package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
)

// ReconcileComponentOps drives the per-Component pipeline for one
// reconcile pass. Lifted out of the top-level ReconcileComponent so the
// deferred status write in the caller runs unconditionally, even on
// early returns from this path.
//
// As of the workload.Reconcile extraction the per-Instance dispatch
// loop has moved into workload.Reconcile — this function now collapses
// to:
//
//  1. ExpireOperationsIfDeadlinePassed (v1beta1-typed, stays here).
//  2. Build the workload.Deps + workload.ReconcileInput pair with the
//     ISVC-specific UpdateGate (coordination gates) and
//     DetectMigrationRequest (ISVC-annotation + ledger scan) wired
//     in.
//  3. Delegate to workload.Reconcile.
//
// The shrink replaces ~170 LOC of explicit per-op dispatch + gate
// inlining (Create / Delete / Restart / Migrate / Update branches plus
// the within-pass in-flight surge / unavail counter tracking and the 4
// coordination gate calls) with one workload.Reconcile call. The same
// dispatch order and same gate semantics; the work is now in workload
// where the future InferenceReplica controller can reuse it.
func ReconcileComponentOps(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if err := ExpireOperationsIfDeadlinePassed(ctx, p, plan); err != nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: expire timeouts (component=%s): %w", plan.Component, err)
	}

	// Self-trigger migration for Instances stuck in Phase=Creating /
	// Updating longer than MigrationPolicy.ProgressDeadlineSeconds.
	// Writes synthetic ledger entries (Reason=AutoRecover) so the
	// next-pass detectMigrationRequest picks them up via Source-2
	// (in-flight ledger) and drives surge-on-different-node recovery.
	// No-op when ProgressDeadlineSeconds is unset (opt-in).
	v1component := v1beta1convert.ComponentTypeFromWorkload(plan.Component)
	if err := detectStuckInstancesForAutoMigration(ctx, p, v1component); err != nil {
		return ctrl.Result{}, fmt.Errorf("OMENative reconciler: detect stuck instances for auto-migration (component=%s): %w", plan.Component, err)
	}

	deps := workload.Deps{
		Client:       p.Client,
		APIReader:    p.APIReader,
		Recorder:     p.Recorder,
		Expectations: p.ExpectationsCache(),
		RenderHook:   core.ISVCRenderHook(p.ISVC),
	}
	input := internalsource.BuildReconcileInput(internalsource.BuildInputDeps{
		ISVC:                    p.ISVC,
		Component:               p.Component,
		ObjectMeta:              p.ObjectMeta,
		PodSpec:                 p.PodSpec,
		WorkerPodSpec:           p.WorkerPodSpec,
		Client:                  p.Client,
		Expectations:            p.ExpectationsCache(),
		GangSchedulingAvailable: p.GangSchedulingAvailable,
		ClearMigrationRequestAnnotation: func(ctx context.Context, uuid string) error {
			return clearMigrationRequestAnnotation(ctx, p, uuid)
		},
	})

	// DetectMigrationRequest wires the legacy v1beta1-typed
	// detectMigrationRequest into the workload dispatcher. The body
	// stays in migrate.go because it reads ISVC annotations + the audit
	// ledger directly; workload code can't see either without taking
	// on a v1beta1 import or duplicating the ledger schema.
	//
	// workload.MigrationRequestSummary is a type alias for
	// audit.MigrationRequest so the pointer carries through unchanged.
	input.DetectMigrationRequest = func(ctx context.Context) (string, *workload.MigrationRequestSummary, int32, bool, error) {
		return detectMigrationRequest(ctx, p, v1component)
	}

	// UpdateGate wires the coordination stack — workload code can't
	// see omenative/coordination (the workload package's audit-grep
	// boundary), so the gate decisions cross via this callback.
	// Body lives in evaluateUpdateGate so the gate order + the
	// in-place RatioGate bypass have a single testable site.
	input.UpdateGate = func(strategy workload.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, string) {
		return evaluateUpdateGate(p.ISVC, v1component, p.Recorder, strategy, inFlightSurge, inFlightUnavail)
	}

	return workload.Reconcile(ctx, deps, input, workloadPlanFromCore(plan), target)
}

// evaluateUpdateGate is the ISVC-direct path's adapter onto the shared
// coordination.EvaluateUpdateGate decision site. The gate body — order
// (ratio -> surge/unavail -> sequential), the in-place RatioGate bypass,
// and the per-tick ResolveGateContext prelude — lives in the coordination
// package so the IR-managed path (inferencereplica), which is the
// production default, drives the byte-identical logic. See
// coordination.EvaluateUpdateGate.
//
// Kept as a named wrapper so the existing dispatch_test.go gate-order /
// in-place-bypass tests retain a stable entry point on this path.
func evaluateUpdateGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, recorder record.EventRecorder, strategy workload.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, string) {
	return coordination.EvaluateUpdateGate(isvc, component, recorder, strategy, inFlightSurge, inFlightUnavail)
}

// workloadPlanFromCore lifts core.ComponentPlan onto workload.ComponentPlan
// for the workload.Reconcile call. core.ComponentPlan is already a type
// alias of workload.ComponentPlan (see omenative/core/params.go), so this
// is a verbatim identity at the type level — kept as a named helper for
// readability.
func workloadPlanFromCore(plan core.ComponentPlan) workload.ComponentPlan {
	return plan
}
