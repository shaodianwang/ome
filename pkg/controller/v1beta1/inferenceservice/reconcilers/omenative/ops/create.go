// Package ops carries the ISVC-OMENative per-operation entry points the
// dispatcher routes Update / Restart / Migrate / Create / Delete work
// to. This file is the dispatch shim for Create — the per-Instance
// state machine itself lives in pkg/controller/v1beta1/workload/ops
// after the workload-extraction refactor (part 2 slice 4b of N).
//
// The shim's only job is to build the workload.ReconcileInput +
// workload.Deps pair from the legacy core.ReconcileParams and delegate.
// Helpers needed by peer ops (delete.go, update.go, restart.go,
// migrate.go, timeouts.go) that previously lived in this file have
// moved to legacy_create_helpers.go where they keep the v1beta1-typed
// signatures the peers still consume; they shrink as each peer migrates
// in subsequent slices.
package ops

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// Create is the ISVC-adapter entry point. It projects the legacy
// core.ReconcileParams onto workload.Deps + workload.ReconcileInput
// and delegates to workloadops.Create — the source-agnostic body of
// the per-Instance Create state machine.
//
// Signature preserved for backwards compatibility with the dispatcher
// (dispatch.go calls Create(ctx, p, plan, target) directly). The IR
// controller will skip this wrapper and build workload.ReconcileInput
// from its own source types.
//
// Wires deps.RenderHook to core.ISVCRenderHook(p.ISVC) so
// coordination.InjectPeerEnv runs on every rendered pod — matches the
// legacy core.RenderWithRevisionForISVC behavior.
func Create(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	// Resolve the Expectations cache eagerly so a test that swaps
	// core.DefaultExpectations between reconciles observes the swap on
	// the workload side. p.ExpectationsCache() is the same accessor the
	// legacy peer ops use; routing through it keeps every dispatch
	// surface backed by one cache instance per reconcile.
	deps := workload.Deps{
		Client:       p.Client,
		APIReader:    p.APIReader,
		Recorder:     p.Recorder,
		Expectations: p.ExpectationsCache(),
		RenderHook:   core.ISVCRenderHook(p.ISVC),
	}
	input := internalsource.BuildReconcileInput(internalsource.BuildInputDeps{
		ISVC:          p.ISVC,
		Component:     p.Component,
		ObjectMeta:    p.ObjectMeta,
		PodSpec:       p.PodSpec,
		WorkerPodSpec: p.WorkerPodSpec,
		Client:        p.Client,
	})
	return workloadops.Create(ctx, deps, input, plan, target)
}
