// Dispatch shim for Delete — the per-Instance Delete state machine
// itself lives in pkg/controller/v1beta1/workload/ops after the
// workload-extraction refactor (part 2 slice 4c of N).
//
// The shim's only job is to build the workload.ReconcileInput +
// workload.Deps pair from the legacy core.ReconcileParams and delegate.
// Helpers needed by peer ops (update.go, restart.go, migrate.go,
// timeouts.go) that previously lived in this file have moved to
// legacy_create_helpers.go where they keep the v1beta1-typed
// signatures the peers still consume; they shrink as each peer migrates
// in subsequent slices.
package ops

import (
	"context"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// Delete is the ISVC-adapter entry point. It projects the legacy
// core.ReconcileParams onto workload.Deps + workload.ReconcileInput
// and delegates to workloadops.Delete — the source-agnostic body of
// the per-Instance Delete state machine.
//
// Signature preserved for backwards compatibility with the dispatcher
// (dispatch.go calls Delete(ctx, p, plan, idx) directly). The IR
// controller will skip this wrapper and build workload.ReconcileInput
// from its own source types.
//
// Wires deps.RenderHook to core.ISVCRenderHook(p.ISVC) so any future
// re-render path in Delete (none today, but kept consistent with the
// Create shim) runs coordination.InjectPeerEnv on every rendered pod.
func Delete(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, idx int32) (bool, error) {
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
		Expectations:  p.ExpectationsCache(),
	})
	return workloadops.Delete(ctx, deps, input, plan, idx)
}
