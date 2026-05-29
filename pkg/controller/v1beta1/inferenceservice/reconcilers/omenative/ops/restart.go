// Dispatch shim for Restart — the per-Instance Restart state machine
// itself lives in pkg/controller/v1beta1/workload/ops after the
// workload-extraction refactor (part 2 slice 4e of N).
//
// The shim's only job is to build the workload.ReconcileInput +
// workload.Deps pair from the legacy core.ReconcileParams and delegate.
// The Restart trigger detector (detectRestartTrigger) and its
// Migrate-suppression helper (isMigrateOwned) stay in this package
// because the dispatcher (dispatch.go) and the Update detector
// (update.go) still call them directly. They migrate to workload/ops
// alongside their callers in later slices.
package ops

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// Restart is the ISVC-adapter entry point for the per-Instance Restart
// state machine. It projects the legacy core.ReconcileParams onto
// workload.Deps + workload.ReconcileInput and delegates to
// workloadops.Restart — the source-agnostic body.
//
// Signature preserved for backwards compatibility with the dispatcher
// (dispatch.go calls Restart(ctx, p, plan, inst, reason) directly). The
// IR controller will skip this wrapper and build workload.ReconcileInput
// from its own source types.
//
// Wires deps.RenderHook to core.ISVCRenderHook(p.ISVC) so the Phase B
// recreate path runs coordination.InjectPeerEnv on every rendered pod —
// matches the legacy core.RenderWithRevisionForISVC behavior.
func Restart(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, reason string) (bool, error) {
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
	// core.InstancePlan and workload.InstancePlan are structurally
	// identical — both packages own their own type set after slices
	// 3+4. Convert field-for-field so the workload layer sees its own
	// type.
	winst := workload.InstancePlan{
		Index:       inst.Index,
		Incarnation: inst.Incarnation,
	}
	for _, r := range inst.Runners {
		winst.Runners = append(winst.Runners, workload.RunnerPlan(r))
	}
	if inst.MigrationOverlay != nil {
		winst.MigrationOverlay = &workload.MigrationOverlay{
			FromNode:        inst.MigrationOverlay.FromNode,
			HintTargetNodes: append([]string(nil), inst.MigrationOverlay.HintTargetNodes...),
		}
	}
	return workloadops.Restart(ctx, deps, input, plan, winst, reason)
}

// detectRestartTrigger fires when the Instance is mid-restart, or when
// Phase=Ready and a pod is Failed / the live pod count is below desired.
// Creating / Deleting are not interruptible — they finish first. A
// Migrate-owned Instance (source mid-drain, surge mid-create) is
// suppressed explicitly because Migrate's source-pod deletion will
// otherwise trip the "pod count below desired" trigger on the source.
//
// Kept here as a v1beta1-typed function because dispatch.go (in this
// package, still using v1beta1.InstanceRestartPolicy on the plan)
// calls it directly. Migrates to workload/ops alongside the dispatcher.
func detectRestartTrigger(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan) (bool, string, error) {
	s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index)
	if s == nil {
		return false, "", nil
	}
	if isMigrateOwned(s) {
		return false, "", nil
	}
	if s.Phase == v1beta1.OMENativeInstanceRestarting {
		return true, "", nil
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		return false, "", nil
	}

	pods, err := query.ListPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, "", err
	}
	expected := inst.TotalPods()

	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodFailed {
			return true, fmt.Sprintf("pod %s Failed", pod.Name), nil
		}
	}
	if int32(len(pods)) < expected {
		return true, fmt.Sprintf("pod count %d below desired %d", len(pods), expected), nil
	}
	return false, "", nil
}

// isMigrateOwned reports whether Migrate is the active owner of this
// Instance's lifecycle. Source pods are Phase=Migrating; surge pods
// during create carry Operation.Type=Migrate even while Phase=Creating.
// Detectors (restart, update) consult this to keep their guards explicit
// rather than relying on the incidental "Phase != Ready" skip.
//
// Kept here as a v1beta1-typed function because the Update detector
// in update.go (peer file in this package) still calls it directly.
// Migrates to workload/ops alongside Update.
func isMigrateOwned(s *v1beta1.OMENativeInstanceStatus) bool {
	if s == nil {
		return false
	}
	if s.Phase == v1beta1.OMENativeInstanceMigrating {
		return true
	}
	return s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationMigrate
}
