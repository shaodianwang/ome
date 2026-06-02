// Dispatch shim for Update — the per-Instance Update state machine
// itself (in-place / recreate / surge variants) lives in
// pkg/controller/v1beta1/workload/ops after the workload-extraction
// refactor. update.go was the largest op move and carried the
// SurgeThenDrain state machine; the workload-side body is
// byte-for-byte equivalent with types swapped from v1beta1 to
// workload-owned mirrors.
//
// The shim's only job is to build the workload.ReconcileInput +
// workload.Deps pair from the legacy core.ReconcileParams and delegate.
// The remaining v1beta1-typed helpers used by callers still in this
// package live in legacy_create_helpers.go; they shrink out as each
// peer migrates in subsequent slices.
package ops

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// updateStepInPlace identifies the InPlace step a Type=Update operation
// uses while the in-place image patch is being driven. The omenative
// timeouts handler reads this constant to recognize in-flight in-place
// updates that need to fall back to recreate when their deadline elapses
// — without it the handler would have to either reach into workload/ops
// for the constant or hardcode the string.
const updateStepInPlace = workloadops.UpdateStepInPlace

// updateStepDrain identifies the recreate-drain step a Type=Update
// operation uses while the old pods are being drained ahead of the
// recreate-with-bumped-incarnation pass. Re-exported for the same reason
// as updateStepInPlace.
const updateStepDrain = workloadops.UpdateStepDrain

// updateStepSurge identifies the SurgeThenDrain entry step. Re-exported
// for the timeouts test that asserts a surge deadline escalates Phase
// to Failed (test uses the constant in assertions on the post-escalation
// Operation.Step).
const updateStepSurge = workloadops.UpdateStepSurge

// Update is the ISVC-adapter entry point. It projects the legacy
// core.ReconcileParams onto workload.Deps + workload.ReconcileInput
// and delegates to workloadops.Update — the source-agnostic body of
// the per-Instance Update state machine.
//
// Signature preserved for backwards compatibility with the dispatcher
// (dispatch.go calls Update(ctx, p, plan, inst, target, targetSpec)
// directly). The IR controller will skip this wrapper and build
// workload.ReconcileInput from its own source types.
//
// Wires deps.RenderHook to core.ISVCRenderHook(p.ISVC) so any re-render
// path in recreate / surge (Phase B / Phase 1 createMissingPods) runs
// coordination.InjectPeerEnv on every rendered pod — matches the
// legacy core.RenderWithRevisionForISVC behavior.
func Update(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
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
	workloadInst := workloadInstancePlanFromCore(inst)
	workloadPlan := workloadComponentPlanFromCore(plan)
	return workloadops.Update(ctx, deps, input, workloadPlan, workloadInst, target, targetSpec)
}

// workloadInstancePlanFromCore projects the legacy core.InstancePlan
// onto the workload.InstancePlan shape that the workload-side state
// machines consume. Field-for-field copy; the runner sub-type also
// mirrors directly.
func workloadInstancePlanFromCore(inst core.InstancePlan) workload.InstancePlan {
	runners := make([]workload.RunnerPlan, len(inst.Runners))
	for i, r := range inst.Runners {
		runners[i] = workload.RunnerPlan(r)
	}
	return workload.InstancePlan{
		Index:       inst.Index,
		Incarnation: inst.Incarnation,
		Runners:     runners,
	}
}

// workloadComponentPlanFromCore projects the legacy core.ComponentPlan
// onto the workload.ComponentPlan shape that the workload-side state
// machines consume. Only the fields the Update state machine reads are
// populated; the rest stay zero — workload code doesn't reach for them
// from this dispatch path.
func workloadComponentPlanFromCore(plan core.ComponentPlan) workload.ComponentPlan {
	instances := make([]workload.InstancePlan, len(plan.Instances))
	for i, inst := range plan.Instances {
		instances[i] = workloadInstancePlanFromCore(inst)
	}
	return workload.ComponentPlan{
		Component:            plan.Component,
		Replicas:             plan.Replicas,
		Instances:            instances,
		RestartPolicy:        plan.RestartPolicy,
		UpdateStrategy:       plan.UpdateStrategy,
		ReadyPolicy:          plan.ReadyPolicy,
		InstanceReadyTimeout: plan.InstanceReadyTimeout,
		MigrationMode:        plan.MigrationMode,
	}
}
