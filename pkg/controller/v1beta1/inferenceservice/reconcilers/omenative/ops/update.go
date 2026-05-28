// Dispatch shim for Update — the per-Instance Update state machine
// itself (in-place / recreate / surge variants) lives in
// pkg/controller/v1beta1/workload/ops after the workload-extraction
// refactor (part 2 slice 4f of N). update.go was the BIGGEST op move
// and carried the SurgeThenDrain state machine; the workload-side body
// is byte-for-byte equivalent with types swapped from v1beta1 to
// workload-owned mirrors.
//
// The shim's only job is to build the workload.ReconcileInput +
// workload.Deps pair from the legacy core.ReconcileParams and delegate.
// Peer ops (restart.go, migrate.go, timeouts.go) still call into
// v1beta1-typed helpers that previously lived in this file — those are
// either kept here as thin wrappers (drainServiceForPod,
// patchInstanceStatusReadyOnRevision) or in legacy_create_helpers.go;
// they shrink out as each peer migrates in subsequent slices.
package ops

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// updateRequeueInterval is the wait between passes while an Update is
// in flight. Aliased to the workload-side constant so the dispatcher
// (dispatch.go) and the per-Instance state machine cannot drift.
const updateRequeueInterval = workloadops.UpdateRequeueInterval

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

// detectUpdateTrigger fires when the Instance is mid-update, or when
// Phase=Ready and a pod's PodSpec hashes to a non-target revision.
// Creating / Deleting / Restarting / Migrating are not interruptible.
// The Migrate-owned suppression is explicit because a just-promoted
// surge briefly carries RunningRevision=sourceRev while the post-
// promote scale-down pass runs — without the guard the spec-bump that
// triggered the migration would race the surge promote.
//
// Shim that wraps workloadops.DetectUpdateTrigger. Kept as a
// package-internal helper because the dispatcher (dispatch.go) calls
// it directly and dispatch.go has not migrated to workload/ops yet.
func detectUpdateTrigger(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	deps := workload.Deps{
		Client:       p.Client,
		APIReader:    p.APIReader,
		Recorder:     p.Recorder,
		Expectations: p.ExpectationsCache(),
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
	return workloadops.DetectUpdateTrigger(ctx, deps, input, workloadPlan, workloadInst, target, targetSpec)
}

// drainServiceForPod returns the per-revision *routed* Service name to
// gate drain against for pod. Kept as a v1beta1-typed wrapper for any
// remaining v1beta1 caller; main's restart.go / migrate.go have already
// migrated to workload/ops and call the typed workload helper directly,
// so this shim is currently call-site-orphaned. Retained until the
// dispatcher and any straggler v1beta1 path also migrate; remove in a
// follow-up sweep once the omenative/ops package is empty of state
// machine code.
//
//nolint:unused
func drainServiceForPod(isvcName string, component v1beta1.ComponentType, pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	hash := pod.Labels["ome.io/revision-hash"]
	if hash == "" {
		return ""
	}
	return coordination.PerRevisionServiceName(isvcName, component, hash)
}

// patchInstanceStatusReadyOnRevision idempotently moves to Phase=Ready
// with RunningRevision=rev; clears Operation + TargetRevision. Kept as
// a v1beta1-typed wrapper for any remaining v1beta1 caller; main's
// migrate.go has already migrated to workload/ops and uses the typed
// workload helper directly, so this shim is currently call-site-
// orphaned. Retained until the omenative/ops package fully empties out;
// remove in a follow-up sweep.
//
//nolint:unused
func patchInstanceStatusReadyOnRevision(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, rev string) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		return true
	})
}

// workloadInstancePlanFromCore projects the legacy core.InstancePlan
// onto the workload.InstancePlan shape that the workload-side state
// machines consume. Field-for-field copy; the runner sub-type also
// mirrors directly.
func workloadInstancePlanFromCore(inst core.InstancePlan) workload.InstancePlan {
	runners := make([]workload.RunnerPlan, len(inst.Runners))
	for i, r := range inst.Runners {
		runners[i] = workload.RunnerPlan{Name: r.Name, Size: r.Size}
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
