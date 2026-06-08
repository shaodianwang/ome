// Restart trigger detection for the omenative/ops package.
//
// The per-Instance Restart state machine itself lives in
// pkg/controller/v1beta1/workload/ops after the workload-extraction
// refactor; the ISVC dispatch path drives it via workload.Reconcile
// (see dispatch.go), NOT through this package. The former ops.Restart
// adapter shim was removed because it had zero callers — production
// dispatches through workload.Reconcile and the workload/ops Restart
// body is covered by workload/ops/{restart,surface_restart}_test.go
// (see .claude/specs/2026-06-08-omenative-workload-code-review.md §4).
//
// The Restart trigger detector (detectRestartTrigger) and its
// Migrate-suppression helper (isMigrateOwned) stay in this package
// because the dispatcher (dispatch.go) still consults them directly.
// They migrate to workload/ops alongside their callers in later slices.
package ops

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

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
	s := findInstanceStatusForISVC(p.ISVC, p.Component, inst.Index)
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

	allPods, err := query.ListOMENativePodsByName(ctx, p.Client, p.ISVC.Namespace, p.ISVC.Name, plan.Component)
	if err != nil {
		return false, "", err
	}
	pods := make([]*corev1.Pod, 0, len(allPods))
	for _, pod := range allPods {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == inst.Index {
			pods = append(pods, pod)
		}
	}
	expected := inst.TotalPods()

	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodFailed {
			// Richer reason from the failed pod's container termination so
			// the RestartTriggered event names the cause (OOMKilled exit
			// 137, CrashLoopBackOff, ...). Parity with the workload-driven
			// DetectRestartTrigger; the shared workloadops.Restart body
			// also preserves the full diagnostics into LastFailure.
			if t := workload.PodTermination(pod, metav1.Now()); t != nil {
				return true, t.ShortString(), nil
			}
			return true, fmt.Sprintf("pod %s Failed", pod.Name), nil
		}
	}
	if int32(len(pods)) < expected {
		return true, fmt.Sprintf("pod count %d below desired %d", len(pods), expected), nil
	}
	return false, "", nil
}

// findInstanceStatusForISVC returns the cached InstanceStatus entry for
// the given (component, idx) from the ISVC.Status, nil if absent. Inline
// helper here because the workload-side query package is v1beta1-free
// — adapters resolve their own typed status surface.
func findInstanceStatusForISVC(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.Lifecycle == nil {
		return nil
	}
	for i := range cs.Lifecycle.InstanceStatuses {
		if cs.Lifecycle.InstanceStatuses[i].Index == idx {
			return &cs.Lifecycle.InstanceStatuses[i]
		}
	}
	return nil
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
