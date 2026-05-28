package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
)

// drainServiceForPod returns the per-revision *routed* Service name to
// gate drain against for pod. Read from the pod's ome.io/revision-hash
// label (stamped by Render). Empty string when the label is missing —
// caller treats that as "no routed Service to check" and proceeds
// straight to delete.
//
// We deliberately do NOT use the Component's headless Service here.
// That Service sets PublishNotReadyAddresses=true so peer-discovery DNS
// resolves before pods are Ready; kube-proxy then publishes the
// endpoint with Conditions.Ready=true regardless of the pod's actual
// Ready state, which would make drain.IsPodDrained wait forever. The
// per-revision routed Service (created by the coordination layer with
// PublishNotReadyAddresses=false) reflects the controller-owned
// ome.io/serving gate correctly.
func drainServiceForPod(isvcName string, component v1beta1.ComponentType, pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" {
		return ""
	}
	return coordination.PerRevisionServiceName(isvcName, component, hash)
}

// updateDrainKey identifies an in-place or recreate Update writer
// against a specific Instance materialization. Same key on Add and
// Remove so an in-place flow ends with the gate flipping back True.
func updateDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// updateRequeueInterval is the wait between passes while an Update is
// in flight.
const updateRequeueInterval = 5 * time.Second

// Update drives one Instance toward the target ControllerRevision. Mode
// is chosen per Instance:
//
//   - RecreatePod: always drain + delete + recreate at bumped Incarnation.
//   - InPlaceIfPossible: image-patch when the diff is container images
//     only; otherwise fall through to recreate.
//   - InPlaceOnly: image-patch when eligible; error otherwise.
//
// done=true once Phase=Ready with RunningRevision=target.Name.
func Update(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Update: nil client")
	}
	if target == nil || targetSpec == nil {
		return false, fmt.Errorf("Update: nil target revision or spec (instance=%d)", inst.Index)
	}

	pods, err := query.ListPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Update: list pods (instance=%d): %w", inst.Index, err)
	}

	// Eligibility compares against the recorded running revision, NOT the
	// live pod. Live pods carry apiserver-defaulted fields and Render
	// overlays (hostname/subdomain/serving gate); byte-comparing those
	// against the freshly-rendered target would always declare ineligible.
	// The recorded revision's PodSpec is the pre-defaulted canonical form
	// the target hashes against.
	runningSpec, err := loadRunningRevisionPodSpec(ctx, p.Reader(), p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Update: load running revision (instance=%d): %w", inst.Index, err)
	}

	multiPod := inst.TotalPods() > 1
	mode, err := chooseUpdateModeForInstance(plan.UpdateStrategy.Type, runningSpec, targetSpec, multiPod)
	if err != nil {
		// InPlaceOnly + ineligible diff is the only error path here.
		status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonInPlaceUpdateNotPossible,
			"OMENative %s rejected update: %v",
			status.InstanceKey(plan.Component, inst.Index), err)
		return false, fmt.Errorf("choose update mode (instance=%d): %w", inst.Index, err)
	}
	// Surfaces the fallback once per dispatch so operators can correlate a
	// recreate-flavored rollout with the SurgeThenDrain spec they wrote.
	// Multi-pod SurgeThenDrain needs per-gang-member ordinal allocation
	// (a separate OEP amendment, listed as Phase 5 / "future" in
	// the OMENative multi-node plan); until that lands, recreate is the
	// safe execution. The spec field stays "SurgeThenDrain" — when the
	// real implementation lands, this fallback goes away with zero
	// user-facing churn.
	if multiPod && plan.UpdateStrategy.Type == v1beta1.OMENativeUpdateStrategySurgeThenDrain && mode == updateModeRecreate {
		status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonRecreateUpdateStarted,
			"OMENative %s: multi-pod SurgeThenDrain falls back to recreate (per-gang-member surge deferred)",
			status.InstanceKey(plan.Component, inst.Index))
	}

	// Status stamping is per-mode: recreate bumps Incarnation atomically
	// with the Updating transition; in-place must NOT bump (same
	// materialization); surge advances ActiveOrdinal on promote. A
	// generic Updating write here would let recreate's bump branch
	// idempotency-skip on the second pass.
	switch mode {
	case updateModeInPlace:
		return inPlaceUpdate(ctx, p, plan, inst, target, targetSpec, pods)
	case updateModeRecreate:
		return recreateUpdate(ctx, p, plan, inst, target, pods)
	case updateModeSurge:
		return surgeUpdate(ctx, p, plan, inst, target, pods)
	}
	return false, fmt.Errorf("update mode unknown (instance=%d)", inst.Index)
}

// updateMode is the resolved per-Instance dispatch decision.
type updateMode int

const (
	updateModeRecreate updateMode = iota
	updateModeInPlace
	// updateModeSurge runs the SurgeThenDrain rollout — create a new
	// pod at the other ordinal slot, wait Ready, drain + delete the
	// old. Zero-downtime per Instance.
	updateModeSurge
)

// chooseUpdateModeForInstance is chooseUpdateMode with the multi-pod
// fallback layered on top.
//
// Multi-pod adjustments:
//   - SurgeThenDrain → recreate, because the per-gang-member surge
//     ordinal scheme is not yet implemented (deferred per OEP-0007 §
//     "Multi-pod SurgeThenDrain is a separate phase"). The defaulter
//     still stamps SurgeThenDrain on the user's spec; this is the
//     execution-side adapter that picks the safe mode at dispatch time.
//   - In-place modes (InPlaceIfPossible / InPlaceOnly) → recreate when
//     multi-pod, because inPlaceEligible compares only the leader's
//     PodSpec. A worker-only image bump would otherwise produce an
//     in-place rollout that patches the leader (no-op for the worker
//     image change) and leaves workers on the old image — split-brain.
//     The revision hash includes WorkerPodSpec (see HashWithWorker), so
//     the trigger fires; the chooser routes to recreate so every member
//     of the gang is replaced.
//
// RecreatePod is already recreate; multiPod has no additional effect.
func chooseUpdateModeForInstance(strategy v1beta1.OMENativeUpdateStrategyType, running, target *corev1.PodSpec, multiPod bool) (updateMode, error) {
	mode, err := chooseUpdateMode(strategy, running, target)
	if err != nil {
		// InPlaceOnly + ineligible diff is the only error path. For
		// multi-pod we fall back to recreate even on this error: the
		// in-place pipeline cannot safely roll a worker-only change, so
		// even if the user pinned InPlaceOnly, the realistic execution
		// is recreate. Without this branch a multi-pod InPlaceOnly diff
		// that is image-only-on-leader-and-something-else-on-worker would
		// fail loudly when the more useful behavior is to roll the whole
		// gang. Single-pod keeps the strict semantic — surface the error.
		if multiPod && strategy == v1beta1.OMENativeUpdateStrategyInPlaceOnly {
			return updateModeRecreate, nil
		}
		return mode, err
	}
	if multiPod && (mode == updateModeSurge || mode == updateModeInPlace) {
		return updateModeRecreate, nil
	}
	return mode, nil
}

// chooseUpdateMode resolves strategy + eligibility into a mode. nil
// running spec is treated as ineligible, forcing recreate from a known
// baseline. InPlaceOnly + ineligible returns an error.
//
// Pod-count-agnostic; callers in production should route through
// chooseUpdateModeForInstance which layers the multi-pod surge fallback
// on top. The bare chooser is preserved for tests and direct callers
// that don't have an Instance in scope.
func chooseUpdateMode(strategy v1beta1.OMENativeUpdateStrategyType, running, target *corev1.PodSpec) (updateMode, error) {
	eligible := inPlaceEligible(running, target)
	switch strategy {
	case v1beta1.OMENativeUpdateStrategySurgeThenDrain:
		return updateModeSurge, nil
	case v1beta1.OMENativeUpdateStrategyRecreatePod:
		return updateModeRecreate, nil
	case v1beta1.OMENativeUpdateStrategyInPlaceOnly:
		if !eligible {
			return 0, fmt.Errorf("InPlaceOnly strategy but diff exceeds container images")
		}
		return updateModeInPlace, nil
	case v1beta1.OMENativeUpdateStrategyInPlaceIfPossible:
		if eligible {
			return updateModeInPlace, nil
		}
		return updateModeRecreate, nil
	case "":
		// updateStrategyWithDefaults should have filled this in
		// upstream; the empty-string branch matches the SurgeThenDrain
		// default so a direct caller (tests, fuzz) gets safe behavior
		// even if it skips the defaulter.
		return updateModeSurge, nil
	default:
		return 0, fmt.Errorf("unknown UpdateStrategy.Type %q", strategy)
	}
}

// inPlaceEligible reports whether the diff is regular-container images
// only. Both sides are stripped of regular-container images then
// byte-compared; init images are preserved so an init-image diff routes
// to recreate (kubelet can't re-run init containers after an image patch).
// nil on either side returns false — can't prove image-only.
func inPlaceEligible(running, target *corev1.PodSpec) bool {
	if running == nil || target == nil {
		return false
	}
	runningStripped, err := podSpecWithoutImages(running)
	if err != nil {
		return false
	}
	targetStripped, err := podSpecWithoutImages(target)
	if err != nil {
		return false
	}
	return string(runningStripped) == string(targetStripped)
}

// loadRunningRevisionPodSpec returns the PodSpec from the CR named by
// InstanceStatus.RunningRevision. Returns (nil, nil) when no status
// exists, no RunningRevision is recorded, or the CR is gone — callers
// treat that as no baseline and route to recreate.
func loadRunningRevisionPodSpec(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) (*corev1.PodSpec, error) {
	payload, err := loadRunningRevisionPayload(ctx, reads, isvc, component, idx)
	if err != nil || payload == nil {
		return nil, err
	}
	return payload.PodSpec, nil
}

// loadRunningRevisionPayload is loadRunningRevisionPodSpec's superset
// that returns the full revision.DataPayload (PodSpec + PodMeta +
// WorkerPodSpec). Used by inPlaceUpdate's annotation-reconciliation
// pass to compute the diff between the running PodMeta (annotations
// owned by the previous revision) and the target PodMeta (annotations
// the user just edited). Returns (nil, nil) on the same not-found /
// no-status edge cases as loadRunningRevisionPodSpec.
func loadRunningRevisionPayload(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) (*revision.DataPayload, error) {
	s := query.FindInstanceStatus(isvc, component, idx)
	if s == nil || s.RunningRevision == "" {
		return nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: s.RunningRevision}
	if err := reads.Get(ctx, key, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get running CR %s: %w", s.RunningRevision, err)
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal running CR %s data: %w", s.RunningRevision, err)
	}
	return &payload, nil
}

// loadControllerRevisionPayload unmarshals a CR's raw data into a
// revision.DataPayload. The CR is the source of truth for the
// per-revision pod-template metadata; in-place rollouts read PodMeta
// from here to reconcile the live pod's annotations against the new
// revision. Returns (nil, nil) when the CR is nil or its data is
// empty so callers can fall through without erroring on benign
// edge cases (e.g., a freshly-created CR observed mid-write).
func loadControllerRevisionPayload(cr *appsv1.ControllerRevision) (*revision.DataPayload, error) {
	if cr == nil || len(cr.Data.Raw) == 0 {
		return nil, nil
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal CR %s data: %w", cr.Name, err)
	}
	return &payload, nil
}

// podSpecWithoutImages returns the JSON form with regular-container
// images stripped; init-container images stay so an init-image-only diff
// fails inPlaceEligible and routes to recreate.
func podSpecWithoutImages(spec *corev1.PodSpec) ([]byte, error) {
	clone := spec.DeepCopy()
	for i := range clone.Containers {
		clone.Containers[i].Image = ""
	}
	return json.Marshal(clone)
}

// inPlaceUpdate runs the in-place rollout: drain (optional), patch
// container images, propagate pod-template annotations from the
// target revision, wait for runtime ready on the target image, flip
// serving back, promote Ready. Multi-pass; idempotent.
//
// Annotation propagation (Bug #51): the previous behavior patched
// only container images, leaving pod.metadata.annotations stuck at
// the value the pod was created with. Operators editing
// spec.{engine,decoder,router}.annotations during an
// in-place-eligible rollout saw the new annotation surface on the
// ControllerRevision but never on the live Pod, breaking observability
// hints (linkerd.io/inject, prometheus.io/* etc.) that users
// reasonably expect to apply without bouncing pods.
//
// Design choice (a): in-place rolls now reconcile pod annotations
// against the new revision's PodMeta. Add/update keys present in the
// new spec, delete keys the user removed (scoped to keys the previous
// revision once authored — third-party annotations stay), leave
// foreign keys alone. (b) "force recreate on any annotation change"
// was considered and rejected: annotations are not load-bearing for
// runtime image semantics and the recreate would defeat the
// zero-downtime contract operators expect from
// InPlaceIfPossible / InPlaceOnly.
func inPlaceUpdate(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, pods []*corev1.Pod) (bool, error) {
	// In-place keeps the same Incarnation — only the container image rolls.
	wasNotUpdating := true
	if s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil && s.Phase == v1beta1.OMENativeInstanceUpdating {
		wasNotUpdating = false
	}
	if err := patchInstanceStatusUpdating(ctx, p, plan.Component, inst.Index, target.Name, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Updating (instance=%d): %w", inst.Index, err)
	}
	if wasNotUpdating {
		status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonInPlaceUpdateStarted,
			"OMENative %s in-place update to revision %s",
			status.InstanceKey(plan.Component, inst.Index), target.Name)
	}

	markNotReady := true
	if plan.UpdateStrategy.InPlaceUpdateStrategy != nil &&
		plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle != nil {
		markNotReady = *plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle
	}

	// Drain first when the strategy requires it.
	if markNotReady {
		for _, pod := range pods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, p.Client, pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live reader on drain check so kube-proxy isn't still routing.
		for _, pod := range pods {
			serviceName := drainServiceForPod(p.ISVC.Name, plan.Component, pod)
			if serviceName == "" {
				continue
			}
			drained, err := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
	}

	// Pre-load the target + running revision metadata once, before the
	// per-pod patch loop. The CR's PodMeta is the source of truth for the
	// annotation set the new revision authored; the running revision's
	// PodMeta tells us which keys the previous spec owned (so we can
	// delete the ones the user just removed without clobbering keys set
	// by other controllers / pod-mutating webhooks). Both can be nil on
	// edge cases (CR with no PodMeta yet, first in-place update with no
	// recorded running CR) — annotationsDiff handles nil-as-empty.
	targetPayload, err := loadControllerRevisionPayload(target)
	if err != nil {
		return false, fmt.Errorf("load target CR data (instance=%d): %w", inst.Index, err)
	}
	runningPayload, err := loadRunningRevisionPayload(ctx, p.Reader(), p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("load running CR data (instance=%d): %w", inst.Index, err)
	}
	var targetAnnotations, previousAnnotations map[string]string
	if targetPayload != nil && targetPayload.PodMeta != nil {
		targetAnnotations = targetPayload.PodMeta.Annotations
	}
	if runningPayload != nil && runningPayload.PodMeta != nil {
		previousAnnotations = runningPayload.PodMeta.Annotations
	}

	// Image patch. When markNotReady=true, re-read each pod and re-check
	// podreadiness.IsServing immediately before patching — a concurrent writer (another
	// controller, a pod-mutating webhook re-injecting a sidecar) could flip
	// serving back to True after the drain check passed, and patching a
	// serving pod violates the always-drain-first invariant. On detection,
	// re-mark NotServing and requeue. Live reader so a stale serving=False
	// view doesn't fail the recheck open.
	//
	// The annotation patch piggybacks on this loop: per-pod, after a
	// successful (or no-op) image patch, reconcile annotations toward
	// the target. Annotations apply immediately (no kubelet roll needed),
	// so we don't need to gate convergence on them — but they MUST land
	// before we mark Ready or operators see an out-of-date pod.
	patched := false
	for _, pod := range pods {
		needsImagePatch := !podImagesMatch(pod, targetSpec)
		if markNotReady && needsImagePatch {
			fresh := &corev1.Pod{}
			if gerr := p.Reader().Get(ctx, client.ObjectKeyFromObject(pod), fresh); gerr != nil {
				if apierrors.IsNotFound(gerr) {
					continue
				}
				return false, fmt.Errorf("re-read pod for serving recheck (instance=%d, pod=%s): %w", inst.Index, pod.Name, gerr)
			}
			if podreadiness.IsServing(fresh) {
				if err := podreadiness.MarkPodNotServing(ctx, p.Client, fresh, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
					return false, fmt.Errorf("re-mark not serving (instance=%d, pod=%s): %w", inst.Index, fresh.Name, err)
				}
				return false, nil
			}
			pod = fresh
		}
		if needsImagePatch {
			if err := patchPodImages(ctx, p.Client, pod, targetSpec); err != nil {
				return false, fmt.Errorf("patch images (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			patched = true
		}
		if err := patchPodAnnotations(ctx, p.Client, pod, previousAnnotations, targetAnnotations); err != nil {
			return false, fmt.Errorf("patch annotations (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}
	if patched {
		// Kubelet needs another pass to roll the containers.
		return false, nil
	}

	// Convergence: ContainersReady alone isn't enough — the cache can
	// still show the old condition immediately after a spec.image flip.
	// Also require Status.ContainerStatuses[*].Image to match so we
	// declare done only after kubelet actually rolled the container.
	if !query.AllPodsRuntimeReady(pods) {
		return false, nil
	}
	for _, pod := range pods {
		if !podImagesMatch(pod, targetSpec) {
			return false, nil
		}
		if !podRuntimeImagesMatch(pod, targetSpec) {
			return false, nil
		}
	}

	for _, pod := range pods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, p.Client, pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	if err := patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonInPlaceUpdateCompleted,
		"OMENative %s in-place update to revision %s complete",
		status.InstanceKey(plan.Component, inst.Index), target.Name)
	return true, nil
}

// recreateUpdate runs the recreate rollout: drain + delete every
// current-incarnation pod, recreate at the bumped Incarnation. Shares
// the drain/delete/recreate skeleton with Restart but pivots on
// template-change rather than pod-loss. Multi-pass; idempotent.
func recreateUpdate(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, error) {
	// "First pass of recreate" = no in-flight Update or Step != Drain;
	// the recreate stamp sets Step=Drain.
	wasNotRecreating := true
	if s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil &&
		s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationUpdate &&
		s.Operation.Step == updateStepDrain {
		wasNotRecreating = false
	}
	newInc, err := patchInstanceStatusRecreatingForUpdate(ctx, p, plan.Component, inst.Index, target.Name, plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("bump Incarnation (instance=%d): %w", inst.Index, err)
	}
	if wasNotRecreating {
		status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonRecreateUpdateStarted,
			"OMENative %s recreate to revision %s (incarnation=%d)",
			status.InstanceKey(plan.Component, inst.Index), target.Name, newInc)
	}

	oldPods, newPods, unknownPods := query.PartitionPodsByIncarnation(pods, newInc)

	// Refuse to proceed on orphan pods (label-missing). Without this guard
	// a stripped label or third-party reuse of managed-by would get torn
	// down by Phase A. Same rationale as Restart.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to recreate-update",
				status.InstanceKey(plan.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	// Phase A: drain + delete old pods.
	if len(oldPods) > 0 {
		for _, pod := range oldPods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, p.Client, pod, podreadiness.WriterUpdateRecreateDrain, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live reader on drain check so kube-proxy isn't still routing.
		for _, pod := range oldPods {
			serviceName := drainServiceForPod(p.ISVC.Name, plan.Component, pod)
			if serviceName == "" {
				continue
			}
			drained, err := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			p.ExpectationsCache().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index, 1)
			if err := p.Client.Delete(ctx, pod); err != nil {
				p.ExpectationsCache().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase B: recreate at the bumped Incarnation.
	// Live-read the OLD set first — a stable-name Create with the cache
	// stale would AlreadyExists against a still-terminating previous
	// incarnation. Matches K8s foreground-deletion propagation.
	clear, err := query.LiveOldPodsClearedForRecreate(ctx, p.Reader(), p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("recreateUpdate: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(p.ISVC, plan.Component, newInst)
	existingByName := query.IndexPodsByName(newPods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index) {
			return false, nil
		}
		if _, err := createMissingPods(ctx, p, plan, newInst, inst.Index, missing, revisionHashFromTarget(target)); err != nil {
			return false, err
		}
		return false, nil
	}

	// Phase C: flip serving, promote Ready+RunningRevision.
	if !query.AllPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		// Lifecycle key on fresh pods; the Update-recreate-drain key only
		// lived on the now-deleted old pods.
		if err := podreadiness.MarkPodServing(ctx, p.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}
	if err := patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonRecreateUpdateCompleted,
		"OMENative %s recreate to revision %s complete",
		status.InstanceKey(plan.Component, inst.Index), target.Name)
	return true, nil
}

// detectUpdateTrigger fires when the Instance is mid-update, or when
// Phase=Ready and a pod's PodSpec hashes to a non-target revision.
// Creating / Deleting / Restarting / Migrating are not interruptible.
// The Migrate-owned suppression is explicit because a just-promoted
// surge briefly carries RunningRevision=sourceRev while the post-
// promote scale-down pass runs — without the guard the spec-bump that
// triggered the migration would race the surge promote.
func detectUpdateTrigger(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index)
	if s == nil {
		return false, nil
	}
	if isMigrateOwned(s) {
		return false, nil
	}
	if s.Phase == v1beta1.OMENativeInstanceUpdating {
		return true, nil
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		return false, nil
	}

	// RunningRevision is the cheap fast-path comparison.
	if s.RunningRevision != "" && s.RunningRevision != target.Name {
		return true, nil
	}
	if s.RunningRevision == target.Name {
		return false, nil
	}
	// Empty RunningRevision — fall back to per-pod diff and backfill.
	pods, err := query.ListPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, err
	}
	for _, pod := range pods {
		if !podMatchesTarget(pod, targetSpec) {
			return true, nil
		}
	}
	// Backfill so future reconciles take the fast-path.
	if err := patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("backfill RunningRevision (instance=%d): %w", inst.Index, err)
	}
	return false, nil
}

// podMatchesTarget compares pod's runtime PodSpec to target — same
// image-stripped JSON AND matching container images. Used when
// RunningRevision is missing.
func podMatchesTarget(pod *corev1.Pod, target *corev1.PodSpec) bool {
	if pod == nil || target == nil {
		return false
	}
	gotStripped, err := podSpecWithoutImages(&pod.Spec)
	if err != nil {
		return false
	}
	wantStripped, err := podSpecWithoutImages(target)
	if err != nil {
		return false
	}
	if string(gotStripped) != string(wantStripped) {
		return false
	}
	return podImagesMatch(pod, target)
}

// canonicalImage normalizes a container image reference for the
// runtime-vs-spec equality compare. The runtime (containerd, Docker)
// stamps Status.ContainerStatuses[*].Image with the fully-qualified
// form `docker.io/library/<name>:<tag>` for implicit-registry tags,
// while spec.containers[].image stays as the user wrote it
// (`<name>:<tag>`). String-equality on the two would always return
// false even after kubelet has fully rolled the container, blocking
// in-place updates forever on any image the user tagged without an
// explicit registry. Strip the default Docker-Hub-library prefix so
// `fake-serving:v1` and `docker.io/library/fake-serving:v1` compare
// equal. Images with an explicit registry round-trip unchanged.
func canonicalImage(img string) string {
	const dockerHubLibraryPrefix = "docker.io/library/"
	if len(img) > len(dockerHubLibraryPrefix) && img[:len(dockerHubLibraryPrefix)] == dockerHubLibraryPrefix {
		return img[len(dockerHubLibraryPrefix):]
	}
	return img
}

// podRuntimeImagesMatch is the runtime-truth signal complementing
// podImagesMatch's spec check: spec.image flips immediately on patch but
// kubelet may not have rolled the container yet, leaving the old image
// in Status.ContainerStatuses[*].Image. Only after both match has the
// in-place update actually taken effect.
func podRuntimeImagesMatch(pod *corev1.Pod, target *corev1.PodSpec) bool {
	if pod == nil || target == nil {
		return false
	}
	want := make(map[string]string, len(target.Containers))
	for _, c := range target.Containers {
		want[c.Name] = canonicalImage(c.Image)
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if want[cs.Name] != canonicalImage(cs.Image) {
			return false
		}
	}
	// Need a status for every target container before declaring done.
	return len(pod.Status.ContainerStatuses) >= len(target.Containers)
}

// podImagesMatch returns true when every container in pod has the same
// .Image as the same-named container in target. Pod containers absent
// from target fail the match.
func podImagesMatch(pod *corev1.Pod, target *corev1.PodSpec) bool {
	if pod == nil || target == nil {
		return false
	}
	want := make(map[string]string, len(target.Containers))
	for _, c := range target.Containers {
		want[c.Name] = c.Image
	}
	for _, c := range pod.Spec.Containers {
		if want[c.Name] != c.Image {
			return false
		}
	}
	return true
}

// patchPodImages strategic-merge-patches each changed container image.
// Strategic-merge keys by container name so the patch leaves other
// containers and apiserver-defaulted fields untouched; a plain Update
// would conflict with any other controller writing to the same RV.
func patchPodImages(ctx context.Context, c client.Client, pod *corev1.Pod, target *corev1.PodSpec) error {
	if pod == nil || target == nil {
		return fmt.Errorf("patchPodImages: nil pod or target")
	}
	wantByName := make(map[string]string, len(target.Containers))
	for _, t := range target.Containers {
		wantByName[t.Name] = t.Image
	}
	// Build the strategic-merge patch payload: only the containers
	// whose image differs from current state appear in the patch.
	var containersPatch []map[string]string
	for _, current := range pod.Spec.Containers {
		newImage, ok := wantByName[current.Name]
		if !ok || newImage == current.Image {
			continue
		}
		containersPatch = append(containersPatch, map[string]string{
			"name":  current.Name,
			"image": newImage,
		})
	}
	if len(containersPatch) == 0 {
		return nil
	}
	raw, err := json.Marshal(map[string]any{
		"spec": map[string]any{"containers": containersPatch},
	})
	if err != nil {
		return fmt.Errorf("marshal image patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return fmt.Errorf("patch pod %s/%s images: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// annotationsDiff computes the patch values that reconcile a pod's
// metadata.annotations toward the target revision's PodMeta:
//
//   - keys in target that are missing or have a different value on pod
//     map to their target value (add/update).
//   - keys that existed on the previous revision's PodMeta but are gone
//     from target map to nil (delete via strategic-merge JSON null).
//   - keys on pod that are foreign to both previous and target (sidecar
//     webhook injections, CNI attachments, kubelet-stamped values) are
//     left alone — they are not OMENative-owned.
//
// Returns an empty map when nothing needs to change. Order of map
// iteration doesn't affect the patch payload.
//
// Why we don't blindly replace the whole annotation set: third-party
// controllers (linkerd, istio, the Pod mutator webhook) routinely add
// annotations that aren't on any ControllerRevision. Erasing them on
// every in-place pass would break those integrations on every spec
// edit. The previous-CR diff lets OMENative remove ONLY keys it once
// authored that the user has since deleted from spec.
func annotationsDiff(podAnnotations, previousTargetAnnotations, newTargetAnnotations map[string]string) map[string]any {
	out := map[string]any{}
	// Add/update: every key in the new target whose pod value differs.
	for k, want := range newTargetAnnotations {
		if got, ok := podAnnotations[k]; !ok || got != want {
			out[k] = want
		}
	}
	// Delete: keys that were OMENative-owned in the previous revision
	// but the user removed from spec. Skip keys that already-don't-exist
	// on the pod (no need to issue a delete for a no-op) and skip keys
	// still present in the new target (they were handled by the
	// add/update loop above).
	for k := range previousTargetAnnotations {
		if _, stillTarget := newTargetAnnotations[k]; stillTarget {
			continue
		}
		if _, onPod := podAnnotations[k]; !onPod {
			continue
		}
		out[k] = nil
	}
	return out
}

// patchPodAnnotations applies a strategic-merge patch on the pod's
// metadata.annotations to reconcile it toward the target revision's
// PodMeta. previousTargetAnnotations is the running revision's PodMeta
// (used to compute which keys OMENative owns and is allowed to
// delete). Idempotent — no patch is issued when the diff is empty
// (closes Bug #51: in-place updates previously left
// pod.metadata.annotations stuck at the value the pod was created with,
// so spec.{component}.annotations edits during an in-place-eligible
// rollout never reached the pod).
func patchPodAnnotations(ctx context.Context, c client.Client, pod *corev1.Pod, previousTargetAnnotations, newTargetAnnotations map[string]string) error {
	if pod == nil {
		return fmt.Errorf("patchPodAnnotations: nil pod")
	}
	diff := annotationsDiff(pod.Annotations, previousTargetAnnotations, newTargetAnnotations)
	if len(diff) == 0 {
		return nil
	}
	raw, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": diff},
	})
	if err != nil {
		return fmt.Errorf("marshal annotation patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return fmt.Errorf("patch pod %s/%s annotations: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// updateStep distinguishes which mode wrote the current Operation. Without
// it, a strategy change mid-rollout (InPlaceIfPossible → RecreatePod)
// would let recreate's idempotency check match the in-place state, skip
// the Incarnation bump, classify every pod as fresh, and never recreate.
const (
	updateStepInPlace = "InPlace"
	updateStepDrain   = "Drain"
	// updateStepSurge is the SurgeThenDrain entry step: surge pod
	// being created at the other ordinal slot. Transitions to
	// updateStepSurgeDrain once the surge pod is Ready and the old
	// pod's serving gate is being flipped.
	updateStepSurge = "Surge"
	// updateStepSurgeDrain is the SurgeThenDrain drain phase. Distinct
	// from updateStepDrain so the dispatcher's CheckSurgeGate can
	// recognize in-flight surge operations by step name alone (the
	// surge contributes an extra pod alive — the unavailability gate
	// doesn't apply; the surge gate does).
	updateStepSurgeDrain = "SurgeDrain"
)

// patchInstanceStatusUpdating idempotently stamps Phase=Updating with the
// Update operation in Step=InPlace at targetRev. Skip-write requires
// Step==InPlace so a previous recreate-step write doesn't short-circuit.
func patchInstanceStatusUpdating(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceUpdating &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationUpdate &&
			s.Operation.Step == updateStepInPlace &&
			s.TargetRevision == targetRev {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceUpdating
		s.TargetRevision = targetRev
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           updateStepInPlace,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusRecreatingForUpdate is the recreate entry point.
// Bumps Incarnation (like Restart) and stamps Phase=Updating with
// Step=Drain. Returns the post-write Incarnation. The idempotency guard
// requires Step==Drain so a prior in-place pass at the same target
// doesn't block the bump.
func patchInstanceStatusRecreatingForUpdate(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, targetRev string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceUpdating &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationUpdate &&
			s.Operation.Step == updateStepDrain &&
			s.TargetRevision == targetRev && s.Incarnation > 0 {
			observedIncarnation = s.Incarnation
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		observedIncarnation = s.Incarnation
		s.Phase = v1beta1.OMENativeInstanceUpdating
		s.TargetRevision = targetRev
		now := metav1.NewTime(time.Now())
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           updateStepDrain,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
	return observedIncarnation, err
}

// patchInstanceStatusReadyOnRevision idempotently moves to Phase=Ready
// with RunningRevision=rev; clears Operation + TargetRevision.
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

// patchInstanceStatusSurgingForUpdate is the SurgeThenDrain entry
// point. Stamps Phase=Updating + Op{Step=Surge, TargetRevision} without
// bumping Incarnation (different pod NAME — ordinal slot — keeps old
// and new distinct without reusing identity). ActiveOrdinal is not yet
// advanced; it stays on the old slot until promote completes. The
// idempotency guard requires Step in {Surge, SurgeDrain} so the
// helper does not regress an in-flight SurgeDrain back to Surge when
// surgeUpdate re-runs the entry stamp on a subsequent pass; it does
// require Type==Update so a prior recreate / in-place write at the
// same target doesn't short-circuit (those writes use the same
// TargetRevision but a different Step namespace).
func patchInstanceStatusSurgingForUpdate(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceUpdating &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationUpdate &&
			(s.Operation.Step == updateStepSurge || s.Operation.Step == updateStepSurgeDrain) &&
			s.TargetRevision == targetRev {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceUpdating
		s.TargetRevision = targetRev
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           updateStepSurge,
			TargetRevision: targetRev,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusSurgeStepDrain transitions the surge operation
// from Step=Surge to Step=SurgeDrain once the surge pod is Ready and
// we're about to flip serving gates. Distinct from updateStepDrain
// (which recreate uses) so the dispatcher's CheckSurgeGate can
// identify in-flight surges by step name. Updates LastProgressAt for
// the stuck-operation timeout machinery. Idempotent — no-op if
// already on SurgeDrain.
func patchInstanceStatusSurgeStepDrain(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	now := metav1.NewTime(time.Now())
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Operation == nil ||
			s.Operation.Type != v1beta1.InstanceOperationUpdate {
			return false
		}
		if s.Operation.Step == updateStepSurgeDrain {
			return false
		}
		s.Operation.Step = updateStepSurgeDrain
		s.Operation.LastProgressAt = now
		return true
	})
}

// patchInstanceStatusReadyOnRevisionWithOrdinal is the surge-promote
// terminator: clears Operation, advances ActiveOrdinal to the new slot,
// stamps RunningRevision=rev and Phase=Ready. Distinct from
// patchInstanceStatusReadyOnRevision because surge needs to ALSO bump
// ActiveOrdinal — the old pod is gone, the surge pod is the new
// canonical pod, and future restart / scale-up paths must address that
// slot.
func patchInstanceStatusReadyOnRevisionWithOrdinal(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, rev string, newOrdinal int32) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" &&
			s.ActiveOrdinal == newOrdinal {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		s.ActiveOrdinal = newOrdinal
		return true
	})
}

// surgeDrainKey identifies a SurgeThenDrain drain writer entry on the
// old pod's ome.io/serving gate. Indexed by (instance, surge ordinal)
// so a parallel surge across instances doesn't collide. Removed when
// the old pod is deleted at the end of Phase 2.
func surgeDrainKey(idx int32, newOrdinal int32) string {
	return fmt.Sprintf("update-surge-drain-%d-%d", idx, newOrdinal)
}

// surgeUpdate runs the SurgeThenDrain rollout for one Instance.
//
// State machine (per Instance):
//
//	Phase 1 (Surge):   Op{Step=Surge}. Create the surge pod at
//	                   ordinal=1-ActiveOrdinal with target revision.
//	                   Wait surge pod ContainersReady.
//	Phase 2 (Drain):   Op.Step=Drain. Mark surge pod serving=True
//	                   (joins LB rotation). Mark old pod
//	                   serving=False (leaves rotation). Wait
//	                   drain.IsPodDrained on per-revision routed
//	                   Service. Delete old pod.
//	Phase 3 (Promote): Old pod gone. Advance InstanceStatus.
//	                   ActiveOrdinal to the new slot, set Phase=Ready,
//	                   RunningRevision=target, clear Operation.
//
// Invariants:
//   - serving count never dips below desired — surge always rotates IN
//     before old rotates OUT.
//   - alive count peaks at desired+1 per Instance during the surge
//     window. The dispatcher's CheckSurgeGate (added with the
//     wire-up commit) caps concurrent surges across Instances.
//   - no in-place patching — different pod NAMES throughout.
//   - target stability across reconciles: once an Operation is recorded
//     with Step=Surge / SurgeDrain and TargetRevision=X, the in-flight
//     surge is committed to driving to X. Spec bumps mid-surge (a fresh
//     `target` parameter pointing at Y) are picked up by the NEXT
//     reconcile after this surge promotes — detectUpdateTrigger fires
//     because RunningRevision=X != target.Name=Y. Without this pinning
//     the surge would silently drift the in-flight pod (still on X) to
//     "RunningRevision=Y" in status, leaving pods on the stale rev with
//     status claiming the new rev. See PR #246's MutateInstance mirror
//     for the parallel fix in the post-promote ord-resurrection space.
//
// Single-pod scope (Runner.Size == 1). Multi-pod SurgeThenDrain needs
// per-gang-member ordinal allocation and is deferred.
func surgeUpdate(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, inst core.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, error) {
	// Single-pod guard — multi-pod gangs need a different ordinal
	// dimension (per-member surge) not yet plumbed.
	if len(inst.Runners) == 0 || inst.Runners[0].Size != 1 {
		return false, fmt.Errorf("surgeUpdate: multi-pod not yet supported (instance=%d)", inst.Index)
	}
	runner := inst.Runners[0]

	// Where ActiveOrdinal lives today (0 by default). The surge pod
	// goes to the opposite slot.
	var oldOrdinal int32
	if s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil {
		oldOrdinal = s.ActiveOrdinal
	}
	newOrdinal := int32(1) - oldOrdinal

	// Partition observed pods by ordinal slot.
	oldPods, surgePods, stragglers := partitionPodsBySurgeOrdinal(pods, oldOrdinal, newOrdinal)
	if len(stragglers) > 0 {
		// Pods at neither slot — leftover from a future ordinal scheme
		// or label corruption. Refuse to proceed (same shape recreate
		// uses for unknownPods).
		for _, pod := range stragglers {
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonFoundOrphan,
				"OMENative %s found pod %s/%s with unexpected ordinal label; refusing to surge-update",
				status.InstanceKey(plan.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	// surgeTargetName pins the revision this surge is committed to
	// driving to. On the FIRST pass (no in-flight Op) it equals
	// target.Name — the rev the dispatcher just decided to roll to. On
	// SUBSEQUENT passes (Op.Step in {Surge, SurgeDrain} from a prior
	// reconcile) it is the rev that was recorded when the surge was
	// stamped — even if `target` has since moved because of a fresh
	// spec bump. Pinning prevents the state machine from declaring the
	// in-flight pod "on the new target" when it is actually on the
	// previous target, which is the bump-during-bump corruption mode
	// (X-2 in the SRT suite: 5 engine pods, status says vN but pods are
	// still on vN-1). The next surge cycle for the newer target fires
	// naturally after this surge promotes, because detectUpdateTrigger
	// observes RunningRevision != target.Name.
	surgeTargetName := target.Name
	var runningRevName string
	if s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil {
		runningRevName = s.RunningRevision
		if s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationUpdate &&
			(s.Operation.Step == updateStepSurge || s.Operation.Step == updateStepSurgeDrain) &&
			s.Operation.TargetRevision != "" {
			surgeTargetName = s.Operation.TargetRevision
		}
	}
	surgeTargetHash := query.RevisionHashFromControllerRevisionName(surgeTargetName)
	runningRevHash := query.RevisionHashFromControllerRevisionName(runningRevName)

	// Re-route any pod stranded at the surge ordinal but labeled with
	// the just-promoted RunningRevision through the drain path —
	// otherwise the ordinal partition above treats it as a valid
	// surge and Phase 3 promotes RunningRevision to the in-flight
	// target while the actual pod is on the prior rev. See
	// reclassifyByRevisionHash's header for the X-2 (task #54)
	// recovery path this guards.
	surgePods, oldPods = reclassifyByRevisionHash(surgePods, oldPods, surgeTargetHash, runningRevHash)

	// Phase 1 entry: stamp Phase=Updating + Op.Step=Surge if not
	// already there. Idempotent on the second pass. Writes
	// surgeTargetName so the recorded TargetRevision stays pinned to
	// the in-flight surge's commitment, not the latest target.
	wasNotSurging := true
	if s := query.FindInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil &&
		s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationUpdate &&
		(s.Operation.Step == updateStepSurge || s.Operation.Step == updateStepSurgeDrain) {
		wasNotSurging = false
	}
	if err := patchInstanceStatusSurgingForUpdate(ctx, p, plan.Component, inst.Index, surgeTargetName, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("stamp surge step (instance=%d): %w", inst.Index, err)
	}
	if wasNotSurging {
		status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonRecreateUpdateStarted,
			"OMENative %s surge to revision %s (newOrdinal=%d)",
			status.InstanceKey(plan.Component, inst.Index), surgeTargetName, newOrdinal)
	}

	// Phase 1: create surge pod if missing. The created pod's
	// ome.io/revision-hash label is stamped from surgeTargetName so it
	// matches the in-flight commitment. revisionHashFromName is the
	// pinned-target equivalent of revisionHashFromTarget.
	//
	// Skip the create when the surge ordinal slot is already occupied
	// by a pod the reclassify just moved into the drain set. That's
	// the recovery path (task #54) where a stale-rev pod lives at the
	// surge slot — the Create would AlreadyExists against it, leaving
	// the stale pod in place forever. Falling through to Phase 2
	// drains both the alternate-slot canonical and the stale pod;
	// the NEXT reconcile pass enters Phase 1 with an empty surge slot
	// and creates the correct-rev pod.
	staleAtSurgeSlot := false
	for _, pod := range oldPods {
		if ord, ok := query.PodOrdinalFromLabels(pod); ok && ord == newOrdinal {
			staleAtSurgeSlot = true
			break
		}
	}
	if len(surgePods) == 0 && !staleAtSurgeSlot {
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index) {
			return false, nil
		}
		targets := []podTarget{{
			Name:    query.PodName(p.ISVC.Name, plan.Component, inst.Index, runner.Name, newOrdinal),
			Runner:  runner,
			Ordinal: newOrdinal,
		}}
		if _, err := createMissingPods(ctx, p, plan, inst, inst.Index, targets, query.RevisionHashFromControllerRevisionName(surgeTargetName)); err != nil {
			return false, fmt.Errorf("create surge pod (instance=%d, ordinal=%d): %w", inst.Index, newOrdinal, err)
		}
		return false, nil
	}
	// If the surge slot is occupied by a stale pod (no valid surge
	// alive yet), let Phase 2 drain ALL old pods (including the
	// stale at the surge slot) and a subsequent reconcile re-create
	// the surge pod with the correct rev.
	if staleAtSurgeSlot && len(surgePods) == 0 {
		// Mark the surge step transition once so the dispatcher's
		// CheckSurgeGate accounting matches the in-flight phase even
		// while the surge pod is absent. Idempotent.
		if err := patchInstanceStatusSurgeStepDrain(ctx, p, plan.Component, inst.Index); err != nil {
			return false, fmt.Errorf("transition surge step to drain (instance=%d): %w", inst.Index, err)
		}
		for _, pod := range oldPods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, p.Client, pod, podreadiness.WriterUpdateSurgeDrain, surgeDrainKey(inst.Index, newOrdinal)); err != nil {
				return false, fmt.Errorf("mark old not serving for stale-slot recovery (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live drain check identical to the canonical Phase 2 path.
		for _, pod := range oldPods {
			serviceName := drainServiceForPod(p.ISVC.Name, plan.Component, pod)
			if serviceName == "" {
				continue
			}
			drained, err := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index) {
			return false, nil
		}
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			p.ExpectationsCache().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index, 1)
			if err := p.Client.Delete(ctx, pod); err != nil {
				p.ExpectationsCache().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete stale-slot pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase 1 (cont): wait surge pod ContainersReady. Don't flip serving
	// or touch the old pod until the surge has containers up — that's the
	// no-downtime guarantee.
	if !query.AllPodsRuntimeReady(surgePods) {
		return false, nil
	}

	// Phase 2: surge is Ready. Promote it to serving (joins rotation),
	// then drain the old pod.
	for _, pod := range surgePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, p.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark surge serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	if len(oldPods) > 0 {
		// Transition Step Surge → Drain once. Subsequent passes idempotency-
		// skip inside the helper.
		if err := patchInstanceStatusSurgeStepDrain(ctx, p, plan.Component, inst.Index); err != nil {
			return false, fmt.Errorf("transition surge step to drain (instance=%d): %w", inst.Index, err)
		}
		for _, pod := range oldPods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, p.Client, pod, podreadiness.WriterUpdateSurgeDrain, surgeDrainKey(inst.Index, newOrdinal)); err != nil {
				return false, fmt.Errorf("mark old not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live drain check via per-revision routed Service (PR #230 fix
		// — see drain.IsPodDrained semantics; headless slice would lie
		// because of PublishNotReadyAddresses).
		for _, pod := range oldPods {
			serviceName := drainServiceForPod(p.ISVC.Name, plan.Component, pod)
			if serviceName == "" {
				continue
			}
			drained, err := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDelete BEFORE Delete, rollback via
		// ObservedDelete on error — matches recreateUpdate's contract.
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			p.ExpectationsCache().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index, 1)
			if err := p.Client.Delete(ctx, pod); err != nil {
				p.ExpectationsCache().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, workload.ComponentType(plan.Component), inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete old pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase 3: old gone, surge is canonical. Advance ActiveOrdinal,
	// promote to Ready on the pinned target revision (the rev the
	// surge actually drove the pod to — NOT the latest target if a
	// mid-surge bump moved it). Using target.Name here would write
	// "RunningRevision=<latest-target>" while the pod is on the prior
	// rev — the X-2 corruption mode. detectUpdateTrigger picks up the
	// drift on the next reconcile and fires a fresh surge cycle.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(ctx, p, plan.Component, inst.Index, surgeTargetName, newOrdinal); err != nil {
		return false, fmt.Errorf("promote surge (instance=%d): %w", inst.Index, err)
	}
	status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonRecreateUpdateCompleted,
		"OMENative %s surge to revision %s complete (activeOrdinal=%d)",
		status.InstanceKey(plan.Component, inst.Index), surgeTargetName, newOrdinal)
	return true, nil
}

// partitionPodsBySurgeOrdinal buckets pods by the LabelPodOrdinal label
// for SurgeThenDrain's three-way decision (old/surge/stragglers).
// Stragglers are pods at neither slot — should never happen in normal
// operation; bailing out is safer than risking a wrong action.
func partitionPodsBySurgeOrdinal(pods []*corev1.Pod, oldOrdinal, newOrdinal int32) (old, surge, stragglers []*corev1.Pod) {
	for _, pod := range pods {
		ord, ok := query.PodOrdinalFromLabels(pod)
		if !ok {
			stragglers = append(stragglers, pod)
			continue
		}
		switch ord {
		case oldOrdinal:
			old = append(old, pod)
		case newOrdinal:
			surge = append(surge, pod)
		default:
			stragglers = append(stragglers, pod)
		}
	}
	return
}

// reclassifyByRevisionHash promotes any pod incorrectly bucketed as a
// surge candidate into the drain set when its revision-hash label
// matches the just-promoted RunningRevision — i.e., it is left over
// from the previous surge cycle, not the in-flight one. The ordinal
// partition is load-bearing for the no-downtime invariant (the surge
// slot is the ALTERNATE ordinal), but it can mis-classify in the
// multi-bump recovery path:
//
//   - Bump v1 → v2: surge cycle stamps Op{TargetRev=v2}, creates v2 at
//     ord=1, drains v1 at ord=0, promotes — RunningRev=v2,
//     ActiveOrdinal=1.
//   - Bump v2 → v3 arrives while a fresh reconcile is pending. The
//     dispatcher's NEXT pass sees RunningRev=v2 != target=v3 and fires
//     a fresh surge cycle. surgeUpdate computes oldOrdinal=1,
//     newOrdinal=0, partitions [v2@ord=1, v3-surge-just-created@ord=0]
//     correctly — v2 in old, v3 in surge.
//
// But there are recovery scenarios where the surge slot holds a pod
// labeled with the PRIOR running revision (e.g., a re-entry from a
// previous surge cycle that did not cleanly delete its alternate-slot
// peer, or a controller restart that lost the in-flight bookkeeping
// and re-derived state from observed pod labels). Without this
// recheck the ordinal partition would treat the stale pod as the
// "valid surge" — Phase 3 would promote RunningRevision to the
// pinned target while the actual pod is on a prior rev — exactly the
// X-2 "v2 straggler labeled post-promote as v3" symptom captured on
// cluster msl-cw-use2-dev-2 after the post-#256 deploy (OEP-0007.2
// task #54).
//
// The recheck is narrow: a pod is reclassified into the drain set
// ONLY when its revision-hash label matches the just-promoted
// runningRev (i.e., it is leftover from the previous cycle's
// promote). Pods with arbitrary other labels are kept in the surge
// bucket so existing test fixtures (which stamp a generic
// testRevisionHash regardless of target) and legacy pods (label
// missing) continue to round-trip. The recheck is a no-op when
// runningRev is empty (first-ever surge cycle, no prior promote to
// inherit from) or matches targetHash (no rev difference, the surge
// is already valid).
func reclassifyByRevisionHash(surge, drain []*corev1.Pod, targetHash, runningRevHash string) (newSurge, newDrain []*corev1.Pod) {
	if runningRevHash == "" || runningRevHash == targetHash {
		return surge, drain
	}
	newSurge = surge[:0]
	newDrain = drain
	for _, pod := range surge {
		hash, ok := pod.Labels[query.LabelRevisionHash]
		if !ok || hash == "" {
			// No label — fall back to the ordinal classification so
			// legacy pods aren't churned. partition tests pin this.
			newSurge = append(newSurge, pod)
			continue
		}
		if hash == runningRevHash {
			// Stale: pod is labeled with the just-promoted rev but is
			// sitting at the surge ordinal. Drain + delete so the
			// next reconcile's Phase 1 re-creates with the
			// in-flight target rev. Any pod whose rev is neither the
			// prior promote nor the in-flight target (e.g., test
			// fixtures with a synthetic label) stays in the surge
			// bucket on purpose — production callers always stamp
			// rev-hash from createMissingPods's surgeTargetHash
			// argument, so a non-matching rev outside the prior /
			// current pair shouldn't happen and shouldn't be punished.
			newDrain = append(newDrain, pod)
			continue
		}
		newSurge = append(newSurge, pod)
	}
	return
}
