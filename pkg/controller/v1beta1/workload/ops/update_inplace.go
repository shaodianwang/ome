package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// inPlaceUpdate runs the in-place rollout: drain (optional), patch
// container images, reconcile pod-template annotations from the
// target revision's PodMeta, wait for runtime ready on the target
// image, flip serving back, promote Ready. Multi-pass; idempotent.
//
// Annotation handling adds / updates keys present in the new spec and
// deletes keys the previous revision once authored; foreign
// (third-party) annotations are left alone. Recreate-on-annotation-
// change was rejected because annotations are not load-bearing for
// runtime image semantics and would defeat the zero-downtime
// contract.
func inPlaceUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, pods []*corev1.Pod) (bool, error) {
	// In-place keeps the same Incarnation — only the container image rolls.
	wasNotUpdating := true
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil && s.Phase == workload.InstancePhaseUpdating {
		wasNotUpdating = false
	}
	if err := patchInstanceStatusUpdating(ctx, input, inst.Index, target.Name, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Updating (instance=%d): %w", inst.Index, err)
	}
	if wasNotUpdating {
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInPlaceUpdateStarted,
			"OMENative %s in-place update to revision %s",
			instanceKey(input.Key.Component, inst.Index), target.Name)
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
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live reader on drain check so kube-proxy isn't still routing.
		for _, pod := range pods {
			serviceName := drainServiceForPod(input, plan, pod)
			if serviceName == "" {
				continue
			}
			drained, err := drain.IsPodDrained(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
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
	runningPayload, err := loadRunningRevisionPayload(ctx, deps.Reader(), input, inst.Index)
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
			if gerr := deps.Reader().Get(ctx, client.ObjectKeyFromObject(pod), fresh); gerr != nil {
				if apierrors.IsNotFound(gerr) {
					continue
				}
				return false, fmt.Errorf("re-read pod for serving recheck (instance=%d, pod=%s): %w", inst.Index, pod.Name, gerr)
			}
			if podreadiness.IsServing(fresh) {
				if err := podreadiness.MarkPodNotServing(ctx, deps.Client, fresh, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
					return false, fmt.Errorf("re-mark not serving (instance=%d, pod=%s): %w", inst.Index, fresh.Name, err)
				}
				return false, nil
			}
			pod = fresh
		}
		if needsImagePatch {
			if err := patchPodImages(ctx, deps.Client, pod, targetSpec); err != nil {
				return false, fmt.Errorf("patch images (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			patched = true
		}
		if err := patchPodAnnotations(ctx, deps.Client, pod, previousAnnotations, targetAnnotations); err != nil {
			return false, fmt.Errorf("patch annotations (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
		// Restamp the revision-hash label so the in-place-rolled pod is
		// recognized as the target revision by per-revision Service routing,
		// drain, and stuck-pod detection (all keyed on this label). Idempotent.
		if err := patchPodRevisionHashLabel(ctx, deps.Client, pod, query.RevisionOf(target).Hash()); err != nil {
			return false, fmt.Errorf("patch revision-hash label (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
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
		if err := podreadiness.MarkPodServing(ctx, deps.Client, pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	if err := patchInstanceStatusReadyOnRevision(ctx, input, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInPlaceUpdateCompleted,
		"OMENative %s in-place update to revision %s complete",
		instanceKey(input.Key.Component, inst.Index), target.Name)
	return true, nil
}
