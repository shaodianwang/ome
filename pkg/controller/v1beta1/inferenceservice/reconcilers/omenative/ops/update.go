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
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/revision"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
)

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

	mode, err := chooseUpdateMode(plan.UpdateStrategy.Type, runningSpec, targetSpec)
	if err != nil {
		// InPlaceOnly + ineligible diff is the only error path here.
		status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonInPlaceUpdateNotPossible,
			"OMENative %s rejected update: %v",
			status.InstanceKey(plan.Component, inst.Index), err)
		return false, fmt.Errorf("choose update mode (instance=%d): %w", inst.Index, err)
	}

	// Status stamping is per-mode: recreate bumps Incarnation atomically
	// with the Updating transition; in-place must NOT bump (same
	// materialization). A generic Updating write here would let recreate's
	// bump branch idempotency-skip on the second pass.
	switch mode {
	case updateModeInPlace:
		return inPlaceUpdate(ctx, p, plan, inst, target, targetSpec, pods)
	case updateModeRecreate:
		return recreateUpdate(ctx, p, plan, inst, target, pods)
	}
	return false, fmt.Errorf("update mode unknown (instance=%d)", inst.Index)
}

// updateMode is the resolved per-Instance dispatch decision.
type updateMode int

const (
	updateModeRecreate updateMode = iota
	updateModeInPlace
)

// chooseUpdateMode resolves strategy + eligibility into a mode. nil
// running spec is treated as ineligible, forcing recreate from a known
// baseline. InPlaceOnly + ineligible returns an error.
//
// SurgeThenDrain currently delegates to recreate. Real surge-then-drain
// semantics (create new alongside old, wait Ready, drain old) require
// pod-naming changes or surge-index plumbing similar to Migrate, both
// substantial architectural moves. Until then, SurgeThenDrain ==
// RecreatePod, which is safer than in-place because the dispatcher's
// MaxUnavailable gate throttles one drain at a time.
func chooseUpdateMode(strategy v1beta1.OMENativeUpdateStrategyType, running, target *corev1.PodSpec) (updateMode, error) {
	eligible := inPlaceEligible(running, target)
	switch strategy {
	case v1beta1.OMENativeUpdateStrategySurgeThenDrain:
		// TODO: real surge-then-drain with new pod alongside old. For
		// now route to recreate so the safety guarantee (no in-place
		// gate bypass) holds, even if we don't yet deliver zero-downtime.
		return updateModeRecreate, nil
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
		// upstream; this empty-string branch matches the SurgeThenDrain
		// default so a direct caller (tests, fuzz) gets safe behavior
		// even if it skips the defaulter.
		return updateModeRecreate, nil
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
	return payload.PodSpec, nil
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
// container images, wait for runtime ready on the target image, flip
// serving back, promote Ready. Multi-pass; idempotent.
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
		serviceName := query.HeadlessServiceName(p.ISVC.Name, plan.Component)
		for _, pod := range pods {
			drained, err := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
	}

	// Image patch. When markNotReady=true, re-read each pod and re-check
	// podreadiness.IsServing immediately before patching — a concurrent writer (another
	// controller, a pod-mutating webhook re-injecting a sidecar) could flip
	// serving back to True after the drain check passed, and patching a
	// serving pod violates the always-drain-first invariant. On detection,
	// re-mark NotServing and requeue. Live reader so a stale serving=False
	// view doesn't fail the recheck open.
	patched := false
	for _, pod := range pods {
		if podImagesMatch(pod, targetSpec) {
			continue
		}
		if markNotReady {
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
		if err := patchPodImages(ctx, p.Client, pod, targetSpec); err != nil {
			return false, fmt.Errorf("patch images (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
		patched = true
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
		serviceName := query.HeadlessServiceName(p.ISVC.Name, plan.Component)
		for _, pod := range oldPods {
			drained, err := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			p.ExpectationsCache().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, 1)
			if err := p.Client.Delete(ctx, pod); err != nil {
				p.ExpectationsCache().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index)
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
	clear, err := query.LiveOldPodsClearedForRecreate(ctx, p, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("recreateUpdate: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(p.ISVC.Name, plan.Component, newInst)
	existingByName := query.IndexPodsByName(newPods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
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
		want[c.Name] = c.Image
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if want[cs.Name] != cs.Image {
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

// updateStep distinguishes which mode wrote the current Operation. Without
// it, a strategy change mid-rollout (InPlaceIfPossible → RecreatePod)
// would let recreate's idempotency check match the in-place state, skip
// the Incarnation bump, classify every pod as fresh, and never recreate.
const (
	updateStepInPlace = "InPlace"
	updateStepDrain   = "Drain"
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
