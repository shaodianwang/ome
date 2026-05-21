package omenative

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// updateRequeueInterval is the wait between passes while an Update is
// in flight.
const updateRequeueInterval = 5 * time.Second

// Update drives one Instance toward the target ControllerRevision. The
// rollout mode is chosen per Instance:
//
//   - RecreatePod: always drain + delete + create at the bumped Incarnation.
//   - InPlaceIfPossible: try image-patch first when every pod's diff to
//     the target is restricted to container images; otherwise recreate.
//   - InPlaceOnly: image-patch when eligible; refuse with an error
//     otherwise (operator must pick a different strategy or fix the
//     spec to keep the diff image-only).
//
// done=true means the Instance is back at Phase=Ready with
// RunningRevision pointing at the target.
func Update(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Update: nil client")
	}
	if target == nil || targetSpec == nil {
		return false, fmt.Errorf("Update: nil target revision or spec (instance=%d)", inst.Index)
	}

	pods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Update: list pods (instance=%d): %w", inst.Index, err)
	}

	// Eligibility is decided against the recorded running revision, not
	// the live pod. Live pods carry API-server-defaulted fields
	// (restartPolicy, dnsPolicy, imagePullPolicy, terminationMessagePath,
	// schedulerName, defaulted securityContext, ...) plus OMENative's own
	// Render overlays (hostname/subdomain/serving gate). Byte-comparing
	// against the freshly-rendered target would always declare every
	// rollout ineligible. The recorded running revision's PodSpec is the
	// pre-defaulted, pre-overlay canonical form, the same shape the
	// target hashes against.
	runningSpec, err := loadRunningRevisionPodSpec(ctx, p.reader(), p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Update: load running revision (instance=%d): %w", inst.Index, err)
	}

	mode, err := chooseUpdateMode(plan.UpdateStrategy.Type, runningSpec, targetSpec)
	if err != nil {
		return false, fmt.Errorf("choose update mode (instance=%d): %w", inst.Index, err)
	}

	// Status stamping is per-mode because recreate needs to bump
	// Incarnation atomically with the Updating-transition write while
	// in-place must NOT bump (the spec says in-place is the same
	// materialization). Doing a generic Updating write here first would
	// let recreate's bump branch idempotency-skip on the second pass.
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

// chooseUpdateMode resolves the strategy enum + eligibility check to a
// concrete mode. InPlaceOnly with an ineligible diff is rejected — the
// operator wrote a strategy the spec change can't satisfy. running is
// the PodSpec recorded on the Instance's current RunningRevision CR;
// passing nil (no recorded revision yet) is treated as ineligible and
// forces recreate so the rollout starts from a known baseline.
func chooseUpdateMode(strategy v1beta1.OMENativeUpdateStrategyType, running, target *corev1.PodSpec) (updateMode, error) {
	eligible := inPlaceEligible(running, target)
	switch strategy {
	case v1beta1.OMENativeUpdateStrategyRecreatePod:
		return updateModeRecreate, nil
	case v1beta1.OMENativeUpdateStrategyInPlaceOnly:
		if !eligible {
			return 0, fmt.Errorf("InPlaceOnly strategy but diff exceeds container images")
		}
		return updateModeInPlace, nil
	case v1beta1.OMENativeUpdateStrategyInPlaceIfPossible, "":
		if eligible {
			return updateModeInPlace, nil
		}
		return updateModeRecreate, nil
	default:
		return 0, fmt.Errorf("unknown UpdateStrategy.Type %q", strategy)
	}
}

// inPlaceEligible reports whether the diff between the running and
// target PodSpecs is restricted to regular-container images. Both
// sides are stripped of regular-container images (init images are
// preserved so an init-image-only diff falls through to recreate)
// then byte-compared; equality means images are the only field that
// differs.
//
// Returns false when either side is nil — a missing running spec
// (no recorded revision yet) is treated as "cannot prove image-only,"
// forcing recreate.
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

// loadRunningRevisionPodSpec returns the PodSpec recorded on the
// ControllerRevision named by the Instance's RunningRevision status
// field. Returns (nil, nil) when:
//
//   - the Instance has no InstanceStatus yet (first reconcile),
//   - the Instance has no RunningRevision recorded (legacy pod created
//     before status started tracking it), or
//   - the named CR no longer exists (operator force-deleted, or
//     retention swept too aggressively).
//
// Caller treats a nil return as "no baseline" — eligibility falls
// through to recreate so the rollout starts from a known revision.
func loadRunningRevisionPodSpec(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) (*corev1.PodSpec, error) {
	s := findInstanceStatus(isvc, component, idx)
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
	var payload revisionDataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal running CR %s data: %w", s.RunningRevision, err)
	}
	return payload.PodSpec, nil
}

// podSpecWithoutImages returns the JSON form of spec with regular-
// container images stripped. Init-container images are intentionally
// preserved — init containers run once at pod creation, and kubelet
// has no machinery to re-run them after an image patch, so an
// init-image diff cannot be applied in place. Leaving InitContainer
// images in the canonical form forces an init-image-only diff to fail
// the inPlaceEligible byte-compare and route to recreate.
func podSpecWithoutImages(spec *corev1.PodSpec) ([]byte, error) {
	clone := spec.DeepCopy()
	for i := range clone.Containers {
		clone.Containers[i].Image = ""
	}
	return json.Marshal(clone)
}

// inPlaceUpdate executes the in-place rollout sequence: drain, patch
// container images on every live pod, wait for runtime ready on the
// target image, flip serving=True, mark Phase=Ready with
// RunningRevision=target.Name.
//
// Multi-pass; idempotent at every step.
func inPlaceUpdate(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, pods []*corev1.Pod) (bool, error) {
	// Step 1: stamp Phase=Updating without bumping Incarnation —
	// in-place keeps the same materialization, just rewrites the image.
	if err := patchInstanceStatusUpdating(ctx, p, plan.Component, inst.Index, target.Name, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Updating (instance=%d): %w", inst.Index, err)
	}

	markNotReady := true
	if plan.UpdateStrategy.InPlaceUpdateStrategy != nil &&
		plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle != nil {
		markNotReady = *plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle
	}

	// Step 2: flip serving=False if the strategy requires draining first.
	if markNotReady {
		for _, pod := range pods {
			if !IsServing(pod) {
				continue
			}
			if err := MarkPodNotServing(ctx, p.Client, pod, "Updating", "OMENative draining pod for in-place update"); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Step 3: wait for EndpointSlice drain before mutating the pod.
		serviceName := headlessServiceName(p.ISVC.Name, plan.Component)
		for _, pod := range pods {
			drained, err := IsPodDrained(ctx, p.Client, p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
	}

	// Step 4: patch container images on every pod.
	patched := false
	for _, pod := range pods {
		if podImagesMatch(pod, targetSpec) {
			continue
		}
		if err := patchPodImages(ctx, p.Client, pod, targetSpec); err != nil {
			return false, fmt.Errorf("patch images (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
		patched = true
	}
	if patched {
		// Just patched — kubelet needs another pass to roll the
		// containers; readiness can't possibly be re-converged yet.
		return false, nil
	}

	// Step 5: every pod must report runtime ready on the target image
	// before we proceed. ContainersReady=True alone is not enough — the
	// cache can still show the old condition while spec.image has just
	// been updated. Also require Status.ContainerStatuses[*].Image to
	// match the target so we only declare convergence after kubelet has
	// actually rolled the container.
	if !allPodsRuntimeReady(pods) {
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

	// Step 6: flip serving=True on every pod.
	for _, pod := range pods {
		if IsServing(pod) {
			continue
		}
		if err := MarkPodServing(ctx, p.Client, pod); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// Step 7: status to Ready, RunningRevision=target.
	if err := patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	return true, nil
}

// recreateUpdate executes the recreate rollout sequence: drain + delete
// every current pod, then create the new pod set at the bumped
// Incarnation. Multi-pass and idempotent.
//
// Shares the drain/delete/recreate skeleton with Restart but pivots on
// template-change rather than pod-loss. pods is the full observed
// instance pod set; partitionPodsByIncarnation splits it into the
// old-incarnation pods that need draining and the new-incarnation
// pods that should be left alone (or created if missing).
func recreateUpdate(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, error) {
	// Bump Incarnation if we haven't already on this rollout.
	newInc, err := patchInstanceStatusRecreatingForUpdate(ctx, p, plan.Component, inst.Index, target.Name, plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("bump Incarnation (instance=%d): %w", inst.Index, err)
	}

	oldPods, newPods := partitionPodsByIncarnation(pods, newInc)

	// Phase A: drain + delete old pods (same as Restart's old-pod phase).
	if len(oldPods) > 0 {
		for _, pod := range oldPods {
			if !IsServing(pod) {
				continue
			}
			if err := MarkPodNotServing(ctx, p.Client, pod, "Updating", "OMENative draining pod for recreate update"); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		serviceName := headlessServiceName(p.ISVC.Name, plan.Component)
		for _, pod := range oldPods {
			drained, err := IsPodDrained(ctx, p.Client, p.ISVC.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
		if !DefaultExpectations.Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}
		deleted := 0
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			if err := p.Client.Delete(ctx, pod); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			deleted++
		}
		if deleted > 0 {
			DefaultExpectations.ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, deleted)
		}
		return false, nil
	}

	// Phase B: create the new pod set at the bumped Incarnation.
	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(p.ISVC.Name, plan.Component, newInst)
	existingByName := indexPodsByName(newPods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !DefaultExpectations.Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}
		created, err := createMissingPods(ctx, p, plan, newInst, missing)
		if err != nil {
			return false, err
		}
		if created > 0 {
			DefaultExpectations.ExpectCreates(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, created)
		}
		return false, nil
	}

	// Phase C: wait runtime-ready, flip serving, mark Ready+RunningRevision.
	if !allPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if IsServing(pod) {
			continue
		}
		if err := MarkPodServing(ctx, p.Client, pod); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}
	if err := patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	return true, nil
}

// detectUpdateTrigger reports whether the Instance needs to start (or
// continue) an Update. Returns (true, currentTarget) when:
//
//   - the Instance is already mid-update (Phase=Updating), or
//   - the Instance is Phase=Ready but at least one of its pods carries
//     a PodSpec that hashes to a revision other than the target.
//
// Phase=Creating / Phase=Deleting / Phase=Restarting / Phase=Migrating
// are not interruptible by Update; those operations finish first.
//
// target is the ControllerRevision the Component currently desires
// (caller computes via EnsureControllerRevision).
func detectUpdateTrigger(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	s := findInstanceStatus(p.ISVC, plan.Component, inst.Index)
	if s == nil {
		return false, nil
	}
	if s.Phase == v1beta1.OMENativeInstanceUpdating {
		return true, nil
	}
	if s.Phase != v1beta1.OMENativeInstanceReady {
		return false, nil
	}

	// Ready Instance: check whether the live pods match the target.
	// If any pod's PodSpec hashes differently from the target's, we need
	// to update. RunningRevision is the cheap fast-path: matches target =
	// done, mismatch = update needed.
	if s.RunningRevision != "" && s.RunningRevision != target.Name {
		return true, nil
	}
	if s.RunningRevision == target.Name {
		return false, nil
	}
	// No RunningRevision recorded (first reconcile after the feature
	// landed, or an instance created before status started tracking it).
	// Compare observed pods to target. Any mismatch triggers update.
	pods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, inst.Index)
	if err != nil {
		return false, err
	}
	for _, pod := range pods {
		if !podMatchesTarget(pod, targetSpec) {
			return true, nil
		}
	}
	// All pods already match the target — backfill RunningRevision so
	// subsequent reconciles take the cheap fast-path comparison instead
	// of re-running the full per-pod diff every cycle.
	if err := patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("backfill RunningRevision (instance=%d): %w", inst.Index, err)
	}
	return false, nil
}

// podMatchesTarget returns true when pod's runtime PodSpec is equivalent
// to target — same image-stripped JSON AND matching container images.
// Used by detectUpdateTrigger when RunningRevision is missing.
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

// podRuntimeImagesMatch returns true when kubelet has confirmed the
// pod is running the target image — every Status.ContainerStatuses
// entry's .Image matches the target's same-named container. This is
// the runtime-truth signal that complements podImagesMatch's spec
// check: after a patchPodImages call, spec.image flips immediately
// but kubelet may not have rolled the container yet, leaving stale
// ContainersReady=True and an old runtime image. Only after both
// match do we know the in-place update has actually taken effect.
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
	// All container statuses must be present; if kubelet hasn't yet
	// emitted a status for one of the target containers we cannot
	// confirm convergence.
	return len(pod.Status.ContainerStatuses) >= len(target.Containers)
}

// podImagesMatch returns true when every container in pod matches the
// container of the same name in target by .Image. Containers with no
// counterpart in target fail the match.
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

// patchPodImages updates each container's image field to match the
// corresponding container in target via a strategic-merge patch. Only
// the changed images go over the wire; unrelated fields (Volumes,
// EnvVars, etc.) the API server may have defaulted are not touched.
// Kubelet observes the change and performs a rolling container
// restart inside the pod.
//
// Strategic-merge is the correct vehicle for "only changing N
// fields by name" — it merges by patch-merge-key, which for
// PodSpec.Containers is the container name. A plain client.Update
// would write the entire pod object and conflict with any other
// controller that touched the same RV in between.
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
	patch := map[string]any{
		"spec": map[string]any{
			"containers": containersPatch,
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal image patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return fmt.Errorf("patch pod %s/%s images: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// updateStepInPlace and updateStepDrain distinguish which mode wrote
// the current Operation. Without this discriminator, a strategy
// change mid-rollout (e.g., InPlaceIfPossible -> RecreatePod) would
// let patchInstanceStatusRecreatingForUpdate's idempotency check
// match the existing in-place state and skip the Incarnation bump —
// recreate would then classify every pod as fresh, jump straight to
// Phase=Ready, and never actually recreate anything.
const (
	updateStepInPlace = "InPlace"
	updateStepDrain   = "Drain"
)

// patchInstanceStatusUpdating sets Phase=Updating, Operation=Update with
// Step=InPlace, TargetRevision=target. Idempotent — skip-write requires
// Step==InPlace, so a previous recreate-step write does NOT short-
// circuit the in-place transition.
func patchInstanceStatusUpdating(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
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

// patchInstanceStatusRecreatingForUpdate is the recreate-mode entry
// point: it bumps Incarnation (like Restart) and ensures Phase=Updating
// with Operation.Step=Drain and the target revision recorded. Returns
// the post-write Incarnation.
//
// The idempotency guard requires Step==Drain in addition to the
// Phase/Type/TargetRevision match — otherwise a prior in-place pass
// that wrote Step==InPlace at the same target would prevent the bump,
// even though recreate has not yet drained or deleted anything.
func patchInstanceStatusRecreatingForUpdate(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, targetRev string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		// Already in recreate-shape with a bumped incarnation? Reuse.
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

// patchInstanceStatusReadyOnRevision sets Phase=Ready, RunningRevision=rev,
// clears Operation and TargetRevision. Idempotent.
func patchInstanceStatusReadyOnRevision(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, rev string) error {
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
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
