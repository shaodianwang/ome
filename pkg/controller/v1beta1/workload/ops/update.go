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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// updateDrainKey identifies an in-place or recreate Update writer
// against a specific Instance materialization. Same key on Add and
// Remove so an in-place flow ends with the gate flipping back True.
func updateDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// drainServiceForPod is declared in migrate.go — Update and Migrate
// share the same per-revision routed Service lookup.

// UpdateRequeueInterval is the wait between passes while an Update is
// in flight. Exported so the dispatcher's requeue cadence stays in
// lockstep with the per-Instance state machine.
const UpdateRequeueInterval = 5 * time.Second

// Update drives one Instance toward the target ControllerRevision.
// Mode is chosen per Instance:
//
//   - RecreatePod: always drain + delete + recreate at bumped
//     Incarnation.
//   - InPlaceIfPossible: image-patch when the diff is container images
//     only; otherwise fall through to recreate.
//   - InPlaceOnly: image-patch when eligible; error otherwise.
//
// done=true once Phase=Ready with RunningRevision=target.Name.
func Update(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Update: nil client")
	}
	if target == nil || targetSpec == nil {
		return false, fmt.Errorf("Update: nil target revision or spec (instance=%d)", inst.Index)
	}

	pods, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component)
	if err != nil {
		return false, fmt.Errorf("Update: list pods (instance=%d): %w", inst.Index, err)
	}
	pods = filterPodsByInstance(pods, inst.Index)

	// Eligibility compares against the recorded running revision, NOT the
	// live pod. Live pods carry apiserver-defaulted fields and Render
	// overlays (hostname/subdomain/serving gate); byte-comparing those
	// against the freshly-rendered target would always declare ineligible.
	// The recorded revision's PodSpec is the pre-defaulted canonical form
	// the target hashes against.
	runningSpec, err := loadRunningRevisionPodSpec(ctx, deps.Reader(), input, input.Key.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Update: load running revision (instance=%d): %w", inst.Index, err)
	}

	multiPod := inst.TotalPods() > 1
	// Strategy on the plan is the workload-mirror UpdateStrategyType;
	// the chooser compares against the workload constants directly.
	strategy := plan.UpdateStrategy.Type
	mode, err := chooseUpdateModeForInstance(strategy, runningSpec, targetSpec, multiPod)
	if err != nil {
		// InPlaceOnly + ineligible diff is the only error path here.
		recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonInPlaceUpdateNotPossible,
			"OMENative %s rejected update: %v",
			instanceKey(input.Key.Component, inst.Index), err)
		return false, fmt.Errorf("choose update mode (instance=%d): %w", inst.Index, err)
	}

	// Status stamping is per-mode: recreate bumps Incarnation atomically
	// with the Updating transition; in-place must NOT bump (same
	// materialization); surge advances ActiveOrdinal (single-pod) or
	// promotes the surge index (gang) on completion.
	switch mode {
	case updateModeInPlace:
		return inPlaceUpdate(ctx, deps, input, plan, inst, target, targetSpec, pods)
	case updateModeRecreate:
		return recreateUpdate(ctx, deps, input, plan, inst, target, pods)
	case updateModeSurge:
		return surgeUpdate(ctx, deps, input, plan, inst, target, pods)
	}
	return false, fmt.Errorf("update mode unknown (instance=%d)", inst.Index)
}

// filterPodsByInstance drops pods whose ome.io/instance-index label
// doesn't match idx.
func filterPodsByInstance(pods []*corev1.Pod, idx int32) []*corev1.Pod {
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out
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
// adjustment layered on top.
//
// Multi-pod adjustment:
//   - In-place modes → recreate: inPlaceEligible compares only the
//     leader's PodSpec, so a worker-only image bump would produce an
//     in-place rollout that patches only the leader, leaving workers
//     on the old image (split-brain).
//
// SurgeThenDrain keeps updateModeSurge for gangs — surgeUpdate performs a
// per-gang index surge (a whole replacement gang at a fresh instance
// index, gang-scheduled via its own PodGroup, then the source gang is
// drained). RecreatePod is already recreate; multiPod has no additional
// effect on it.
func chooseUpdateModeForInstance(strategy workload.UpdateStrategyType, running, target *corev1.PodSpec, multiPod bool) (updateMode, error) {
	mode, err := chooseUpdateMode(strategy, running, target)
	if err != nil {
		// InPlaceOnly + ineligible diff is the only error path. For
		// multi-pod fall back to recreate: in-place cannot safely roll
		// a worker-only change. Single-pod keeps the strict semantic.
		if multiPod && strategy == workload.UpdateStrategyInPlaceOnly {
			return updateModeRecreate, nil
		}
		return mode, err
	}
	if multiPod && mode == updateModeInPlace {
		return updateModeRecreate, nil
	}
	return mode, nil
}

// chooseUpdateMode resolves strategy + eligibility into a mode. nil
// running spec is treated as ineligible, forcing recreate from a
// known baseline. InPlaceOnly + ineligible returns an error.
// Pod-count-agnostic; production callers route through
// chooseUpdateModeForInstance.
func chooseUpdateMode(strategy workload.UpdateStrategyType, running, target *corev1.PodSpec) (updateMode, error) {
	eligible := inPlaceEligible(running, target)
	switch strategy {
	case workload.UpdateStrategySurgeThenDrain:
		return updateModeSurge, nil
	case workload.UpdateStrategyRecreatePod:
		return updateModeRecreate, nil
	case workload.UpdateStrategyInPlaceOnly:
		if !eligible {
			return 0, fmt.Errorf("InPlaceOnly strategy but diff exceeds container images")
		}
		return updateModeInPlace, nil
	case workload.UpdateStrategyInPlaceIfPossible:
		if eligible {
			return updateModeInPlace, nil
		}
		return updateModeRecreate, nil
	case "":
		// Default matches SurgeThenDrain so direct callers (tests,
		// fuzz) skipping the defaulter get safe behavior.
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
//
// component is unused (the InstanceStatus lookup keys on idx alone)
// but kept for symmetry and future evolution toward multi-Component
// input payloads.
func loadRunningRevisionPodSpec(ctx context.Context, reads client.Reader, input workload.ReconcileInput, _ workload.ComponentType, idx int32) (*corev1.PodSpec, error) {
	payload, err := loadRunningRevisionPayload(ctx, reads, input, idx)
	if err != nil || payload == nil {
		return nil, err
	}
	return payload.PodSpec, nil
}

// loadRunningRevisionPayload returns the full revision.DataPayload
// (PodSpec + PodMeta + WorkerPodSpec). Used by inPlaceUpdate's
// annotation-reconciliation pass.
func loadRunningRevisionPayload(ctx context.Context, reads client.Reader, input workload.ReconcileInput, idx int32) (*revision.DataPayload, error) {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, idx)
	if s == nil || s.RunningRevision == "" {
		return nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	key := client.ObjectKey{Namespace: input.Key.Namespace, Name: s.RunningRevision}
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
// revision.DataPayload. Returns (nil, nil) when the CR is nil or its
// data is empty (benign mid-write edge case).
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

// DetectUpdateTrigger fires when the Instance is mid-update, or when
// Phase=Ready and a pod's PodSpec hashes to a non-target revision.
// Creating / Deleting / Restarting / Migrating are not interruptible.
// The Migrate-owned suppression is explicit because a just-promoted
// surge briefly carries RunningRevision=sourceRev while the post-
// promote scale-down pass runs — without the guard the spec-bump that
// triggered the migration would race the surge promote.
//
// Exported for the dispatcher (omenative/ops/dispatch.go) which iterates
// Instances and consults this before invoking Update.
func DetectUpdateTrigger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	if s == nil {
		return false, nil
	}
	if isMigrateOwnedStatus(s) {
		return false, nil
	}
	if s.Phase == workload.InstancePhaseUpdating {
		return true, nil
	}
	if s.Phase != workload.InstancePhaseReady {
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
	pods, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component)
	if err != nil {
		return false, err
	}
	pods = filterPodsByInstance(pods, inst.Index)
	for _, pod := range pods {
		if !podMatchesTarget(pod, targetSpec) {
			return true, nil
		}
	}
	// Backfill so future reconciles take the fast-path.
	if err := patchInstanceStatusReadyOnRevision(ctx, input, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("backfill RunningRevision (instance=%d): %w", inst.Index, err)
	}
	return false, nil
}

// isMigrateOwnedStatus reports whether the InstanceStatus carries an
// in-flight Migrate operation (either source-side or surge-side). The
// post-promote scale-down pass briefly leaves RunningRevision pointing
// at the source rev while the surge has already been promoted; without
// this guard a spec-bump arriving during that window would race the
// surge promote.
func isMigrateOwnedStatus(s *workload.InstanceStatus) bool {
	if s == nil {
		return false
	}
	if s.Phase == workload.InstancePhaseMigrating {
		return true
	}
	if s.Operation != nil && s.Operation.Type == workload.InstanceOperationMigrate {
		return true
	}
	return false
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
