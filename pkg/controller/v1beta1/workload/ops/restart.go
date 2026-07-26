package ops

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// RestartRequeueInterval is the wait between passes while a Restart
// is in flight. Exported so the dispatcher's pacing stays in
// lockstep.
const RestartRequeueInterval = 5 * time.Second

// DetectRestartTrigger fires when the Instance is mid-restart, or
// when Phase=Ready and a pod is Failed / the live pod count is below
// desired. Creating / Deleting are not interruptible — they finish
// first. A Migrate-owned Instance is suppressed because Migrate's
// source-pod deletion would otherwise trip the "pod count below
// desired" trigger on the source.
//
// Returns (needsRestart, reason, err). When needsRestart is true the
// reason lands on Operation.Reason on the first pass through Restart.
//
// Self-lists this Instance's pods live. The dispatcher's per-reconcile
// restart pass instead calls DetectRestartTriggerWithPods after a single
// live List + bucket, so a Component with N gangs costs one live List per
// reconcile instead of N — see that variant.
func DetectRestartTrigger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) (bool, string, error) {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	// Only Phase=Ready reaches the pod-set comparison; every other state
	// (absent, Migrate-owned, Restarting, or non-Ready) short-circuits in
	// WithPods without inspecting pods, so the live List is wasted there.
	if s == nil || isMigrateOwnedStatus(s) || s.Phase != workload.InstancePhaseReady {
		needs, reason := DetectRestartTriggerWithPods(input, plan, inst, nil)
		return needs, reason, nil
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, "", err
	}
	needs, reason := DetectRestartTriggerWithPods(input, plan, inst, pods)
	return needs, reason, nil
}

// DetectRestartTriggerWithPods is DetectRestartTrigger with this
// Instance's pods supplied by the caller — the dispatcher does a single
// per-Component live List + bucket once per reconcile and threads each
// Instance's slice here, instead of one live List per Instance.
// instancePods must already be filtered to inst.Index. Semantics are
// identical to DetectRestartTrigger; only the read source differs.
func DetectRestartTriggerWithPods(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, instancePods []*corev1.Pod) (bool, string) {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	if s == nil {
		return false, ""
	}
	if isMigrateOwnedStatus(s) {
		return false, ""
	}
	if s.Phase == workload.InstancePhaseRestarting {
		return true, ""
	}
	if s.Phase != workload.InstancePhaseReady {
		return false, ""
	}

	expected := inst.TotalPods()
	for _, pod := range instancePods {
		if pod.Status.Phase == corev1.PodFailed {
			// Build a richer reason from the failed pod's container
			// termination so the RestartTriggered event names the actual
			// cause (OOMKilled exit 137, CrashLoopBackOff, ...) instead of
			// the bare "pod X Failed". Falls back to the pod name when no
			// per-container detail is available.
			if t := workload.PodTermination(pod, metav1.NewTime(input.Now())); t != nil {
				return true, t.ShortString()
			}
			return true, fmt.Sprintf("pod %s Failed", pod.Name)
		}
	}
	if int32(len(instancePods)) < expected {
		return true, fmt.Sprintf("pod count %d below desired %d", len(instancePods), expected)
	}
	return false, ""
}

// restartDrainKey identifies a Restart drain writer against one Instance
// materialization. Same shape as the Update drain key, but the writer
// userAgent differs so the two can't cancel each other out.
func restartDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// Restart drives one Instance through Restart-on-pod-loss. Multi-pass,
// idempotent. Three phases:
//
//	A. Drain + delete every pod at the old incarnation.
//	B. Recreate the pod set at the bumped incarnation
//	   (distinguished by the ome.io/instance-incarnation label).
//	C. Wait for runtime ready, flip ome.io/serving=True, promote to Ready.
//
// reason lands on Operation.Reason on the first pass only; later passes
// preserve it via the skip-write guard.
//
// Returns done=true once the Instance is back at Phase=Ready with
// Operation=nil; done=false means the caller should requeue and resume.
//
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types — today the InferenceReplica adapter
// (inferencereplica.buildReconcileInput) wires this entry point.
func Restart(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, reason string) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}

	// Use the post-patch Incarnation, not inst.Incarnation — BuildPlan
	// ran against an earlier read.
	wasNotRestarting := true
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil && s.Phase == workload.InstancePhaseRestarting {
		wasNotRestarting = false
	}
	newInc, err := patchInstanceStatusRestarting(ctx, input, inst.Index, reason, plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("patch status Restarting (instance=%d): %w", inst.Index, err)
	}
	if wasNotRestarting {
		recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonRestartTriggered,
			"OMENative %s restart triggered: %s (incarnation=%d)",
			instanceKey(input.Key.Component, inst.Index), reason, newInc)
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: list pods (instance=%d): %w", inst.Index, err)
	}

	// On the first pass, preserve the failing pod's diagnostics into
	// InstanceStatus.LastFailure BEFORE Phase A drains and deletes it —
	// otherwise the container termination reason / exit code vanish with
	// the pod and a gang that keeps restarting leaves no failure trace.
	// Only a genuine pod-failure restart populates this; a "pod count
	// below desired" restart (the pod already vanished) finds nothing to
	// capture and leaves any prior LastFailure intact.
	if wasNotRestarting {
		if t := firstFailedPodTermination(pods); t != nil {
			if err := patchInstanceLastFailure(ctx, input, inst.Index, t); err != nil {
				return false, fmt.Errorf("record LastFailure (instance=%d): %w", inst.Index, err)
			}
		}
	}

	oldPods, newPods, unknownPods := query.PartitionPodsByIncarnation(pods, newInc)

	// Surface orphan pods (label-missing) as warnings and refuse to delete.
	// A stripped label or a third-party operator stamping the OMENative
	// managed-by label would otherwise get torn down by Phase A. Operator
	// must re-classify or delete the pod for Restart to proceed.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to delete",
				instanceKey(input.Key.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	// Phase A: drain + delete old pods.
	if len(oldPods) > 0 {
		for _, pod := range oldPods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, pod, podreadiness.WriterRestartDrain, restartDrainKey(inst.Index, newInc)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}

		// Live reader on drain check so kube-proxy isn't still routing.
		// Drain target is the per-revision *routed* Service. Read from
		// the pod's ome.io/revision-hash label (stamped by Render). The
		// per-Component headless Service publishes not-ready endpoints
		// so peer-discovery DNS resolves during gang init — using it
		// here would make IsPodDrained wait forever.
		//
		// Batcher lists each per-revision Service's EndpointSlices once
		// and reuses them across the gang, avoiding the N+1 per-pod LIST.
		drainer := drain.NewBatcher(deps.Reader(), input.Key.Namespace)
		for _, pod := range oldPods {
			hash := pod.Labels[query.LabelRevisionHash]
			if hash == "" {
				continue
			}
			serviceName := query.PerRevisionServiceName(input.Key.OwnerName, plan.Component, hash)
			drained, err := drainer.IsPodDrained(ctx, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}

		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase B: recreate at the bumped Incarnation.
	// Live-read the OLD pod set first — a stable-name Create with the
	// cache stale would AlreadyExists against the still-terminating
	// previous incarnation. Matches K8s foreground-deletion propagation.
	clear, err := query.LiveOldPodsClearedForRecreate(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("Restart: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	// Override Incarnation on the ad-hoc plan so Render stamps the new label.
	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(input, plan, newInst)
	existingByName := query.IndexPodsByName(newPods)
	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		if _, err := createMissingPods(ctx, deps, input, plan, newInst, inst.Index, missing, revisionHashForInstance(input, inst.Index)); err != nil {
			return false, err
		}
		return false, nil
	}

	// Phase C: flip serving, promote Ready.
	if !query.AllPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		// Lifecycle key on fresh pods; the Restart-drain key only lived
		// on the now-deleted OLD pods.
		if err := podreadiness.MarkPodServing(ctx, deps.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	if err := patchInstanceStatusReady(ctx, input, inst.Index); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRestartCompleted,
		"OMENative %s restart complete (incarnation=%d)",
		instanceKey(input.Key.Component, inst.Index), newInc)
	return true, nil
}

// patchInstanceStatusRestarting idempotently stamps Phase=Restarting +
// Restart/Drain with the given reason and increments Incarnation by one.
// Returns the post-write Incarnation; if a previous pass already moved
// into Restart, the existing Incarnation is preserved.
func patchInstanceStatusRestarting(ctx context.Context, input workload.ReconcileInput, idx int32, reason string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseRestarting &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationRestart {
			observedIncarnation = s.Incarnation
			return false
		}
		// Bump first so old pods can be distinguished from the new set.
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		observedIncarnation = s.Incarnation
		s.Phase = workload.InstancePhaseRestarting
		now := metav1.NewTime(input.Now())
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("restart-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationRestart,
			Step:           "Drain",
			Reason:         reason,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
	return observedIncarnation, err
}

// firstFailedPodTermination returns the InstanceTermination of the first
// pod that is Phase=Failed or carries any container-termination diagnostics
// worth preserving across the recreate. Returns nil when no pod yields a
// signal (e.g. the failing pod already vanished — the "pod count below
// desired" trigger). Order-deterministic by the input slice.
func firstFailedPodTermination(pods []*corev1.Pod) *workload.InstanceTermination {
	now := metav1.Now()
	// Prefer an explicitly Failed pod — that's the pod the trigger fired on.
	for _, pod := range pods {
		if pod != nil && pod.Status.Phase == corev1.PodFailed {
			if t := workload.PodTermination(pod, now); t != nil {
				return t
			}
		}
	}
	// Otherwise capture any pod showing a crash / wedge in its container
	// states (a not-yet-Failed-phase pod mid-CrashLoopBackOff still counts
	// as the cause when Restart was triggered for it).
	for _, pod := range pods {
		if pod == nil || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if t := workload.PodTermination(pod, now); t != nil {
			return t
		}
	}
	return nil
}

// patchInstanceLastFailure stamps InstanceStatus.LastFailure with the
// captured termination. Idempotent: a no-op when an identical record is
// already stored (same pod + reason + exit code), so a repeated first-pass
// (status conflict retry) doesn't churn the field.
func patchInstanceLastFailure(ctx context.Context, input workload.ReconcileInput, idx int32, t *workload.InstanceTermination) error {
	if t == nil {
		return nil
	}
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if sameTermination(s.LastFailure, t) {
			return false
		}
		captured := *t
		s.LastFailure = &captured
		return true
	})
}

// sameTermination reports whether two termination records carry the same
// operator-relevant identity (pod, container, reason, exit code). Time and
// Message are excluded so a re-capture that differs only in the recorded
// timestamp doesn't trigger a redundant status write.
func sameTermination(a, b *workload.InstanceTermination) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.PodName != b.PodName || a.ContainerName != b.ContainerName || a.Reason != b.Reason {
		return false
	}
	switch {
	case a.ExitCode == nil && b.ExitCode == nil:
		return true
	case a.ExitCode == nil || b.ExitCode == nil:
		return false
	default:
		return *a.ExitCode == *b.ExitCode
	}
}

// revisionHashForInstance returns the revision hash to stamp on pods
// being recreated for one Instance. Restart keeps the Instance on its
// existing revision, so we read RunningRevision from the observed
// status. Returns "" when no per-Instance running revision is recorded
// (initial-create paths shouldn't hit Restart, but the empty fallback
// keeps the function total).
func revisionHashForInstance(input workload.ReconcileInput, idx int32) string {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, idx)
	if s == nil {
		return ""
	}
	if s.RunningRevision != "" {
		return query.RevisionHashFromControllerRevisionName(s.RunningRevision)
	}
	if s.TargetRevision != "" {
		return query.RevisionHashFromControllerRevisionName(s.TargetRevision)
	}
	return ""
}
