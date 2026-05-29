package omenative

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// AggregateAndWriteStatus computes Component + per-Instance counters from
// the live pod set and EndpointSlices, stamps the desired revision, and
// (on full convergence) promotes UpdateRevision → CurrentRevision. Never
// overwrites lifecycle fields (Phase, Operation, Incarnation,
// RunningRevision, TargetRevision) — those belong to the per-op writers.
// target=nil (no rendered PodSpec; e.g. MinReplicas=0) leaves revision
// fields untouched; counters still run.
func AggregateAndWriteStatus(ctx context.Context, p ReconcileParams, plan ComponentPlan, target *appsv1.ControllerRevision) error {
	if p.Client == nil {
		return fmt.Errorf("AggregateAndWriteStatus: nil client")
	}

	pods, err := query.ListOMENativePods(ctx, p.Client, p.ISVC, plan.Component)
	if err != nil {
		return fmt.Errorf("status: list pods: %w", err)
	}
	byIndex := query.BucketPodsByInstanceIdx(pods)

	serviceName := query.HeadlessServiceName(p.ISVC.Name, plan.Component)
	availableByPod, err := availablePodSet(ctx, p.Reader(), p.ISVC.Namespace, serviceName)
	if err != nil {
		return fmt.Errorf("status: compute availability: %w", err)
	}

	// Re-read + recompute on each retry so a parallel reconciler that
	// added/removed an Instance doesn't get its bookkeeping clobbered.
	// Captured outside the closure so the post-commit retention sweep
	// sees the names that actually committed.
	var liveRevisionNames []string
	isvcGone := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// ISVC deleted during reconcile — nothing to aggregate
				// into. Drop the work cleanly to avoid retry storms.
				isvcGone = true
				return nil
			}
			return fmt.Errorf("status: re-read ISVC: %w", err)
		}

		if fresh.Status.Components == nil {
			fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
		}
		cs := fresh.Status.Components[plan.Component]
		if cs.OMENative == nil {
			cs.OMENative = &v1beta1.OMENativeComponentStatus{}
		}

		// Per-Instance counters and Conditions.
		for i := range cs.OMENative.InstanceStatuses {
			s := &cs.OMENative.InstanceStatuses[i]
			instPods := byIndex[s.Index]
			s.PodCount = int32(len(instPods))
			s.ReadyPodCount = countReadyPods(instPods)
			s.ServingPodCount = countServingPods(instPods)
			s.ScheduledPodCount = countScheduledPods(instPods)
			s.AvailablePodCount = countAvailablePods(instPods, availableByPod)
			s.NodesOccupied = uniqueNodes(instPods)
			status.ApplyInstanceConditions(s, status.ComputeInstanceConditions(s, fresh.Generation))
		}

		// Component-level counters.
		//
		// desiredByIdx maps each Instance index to the desired number of
		// canonical pods (the plan's Runner Size sum, NOT the observed
		// pod count). Surge transiently adds an extra pod per Instance —
		// the observed PodCount briefly equals desired+1. Counting
		// "instance is serving when ServingPodCount == PodCount" would
		// mis-classify mid-surge Instances as not-serving and drop
		// ServingReplicas / ReadyReplicas / AvailableReplicas, even
		// though the canonical pod is still in rotation. The fix:
		// classify an Instance based on whether desiredPodCount-many
		// pods meet the condition.
		desiredByIdx := desiredPodCountByInstance(plan)
		cs.OMENative.Replicas = int32(len(cs.OMENative.InstanceStatuses))
		cs.OMENative.ReadyReplicas = countReadyInstances(cs.OMENative.InstanceStatuses, desiredByIdx)
		cs.OMENative.ServingReplicas = countServingInstances(cs.OMENative.InstanceStatuses, desiredByIdx)
		cs.OMENative.AvailableReplicas = countAvailableInstances(cs.OMENative.InstanceStatuses, desiredByIdx)
		if target != nil {
			cs.OMENative.UpdatedReplicas, cs.OMENative.UpdatedReadyReplicas =
				countUpdatedInstances(cs.OMENative.InstanceStatuses, target.Name, desiredByIdx)
			cs.OMENative.UpdateRevision = target.Name
			if rolloutComplete(cs.OMENative.InstanceStatuses, target.Name) {
				cs.OMENative.CurrentRevision = target.Name
			}
		}

		cs.OMENative.ObservedGeneration = fresh.Generation
		cs.OMENative.LabelSelector = componentLabelSelectorString(p.ISVC.Name, plan.Component)

		// Component-level Conditions. Derived after counters so
		// AvailableReplicas, UpdateRevision, etc. are current.
		status.ApplyComponentConditions(cs.OMENative, status.ComputeComponentConditions(cs.OMENative, fresh.Generation))

		fresh.Status.Components[plan.Component] = cs

		// Top-level component-ready condition (EngineReady /
		// DecoderReady / RouterReady). Mirrors the contract
		// PropagateRawStatus / PropagateMultiNodeStatus apply for
		// Deployment / LWS so the ingress reconciler, aggregate Ready
		// rollup, and `kubectl wait --for=condition=Ready` treat
		// OMENative the same as those modes. Without it, the cluster
		// status carries Component-internal OMENative.* counters but
		// never flips the top-level condition past InitializeComponent
		// Condition's False/Initializing — and the outer reconciler's
		// updateStatus flush stamps Ready=False forever.
		topCond := topLevelComponentReadyFromOMENative(plan.Component, cs.OMENative)
		if topCond != nil {
			fresh.Status.SetCondition(topCond.Type, topCond)
		}

		if err := p.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				isvcGone = true
				return nil
			}
			return fmt.Errorf("status: update: %w", err)
		}
		// Capture after a successful Update so a retry replays the names
		// off the post-conflict re-read.
		liveRevisionNames = core.CollectLiveRevisionNamesForISVC(cs.OMENative)

		// Mirror the top-level component-ready condition onto the
		// cached ISVC the parent reconciler is holding. The outer
		// updateStatus flush copies p.ISVC.Status onto a fresh re-read
		// of the cluster — without this mirror, the value we just
		// committed to the cluster gets clobbered next time the parent
		// flushes (which it does every reconcile).
		if topCond != nil {
			p.ISVC.Status.SetCondition(topCond.Type, topCond)
		}
		// Mirror the just-committed Component-level revision fields
		// (UpdateRevision, CurrentRevision) onto the cached ISVC the
		// caller is reading. Same shape as PR #246's MutateInstance
		// mirror and bumpCollisionCount's CollisionCount mirror in
		// reconciler.go: any downstream caller that reads
		// p.ISVC.Status.Components[X].OMENative within the same
		// reconcile (coordination's latestRevisionHash, ops/create.go's
		// revisionHashForInstance, etc.) sees the post-write state, not
		// the stale snapshot from when the reconcile started.
		// Without the mirror, a future code path that fires AFTER
		// AggregateAndWriteStatus would observe a stale
		// UpdateRevision / CurrentRevision and could re-compute a
		// downstream hash off the wrong value, surfacing as the A-2
		// orphan-hash drift symptom (status field references a CR
		// that no longer matches what the controller just decided).
		mirrorComponentRevisionFields(p.ISVC, plan.Component, cs.OMENative)
		return nil
	})
	if isvcGone {
		return nil
	}
	if err != nil {
		return err
	}

	// Fast-failure escalation: any Instance currently in a transient
	// phase (Creating / Updating / Restarting / Migrating) whose pods
	// have been stuck in a non-recoverable kubelet waiting state
	// (ImagePullBackOff / ErrImagePull / InvalidImageName /
	// CreateContainerError / CrashLoopBackOff / RunContainerError) past
	// the stuck-pod grace window gets escalated to Phase=Failed
	// immediately, without waiting for the (default 30 min)
	// per-Instance Operation deadline. This is the operator-visible fix
	// for the SurgeThenDrain bad-image scenario — bumping a Component
	// to a missing image tag now surfaces Component.Failed within
	// seconds of the kubelet stabilizing on the terminal reason, not
	// 30+ minutes later. The slow Deadline path in ops/timeouts.go is
	// the backstop for other op-step wedges (drain races, runtime
	// hangs, etc.) where there's no kubelet signal to act on early.
	if eerr := escalateStuckPodFailures(ctx, p, plan, byIndex); eerr != nil {
		return fmt.Errorf("status: escalate stuck pod failures: %w", eerr)
	}
	// Component-level Conditions read InstanceStatus.Phase; the
	// escalator above may have just flipped one to Failed. Re-run the
	// Component-level condition compute against the post-escalation
	// state so the same-reconcile-pass condition write reflects reality
	// — without the refresh, Progressing stays True/RolloutInProgress
	// until the NEXT reconcile pass, hiding the failure from operators
	// who page on `kubectl wait --for=condition=Progressing=False`.
	// No-op when nothing escalated: the closure returns false on
	// idempotent passes and the apiserver write is skipped.
	if cerr := refreshComponentConditionsPostEscalation(ctx, p, plan); cerr != nil {
		return fmt.Errorf("status: refresh conditions post-escalation: %w", cerr)
	}

	// Retention runs post-commit. The liveNames union ensures in-flight
	// migrations whose RunningRevision points to an older CR aren't
	// retention-deleted out from under them.
	if rerr := core.RetainControllerRevisionsForISVC(
		ctx,
		p.Client,
		p.Reader(),
		p.ISVC,
		plan.Component,
		defaultRevisionRetention,
		liveRevisionNames...,
	); rerr != nil {
		return fmt.Errorf("status: retain revisions: %w", rerr)
	}
	return nil
}

// StuckPodFailureGracePeriod is the wait window after a pod's
// CreationTimestamp before a terminal kubelet waiting state
// (ImagePullBackOff, ErrImagePull, InvalidImageName,
// CreateContainerConfigError, CreateContainerError) escalates the
// owning Instance to Phase=Failed.
//
// Tests override the value to keep runtime sane (a hardcoded constant
// would force a 60-second wait per case). Production callers leave it
// at the default. The omenative-side var is the single source of
// truth — both the legacy in-package escalator wrapper and the
// workload-side escalator (via the closure below) read it on each
// call.
var StuckPodFailureGracePeriod = 60 * time.Second

// escalateStuckPodFailures is the ISVC-side adapter that bridges the
// legacy AggregateAndWriteStatus body to the workload-side
// EscalateStuckPodFailures. Builds a workload.ReconcileInput just for
// the escalation step (MutateInstance + WarnInstanceFailed) and
// delegates the policy logic to the workload package.
//
// The two-stage build keeps the BIG mirror retry loop in this file
// (the apiserver round-trip for counters / conditions / top-level
// EngineReady is still ISVC-shape) while routing the
// adapter-agnostic escalator through the shared workload helper.
//
// Reads byIndex (the per-Instance live pod buckets the caller already
// computed for counters) so we don't re-list pods. Walks
// p.ISVC.Status.Components after the AggregateAndWriteStatus retry has
// committed, so the InstanceStatus.Phase we read is the post-counters
// version (no pre-commit / post-commit race).
func escalateStuckPodFailures(ctx context.Context, p ReconcileParams, plan ComponentPlan, byIndex map[int32][]*corev1.Pod) error {
	if p.ISVC == nil {
		return nil
	}
	cs, ok := p.ISVC.Status.Components[plan.Component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	// Override the workload-side grace period with the ISVC-side var
	// for the duration of this call. Tests bind the omenative-side var
	// via withStuckPodGracePeriod (writes restored by t.Cleanup), and
	// the workload-side default would otherwise force the 60-second
	// production grace through.
	prevWorkloadGrace := workload.StuckPodFailureGracePeriod
	workload.StuckPodFailureGracePeriod = StuckPodFailureGracePeriod
	defer func() { workload.StuckPodFailureGracePeriod = prevWorkloadGrace }()

	// Bridge the v1beta1-typed status surface into a workload-typed
	// ReconcileInput. The MutateInstance closure is what the
	// workload-side StampInstanceFailedForStuckPod calls; it round-trips
	// the workload.InstanceStatus mutation back to v1beta1 via the
	// existing status.MutateInstance helper. WarnInstanceFailed wraps
	// the legacy status.RecordWarning emitter. The remaining callbacks
	// aren't exercised by EscalateStuckPodFailures but satisfy the
	// panic-on-nil contract documented on workload.ReconcileInput.
	input := workload.ReconcileInput{
		OwnerObject: p.ISVC,
		EventTarget: p.ISVC,
		MutateInstance: func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			return status.MutateInstance(ctx, p.Client, p.ISVC, plan.Component, idx, func(v *v1beta1.OMENativeInstanceStatus) bool {
				w := internalsource.InstanceStatusToWorkload(*v)
				if !mutate(&w) {
					return false
				}
				*v = internalsource.InstanceStatusFromWorkload(w)
				return true
			})
		},
		RemoveInstance:          func(_ context.Context, _ int32) (bool, error) { return false, nil },
		WriteAggregateCondition: func(_ context.Context, _ metav1.Condition) error { return nil },
		ClearMigrationRequest:   func(_ context.Context, _ string) error { return nil },
		WarnInstanceFailed: func(idx int32, podName, reason string) {
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonInstanceFailed,
				"OMENative %s escalated to Phase=Failed: pod %s stuck in %s (no kubelet self-recovery)",
				status.InstanceKey(plan.Component, idx), podName, reason)
		},
	}

	return workload.EscalateStuckPodFailures(ctx, input, workload.EscalationParams{
		Instances:           internalsource.InstanceStatusSliceToWorkload(cs.OMENative.InstanceStatuses),
		PodsByIndex:         byIndex,
		CurrentRevisionHash: query.RevisionHashFromControllerRevisionName(cs.OMENative.CurrentRevision),
		RevisionHashLabel:   query.LabelRevisionHash,
	})
}

// refreshComponentConditionsPostEscalation re-runs ComputeComponentConditions
// against the post-escalation Component status and writes the result
// when the desired condition set differs from what's already there.
// The escalator above may have flipped an Instance Phase from
// transient to Failed; the Component-level Progressing / Available
// conditions read InstanceStatus.Phase to derive their values, so
// without this refresh the conditions written by the AggregateAndWriteStatus
// retry loop reflect the pre-escalation state and operators see
// `Progressing=True/RolloutInProgress` on a rollout that has terminally
// failed (bug #52).
//
// Fast-path skip: the escalator mirrors any Phase mutation back onto
// p.ISVC via status.MutateInstance. We read off the in-memory cache
// first to compute the desired condition set; if it matches what's
// already on the Component, we skip the apiserver round-trip entirely.
// The happy path (no stuck pods) is therefore zero-allocation +
// zero-network past the in-memory equality check.
//
// On a real escalation (Phase=Failed lands), the helper does ONE
// apiserver Get + Update via retry.RetryOnConflict, mirroring the
// post-write Component status onto p.ISVC so coordination.Reconcile
// in controller.go reads the fresh conditions instead of the stale
// pre-escalation slice.
func refreshComponentConditionsPostEscalation(ctx context.Context, p ReconcileParams, plan ComponentPlan) error {
	if p.ISVC == nil || p.Client == nil {
		return nil
	}
	// Fast-path: compute against the in-memory mirror first. The
	// escalator's MutateInstance has already mirrored Phase=Failed
	// back to p.ISVC, so this compute reflects post-escalation state
	// without any apiserver call.
	cached, ok := p.ISVC.Status.Components[plan.Component]
	if !ok || cached.OMENative == nil {
		return nil
	}
	desired := status.ComputeComponentConditions(cached.OMENative, p.ISVC.Generation)
	if conditionSetUnchanged(cached.OMENative.Conditions, desired) {
		// Escalator was a no-op (no pods stuck) OR escalator fired but
		// nothing meaningful changed on the Component conditions
		// (e.g., already in Failed state from a prior pass). Either
		// way, the in-memory cache matches what the retry loop wrote,
		// so the apiserver state matches too. Skip the round-trip.
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// ISVC deleted mid-reconcile — nothing to refresh.
				return nil
			}
			return fmt.Errorf("re-read ISVC for post-escalation refresh: %w", err)
		}
		if fresh.Status.Components == nil {
			return nil
		}
		cs, ok := fresh.Status.Components[plan.Component]
		if !ok || cs.OMENative == nil {
			return nil
		}
		// Recompute against fresh state in case a peer reconcile bumped
		// counters between the cached read and now.
		fresherDesired := status.ComputeComponentConditions(cs.OMENative, fresh.Generation)
		if conditionSetUnchanged(cs.OMENative.Conditions, fresherDesired) {
			return nil
		}
		status.ApplyComponentConditions(cs.OMENative, fresherDesired)
		fresh.Status.Components[plan.Component] = cs
		if err := p.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("write refreshed component conditions: %w", err)
		}
		// Mirror onto the caller's in-memory ISVC so coordination.Reconcile
		// (which runs after every per-Component reconciler in
		// controller.go) reads the fresh conditions instead of the
		// stale pre-escalation slice.
		if p.ISVC.Status.Components == nil {
			p.ISVC.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
		}
		mirror := p.ISVC.Status.Components[plan.Component]
		if mirror.OMENative == nil {
			mirror.OMENative = &v1beta1.OMENativeComponentStatus{}
		}
		mirror.OMENative.Conditions = append(mirror.OMENative.Conditions[:0], cs.OMENative.Conditions...)
		p.ISVC.Status.Components[plan.Component] = mirror
		return nil
	})
}

// conditionSetUnchanged reports whether desired's condition Type+Status+
// Reason set is a subset of existing. ApplyComponentConditions uses
// apimeta.SetStatusCondition under the hood, which only touches a
// condition when one of those three fields changes — so this comparison
// matches the "would the write actually mutate anything" predicate.
//
// Message / ObservedGeneration / LastTransitionTime intentionally are
// not compared: SetStatusCondition treats those as derived. Comparing
// them would force a write on every reconcile even when the meaningful
// state is unchanged, defeating the idempotency goal.
func conditionSetUnchanged(existing, desired []metav1.Condition) bool {
	for _, want := range desired {
		var matched bool
		for _, have := range existing {
			if have.Type != want.Type {
				continue
			}
			if have.Status == want.Status && have.Reason == want.Reason {
				matched = true
			}
			break
		}
		if !matched {
			return false
		}
	}
	return true
}

// defaultRevisionRetention is the legacy alias for the workload-side
// constant. Both names resolve to the same value (10) so callers
// importing either one see the same retention cap.
const defaultRevisionRetention = workload.DefaultRevisionRetention

// availablePodSet is the legacy adapter-shape wrapper around
// workload.AvailablePodSet. New callers should use the workload
// helper directly; this thin wrapper exists so the ISVC-side
// AggregateAndWriteStatus body keeps its existing call site.
func availablePodSet(ctx context.Context, reads client.Reader, namespace, serviceName string) (map[string]struct{}, error) {
	return workload.AvailablePodSet(ctx, reads, namespace, serviceName)
}

// countReadyPods / countServingPods / countScheduledPods /
// countAvailablePods / uniqueNodes are unexported aliases that delegate
// to the workload-side counters. Kept as adapter-package wrappers so
// the in-package status_aggregate.go body keeps its existing call sites
// (and so the per-Instance loop reads a single-line counter for
// readability).
func countReadyPods(pods []*corev1.Pod) int32 { return workload.CountReadyPods(pods) }
func countServingPods(pods []*corev1.Pod) int32 {
	return workload.CountServingPods(pods)
}
func countScheduledPods(pods []*corev1.Pod) int32 {
	return workload.CountScheduledPods(pods)
}
func countAvailablePods(pods []*corev1.Pod, availableByName map[string]struct{}) int32 {
	return workload.CountAvailablePods(pods, availableByName)
}
func uniqueNodes(pods []*corev1.Pod) []string { return workload.UniqueNodes(pods) }

// desiredPodCountByInstance maps each planned Instance index to its
// desired canonical pod count (sum of Runner sizes). Delegates to the
// workload-side helper with the legacy ComponentPlan shape; the
// underlying logic is shared with the IR adapter.
func desiredPodCountByInstance(plan ComponentPlan) map[int32]int32 {
	return workload.DesiredPodCountByInstance(plan)
}

// countReadyInstances / countServingInstances / countAvailableInstances /
// countUpdatedInstances are ISVC-shape wrappers that round-trip the
// v1beta1 InstanceStatus slice through workload.InstanceStatus before
// delegating to the workload-side counters. The conversion is
// field-for-field and round-trips cleanly; per-Instance counters are the
// only fields the workload helpers read so a partial conversion would
// still produce the correct result, but the full converter keeps the
// boundary contract uniform.
func countReadyInstances(insts []v1beta1.OMENativeInstanceStatus, desiredByIdx map[int32]int32) int32 {
	return workload.CountReadyInstances(internalsource.InstanceStatusSliceToWorkload(insts), desiredByIdx)
}

func countServingInstances(insts []v1beta1.OMENativeInstanceStatus, desiredByIdx map[int32]int32) int32 {
	return workload.CountServingInstances(internalsource.InstanceStatusSliceToWorkload(insts), desiredByIdx)
}

func countAvailableInstances(insts []v1beta1.OMENativeInstanceStatus, desiredByIdx map[int32]int32) int32 {
	return workload.CountAvailableInstances(internalsource.InstanceStatusSliceToWorkload(insts), desiredByIdx)
}

func countUpdatedInstances(insts []v1beta1.OMENativeInstanceStatus, targetRevName string, desiredByIdx map[int32]int32) (updated, updatedReady int32) {
	return workload.CountUpdatedInstances(internalsource.InstanceStatusSliceToWorkload(insts), targetRevName, desiredByIdx)
}

// rolloutComplete is the CurrentRevision promotion gate: every Instance
// on targetRevName AND Phase=Ready. ISVC-shape wrapper around
// workload.RolloutComplete.
func rolloutComplete(insts []v1beta1.OMENativeInstanceStatus, targetRevName string) bool {
	return workload.RolloutComplete(internalsource.InstanceStatusSliceToWorkload(insts), targetRevName)
}

// componentLabelSelectorString is the canonical "k=v,k=v" selector
// string consumed by HPA via the Component's status.labelSelector.
func componentLabelSelectorString(isvc string, component v1beta1.ComponentType) string {
	return labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}).String()
}

// topLevelComponentReadyFromOMENative derives the standard top-level
// component-ready condition (EngineReady / DecoderReady / RouterReady)
// from OMENative counters. Returns nil for unknown Component types so
// callers leave the condition surface untouched.
//
// Ready=True iff ReadyReplicas == Replicas > 0 — i.e., every desired
// Instance has all its pods ContainersReady. Mirrors the contract
// PropagateRawStatus uses (Deployment.Available) so the ingress
// reconciler, aggregate Ready rollup, and `kubectl wait
// --for=condition=Ready` treat OMENative the same as RawDeployment /
// MultiNode.
func topLevelComponentReadyFromOMENative(component v1beta1.ComponentType, cs *v1beta1.OMENativeComponentStatus) *apis.Condition {
	var readyType apis.ConditionType
	switch component {
	case v1beta1.EngineComponent:
		readyType = v1beta1.EngineReady
	case v1beta1.DecoderComponent:
		readyType = v1beta1.DecoderReady
	case v1beta1.RouterComponent:
		readyType = v1beta1.RouterReady
	default:
		return nil
	}
	cond := &apis.Condition{Type: readyType}
	switch {
	case cs == nil || cs.Replicas == 0:
		cond.Status = corev1.ConditionFalse
		cond.Reason = "NoReplicas"
		cond.Message = "Component has no desired Instances"
	case cs.ReadyReplicas == cs.Replicas:
		cond.Status = corev1.ConditionTrue
		cond.Reason = "Ready"
		cond.Message = "All Instances are Ready"
	default:
		cond.Status = corev1.ConditionFalse
		cond.Reason = "NotAllReady"
		cond.Message = fmt.Sprintf("%d/%d Instances ready", cs.ReadyReplicas, cs.Replicas)
	}
	return cond
}

// mirrorComponentRevisionFields copies UpdateRevision + CurrentRevision
// from the just-committed OMENativeComponentStatus onto the caller's
// in-memory ISVC snapshot. The dispatcher and the parent reconciler can
// then observe the post-write revision pointers within the same
// reconcile pass, instead of the stale pre-reconcile values.
//
// Pairs with the mirror in status/mutate.go's MutateInstance (per-Instance
// fields) and reconciler.go's bumpCollisionCount (CollisionCount):
// every Component-level field AggregateAndWriteStatus owns lands back
// onto p.ISVC, so downstream callers reading p.ISVC.Status.Components[X]
// see the canonical post-write shape.
//
// Creates the Components map / OMENative sub-status on demand so the
// caller doesn't need to pre-allocate when this is the first
// AggregateAndWriteStatus pass for a brand-new Component.
//
// Bug context: the A-2 SurgeThenDrain orphan-hash symptom (task #56)
// observed UpdateRevision and CurrentRevision pointing at hashes with
// no backing ControllerRevision. The most likely fault mode is a
// downstream caller that reads UR/CR from p.ISVC (stale, pre-write)
// while AggregateAndWriteStatus had already written different values to
// the apiserver — the parent reconciler's updateStatus flush then
// commits the stale in-memory values via desiredService.Status,
// overwriting the OMENative reconciler's write. The parent's
// mergeOMENativeComponentStatus restores the OMENative substatus from
// the post-conflict re-read, so in practice this race is closed there
// — but mirroring the values onto p.ISVC also closes the in-reconcile
// observability gap and matches the precedent set by PR #246 for
// per-Instance fields.
func mirrorComponentRevisionFields(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, src *v1beta1.OMENativeComponentStatus) {
	if isvc == nil || src == nil {
		return
	}
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[component]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}
	cs.OMENative.UpdateRevision = src.UpdateRevision
	cs.OMENative.CurrentRevision = src.CurrentRevision
	isvc.Status.Components[component] = cs
}
