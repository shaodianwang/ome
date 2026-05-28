package omenative

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/revision"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
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
		liveRevisionNames = revision.CollectLiveRevisionNames(cs.OMENative)

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
	// CreateContainerError) past the stuck-pod grace window gets
	// escalated to Phase=Failed immediately, without waiting for the
	// (default 30 min) per-Instance Operation deadline. This is the
	// operator-visible fix for the SurgeThenDrain bad-image scenario
	// — bumping a Component to a missing image tag now surfaces
	// Component.Failed within seconds of the kubelet stabilizing on
	// ImagePullBackOff, not 30+ minutes later. The slow Deadline path
	// in ops/timeouts.go is the backstop for other op-step wedges
	// (drain races, runtime hangs, etc.) where there's no kubelet
	// signal to act on early.
	if eerr := escalateStuckPodFailures(ctx, p, plan, byIndex); eerr != nil {
		return fmt.Errorf("status: escalate stuck pod failures: %w", eerr)
	}

	// Retention runs post-commit. The liveNames union ensures in-flight
	// migrations whose RunningRevision points to an older CR aren't
	// retention-deleted out from under them.
	if rerr := revision.RetainControllerRevisions(
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
// CreateContainerConfigError, CreateContainerError) escalates the owning
// Instance to Phase=Failed. Tuned to debounce the transient
// ContainerCreating / ImagePulling states that a real kubelet briefly
// passes through on initial pod placement — 60 seconds is comfortable
// headroom for a 5–10 second pull cycle on a healthy node before
// committing to "this is genuinely stuck."
//
// Exported so tests can lower the value to keep test runtime sane while
// still exercising the real escalation path (a hardcoded constant would
// force tests to spin for 60+ seconds per case, gating the suite on a
// wall-clock that adds nothing to coverage).
var StuckPodFailureGracePeriod = 60 * time.Second

// escalateStuckPodFailures walks every InstanceStatus and escalates to
// Phase=Failed when one of two conditions holds:
//
//  1. (Transient-phase path, the original PR #257 surface) The Instance
//     has an in-flight Operation AND its Phase is one of {Creating,
//     Updating, Restarting, Migrating} AND at least one of its pods has
//     been in a terminal kubelet waiting state past
//     StuckPodFailureGracePeriod.
//
//  2. (Wedged-pod recovery path, task #52 follow-up) The Instance has
//     at least one pod whose ome.io/revision-hash label disagrees with
//     the Component's CurrentRevision hash AND that pod is stuck past
//     the grace window. This catches the post-surge cluster shape where
//     the per-Instance state machine has been reset out of the transient
//     phase set but a real wedged surge pod still exists (the X-1
//     53-minute reproduction against the dev cluster). The
//     CurrentRevision compare is the discriminator vs. the steady-state
//     "operator manually broke the canonical pod" scenario, where the
//     pod's hash equals CurrentRevision and there is nothing to
//     escalate (no rollout in flight).
//
// Idempotent: a second pass on an already-Failed Instance is a no-op
// (no status write, no event). The coordination layer's
// buildComponentObservation sees the Phase=Failed flip on the next
// read and lifts it to Component.Failed → group Phase=Failed.
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
	now := time.Now()
	// currentHash is the steady-state pin: pods carrying this hash are
	// on the controller's CurrentRevision, so even if they're stuck the
	// wedge is not driven by an in-flight rollout — only pods that
	// disagree with it qualify for the wedged-pod recovery branch.
	// Empty when no rollout has ever promoted; the wedged branch then
	// gates on "pod has a hash AND it doesn't match (the empty current
	// hash)" which would over-fire. Treat empty CurrentRevision as
	// "every pod hash matches current" for that branch (safe default —
	// the original transient-phase guard still fires for genuine
	// in-flight cases).
	currentHash := query.RevisionHashFromControllerRevisionName(cs.OMENative.CurrentRevision)
	for _, s := range cs.OMENative.InstanceStatuses {
		if !shouldCheckForStuckPods(&s, byIndex[s.Index], currentHash) {
			continue
		}
		stuckPod, reason := firstStuckPodForInstance(byIndex[s.Index], now)
		if stuckPod == nil {
			continue
		}
		// Idempotent: skip the mutate when already at Phase=Failed so
		// the operator-facing event fires exactly once per escalation.
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			continue
		}
		if err := stampInstanceFailedForStuckPod(ctx, p, plan.Component, s.Index); err != nil {
			return fmt.Errorf("stamp Phase=Failed (instance=%d): %w", s.Index, err)
		}
		status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonInstanceFailed,
			"OMENative %s escalated to Phase=Failed: pod %s stuck in %s (no kubelet self-recovery)",
			status.InstanceKey(plan.Component, s.Index), stuckPod.Name, reason)
	}
	return nil
}

// shouldCheckForStuckPods reports whether s qualifies for the
// fast-failure check. Two paths qualify:
//
//   - Transient-phase path: an Instance with an in-flight Operation in
//     the {Creating, Updating, Restarting, Migrating} set. This is the
//     in-flight surge / create / migrate / restart surface PR #257
//     originally added.
//
//   - Wedged-pod recovery path: an Instance with at least one pod
//     whose ome.io/revision-hash label disagrees with the Component's
//     CurrentRevision hash (currentHash). Regardless of Phase /
//     Operation, the controller's bookkeeping has lost the trail of a
//     real in-flight pod — the surge-attempt evidence on the cluster is
//     the only remaining signal. Catches the task-#52 X-1 reproduction
//     (post-#257 cluster shape where the Instance is Phase=Ready /
//     Operation=nil but a stuck surge pod at the new revision still
//     exists).
//
// Skips Failed (already done) and Deleting (Delete pass owns its own
// deadline). Skips empty currentHash (the no-rollout-ever case) for
// the wedged branch to avoid over-firing during initial Create.
func shouldCheckForStuckPods(s *v1beta1.OMENativeInstanceStatus, pods []*corev1.Pod, currentHash string) bool {
	if s == nil {
		return false
	}
	// Terminal phases are settled; no escalation is meaningful.
	if s.Phase == v1beta1.OMENativeInstanceFailed || s.Phase == v1beta1.OMENativeInstanceDeleting {
		return false
	}
	// Transient-phase path: in-flight Operation in a recoverable phase.
	if s.Operation != nil {
		switch s.Phase {
		case v1beta1.OMENativeInstanceCreating,
			v1beta1.OMENativeInstanceUpdating,
			v1beta1.OMENativeInstanceRestarting,
			v1beta1.OMENativeInstanceMigrating:
			return true
		}
	}
	// Wedged-pod recovery path: any pod with a hash that disagrees
	// with the Component's CurrentRevision is a candidate for the
	// stuck-pod check below, regardless of Phase / Operation. Empty
	// currentHash means the Component has never promoted a revision
	// (initial Create, no UpdateRevision yet) — the transient-phase
	// path covers that case and we don't second-guess it here.
	if currentHash == "" {
		return false
	}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" {
			continue
		}
		if hash != currentHash {
			return true
		}
	}
	return false
}

// firstStuckPodForInstance returns the first pod in pods that is stuck
// in a terminal kubelet waiting state past the grace window, plus the
// kubelet reason string. Returns (nil, "") when no pod is stuck —
// caller skips escalation.
//
// "First" is order-deterministic by the input slice; callers that need
// a stable index should sort pods before calling. For event emission
// the order doesn't matter (the message names the specific pod).
func firstStuckPodForInstance(pods []*corev1.Pod, now time.Time) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		// Skip pods marked for deletion — kubelet may be tearing them
		// down, and "ImagePullBackOff on a deleting pod" is not an
		// escalation signal (the pod is about to be gone anyway).
		if pod.DeletionTimestamp != nil {
			continue
		}
		if stuck, reason := status.PodStuckPullFailure(pod, now, StuckPodFailureGracePeriod); stuck {
			return pod, reason
		}
	}
	return nil, ""
}

// stampInstanceFailedForStuckPod is the status-side counterpart to
// ops/timeouts.go's stampInstanceFailed — escalates Phase to Failed
// when a stuck pod past grace forces the controller to give up on
// converging the Instance. The Operation field is preserved (when
// present) — operators want to see WHAT was in flight when the kubelet
// wedge surfaced.
//
// Guards against resurrecting a deliberately-removed InstanceStatus:
// MutateInstance re-fetches and appends an empty slot
// (OMENativeInstanceStatus{Index: idx}) when the (component, idx) entry
// is gone (e.g., the migrate-abandon path's surge cleanup). A fresh
// slot has Phase=="" and Incarnation==0 — we use Phase=="" as the
// "this is a brand-new empty slot, not a real Instance" sentinel so
// the wedged-pod recovery path (which legitimately has Operation==nil
// but Phase=Ready) is still allowed to escalate. The original
// transient-phase path's Operation==nil shape was always indistinguishable
// from the resurrected-slot shape, so removing the Operation guard
// would have over-fired; gating on Phase=="" instead is the narrower
// "is this a real Instance with prior status?" check that also handles
// the new wedged-pod branch correctly.
//
// The OperationStuck condition stays False here (only the Deadline path
// stamps OperationStuck=True via Operation.Reason=DeadlineExceeded —
// the stuck-pod path is a different failure mode and gets its own
// surface via the InstanceFailed event + Phase=Failed alone).
func stampInstanceFailedForStuckPod(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == "" {
			// Fresh-empty slot from MutateInstance's append path — the
			// real InstanceStatus was deleted out from under us
			// (migrate-abandon cleanup, etc.). Don't resurrect.
			return false
		}
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceFailed
		return true
	})
}

// defaultRevisionRetention caps non-live ControllerRevisions per
// Component (StatefulSet default; rollback headroom without unbounded
// etcd growth).
const defaultRevisionRetention = 10

// availablePodSet returns pod names that are Ready and non-terminating
// in any EndpointSlice for serviceName. drain.EndpointAvailable excludes
// terminating-but-Ready endpoints — they're not getting new traffic.
// Single slice list (vs. drain.IsPodInRotation per pod) avoids N+1 reads.
func availablePodSet(ctx context.Context, reads client.Reader, namespace, serviceName string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	slices, err := drain.EndpointSlicesForService(ctx, reads, namespace, serviceName)
	if err != nil {
		return nil, err
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Kind != "" && ep.TargetRef.Kind != "Pod" {
				continue
			}
			if !drain.EndpointAvailable(ep) {
				continue
			}
			out[ep.TargetRef.Name] = struct{}{}
		}
	}
	return out, nil
}

func countReadyPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if podreadiness.IsContainersReady(p) {
			n++
		}
	}
	return n
}

// countServingPods counts pods that are BOTH ContainersReady AND have
// the controller's serving gate set to True — i.e., pods actually in
// the load-balancer rotation. This is the count MaxUnavailable budgets
// in coordination/ratio.go gate against; ContainersReady alone misses
// the case where the controller has flipped serving=False (in-place
// update drain, recreate Phase A) while containers technically remain
// Ready.
func countServingPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if podreadiness.IsContainersReady(p) && podreadiness.IsServing(p) {
			n++
		}
	}
	return n
}

func countScheduledPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if p.Spec.NodeName != "" {
			n++
		}
	}
	return n
}

func countAvailablePods(pods []*corev1.Pod, availableByName map[string]struct{}) int32 {
	var n int32
	for _, p := range pods {
		if _, ok := availableByName[p.Name]; ok {
			n++
		}
	}
	return n
}

func uniqueNodes(pods []*corev1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, p := range pods {
		if p.Spec.NodeName == "" {
			continue
		}
		seen[p.Spec.NodeName] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// desiredPodCountByInstance maps each planned Instance index to its
// desired canonical pod count (sum of Runner sizes). Returns nil for
// an empty plan so callers fall back to the observed PodCount
// comparison (pre-existing behavior for Instances absent from the
// plan — e.g., during scale-down with stale status entries).
func desiredPodCountByInstance(plan ComponentPlan) map[int32]int32 {
	if len(plan.Instances) == 0 {
		return nil
	}
	out := make(map[int32]int32, len(plan.Instances))
	for _, inst := range plan.Instances {
		out[inst.Index] = query.TotalPodsForInstance(inst)
	}
	return out
}

// desiredFor returns the desired pod count for idx — the plan entry's
// value when present, otherwise the observed PodCount (no plan info ⇒
// use the observed count, matching pre-fix behavior for stale-status
// scale-down Instances).
func desiredFor(desiredByIdx map[int32]int32, idx int32, observedPodCount int32) int32 {
	if desiredByIdx == nil {
		return observedPodCount
	}
	if d, ok := desiredByIdx[idx]; ok {
		return d
	}
	return observedPodCount
}

// instanceMeetsThreshold is the shared classifier the count*Instances
// helpers share: an Instance "counts" when at least its desired pod
// count of pods satisfy the relevant predicate (Ready / Serving /
// Available). Differs from the pre-fix `observed == PodCount`
// comparison: SurgeThenDrain transiently puts an extra pod alive on
// the Instance, lifting PodCount above desired. The old comparison
// would mis-classify mid-surge Instances as not-Ready / not-Serving
// the whole time the surge pod is starting up — that's the user-
// visible serving dip seen in PR #238 sampler runs.
//
// The desired value falls back to observedPodCount when the plan
// has no entry for this index (scale-down with stale status).
func instanceMeetsThreshold(observedPodCount, observedSatisfying, desired int32) bool {
	if observedPodCount == 0 {
		return false
	}
	if desired <= 0 {
		return observedSatisfying == observedPodCount
	}
	return observedSatisfying >= desired
}

// countReadyInstances counts Instances whose desired-many pods are
// ContainersReady=True. See instanceMeetsThreshold for the
// surge-tolerant semantic.
func countReadyInstances(insts []v1beta1.OMENativeInstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := desiredFor(desiredByIdx, s.Index, s.PodCount)
		if instanceMeetsThreshold(s.PodCount, s.ReadyPodCount, desired) {
			n++
		}
	}
	return n
}

// countServingInstances counts Instances whose desired-many pods are
// BOTH ContainersReady AND serving=True. This is the count
// CheckUnavailabilityGate uses — "instances actually in the load
// balancer rotation right now."
func countServingInstances(insts []v1beta1.OMENativeInstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := desiredFor(desiredByIdx, s.Index, s.PodCount)
		if instanceMeetsThreshold(s.PodCount, s.ServingPodCount, desired) {
			n++
		}
	}
	return n
}

// countAvailableInstances counts Instances whose desired-many pods
// are in EndpointSlice rotation. Strict sub-condition of Ready —
// kube-proxy only publishes Ready pods.
func countAvailableInstances(insts []v1beta1.OMENativeInstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := desiredFor(desiredByIdx, s.Index, s.PodCount)
		if instanceMeetsThreshold(s.PodCount, s.AvailablePodCount, desired) {
			n++
		}
	}
	return n
}

// countUpdatedInstances returns (UpdatedReplicas, UpdatedReadyReplicas)
// for Instances on targetRevName. UpdatedReadyReplicas adds the Ready
// gate — what HPA's scale-aware rollout pacing reads. The Ready check
// shares the same surge-tolerant classifier as countReadyInstances.
func countUpdatedInstances(insts []v1beta1.OMENativeInstanceStatus, targetRevName string, desiredByIdx map[int32]int32) (updated, updatedReady int32) {
	for _, s := range insts {
		if s.RunningRevision != targetRevName {
			continue
		}
		updated++
		desired := desiredFor(desiredByIdx, s.Index, s.PodCount)
		if instanceMeetsThreshold(s.PodCount, s.ReadyPodCount, desired) {
			updatedReady++
		}
	}
	return updated, updatedReady
}

// rolloutComplete is the CurrentRevision promotion gate: every Instance
// on targetRevName AND Phase=Ready.
func rolloutComplete(insts []v1beta1.OMENativeInstanceStatus, targetRevName string) bool {
	if len(insts) == 0 {
		return false
	}
	for _, s := range insts {
		if s.Phase != v1beta1.OMENativeInstanceReady {
			return false
		}
		if s.RunningRevision != targetRevName {
			return false
		}
	}
	return true
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
