package omenative

import (
	"context"
	"fmt"
	"sort"

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
		return nil
	})
	if isvcGone {
		return nil
	}
	if err != nil {
		return err
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
