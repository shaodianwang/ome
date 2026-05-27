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
		cs.OMENative.Replicas = int32(len(cs.OMENative.InstanceStatuses))
		cs.OMENative.ReadyReplicas = countReadyInstances(cs.OMENative.InstanceStatuses)
		cs.OMENative.ServingReplicas = countServingInstances(cs.OMENative.InstanceStatuses)
		cs.OMENative.AvailableReplicas = countAvailableInstances(cs.OMENative.InstanceStatuses)
		if target != nil {
			cs.OMENative.UpdatedReplicas, cs.OMENative.UpdatedReadyReplicas =
				countUpdatedInstances(cs.OMENative.InstanceStatuses, target.Name)
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

// countReadyInstances counts Instances whose every pod is
// ContainersReady=True (PodCount > 0).
func countReadyInstances(insts []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range insts {
		if s.PodCount > 0 && s.ReadyPodCount == s.PodCount {
			n++
		}
	}
	return n
}

// countServingInstances counts Instances whose every pod is BOTH
// ContainersReady AND serving=True. This is the count
// CheckUnavailabilityGate uses — "instances actually in the load
// balancer rotation right now."
func countServingInstances(insts []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range insts {
		if s.PodCount > 0 && s.ServingPodCount == s.PodCount {
			n++
		}
	}
	return n
}

// countAvailableInstances counts Instances whose every pod is in
// EndpointSlice rotation. Strict sub-condition of Ready — kube-proxy
// only publishes Ready pods.
func countAvailableInstances(insts []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range insts {
		if s.PodCount > 0 && s.AvailablePodCount == s.PodCount {
			n++
		}
	}
	return n
}

// countUpdatedInstances returns (UpdatedReplicas, UpdatedReadyReplicas)
// for Instances on targetRevName. UpdatedReadyReplicas adds the Ready
// gate — what HPA's scale-aware rollout pacing reads.
func countUpdatedInstances(insts []v1beta1.OMENativeInstanceStatus, targetRevName string) (updated, updatedReady int32) {
	for _, s := range insts {
		if s.RunningRevision != targetRevName {
			continue
		}
		updated++
		if s.PodCount > 0 && s.ReadyPodCount == s.PodCount {
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
