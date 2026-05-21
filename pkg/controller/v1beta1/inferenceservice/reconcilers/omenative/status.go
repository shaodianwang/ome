package omenative

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// AggregateAndWriteStatus computes Component-level counters and per-
// Instance counters from the live pod set plus the Component Service's
// EndpointSlices, stamps the desired revision and (when fully converged)
// promotes it from UpdateRevision to CurrentRevision, and persists the
// merged shape in a single Status().Update.
//
// The function never overwrites lifecycle fields written by the ops —
// Phase, Operation, Incarnation, RunningRevision, TargetRevision are
// preserved on each InstanceStatus.
//
// target may be nil when the Component has no rendered PodSpec for this
// reconcile (e.g., MinReplicas=0); revision fields are left untouched
// in that case but counters still run.
func AggregateAndWriteStatus(ctx context.Context, p ReconcileParams, plan ComponentPlan, target *appsv1.ControllerRevision) error {
	if p.Client == nil {
		return fmt.Errorf("AggregateAndWriteStatus: nil client")
	}

	pods, err := listOMENativePods(ctx, p.Client, p.ISVC, plan.Component)
	if err != nil {
		return fmt.Errorf("status: list pods: %w", err)
	}
	byIndex := bucketPodsByInstanceIdx(pods)

	serviceName := headlessServiceName(p.ISVC.Name, plan.Component)
	availableByPod, err := availablePodSet(ctx, p.reader(), p.ISVC.Namespace, serviceName)
	if err != nil {
		return fmt.Errorf("status: compute availability: %w", err)
	}

	// Re-read the ISVC so per-op writes from this reconcile are visible.
	fresh := &v1beta1.InferenceService{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
		return fmt.Errorf("status: re-read ISVC: %w", err)
	}

	if fresh.Status.Components == nil {
		fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := fresh.Status.Components[plan.Component]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}

	// Per-Instance counters.
	for i := range cs.OMENative.InstanceStatuses {
		s := &cs.OMENative.InstanceStatuses[i]
		instPods := byIndex[s.Index]
		s.PodCount = int32(len(instPods))
		s.ReadyPodCount = countReadyPods(instPods)
		s.ScheduledPodCount = countScheduledPods(instPods)
		s.AvailablePodCount = countAvailablePods(instPods, availableByPod)
		s.NodesOccupied = uniqueNodes(instPods)
	}

	// Component-level counters.
	cs.OMENative.Replicas = int32(len(cs.OMENative.InstanceStatuses))
	cs.OMENative.ReadyReplicas = countReadyInstances(cs.OMENative.InstanceStatuses)
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

	fresh.Status.Components[plan.Component] = cs
	if err := p.Client.Status().Update(ctx, fresh); err != nil {
		return fmt.Errorf("status: update: %w", err)
	}
	return nil
}

// availablePodSet returns the set of pod names whose endpoint is Ready
// in any EndpointSlice for serviceName. Equivalent to "IsPodInRotation
// for every pod at once" but with a single slice list to avoid the N+1
// reads AggregateAndWriteStatus would otherwise do.
func availablePodSet(ctx context.Context, reads client.Reader, namespace, serviceName string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	slices, err := endpointSlicesForService(ctx, reads, namespace, serviceName)
	if err != nil {
		return nil, err
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Kind != "" && ep.TargetRef.Kind != "Pod" {
				continue
			}
			if !endpointIsReady(ep) {
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
		if IsContainersReady(p) {
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

// countReadyInstances returns the number of Instances whose every pod
// reports ContainersReady=True and whose PodCount is at least 1.
func countReadyInstances(insts []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range insts {
		if s.PodCount > 0 && s.ReadyPodCount == s.PodCount {
			n++
		}
	}
	return n
}

// countAvailableInstances returns the number of Instances whose every
// pod is in EndpointSlice rotation. Sub-condition of Ready: a pod must
// be Ready for kube-proxy to publish its endpoint as Ready=true.
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
// counting Instances on targetRevName. UpdatedReadyReplicas additionally
// requires every pod of the Instance to be Ready — what HPA's
// scale-aware rollout pacing depends on.
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

// rolloutComplete reports whether every Instance has converged onto
// targetRevName AND is in steady Ready state. Triggers CurrentRevision
// promotion.
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

// componentLabelSelectorString renders the pod label set as a
// canonical "k=v,k=v" selector string. Consumed by HPA via the
// Component's status.labelSelector subresource.
func componentLabelSelectorString(isvc string, component v1beta1.ComponentType) string {
	return labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		labelManagedBy:                        managedByOMENative,
	}).String()
}
