/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pod_reconciler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slicev1beta1 "tpu.google.com/slice-scheduler/apis/slice/v1beta1"
	"tpu.google.com/slice-scheduler/internal/interfaces"
	"tpu.google.com/slice-scheduler/internal/policy"
	"tpu.google.com/slice-scheduler/internal/watcher"
)

const (
	SliceNameLabel = "cloud.google.com/gke-tpu-slice"
)

// Retry delays for pod reconciler operations.
const (
	retryDelayNodeOccupied  = 1 * time.Second
	retryDelayEvictionDrain = 2 * time.Second
)

// PodReconciler reconciles Slice objects by managing pod lifecycle
// (eviction and binding) for pods associated with each Slice.
type PodReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Log               logr.Logger
	Cache             *watcher.PartitionCache
	Recorder          record.EventRecorder
	Workers           int
	WaitForSliceReady bool
	Policy            policy.SlicePolicy
}

func NewPodReconciler(k8sClient client.Client, scheme *runtime.Scheme, logger logr.Logger, cache *watcher.PartitionCache, recorder record.EventRecorder, workers int, waitForSliceReady bool, slicePolicy policy.SlicePolicy) *PodReconciler {
	return &PodReconciler{
		Client:            k8sClient,
		Scheme:            scheme,
		Log:               logger,
		Cache:             cache,
		Recorder:          recorder,
		Workers:           workers,
		WaitForSliceReady: waitForSliceReady,
		Policy:            slicePolicy,
	}
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods/binding,verbs=create
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("slice", req.NamespacedName)

	// Fetch the Slice instance
	slice := &slicev1beta1.Slice{}
	if err := r.Get(ctx, req.NamespacedName, slice); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch Slice")
		return ctrl.Result{}, err
	}

	// Fetch all Pods associated with this Slice
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.MatchingLabels{SliceNameLabel: slice.Name}); err != nil {
		log.Error(err, "unable to list Pods for Slice", "sliceName", slice.Name)
		return ctrl.Result{}, err
	}

	// Filter pods managed by this scheduler
	var managedPods []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Spec.SchedulerName == interfaces.SliceSchedulerName {
			managedPods = append(managedPods, pod)
		}
	}

	// Check for eviction: annotation-based (preemption) or policy-based (failure state)
	hasEvictAnnotation := slice.Annotations != nil && slice.Annotations[interfaces.EvictPodsAnnotation] == interfaces.TrueStr
	partitionBroken := r.isPartitionBroken(slice.Spec.PartitionIds)
	shouldEvict := hasEvictAnnotation || r.Policy.ShouldEvictPod(slice, partitionBroken)

	if shouldEvict {
		return r.handleEviction(ctx, log, slice, managedPods, hasEvictAnnotation)
	}

	return r.handleScheduling(ctx, log, slice, managedPods)
}

// handleEviction evicts all managed pods for the slice. When all pods are
// terminated, it clears PartitionIds and removes the eviction annotation.
func (r *PodReconciler) handleEviction(ctx context.Context, log logr.Logger, slice *slicev1beta1.Slice, managedPods []corev1.Pod, hasEvictAnnotation bool) (ctrl.Result, error) {
	if len(managedPods) > 0 {
		log.Info("Evicting pods for slice", "sliceName", slice.Name, "count", len(managedPods),
			"evictAnnotation", hasEvictAnnotation)
		var evictionErrs []error
		for i := range managedPods {
			pod := &managedPods[i]
			if pod.DeletionTimestamp.IsZero() {
				eviction := &policyv1.Eviction{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pod.Name,
						Namespace: pod.Namespace,
					},
				}
				log.Info("Triggering eviction for pod", "pod", pod.Name)
				if err := r.Client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
					if !apierrors.IsNotFound(err) {
						log.Error(err, "unable to evict pod", "pod", pod.Name)
						evictionErrs = append(evictionErrs, fmt.Errorf("evict %s: %w", pod.Name, err))
						continue
					}
				}
				r.Recorder.Eventf(pod, corev1.EventTypeWarning, "Evicted", "Slice %s eviction triggered", slice.Name)
			}
		}
		if len(evictionErrs) > 0 {
			return ctrl.Result{}, errors.Join(evictionErrs...)
		}
		// Requeue to check if pods are gone.
		return ctrl.Result{RequeueAfter: retryDelayEvictionDrain}, nil
	}

	// Pod count is 0 — safe to clean up the slice
	log.Info("All pods evicted, unbinding slice", "sliceName", slice.Name,
		"evictAnnotation", hasEvictAnnotation)
	// Unbind: clear PartitionIds
	slice.Spec.PartitionIds = []string{}
	// Remove annotation if present
	if hasEvictAnnotation {
		delete(slice.Annotations, interfaces.EvictPodsAnnotation)
	}
	if err := r.Update(ctx, slice); err != nil {
		log.Error(err, "unable to update slice after eviction")
		return ctrl.Result{}, err
	}
	log.Info("Slice unbound successfully", "sliceName", slice.Name)
	return ctrl.Result{}, nil
}

// handleScheduling handles the normal pod binding flow for a slice.
func (r *PodReconciler) handleScheduling(ctx context.Context, log logr.Logger, slice *slicev1beta1.Slice, managedPods []corev1.Pod) (ctrl.Result, error) {
	if len(slice.Spec.PartitionIds) == 0 {
		log.Info("Slice has no partitions assigned yet", "sliceName", slice.Name)
		for i := range managedPods {
			pod := &managedPods[i]
			if pod.Spec.NodeName != "" && pod.DeletionTimestamp.IsZero() {
				log.Info("Evicting pod because slice has no partitions", "pod", pod.Name)
				eviction := &policyv1.Eviction{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pod.Name,
						Namespace: pod.Namespace,
					},
				}
				if err := r.Client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
					if !apierrors.IsNotFound(err) {
						log.Error(err, "unable to evict pod")
						return ctrl.Result{}, err
					}
				}
				r.Recorder.Eventf(pod, corev1.EventTypeWarning, "Evicted", "Associated Slice %s has no partitions", slice.Name)
			}
		}
		return ctrl.Result{}, nil
	}

	// Check Slice state for readiness if the flag is enabled
	sliceState := policy.GetSliceState(slice)
	if r.WaitForSliceReady && !r.Policy.IsSliceReady(sliceState) {
		log.Info("Waiting for slice to be READY before scheduling pods", "sliceName", slice.Name, "currentState", sliceState)
		return ctrl.Result{}, nil
	}

	// Resolve partition IDs to hostnames in a single lock operation
	nodes, ok := r.Cache.GetNodes(slice.Spec.PartitionIds)
	if !ok {
		log.Info("One or more partitions not found in cache, waiting for cache sync", "partitionIDs", slice.Spec.PartitionIds)
		return ctrl.Result{}, fmt.Errorf("one or more partitions from slice %s not found in cache", slice.Name)
	}

	var hostNames []string
	for _, pID := range slice.Spec.PartitionIds {
		node := nodes[pID]
		var pHostNames []string
		appendHostNamesRecursive(node, &pHostNames)
		hostNames = append(hostNames, pHostNames...)
	}

	if len(hostNames) == 0 {
		log.Info("No hostnames found for slice partitions", "sliceName", slice.Name)
		return ctrl.Result{}, fmt.Errorf("no hostnames found for slice %s", slice.Name)
	}

	for i := range managedPods {
		pod := &managedPods[i]
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}

		index, err := parsePodIndex(pod)
		if err != nil {
			log.Error(err, "unable to parse index from pod", "pod", pod.Name)
			return ctrl.Result{}, err
		}

		if index < 0 || index >= len(hostNames) {
			err := fmt.Errorf("pod index %d out of bounds [0, %d)", index, len(hostNames))
			log.Error(err, "Pod index out of bounds", "index", index, "numHostNames", len(hostNames))
			return ctrl.Result{}, err
		}

		targetNode := hostNames[index]

		if pod.Spec.NodeName != "" {
			if pod.Spec.NodeName == targetNode {
				continue
			}
			log.Info("Deleting pod because its node does not match target node", "pod", pod.Name, "currentNode", pod.Spec.NodeName, "targetNode", targetNode)
			eviction := &policyv1.Eviction{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pod.Name,
					Namespace: pod.Namespace,
				},
			}
			if err := r.Client.SubResource("eviction").Create(ctx, pod, eviction); err != nil {
				log.Error(err, "unable to evict pod")
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(pod, corev1.EventTypeWarning, "Evicted", "Pod evicted because its node %s does not match target node %s", pod.Spec.NodeName, targetNode)
			continue
		}

		occupied, err := r.isNodeOccupied(ctx, targetNode)
		if err != nil {
			log.Error(err, "unable to check if node is occupied", "node", targetNode)
			return ctrl.Result{}, err
		}
		if occupied {
			log.Info("Target node is occupied, waiting for termination", "pod", pod.Name, "node", targetNode)
			return ctrl.Result{RequeueAfter: retryDelayNodeOccupied}, nil
		}

		// Node health check — delegated to policy.
		if ready, retryAfter, err := r.Policy.IsNodeReadyForBinding(ctx, r.Client, targetNode, slice.Spec.Topology, slice.Name); !ready {
			if err != nil {
				log.Error(err, "error checking node readiness", "node", targetNode)
			}
			log.Info("Node not ready for binding, waiting", "node", targetNode)
			return ctrl.Result{RequeueAfter: retryAfter}, err
		}

		binding := &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
			Target: corev1.ObjectReference{
				Kind: "Node",
				Name: targetNode,
			},
		}

		log.Info("Binding pod to node", "pod", pod.Name, "node", targetNode)
		if err := r.Client.SubResource("binding").Create(ctx, pod, binding); err != nil {
			log.Error(err, "unable to create binding")
			return ctrl.Result{}, err
		}
		r.Recorder.Eventf(pod, corev1.EventTypeNormal, "Scheduled", "Successfully bound pod to node %s", targetNode)
	}

	return ctrl.Result{}, nil
}

// isNodeOccupied checks if any pods are currently assigned to the given node.
func (r *PodReconciler) isNodeOccupied(ctx context.Context, nodeName string) (bool, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return false, err
	}

	// Only consider pods managed by the slice-scheduler.
	// System DaemonSet pods (kube-system, gmp-system, etc.) are always
	// present on every node and must be ignored.
	for _, p := range podList.Items {
		if p.Spec.SchedulerName != interfaces.SliceSchedulerName {
			continue
		}
		// Succeeded/Failed pods keep spec.nodeName until garbage-collected
		// (e.g. completed Job pods) but no longer hold node resources.
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&slicev1beta1.Slice{}).
		// Watch for Pod events and trigger a reconciliation for the associated Slice.
		// This ensures that when a new Pod is created (or updated/deleted) for a Slice
		// that is already Ready, the reconciler will be notified to perform binding.
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			pod, ok := o.(*corev1.Pod)
			if !ok {
				return nil
			}
			if pod.Spec.SchedulerName != interfaces.SliceSchedulerName {
				return nil
			}
			sliceName, ok := pod.Labels[SliceNameLabel]
			if !ok {
				return nil
			}
			return []reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name: sliceName,
					},
				},
			}
		}), builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				pod, ok := e.Object.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				pod, ok := e.ObjectNew.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				pod, ok := e.Object.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
			GenericFunc: func(e event.GenericEvent) bool {
				pod, ok := e.Object.(*corev1.Pod)
				return ok && pod.Spec.SchedulerName == interfaces.SliceSchedulerName
			},
		})).
		Named("slice-scheduler-pod-reconciler").
		WithOptions(controller.Options{MaxConcurrentReconciles: r.Workers}).
		Complete(r)
}

// isPartitionBroken checks if any partition in the slice is broken (not found
// in cache or marked unhealthy). Uses the same GetPartitionInfo pattern as
// the scheduler's shouldBackoffHealthy.
func (r *PodReconciler) isPartitionBroken(partitionIds []string) bool {
	if len(partitionIds) == 0 {
		return false
	}
	state := watcher.NewPartitionWatcher(r.Cache).GetState()
	infos := state.GetPartitionInfo(partitionIds)
	if len(infos) != len(partitionIds) {
		// Not all partitions found in cache — consider broken
		return true
	}
	for _, info := range infos {
		if info.IsBroken {
			return true
		}
	}
	return false
}
