package omenative

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// createRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up.
const createRequeueInterval = 5 * time.Second

// Create drives the Create / Scale Up operation toward each desired
// Instance in plan.Instances. The operation is multi-pass:
//
//  1. First pass: pods missing for an Instance are created with the
//     ome.io/serving readiness gate set to False (via Render()).
//     InstanceStatus is patched to {Phase: Creating, Operation: {Create}}.
//  2. Subsequent passes: pods are observed and waited for runtime Ready.
//     Once all pods are runtime-ready, the ome.io/serving gate is flipped
//     to True on each pod and InstanceStatus moves to Phase=Ready, Operation=nil.
//
// Scope: single-pod Instances. Caller (ReconcileComponent) rejects
// multi-pod via BuildPlan before this runs.
//
// EndpointSlice convergence is handled by drain.go, not here.
// MinReadySeconds defaults to 0, so runtime-Ready is treated as Available.
func Create(ctx context.Context, p ReconcileParams, plan ComponentPlan) (ctrl.Result, error) {
	if p.Client == nil {
		return ctrl.Result{}, fmt.Errorf("Create: nil client")
	}

	existing, err := listOMENativePods(ctx, p.Client, p.ISVC, plan.Component)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("Create: list pods: %w", err)
	}
	byInstance := bucketPodsByInstanceIdx(existing)

	allReady := true
	for _, inst := range plan.Instances {
		ready, err := reconcileInstance(ctx, p, plan, inst, byInstance[inst.Index])
		if err != nil {
			return ctrl.Result{}, err
		}
		if !ready {
			allReady = false
		}
	}

	if !allReady {
		return ctrl.Result{RequeueAfter: createRequeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

// reconcileInstance drives one Instance toward Ready. Returns ready=true
// only when every pod in the Instance is runtime-ready and has been
// flipped to ome.io/serving=True.
func reconcileInstance(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, existing []*corev1.Pod) (bool, error) {
	desired := expectedPodNamesForInstance(p.ISVC.Name, plan.Component, inst)
	existingByName := indexPodsByName(existing)

	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}

	// Mark Creating before the first pod write, but only when we're
	// actually about to create something — otherwise the next pass would
	// oscillate the status back to Creating after we already observed
	// Ready pods.
	if len(missing) > 0 {
		if err := patchInstanceStatusCreating(ctx, p, plan.Component, inst.Index, inst.Incarnation, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("patch status Creating (instance=%d): %w", inst.Index, err)
		}

		// Skip the create batch if the expectations cache says a previous
		// reconcile's writes haven't been observed yet.
		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}

		created, err := createMissingPods(ctx, p, plan, inst, missing)
		if err != nil {
			return false, err
		}
		p.expectations().ExpectCreates(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index, created)
		// Pods just created — runtime can't possibly be Ready yet. Requeue.
		return false, nil
	}

	// All desired pods exist. Are they runtime-Ready?
	if !allPodsRuntimeReady(existing) {
		return false, nil
	}

	// Flip ome.io/serving=True on each pod.
	for _, pod := range existing {
		if IsServing(pod) {
			continue
		}
		if err := MarkPodServing(ctx, p.Client, pod); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// Record Phase=Ready, Operation=nil.
	if err := patchInstanceStatusReady(ctx, p, plan.Component, inst.Index); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	return true, nil
}

// podTarget describes one pod to create within an Instance.
type podTarget struct {
	Name    string
	Runner  RunnerPlan
	Ordinal int32
}

// expectedPodNamesForInstance enumerates every (Runner, ordinal) tuple
// the Instance plan asks for and returns the targets in stable order.
func expectedPodNamesForInstance(isvc string, component v1beta1.ComponentType, inst InstancePlan) []podTarget {
	var targets []podTarget
	for _, runner := range inst.Runners {
		for o := int32(0); o < runner.Size; o++ {
			targets = append(targets, podTarget{
				Name:    podName(isvc, component, inst.Index, runner.Name, o),
				Runner:  runner,
				Ordinal: o,
			})
		}
	}
	return targets
}

func createMissingPods(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, targets []podTarget) (int, error) {
	created := 0
	for _, t := range targets {
		// PodSpec source: leader uses p.PodSpec; worker uses p.WorkerPodSpec.
		// Single-pod (only Runner "default") always uses p.PodSpec.
		// This branch is here so a future multi-pod Render() can route
		// without changing call sites.
		template := p.PodSpec
		if t.Runner.Name == "worker" && p.WorkerPodSpec != nil {
			template = p.WorkerPodSpec
		}

		pod, err := Render(p.ISVC, template, plan, inst, t.Runner, t.Ordinal)
		if err != nil {
			return created, fmt.Errorf("render pod %s: %w", t.Name, err)
		}

		if err := p.Client.Create(ctx, pod); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// A previous reconcile created this pod but the watch
				// hasn't propagated it to our cache yet. Treat as a
				// successful create — the next pass will observe it
				// through the listed pods.
				continue
			}
			return created, fmt.Errorf("create pod %s: %w", t.Name, err)
		}
		created++
	}
	return created, nil
}

func listOMENativePods(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) ([]*corev1.Pod, error) {
	pods := &corev1.PodList{}
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		labelManagedBy:                        managedByOMENative,
	})
	if err := c.List(ctx, pods, client.InNamespace(isvc.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		out = append(out, &pods.Items[i])
	}
	return out, nil
}

func bucketPodsByInstanceIdx(pods []*corev1.Pod) map[int32][]*corev1.Pod {
	by := make(map[int32][]*corev1.Pod)
	for _, pod := range pods {
		idx, ok := instanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		by[idx] = append(by[idx], pod)
	}
	return by
}

func indexPodsByName(pods []*corev1.Pod) map[string]*corev1.Pod {
	m := make(map[string]*corev1.Pod, len(pods))
	for _, pod := range pods {
		m[pod.Name] = pod
	}
	return m
}

func instanceIdxFromLabels(pod *corev1.Pod) (int32, bool) {
	if pod == nil {
		return 0, false
	}
	raw, ok := pod.Labels[labelInstanceIdx]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

func allPodsRuntimeReady(pods []*corev1.Pod) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if !IsContainersReady(pod) {
			return false
		}
	}
	return true
}

// patchInstanceStatusCreating ensures Status.Components[component].OMENative
// has an InstanceStatus entry for the given index in Phase=Creating with an
// Operation recording the in-flight Create. timeout is the wall-clock budget
// the operation gets before downstream consumers treat it as stuck; the
// computed Deadline is StartedAt+timeout. incarnation is stamped onto the
// InstanceStatus on first-touch (when no Incarnation has been recorded yet);
// existing nonzero incarnations are preserved so an in-flight reconcile
// after a controller restart doesn't downgrade the lifecycle token.
func patchInstanceStatusCreating(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, incarnation int64, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		// Skip the write if status is already in the desired state.
		if s.Phase == v1beta1.OMENativeInstanceCreating &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationCreate &&
			s.Incarnation > 0 {
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = incarnation
		}
		s.Phase = v1beta1.OMENativeInstanceCreating
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationCreate,
			Step:           "CreatePods",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusReady moves the InstanceStatus to Phase=Ready and
// clears the Operation. Idempotent.
func patchInstanceStatusReady(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceReady && s.Operation == nil {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.Operation = nil
		return true
	})
}

// mutateInstanceStatus reads the ISVC, applies mutate to the
// InstanceStatus entry for the given (component, idx) pair (creating it
// if missing), and writes back via Status().Update(). The mutate function
// returns true when an actual change was made; if false, no write is
// issued. Cross-controller condition writes are out of scope of this PR.
func mutateInstanceStatus(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, mutate func(*v1beta1.OMENativeInstanceStatus) bool) error {
	fresh := &v1beta1.InferenceService{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
		return fmt.Errorf("re-read ISVC: %w", err)
	}

	if fresh.Status.Components == nil {
		fresh.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
	}
	cs := fresh.Status.Components[component]
	if cs.OMENative == nil {
		cs.OMENative = &v1beta1.OMENativeComponentStatus{}
	}

	// Locate or append the InstanceStatus entry.
	insts := cs.OMENative.InstanceStatuses
	pos := -1
	for i, s := range insts {
		if s.Index == idx {
			pos = i
			break
		}
	}
	var slot *v1beta1.OMENativeInstanceStatus
	if pos == -1 {
		insts = append(insts, v1beta1.OMENativeInstanceStatus{Index: idx})
		slot = &insts[len(insts)-1]
	} else {
		slot = &insts[pos]
	}

	if !mutate(slot) {
		return nil
	}
	cs.OMENative.InstanceStatuses = insts
	fresh.Status.Components[component] = cs

	if err := p.Client.Status().Update(ctx, fresh); err != nil {
		return fmt.Errorf("update ISVC status: %w", err)
	}
	return nil
}
