package omenative

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// createRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up.
const createRequeueInterval = 5 * time.Second

// Create drives Create / Scale-Up toward each desired Instance. Multi-pass:
//
//  1. Render missing pods with ome.io/serving=False; patch InstanceStatus
//     to Phase=Creating with the Create operation stamped.
//  2. Wait for runtime Ready on every pod, flip ome.io/serving=True,
//     promote InstanceStatus to Phase=Ready (Operation=nil).
//
// Caller restricts to single-pod Instances via BuildPlan; EndpointSlice
// convergence is owned by drain.go.
//
// When target != nil the Ready promote records RunningRevision=target.Name
// so detectUpdateTrigger's fast-path (s.RunningRevision == target.Name) can
// short-circuit. Without it, per-pod podMatchesTarget runs and false-positives
// against the post-Render pod mutations, triggering a spurious recreate.
func Create(ctx context.Context, p ReconcileParams, plan ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
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
		ready, err := reconcileInstance(ctx, p, plan, inst, byInstance[inst.Index], target)
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
// only when every pod is runtime-ready and ome.io/serving=True. target has
// the same nil-OK semantics as Create.
func reconcileInstance(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, existing []*corev1.Pod, target *appsv1.ControllerRevision) (bool, error) {
	desired := expectedPodNamesForInstance(p.ISVC.Name, plan.Component, inst)
	existingByName := indexPodsByName(existing)

	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}

	// Only patch Creating when there's something to create — otherwise the
	// next pass would oscillate status back to Creating after Ready.
	if len(missing) > 0 {
		// Capture before patch — findInstanceStatus uses the cached ISVC,
		// so the patch below doesn't backfill this view.
		isFirstCreate := findInstanceStatus(p.ISVC, plan.Component, inst.Index) == nil
		if err := patchInstanceStatusCreating(ctx, p, plan.Component, inst.Index, inst.Incarnation, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("patch status Creating (instance=%d): %w", inst.Index, err)
		}
		if isFirstCreate {
			recordNormal(p, EventReasonInstanceCreated,
				"OMENative %s materialized; %d pod(s) requested",
				instanceKey(plan.Component, inst.Index), len(missing))
		}

		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, inst.Index) {
			return false, nil
		}

		if _, err := createMissingPods(ctx, p, plan, inst, inst.Index, missing); err != nil {
			return false, err
		}
		return false, nil
	}

	// All desired pods exist. Are they runtime-Ready?
	if !allPodsRuntimeReady(existing) {
		return false, nil
	}

	// Flip ome.io/serving=True. For freshly-created pods the condition
	// doesn't exist yet; the Lifecycle writer creates it on first flip.
	for _, pod := range existing {
		if IsServing(pod) {
			continue
		}
		if err := MarkPodServing(ctx, p.Client, pod, WriterLifecycle, KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// Promote to Ready. Fire InstanceReady only on transition.
	wasReady := false
	if s := findInstanceStatus(p.ISVC, plan.Component, inst.Index); s != nil && s.Phase == v1beta1.OMENativeInstanceReady {
		wasReady = true
	}
	var promoteErr error
	if target != nil {
		promoteErr = patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, inst.Index, target.Name)
	} else {
		promoteErr = patchInstanceStatusReady(ctx, p, plan.Component, inst.Index)
	}
	if promoteErr != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, promoteErr)
	}
	if !wasReady {
		recordNormal(p, EventReasonInstanceReady,
			"OMENative %s is Ready (%d pod(s) serving)",
			instanceKey(plan.Component, inst.Index), len(existing))
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

// createMissingPods renders and creates targets, owning the per-pod
// ExpectCreates bookkeeping. idx is the expectations bucket — pass
// inst.Index for steady-state Create / Restart Phase B; surgeIdx for
// Migrate's surge create. Callers must NOT call ExpectCreates themselves.
func createMissingPods(ctx context.Context, p ReconcileParams, plan ComponentPlan, inst InstancePlan, idx int32, targets []podTarget) (int, error) {
	created := 0
	for _, t := range targets {
		template := p.PodSpec
		if t.Runner.Name == "worker" && p.WorkerPodSpec != nil {
			template = p.WorkerPodSpec
		}

		pod, err := Render(p.ISVC, template, plan, inst, t.Runner, t.Ordinal)
		if err != nil {
			return created, fmt.Errorf("render pod %s: %w", t.Name, err)
		}

		// EXPECT-ORDER: expectation before RPC, rollback via ObservedCreate
		// on error — a failed Create fires no watch event to decrement it.
		p.expectations().ExpectCreates(p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx, 1)
		if err := p.Client.Create(ctx, pod); err != nil {
			p.expectations().ObservedCreate(p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx)
			if apierrors.IsAlreadyExists(err) {
				// Prior reconcile created it; cache hasn't caught up yet.
				continue
			}
			return created, fmt.Errorf("create pod %s: %w", t.Name, err)
		}
		created++
	}
	return created, nil
}

// listOMENativePods returns every pod the OMENative selector matches for
// (isvc, component). Takes a Reader so callers can use either the cached
// client or the live reader (destructive paths need live — see
// liveListPodsForInstance in discover.go).
func listOMENativePods(ctx context.Context, c client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) ([]*corev1.Pod, error) {
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

// patchInstanceStatusCreating idempotently stamps Phase=Creating with the
// Create operation and a Deadline of StartedAt+timeout. incarnation is
// written on first-touch; existing nonzero values are preserved so a
// post-restart reconcile doesn't downgrade the lifecycle token.
func patchInstanceStatusCreating(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, incarnation int64, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
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

// patchInstanceStatusReady idempotently moves to Phase=Ready, clears Operation.
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

// mutateInstanceStatus applies mutate to the (component, idx) entry,
// creating it if missing, and writes via Status().Update(). mutate returns
// true when a real change happened; false skips the write. Wrapped in
// retry.RetryOnConflict; mutate runs against fresh state on each attempt
// so phase-diff callbacks stay correct.
func mutateInstanceStatus(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx int32, mutate func(*v1beta1.OMENativeInstanceStatus) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
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
	})
}
