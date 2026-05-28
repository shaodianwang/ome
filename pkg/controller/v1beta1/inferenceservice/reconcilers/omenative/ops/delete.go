package ops

import (
	"context"
	"fmt"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// deleteDrainKey is the per-Instance key the Delete-drain writer adds to
// the ome.io/serving message list. Same key for every pod in the Instance;
// Delete never RemoveNotReadyKeys — pod deletion clears the entry.
func deleteDrainKey(idx int32) string {
	return strconv.Itoa(int(idx))
}

// deleteRequeueInterval is the wait between drain / gone-confirmation
// checks while a Delete is in flight.
const deleteRequeueInterval = 5 * time.Second

// Delete drives one Instance through Delete / Scale-Down. Multi-pass,
// idempotent so a controller restart resumes wherever it left off:
//
//  1. Patch Phase=Deleting, Operation={Delete, Drain}.
//  2. Flip ome.io/serving=False on each pod.
//  3. Wait for EndpointSlice drain.
//  4. Delete every non-terminating pod.
//  5. Wait until the live pod list is empty.
//  6. Remove the InstanceStatus entry.
//
// Returns done=true once step 6 completes; done=false means requeue.
func Delete(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, idx int32) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Delete: nil client")
	}

	// Live read — a cached "pods gone" view could trick us into removing
	// the InstanceStatus while the apiserver still holds the pods, leaking
	// them.
	pods, err := query.LiveListPodsForInstance(ctx, p.Reader(), p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx)
	if err != nil {
		return false, fmt.Errorf("Delete: list pods (instance=%d): %w", idx, err)
	}

	if len(pods) == 0 {
		if _, err := removeInstanceStatus(ctx, p, plan.Component, idx); err != nil {
			return false, fmt.Errorf("remove InstanceStatus (instance=%d): %w", idx, err)
		}
		return true, nil
	}

	// Stamp Deleting before any mutation so a mid-delete crash still
	// shows the right Phase.
	if err := patchInstanceStatusDeleting(ctx, p, plan.Component, idx, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Deleting (instance=%d): %w", idx, err)
	}

	// Only the Delete RPC batch is expectations-gated; the drain reads
	// below are idempotent on repeat.
	expectationsSatisfied := p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx)

	for _, pod := range pods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, p.Client, pod, podreadiness.WriterDeleteDrain, deleteDrainKey(idx)); err != nil {
			return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", idx, pod.Name, err)
		}
	}

	// Drain check needs live reader so kube-proxy isn't still routing.
	for _, pod := range pods {
		serviceName := drainServiceForPod(p.ISVC.Name, plan.Component, pod)
		if serviceName == "" {
			continue
		}
		drained, err := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", idx, pod.Name, err)
		}
		if !drained {
			return false, nil
		}
	}

	if !expectationsSatisfied {
		return false, nil
	}

	// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
	// ObservedDelete on error — a failed RPC fires no event to decrement.
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		p.ExpectationsCache().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx, 1)
		if err := p.Client.Delete(ctx, pod); err != nil {
			p.ExpectationsCache().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, plan.Component, idx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return false, nil
}

// extraInstanceIndices returns InstanceStatus indices the plan no longer
// covers — these are scale-down targets. Set-difference framing handles
// sparse indices from surge migration: index 7 is "extra" only when no
// core.InstancePlan covers it, regardless of replica count. Mid-migration
// Instances are excluded so the surge pair (source + surge) isn't
// scale-down-deleted out from under Migrate.
func extraInstanceIndices(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, plan core.ComponentPlan) []int32 {
	if isvc == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return nil
	}
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range cs.OMENative.InstanceStatuses {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		if s.Phase == v1beta1.OMENativeInstanceMigrating {
			continue
		}
		if s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationMigrate {
			continue
		}
		extras = append(extras, s.Index)
	}
	return extras
}

// patchInstanceStatusDeleting idempotently stamps Phase=Deleting + the
// Delete operation in Step=Drain.
func patchInstanceStatusDeleting(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceDeleting &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationDelete {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceDeleting
		s.Operation = &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("delete-%d-%d", idx, now.Unix()),
			Type:           v1beta1.InstanceOperationDelete,
			Step:           "Drain",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// removeInstanceStatus drops the InstanceStatus entry for (component, idx).
// hadEntry=true when a real removal happened, false when already cleared.
// Wrapped in retry.RetryOnConflict; re-checks existence on each attempt so
// a parallel clear becomes a no-op.
func removeInstanceStatus(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) (bool, error) {
	var hadEntry bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		hadEntry = false
		fresh := &v1beta1.InferenceService{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
			if apierrors.IsNotFound(err) {
				// ISVC deleted during reconcile — the InstanceStatus
				// entry is gone with it. Treat as a successful no-op.
				return nil
			}
			return fmt.Errorf("re-read ISVC: %w", err)
		}
		if fresh.Status.Components == nil {
			return nil
		}
		cs, ok := fresh.Status.Components[component]
		if !ok || cs.OMENative == nil {
			return nil
		}
		insts := cs.OMENative.InstanceStatuses
		pos := -1
		for i, s := range insts {
			if s.Index == idx {
				pos = i
				break
			}
		}
		if pos == -1 {
			return nil
		}
		cs.OMENative.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
		fresh.Status.Components[component] = cs

		if err := p.Client.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("update ISVC status: %w", err)
		}
		hadEntry = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if hadEntry {
		// Forget so a later index reuse doesn't inherit stale counters.
		p.ExpectationsCache().Forget(p.ISVC.Namespace, p.ISVC.Name, component, idx)
	}
	return hadEntry, nil
}
