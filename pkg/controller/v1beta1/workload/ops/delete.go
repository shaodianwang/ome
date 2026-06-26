package ops

import (
	"context"
	"fmt"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// DeleteRequeueInterval is the wait between Delete passes while a
// scale-down drain / pod-removal is still in flight. Used by the
// workload dispatcher; Delete itself returns done=false and lets the
// caller decide when to retry.
const DeleteRequeueInterval = 5 * time.Second

// deleteDrainKey is the per-Instance key the Delete-drain writer adds to
// the ome.io/serving message list. Same key for every pod in the Instance;
// Delete never RemoveNotReadyKeys — pod deletion clears the entry.
func deleteDrainKey(idx int32) string {
	return strconv.Itoa(int(idx))
}

// Delete drives one Instance through Delete / Scale-Down. Multi-pass,
// idempotent so a controller restart resumes wherever it left off:
//
//  1. Patch Phase=Deleting, Operation={Delete, Drain}.
//  2. Flip ome.io/serving=False on each pod.
//  3. Wait for EndpointSlice drain.
//  4. Delete every non-terminating pod.
//  5. Wait until the live pod list is empty.
//  6. Remove the InstanceStatus entry (via input.RemoveInstance).
//
// Returns done=true once step 6 completes; done=false means requeue.
//
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types. The ISVC-OMENative dispatcher delegates
// via internalsource.BuildReconcileInput; the IR controller will call
// the same entry point with its own ReconcileInput.
func Delete(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, idx int32) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Delete: nil client")
	}

	// Live read — a cached "pods gone" view could trick us into removing
	// the InstanceStatus while the apiserver still holds the pods, leaking
	// them.
	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, idx)
	if err != nil {
		return false, fmt.Errorf("Delete: list pods (instance=%d): %w", idx, err)
	}

	if len(pods) == 0 {
		if _, err := input.RemoveInstance(ctx, idx); err != nil {
			return false, fmt.Errorf("remove InstanceStatus (instance=%d): %w", idx, err)
		}
		return true, nil
	}

	// Stamp Deleting before any mutation so a mid-delete crash still
	// shows the right Phase.
	if err := patchInstanceStatusDeleting(ctx, input, idx, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Deleting (instance=%d): %w", idx, err)
	}

	// Only the Delete RPC batch is expectations-gated; the drain reads
	// below are idempotent on repeat.
	expectationsSatisfied := deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)

	for _, pod := range pods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, deps.Client, pod, podreadiness.WriterDeleteDrain, deleteDrainKey(idx)); err != nil {
			return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", idx, pod.Name, err)
		}
	}

	// Drain check needs live reader so kube-proxy isn't still routing.
	// Batcher lists each per-revision Service's EndpointSlices once and
	// reuses them across the gang, avoiding the N+1 per-pod LIST.
	drainer := drain.NewBatcher(deps.Reader(), input.Key.Namespace)
	for _, pod := range pods {
		// drain target is the per-revision *routed* Service. Read from
		// the pod's ome.io/revision-hash label (stamped by Render). The
		// per-Component headless Service publishes not-ready endpoints
		// so peer-discovery DNS resolves during gang init — using it
		// here would make IsPodDrained wait forever.
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" {
			continue
		}
		serviceName := query.PerRevisionServiceName(input.Key.OwnerName, plan.Component, hash)
		drained, err := drainer.IsPodDrained(ctx, serviceName, pod)
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
		deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return false, nil
}

// patchInstanceStatusDeleting idempotently stamps Phase=Deleting + the
// Delete operation in Step=Drain.
func patchInstanceStatusDeleting(ctx context.Context, input workload.ReconcileInput, idx int32, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseDeleting &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationDelete {
			return false
		}
		s.Phase = workload.InstancePhaseDeleting
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("delete-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationDelete,
			Step:           "Drain",
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}
