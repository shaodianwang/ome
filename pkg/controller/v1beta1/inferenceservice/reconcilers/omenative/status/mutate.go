package status

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// MutateInstance applies mutate to the (component, idx) InstanceStatus
// entry, creating it if missing, and writes via Status().Update().
// mutate returns true when a real change happened; false skips the
// write. Wrapped in retry.RetryOnConflict; mutate runs against fresh
// state on each attempt so phase-diff callbacks stay correct.
//
// Generic across every per-op status writer in ops/ (Create's
// patchInstanceStatusCreating / Ready, Delete's
// patchInstanceStatusDeleting, Restart's patchInstanceStatusRestarting,
// Migrate's source + surge stampers, Timeouts' stuck / fallback
// writers). The shared writer lives here so each op only needs one
// import of status/ rather than depending on whichever op file
// originally hosted it.
//
// In-memory mirror: after a successful write MutateInstance mirrors the
// post-write (component, idx) InstanceStatus onto the caller's `isvc`
// snapshot. The dispatcher (ops.ReconcileComponentOps) drives multiple
// per-Instance operations off a single in-memory ISVC bag
// (ReconcileParams.ISVC) during one reconcile pass — without the mirror,
// the second operation reads pre-write state for every field the first
// operation just mutated. The user-visible symptom is the SurgeThenDrain
// multi-instance ord-0 resurrection: inst-0 promotes (Phase 3 advances
// ActiveOrdinal to 1), Update returns done=true, dispatcher falls through
// to Create on the same reconcile, and Create's
// expectedPodNamesForInstance reads the stale ActiveOrdinal=0 from `isvc`
// and re-creates a pod at the drained slot. The mirror keeps
// `isvc.Status` aligned with what was just committed so the rest of the
// reconcile sees fresh state. Same shape as the bumpCollisionCount
// precedent in reconciler.go.
//
// NotFound handling: if the ISVC was deleted between the caller pulling
// the work item and us re-reading, both the Get and the Status().Update
// return apierrors.IsNotFound. Treat both as "operation succeeded
// trivially" — there is no status left to mutate — and drop the error.
// Otherwise controller-runtime retries the whole reconcile until the
// queue drains, which saturates the work queue and stalls unrelated
// reconciles (root cause of the buildkite CI flakes that showed up as
// "patch status Ready (instance=N): re-read ISVC: ... not found"
// log spam during the load-test windows).
func MutateInstance(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32, mutate func(*v1beta1.OMENativeInstanceStatus) bool) error {
	// committed captures the (component, idx) InstanceStatus that
	// successfully landed at the apiserver on the final retry attempt,
	// for the in-memory mirror below (after the write succeeds).
	var committed *v1beta1.OMENativeInstanceStatus
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		committed = nil
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("re-read ISVC: %w", err)
		}

		if fresh.Status.Components == nil {
			fresh.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
		}
		cs := fresh.Status.Components[component]
		if cs.Lifecycle == nil {
			cs.Lifecycle = &v1beta1.LifecycleStatus{}
		}

		// Locate or append the InstanceStatus entry.
		insts := cs.Lifecycle.InstanceStatuses
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
		cs.Lifecycle.InstanceStatuses = insts
		fresh.Status.Components[component] = cs

		if err := c.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("update ISVC status: %w", err)
		}
		committed = slot.DeepCopy()
		return nil
	})
	if err != nil {
		return err
	}
	if committed != nil {
		mirrorInstanceStatus(isvc, component, *committed)
	}
	return nil
}

// mirrorInstanceStatus copies the just-committed InstanceStatus onto
// the caller's in-memory ISVC snapshot so subsequent operations in the
// same reconcile pass observe the post-write state rather than the
// stale pre-reconcile snapshot. Creates the Components map / OMENative
// sub-status / InstanceStatuses slice / target slot on demand so the
// dispatcher doesn't need to pre-allocate any of those fields.
func mirrorInstanceStatus(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, src v1beta1.OMENativeInstanceStatus) {
	if isvc == nil {
		return
	}
	if isvc.Status.Components == nil {
		isvc.Status.Components = make(map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec)
	}
	cs := isvc.Status.Components[component]
	if cs.Lifecycle == nil {
		cs.Lifecycle = &v1beta1.LifecycleStatus{}
	}
	pos := -1
	for i, s := range cs.Lifecycle.InstanceStatuses {
		if s.Index == src.Index {
			pos = i
			break
		}
	}
	if pos == -1 {
		cs.Lifecycle.InstanceStatuses = append(cs.Lifecycle.InstanceStatuses, src)
	} else {
		cs.Lifecycle.InstanceStatuses[pos] = src
	}
	isvc.Status.Components[component] = cs
}
