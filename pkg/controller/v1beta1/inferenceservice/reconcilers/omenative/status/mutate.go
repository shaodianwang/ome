package status

import (
	"context"
	"fmt"

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
func MutateInstance(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32, mutate func(*v1beta1.OMENativeInstanceStatus) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(isvc), fresh); err != nil {
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

		if err := c.Status().Update(ctx, fresh); err != nil {
			return fmt.Errorf("update ISVC status: %w", err)
		}
		return nil
	})
}
