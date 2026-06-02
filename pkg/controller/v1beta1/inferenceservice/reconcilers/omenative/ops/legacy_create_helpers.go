package ops

// Helpers carved out of create.go and delete.go when those per-Instance
// state machines moved to pkg/controller/v1beta1/workload/ops. The
// helpers below remain in this package because the peer ops still here
// (timeouts.go) call them directly and read the
// v1beta1.OMENativeInstanceStatus shape.
//
// Each helper shrinks out as the matching caller migrates to
// workload/ops in future refactors.

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
)

// patchInstanceStatusReady idempotently moves to Phase=Ready, clears Operation.
func patchInstanceStatusReady(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == v1beta1.OMENativeInstanceReady && s.Operation == nil {
			return false
		}
		s.Phase = v1beta1.OMENativeInstanceReady
		s.Operation = nil
		return true
	})
}

// removeInstanceStatus drops the InstanceStatus entry for (component, idx).
// hadEntry=true when a real removal happened, false when already cleared.
// Wrapped in retry.RetryOnConflict; re-checks existence on each attempt so
// a parallel clear becomes a no-op.
//
// Mirrors the buildRemoveInstance closure in internalsource (which wraps
// the same operation as a workload.ReconcileInput.RemoveInstance callback).
// Kept here as a v1beta1-typed function so peer ops still in this package
// (migrate.go, timeouts.go) call it byte-for-byte unchanged. Migrates to
// workload/ops alongside its callers in follow-up slices.
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
		if !ok || cs.Lifecycle == nil {
			return nil
		}
		insts := cs.Lifecycle.InstanceStatuses
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
		cs.Lifecycle.InstanceStatuses = append(insts[:pos], insts[pos+1:]...)
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
		p.ExpectationsCache().Forget(p.ISVC.Namespace, p.ISVC.Name, v1beta1convert.ComponentTypeToWorkload(component), idx)
	}
	return hadEntry, nil
}
