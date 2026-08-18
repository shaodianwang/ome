package irprojector

import (
	"context"
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ComponentIRStatus returns the authoritative InferenceReplica status for one
// Component of an ISVC, read via the supplied reader (cached client is fine —
// these are non-destructive coordination reads). Returns (nil, nil) when the IR
// does not exist yet, so callers treat "no authoritative status" the same as the
// pre-migration empty projection. Any other read failure is returned as a
// wrapped error — callers MUST distinguish it from the missing-IR case: a
// transient read error is not "no observation yet", and safety gates that treat
// it that way fail open. This is the single source of truth every in-cluster
// decision reads instead of the ISVC's copied LifecycleStatus.
func ComponentIRStatus(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (*v1beta1.InferenceReplicaStatus, error) {
	// A nil reader degrades to "no authoritative status" rather than
	// panicking a reconcile — same result callers get for a missing IR.
	if reads == nil {
		return nil, nil
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: namespace, Name: InferenceReplicaName(isvcName, c)}
	if err := reads.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get InferenceReplica %s/%s: %w", key.Namespace, key.Name, err)
	}
	return &ir.Status, nil
}

// ComponentIRPartition returns the effective rollingUpdate partition for one
// Component of an ISVC, read from the projected InferenceReplica spec
// (spec.lifecycle.updateStrategy.rollingUpdate.partition). The IR spec carries
// the merged ISVC↔runtime lifecycle, so this is the partition the workload
// controller actually stages Instances at — including a partition inherited
// from the ServingRuntime, which the raw ISVC spec never shows. Coordination
// MUST read this value rather than re-deriving it from the unmerged ISVC:
// a raw-spec read reports partition 0 for a runtime-staged Component and
// treats its held Instances as an incomplete rollout forever.
//
// Returns 0 when the IR does not exist yet or no partition is set (the
// API-defined "update every Instance" value). Any other read failure is
// returned as a wrapped error so safety gates can fail closed instead of
// mistaking a flaky read for "no partition".
func ComponentIRPartition(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (int32, error) {
	// A nil reader degrades to "no partition" — same result callers get
	// for a missing IR.
	if reads == nil {
		return 0, nil
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: namespace, Name: InferenceReplicaName(isvcName, c)}
	if err := reads.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("get InferenceReplica %s/%s: %w", key.Namespace, key.Name, err)
	}
	lc := ir.Spec.Lifecycle
	if lc == nil || lc.UpdateStrategy == nil || lc.UpdateStrategy.RollingUpdate == nil || lc.UpdateStrategy.RollingUpdate.Partition == nil {
		return 0, nil
	}
	return *lc.UpdateStrategy.RollingUpdate.Partition, nil
}
