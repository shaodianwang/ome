package holds

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/evidence"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// nodeUnknownHold is a name a silent node still holds. Phase Unknown is
// not terminal — the node stopped reporting and the container may still
// be running — so recycling the name the way a Failed or Succeeded
// occupant is recycled would risk two pods of one identity. The name is
// freed only by the force-delete sweep, on proven node death; until then
// the row reports the wait instead of spending its deadline in silence.
var nodeUnknownHold = authority{
	token:  types.WaitingReasonNodeUnknown,
	mayOwn: anyAttemptOwner,
	report: reportNodeUnknown,
}

// reportNodeUnknown reads the row's own pods for one occupying a name
// the rebuild needs. Only a TARGET name counts: a pod at any other
// ordinal — a surge source, a slot no longer asked for — blocks nothing.
//
// The sweep that frees a held name runs later in the same pass, in the
// verb pass that needs it, and this reading is taken ahead of it. So it
// asks the sweep's own question of every held pod and reports only the
// names the sweep will leave: a pod force-deleted this pass is a name
// freed, not a wait, and recording it would park the attempt's deadline
// for one pass and restart it on the next.
func reportNodeUnknown(ctx context.Context, in PassInput, row Row) (reading, error) {
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return reading{}, err
	}
	var held []*corev1.Pod
	for _, pod := range evidence.UnknownPhaseTargetPods(own, targetPodNames(in, row)) {
		frees, err := evidence.SweepFreesUnknownPod(ctx, in.Deps.Reader(), pod, in.Input.ForceDelete, in.Input.Now())
		if err != nil {
			return reading{}, err
		}
		if !frees {
			held = append(held, pod)
		}
	}
	if len(held) == 0 {
		return reading{release: true}, nil
	}
	return reading{waiting: true, evidence: types.NodeUnknownTermination(held[0])}, nil
}

// targetPodNames enumerates the pod names the row's plan asks for.
//
// Single-pod Runners read the ordinal slot from the row's recorded
// ActiveOrdinal, which SurgeThenDrain alternates between 0 and 1 across
// rollouts; multi-pod Runners occupy every 0..Size-1 ordinal. A row the
// plan no longer covers asks for no name at all.
func targetPodNames(in PassInput, row Row) map[string]struct{} {
	if row.Instance == nil {
		return nil
	}
	names := make(map[string]struct{})
	for _, runner := range row.Instance.Runners {
		if runner.Size == 1 {
			names[query.PodName(in.Input.Key.OwnerName, in.Plan.Component, row.Status.Index, runner.Name, row.Status.ActiveOrdinal)] = struct{}{}
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			names[query.PodName(in.Input.Key.OwnerName, in.Plan.Component, row.Status.Index, runner.Name, o)] = struct{}{}
		}
	}
	return names
}
