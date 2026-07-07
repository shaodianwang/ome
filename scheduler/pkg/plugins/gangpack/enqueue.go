package gangpack

import (
	"context"
	"fmt"

	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling"
)

// With QueueingHints (GA in recent kube-scheduler), a pod a plugin rejected is
// requeued only on cluster events that plugin has REGISTERED — otherwise it waits
// out the scheduler's slow periodic unschedulable flush. GangPack rejects pods
// two ways, each with its own unblocking events:
//
//   - Permit gate / Unreserve: the gang is incomplete. It becomes admissible when
//     a sibling arrives (Pod Add) or the PodGroup changes (e.g. minMember drops).
//   - PreFilter Unschedulable: no domain currently fits the gang. It becomes
//     admissible when capacity frees (Pod Delete) or is added (Node Add/update).
//
// Registering these is what makes a gang recover promptly instead of on the
// periodic flush. We register the events without QueueingHint functions (any
// matching event requeues), matching scheduler-plugins/coscheduling; narrowing
// with hint fns is a later optimization.
func (g *GangPack) EventsToRegister(_ context.Context) ([]framework.ClusterEventWithHint, error) {
	// Custom-resource events are named "<resource>.<version>.<group>".
	pgGVK := fmt.Sprintf("podgroups.v1alpha1.%v", scheduling.GroupName)
	return []framework.ClusterEventWithHint{
		// Sibling arriving (Add) completes a gated gang; a pod leaving (Delete) or
		// being rescheduled (Update) frees a node a placement-blocked gang can take.
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Add | framework.Delete | framework.Update}},
		// A new node, or one that gains allocatable / sheds a taint / turns Ready,
		// may let a gang that fit no domain now fit.
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeAllocatable | framework.UpdateNodeTaint | framework.UpdateNodeCondition}},
		// PodGroup changes (minMember, or the PodGroup first appearing) may make a
		// gang admissible.
		{Event: framework.ClusterEvent{Resource: framework.EventResource(pgGVK), ActionType: framework.Add | framework.Update}},
	}, nil
}
