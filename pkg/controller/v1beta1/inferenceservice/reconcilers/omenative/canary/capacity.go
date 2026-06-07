package canary

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// resolveStepNewCount resolves a step's NewCapacity against the component's
// desired replica count, returning the concrete new-revision pod count. Rounds
// up (so a TrafficWeight>0 step always gets at least one new pod) and clamps to
// [0, desired]. The old revision runs the complement (desired - newCount) via
// the partition — there is no independent old capacity: OldCapacity is validated
// at admission to equal that complement (surge above desired is unsupported in
// v1), so it does not feed the runtime count.
func resolveStepNewCount(step v1beta1.RolloutStep, desired int32) int32 {
	return clamp(scale(step.NewCapacity, desired), desired)
}

// partitionForNewCount maps a desired new-revision count to the StatefulSet-style
// RollingUpdate.Partition the workload reconcile honors: instances with index <
// Partition are held on the old revision, so (desired - newCount) old instances
// are held and newCount roll to the canary revision. Clamped to [0, desired].
func partitionForNewCount(desired, newCount int32) int32 {
	p := desired - newCount
	if p < 0 {
		return 0
	}
	return p
}

func scale(v intstr.IntOrString, total int32) int32 {
	n, err := intstr.GetScaledValueFromIntOrPercent(&v, int(total), true)
	if err != nil || n < 0 {
		return 0
	}
	return int32(n)
}

func clamp(v, max int32) int32 {
	if v < 0 {
		return 0
	}
	if v > max {
		return max
	}
	return v
}
