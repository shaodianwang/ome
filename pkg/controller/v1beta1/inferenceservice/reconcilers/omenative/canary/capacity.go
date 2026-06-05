package canary

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// resolveStepCapacity resolves a step's NewCapacity / OldCapacity against the
// component's desired replica count, returning concrete (newCount, oldCount)
// pod counts. NewCapacity rounds up (so a TrafficWeight>0 step always gets at
// least one new pod). OldCapacity defaults to (desired - newCount) — i.e.
// "100% - NewCapacity" — when unset; an explicit OldCapacity allows a temporary
// surge (both sides may exceed 50%). Counts are clamped to [0, desired].
func resolveStepCapacity(step v1beta1.RolloutStep, desired int32) (newCount, oldCount int32) {
	newCount = clamp(scale(step.NewCapacity, desired), desired)
	if step.OldCapacity != nil {
		oldCount = clamp(scale(*step.OldCapacity, desired), desired)
	} else {
		oldCount = desired - newCount
	}
	return newCount, oldCount
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
