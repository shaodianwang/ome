package canary

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func ptrIOS(v intstr.IntOrString) *intstr.IntOrString { return &v }

func TestResolveStepCapacity(t *testing.T) {
	cases := []struct {
		name             string
		step             v1beta1.RolloutStep
		desired          int32
		wantNew, wantOld int32
	}{
		{"10pct-default-old", v1beta1.RolloutStep{NewCapacity: intstr.FromString("10%")}, 10, 1, 9},
		{"100pct-default-old", v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%")}, 4, 4, 0},
		{"surge-both-100", v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%"), OldCapacity: ptrIOS(intstr.FromString("100%"))}, 4, 4, 4},
		{"absolute-new", v1beta1.RolloutStep{NewCapacity: intstr.FromInt(2)}, 10, 2, 8},
		{"25pct-ceil", v1beta1.RolloutStep{NewCapacity: intstr.FromString("25%")}, 4, 1, 3},
		{"10pct-of-4-ceils-to-1", v1beta1.RolloutStep{NewCapacity: intstr.FromString("10%")}, 4, 1, 3},
		{"explicit-old", v1beta1.RolloutStep{NewCapacity: intstr.FromString("50%"), OldCapacity: ptrIOS(intstr.FromString("50%"))}, 8, 4, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotNew, gotOld := resolveStepCapacity(tc.step, tc.desired)
			if gotNew != tc.wantNew || gotOld != tc.wantOld {
				t.Fatalf("resolveStepCapacity(%+v, %d) = (%d,%d), want (%d,%d)", tc.step, tc.desired, gotNew, gotOld, tc.wantNew, tc.wantOld)
			}
		})
	}
}

// partitionForNewCount maps a desired new-revision count to the StatefulSet-style
// RollingUpdate.Partition the workload reconcile honors (instances with index <
// Partition are held on the old revision).
func TestPartitionForNewCount(t *testing.T) {
	if got := partitionForNewCount(10, 1); got != 9 {
		t.Fatalf("partition for 1 new of 10 = %d, want 9", got)
	}
	if got := partitionForNewCount(4, 4); got != 0 {
		t.Fatalf("partition for full roll = %d, want 0", got)
	}
	if got := partitionForNewCount(4, 7); got != 0 {
		t.Fatalf("partition clamps at 0 when newCount>desired, got %d", got)
	}
}
