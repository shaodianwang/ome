package canary

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestResolveStepNewCount(t *testing.T) {
	cases := []struct {
		name    string
		step    v1beta1.RolloutStep
		desired int32
		wantNew int32
	}{
		{"10pct", v1beta1.RolloutStep{NewCapacity: intstr.FromString("10%")}, 10, 1},
		{"100pct", v1beta1.RolloutStep{NewCapacity: intstr.FromString("100%")}, 4, 4},
		{"absolute-new", v1beta1.RolloutStep{NewCapacity: intstr.FromInt(2)}, 10, 2},
		{"25pct-ceil", v1beta1.RolloutStep{NewCapacity: intstr.FromString("25%")}, 4, 1},
		{"10pct-of-4-ceils-to-1", v1beta1.RolloutStep{NewCapacity: intstr.FromString("10%")}, 4, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveStepNewCount(tc.step, tc.desired); got != tc.wantNew {
				t.Fatalf("resolveStepNewCount(%+v, %d) = %d, want %d", tc.step, tc.desired, got, tc.wantNew)
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
