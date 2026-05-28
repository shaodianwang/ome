package workload_test

import (
	"context"
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// TestReconcileInput_Compiles is a compile-time pin on the
// ReconcileInput struct shape and the MutateInstance callback
// signature. The struct must be value-constructible with no required
// adapter scaffolding (no constructor, no helper), and the
// MutateInstance signature must accept a workload-owned InstanceStatus
// mutator. If a future slice changes either, this test fails to
// compile and forces a deliberate update.
func TestReconcileInput_Compiles(t *testing.T) {
	_ = workload.ReconcileInput{
		MutateInstance: func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			// Exercise the mutate callback shape with a no-op caller.
			var s workload.InstanceStatus
			_ = mutate(&s)
			_ = ctx
			_ = idx
			return nil
		},
	}
}
