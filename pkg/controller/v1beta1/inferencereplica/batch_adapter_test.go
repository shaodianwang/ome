package inferencereplica

// buildApplyInstanceMutations adapter contract: the batched sibling of
// buildMutateInstance — one fresh Get, every mutation applied, ONE
// Status().Update, retry-on-conflict as a whole, zero writes when no
// mutation reports a change, NotFound-as-no-op, and the committed slots
// mirrored onto the caller's in-memory IR.

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// newCountingStatusClient builds a fake client over objs that counts
// status-subresource Update calls, optionally failing the first
// failFirst of them with a Conflict.
func newCountingStatusClient(t *testing.T, failFirst int, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	writes := new(int)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			*writes++
			if *writes <= failFirst {
				return apierrors.NewConflict(
					schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
					obj.GetName(), fmt.Errorf("the object has been modified"))
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build(), writes
}

// failedStamp is a canonical batchable mutation: flip the slot Failed.
func failedStamp(idx int32) workloadtypes.InstanceMutation {
	return workloadtypes.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseFailed {
			return false
		}
		s.Phase = workload.InstancePhaseFailed
		return true
	}}
}

// TestBuildApplyInstanceMutations_OneWriteForBatch pins the headline
// contract: a batch touching several Instances lands in exactly ONE
// Status().Update, every mutation is persisted, and the committed slots
// are mirrored back onto the in-memory IR.
func TestBuildApplyInstanceMutations_OneWriteForBatch(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting, Incarnation: 2},
		{Index: 1, Phase: v1beta1.OMENativeInstanceRestarting, Incarnation: 2},
	}
	c, writes := newCountingStatusClient(t, 0, ir)

	apply := buildApplyInstanceMutations(c, ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0), failedStamp(1),
	})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(1),
		"a multi-instance batch must land in exactly ONE Status().Update")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(2))
	for i, s := range got.Status.InstanceStatuses {
		g.Expect(s.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceFailed), "instance %d", i)
		g.Expect(s.Incarnation).To(gomega.Equal(int64(2)), "instance %d untouched fields survive", i)
	}
	g.Expect(ir.Status.InstanceStatuses).To(gomega.Equal(got.Status.InstanceStatuses),
		"committed slots must be mirrored onto the in-memory IR")
}

// TestBuildApplyInstanceMutations_ConflictRetriesWholeBatch pins the
// conflict contract: a conflicted batch retries AS A WHOLE — the second
// attempt re-reads and re-applies every mutation, and the final state
// carries all of them.
func TestBuildApplyInstanceMutations_ConflictRetriesWholeBatch(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceRestarting},
		{Index: 1, Phase: v1beta1.OMENativeInstanceRestarting},
	}
	c, writes := newCountingStatusClient(t, 1, ir)

	apply := buildApplyInstanceMutations(c, ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0), failedStamp(1),
	})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(2),
		"first write conflicts, the whole batch retries once")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	for i, s := range got.Status.InstanceStatuses {
		g.Expect(s.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceFailed),
			"instance %d must carry its mutation after the batch retry", i)
	}
}

// TestBuildApplyInstanceMutations_NoChangeZeroWrites pins the no-op
// property: a batch whose mutations all report no change performs ZERO
// writes, and a probed-but-unchanged missing index is not appended as a
// phantom slot.
func TestBuildApplyInstanceMutations_NoChangeZeroWrites(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceFailed},
	}
	c, writes := newCountingStatusClient(t, 0, ir)

	apply := buildApplyInstanceMutations(c, ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0), // already Failed — reports no change
		{Index: 7, Mutate: func(s *workload.InstanceStatus) bool {
			// Missing-slot probe that declines: guard sentinel shape.
			return s.Phase != ""
		}},
	})).To(gomega.Succeed())

	g.Expect(*writes).To(gomega.Equal(0),
		"an all-no-op batch must perform ZERO status writes")

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(1),
		"a declined missing-index mutation must not append a phantom slot")
}

// TestBuildApplyInstanceMutations_NotFoundIsNoOp pins the gone-owner
// contract shared with buildMutateInstance: an IR deleted under us is a
// successful no-op, not an error.
func TestBuildApplyInstanceMutations_NotFoundIsNoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	c, writes := newCountingStatusClient(t, 0) // IR never stored

	apply := buildApplyInstanceMutations(c, ir)
	g.Expect(apply(context.Background(), []workloadtypes.InstanceMutation{
		failedStamp(0),
	})).To(gomega.Succeed())
	g.Expect(*writes).To(gomega.Equal(0))
}
