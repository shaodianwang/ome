package inferencereplica

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// reconcileForReadyTimeout runs one pass over a fresh IR and returns the
// stored object. lifecycleJSON of "" leaves the operator ConfigMap
// unwired, which is how an operator who supplies no lifecycle block at
// all is seen from inside the controller.
func reconcileForReadyTimeout(t *testing.T, ir *v1beta1.InferenceReplica, lifecycleJSON string) (*v1beta1.InferenceReplica, []string) {
	t.Helper()
	g := gomega.NewWithT(t)
	r, c := newReconciler(t, ir)
	rec := record.NewFakeRecorder(16)
	r.Recorder = rec
	if lifecycleJSON != "" {
		withLifecycleConfig(r, lifecycleJSON)
	}

	key := client.ObjectKeyFromObject(ir)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	stored := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, stored)).To(gomega.Succeed())
	return stored, drainEvents(rec)
}

func conditionOfType(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

// TestInstanceReadyTimeout_UnconfiguredOpensWithNoDeadline pins the
// honest-unconfigured contract: with no readiness window at either level
// the Create operation opens with NO deadline — never one equal to its
// own start, which the next pass would read as elapsed — and the
// Component says so through a condition and a Warning event.
func TestInstanceReadyTimeout_UnconfiguredOpensWithNoDeadline(t *testing.T) {
	g := gomega.NewWithT(t)

	stored, events := reconcileForReadyTimeout(t, baselineIR("llama-engine", "default", 1), "")

	g.Expect(stored.Status.InstanceStatuses).To(gomega.HaveLen(1))
	op := stored.Status.InstanceStatuses[0].Operation
	g.Expect(op).NotTo(gomega.BeNil())
	g.Expect(op.Deadline.IsZero()).To(gomega.BeTrue(),
		"an unconfigured readiness window must open the operation with no deadline")

	cond := conditionOfType(stored.Status.Conditions, string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured))
	g.Expect(cond).NotTo(gomega.BeNil())
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue))
	g.Expect(cond.Reason).To(gomega.Equal(string(workloadtypes.ReasonInstanceReadyTimeoutUnconfigured)))
	g.Expect(eventsContaining(events, string(workloadtypes.EventReasonInstanceReadyTimeoutUnconfigured))).To(gomega.HaveLen(1))
}

// TestInstanceReadyTimeout_ConfigSuppliesTheWindow pins the fallback: a
// Component that sets none of its own takes the operator's
// lifecycle.instanceReadyTimeout, and the condition clears.
func TestInstanceReadyTimeout_ConfigSuppliesTheWindow(t *testing.T) {
	g := gomega.NewWithT(t)

	before := time.Now()
	stored, events := reconcileForReadyTimeout(t, baselineIR("llama-engine", "default", 1),
		`{"instanceReadyTimeout":"30m"}`)

	g.Expect(stored.Status.InstanceStatuses).To(gomega.HaveLen(1))
	op := stored.Status.InstanceStatuses[0].Operation
	g.Expect(op).NotTo(gomega.BeNil())
	g.Expect(op.Deadline.Time).To(gomega.BeTemporally("~", before.Add(30*time.Minute), time.Minute))

	cond := conditionOfType(stored.Status.Conditions, string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured))
	g.Expect(cond).NotTo(gomega.BeNil())
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(cond.Reason).To(gomega.Equal(string(workloadtypes.ReasonInstanceReadyTimeoutConfigured)))
	g.Expect(eventsContaining(events, string(workloadtypes.EventReasonInstanceReadyTimeoutUnconfigured))).To(gomega.BeEmpty())
}

// TestInstanceReadyTimeout_SpecWinsOverConfig pins the precedence: an
// explicit per-resource window overrides the operator's.
func TestInstanceReadyTimeout_SpecWinsOverConfig(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "default", 1)
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{
		InstanceReadyTimeout: &metav1.Duration{Duration: 5 * time.Minute},
	}

	before := time.Now()
	stored, _ := reconcileForReadyTimeout(t, ir, `{"instanceReadyTimeout":"30m"}`)

	g.Expect(stored.Status.InstanceStatuses).To(gomega.HaveLen(1))
	op := stored.Status.InstanceStatuses[0].Operation
	g.Expect(op).NotTo(gomega.BeNil())
	g.Expect(op.Deadline.Time).To(gomega.BeTemporally("~", before.Add(5*time.Minute), time.Minute))
}
