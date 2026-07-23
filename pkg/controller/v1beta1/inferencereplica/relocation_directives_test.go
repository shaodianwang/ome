package inferencereplica

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// directiveEntry builds one terminal relocation-directive ledger row.
func directiveEntry(uid, component string, idx int32, node string) audit.Entry {
	return audit.Entry{
		RequestUUID:    uid,
		Component:      component,
		SourceInstance: idx,
		Phase:          audit.PhaseCompleted,
		Reason:         audit.ReasonAutoRecover,
		Outcome:        audit.OutcomeRelocateRecreate,
		FromNode:       node,
		StartedAt:      time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
	}
}

// reconcileRelocationDirectives projects the exclusion map from the
// ledger's AutoRecover directives, bounded to the most recent
// autoMigrateBudget DISTINCT nodes per instance, scoped to the IR's
// component, and ignoring non-AutoRecover rows.
func TestReconcileRelocationDirectives_BuildsBoundedExclusionMap(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	r, c := newReconciler(t, ir)

	ledger := &audit.Ledger{}
	// Five directives for instance 0 — dedup happens BEFORE the budget
	// window, so the repeated n4 doesn't shrink the memory: the last
	// three DISTINCT nodes {n2,n3,n4} survive.
	for i, node := range []string{"n1", "n2", "n3", "n4", "n4"} {
		ledger.UpsertEntry(directiveEntry(fmt.Sprintf("u%d", i), "engine", 0, node))
	}
	// Noise: other component + non-AutoRecover operator migration.
	ledger.UpsertEntry(directiveEntry("dec", "decoder", 0, "n9"))
	ledger.UpsertEntry(audit.Entry{RequestUUID: "op1", Component: "engine", SourceInstance: 0,
		Phase: audit.PhaseStarted, Reason: "fragmentation", FromNode: "n8",
		StartedAt: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)})
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 {
		t.Fatalf("map: got %v want exactly instance 0", got)
	}
	nodes := got[0]
	if len(nodes) != 3 || nodes[0] != "n2" || nodes[1] != "n3" || nodes[2] != "n4" {
		t.Errorf("instance 0 exclusions: got %v want [n2 n3 n4] (last 3 distinct nodes)", nodes)
	}

	// Budget 0 (unconfigured) → no exclusions at all.
	if got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 0); got != nil {
		t.Errorf("budget 0: got %v want nil", got)
	}
}

// An instance observed Phase=Ready with no in-flight Operation has
// proven its placement: its AutoRecover directives are pruned from the
// persisted ledger (success-prune mirror of the RetryBlock prune) and
// drop out of the exclusion map. Foreign entries survive.
func TestReconcileRelocationDirectives_PrunesOnReady(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	r, c := newReconciler(t, ir)

	ledger := &audit.Ledger{}
	ledger.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	ledger.UpsertEntry(directiveEntry("u1", "engine", 1, "n2"))
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 || len(got[1]) != 1 || got[1][0] != "n2" {
		t.Fatalf("map: got %v want only instance 1 → [n2] (instance 0 pruned on Ready)", got)
	}

	// The prune persisted: instance 0's directive is gone from the CM,
	// instance 1's survives.
	after, err := audit.LoadLedgerForOwner(context.Background(), c, ir)
	if err != nil {
		t.Fatalf("reload ledger: %v", err)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 0) != 0 {
		t.Errorf("instance 0 directives not pruned: %+v", after.Entries)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 1) != 1 {
		t.Errorf("instance 1 directives must survive: %+v", after.Entries)
	}
}

// A Ready instance with an in-flight Operation (e.g. a fresh update
// just stamped) is NOT pruned — only settled Ready-no-op instances
// reset their relocation memory.
func TestReconcileRelocationDirectives_ReadyWithOpNotPruned(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate,
				StartedAt: metav1.NewTime(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))}},
	}
	r, c := newReconciler(t, ir)
	ledger := &audit.Ledger{}
	ledger.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 || len(got[0]) != 1 || got[0][0] != "n1" {
		t.Fatalf("map: got %v want instance 0 → [n1] (no prune while op in flight)", got)
	}
}
