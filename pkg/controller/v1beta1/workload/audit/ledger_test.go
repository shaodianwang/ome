package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Ledger round-trip + annotation-parsing tests. Moved here from the
// retired omenative/ops package (whose detection layer these helpers
// used to back) — the ledger itself is audit-only now, but dedup and
// trim behavior still guard the history surface.

func TestExtractRequestUUID(t *testing.T) {
	if got := ExtractRequestUUID(MigrationRequestAnnotationPrefix + "abc-123"); got != "abc-123" {
		t.Errorf("uuid: got %q want abc-123", got)
	}
	if got := ExtractRequestUUID("ome.io/something-else"); got != "" {
		t.Errorf("non-migration key should return empty, got %q", got)
	}
}

func TestParseMigrationRequest_Valid(t *testing.T) {
	raw, _ := json.Marshal(MigrationRequest{
		SchemaVersion:   SchemaV1,
		Component:       "engine",
		Instance:        0,
		FromNode:        "node5",
		HintTargetNodes: []string{"node3", "node7"},
		Reason:          "fragmentation",
	})
	req, err := ParseMigrationRequest(string(raw))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if req.Instance != 0 || req.FromNode != "node5" || req.Component != "engine" {
		t.Errorf("parsed request mismatch: %+v", req)
	}
	if len(req.HintTargetNodes) != 2 {
		t.Errorf("hint targets: got %d want 2", len(req.HintTargetNodes))
	}
}

func TestParseMigrationRequest_UnsupportedSchema(t *testing.T) {
	raw := `{"schemaVersion":"v99","component":"engine","instance":0,"from_node":"n"}`
	if _, err := ParseMigrationRequest(raw); err == nil {
		t.Fatal("expected UnsupportedSchemaVersion error")
	}
}

func TestAuditLedger_TrimDropsOldestTerminalButKeepsStarted(t *testing.T) {
	l := &Ledger{}
	// Seed an in-flight entry first — must survive trim.
	l.UpsertEntry(Entry{RequestUUID: "in-flight-pinned", Phase: PhaseStarted})
	for i := 0; i < MaxTerminalEntries+50; i++ {
		l.UpsertEntry(Entry{
			RequestUUID: fmt.Sprintf("done-%04d", i),
			Phase:       PhaseCompleted,
		})
	}
	terminalCount := 0
	startedSurvived := false
	for _, e := range l.Entries {
		switch e.Phase {
		case PhaseCompleted, PhaseFailed:
			terminalCount++
		case PhaseStarted:
			if e.RequestUUID == "in-flight-pinned" {
				startedSurvived = true
			}
		}
	}
	if terminalCount != MaxTerminalEntries {
		t.Errorf("terminal entries: got %d want %d", terminalCount, MaxTerminalEntries)
	}
	if !startedSurvived {
		t.Errorf("Started entry must survive trim; ledger=%v", l.Entries)
	}
	// The oldest terminal (done-0000) should have been dropped.
	for _, e := range l.Entries {
		if e.RequestUUID == "done-0000" {
			t.Errorf("oldest terminal entry should have been trimmed; got %+v", e)
		}
	}
}

func TestAuditLedger_DedupAcrossPersist(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: types.UID("owner-uid"),
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")

	ledger, err := LoadLedgerForOwner(context.Background(), c, owner)
	if err != nil {
		t.Fatalf("load empty ledger: %v", err)
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("expected empty ledger, got %d entries", len(ledger.Entries))
	}

	ledger.UpsertEntry(Entry{RequestUUID: "abc", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(context.Background(), c, owner, gvk, ledger); err != nil {
		t.Fatalf("persist: %v", err)
	}

	reloaded, err := LoadLedgerForOwner(context.Background(), c, owner)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !reloaded.HasCompletedOrFailedRequest("abc") {
		t.Errorf("expected reloaded ledger to record abc as completed")
	}
	if reloaded.HasCompletedOrFailedRequest("xyz") {
		t.Errorf("unrelated UUID should not be flagged")
	}
}
