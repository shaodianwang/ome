package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// migrationRequestJSON builds the canonical Alfred annotation value.
func migrationRequestJSON(component string, instance int32, fromNode string, hints ...string) string {
	req := audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       component,
		Instance:        instance,
		FromNode:        fromNode,
		HintTargetNodes: hints,
		Reason:          "fragmentation",
		RequestedAt:     "2026-05-21T00:00:00Z",
		RequestedBy:     "alfred-controller",
	}
	raw, _ := json.Marshal(req)
	return string(raw)
}

// addMigrationAnnotation writes the Alfred-style annotation onto the
// ISVC and returns the request UUID portion.
func addMigrationAnnotation(t *testing.T, isvc *v1beta1.InferenceService, uuid string, value string) string {
	t.Helper()
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	key := audit.MigrationRequestAnnotationPrefix + uuid
	isvc.Annotations[key] = value
	return uuid
}

// --- audit.go helpers ---

func TestExtractRequestUUID(t *testing.T) {
	if got := audit.ExtractRequestUUID(audit.MigrationRequestAnnotationPrefix + "abc-123"); got != "abc-123" {
		t.Errorf("uuid: got %q want abc-123", got)
	}
	if got := audit.ExtractRequestUUID("ome.io/something-else"); got != "" {
		t.Errorf("non-migration key should return empty, got %q", got)
	}
}

func TestParseMigrationRequest_Valid(t *testing.T) {
	raw := migrationRequestJSON("engine", 0, "node5", "node3", "node7")
	req, err := audit.ParseMigrationRequest(raw)
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
	_, err := audit.ParseMigrationRequest(raw)
	if err == nil {
		t.Fatal("expected UnsupportedSchemaVersion error")
	}
}

func TestAuditLedger_TrimDropsOldestTerminalButKeepsStarted(t *testing.T) {
	l := &audit.Ledger{}
	// Seed an in-flight entry first — must survive trim.
	l.UpsertEntry(audit.Entry{RequestUUID: "in-flight-pinned", Phase: audit.PhaseStarted})
	for i := 0; i < audit.MaxTerminalEntries+50; i++ {
		l.UpsertEntry(audit.Entry{
			RequestUUID: fmt.Sprintf("done-%04d", i),
			Phase:       audit.PhaseCompleted,
		})
	}
	terminalCount := 0
	startedSurvived := false
	for _, e := range l.Entries {
		switch e.Phase {
		case audit.PhaseCompleted, audit.PhaseFailed:
			terminalCount++
		case audit.PhaseStarted:
			if e.RequestUUID == "in-flight-pinned" {
				startedSurvived = true
			}
		}
	}
	if terminalCount != audit.MaxTerminalEntries {
		t.Errorf("terminal entries: got %d want %d", terminalCount, audit.MaxTerminalEntries)
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
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)

	ledger, err := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if err != nil {
		t.Fatalf("load empty ledger: %v", err)
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("expected empty ledger, got %d entries", len(ledger.Entries))
	}

	ledger.UpsertEntry(audit.Entry{RequestUUID: "abc", Phase: audit.PhaseCompleted})
	if err := internalsource.PersistAuditLedgerForISVC(context.Background(), c, isvc, ledger); err != nil {
		t.Fatalf("persist: %v", err)
	}

	reloaded, err := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
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

// --- detectMigrationRequest ---

func TestDetectMigrationRequest_NoAnnotations(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	_, _, _, found, err := detectMigrationRequest(context.Background(), p, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if found {
		t.Errorf("no annotations should report no request")
	}
}

func TestDetectMigrationRequest_PicksValidRequestForComponent(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	addMigrationAnnotation(t, isvc, "req-1", migrationRequestJSON("engine", 0, "node5"))
	// Add a decoder request — must be skipped for engine.
	addMigrationAnnotation(t, isvc, "req-2", migrationRequestJSON("decoder", 0, "node5"))
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	uuid, req, sourceIdx, found, err := detectMigrationRequest(context.Background(), p, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !found {
		t.Fatal("expected to find engine migration request")
	}
	if uuid != "req-1" {
		t.Errorf("uuid: got %q want req-1", uuid)
	}
	if sourceIdx != 0 {
		t.Errorf("sourceIdx: got %d want 0", sourceIdx)
	}
	if req.FromNode != "node5" {
		t.Errorf("FromNode: got %q want node5", req.FromNode)
	}
}

func TestDetectMigrationRequest_SkipsCompletedUUID(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	uuid := addMigrationAnnotation(t, isvc, "already-done", migrationRequestJSON("engine", 0, "node5"))
	c := newFakeClientWithDiscovery(t, isvc)
	// Pre-seed audit ledger with the request as Completed.
	ledger := &audit.Ledger{Entries: []audit.Entry{{RequestUUID: uuid, Phase: audit.PhaseCompleted}}}
	if err := internalsource.PersistAuditLedgerForISVC(context.Background(), c, isvc, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	p := minimalParams(t, isvc, c)

	_, _, _, found, err := detectMigrationRequest(context.Background(), p, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if found {
		t.Errorf("completed UUID must not be re-dispatched")
	}
}

func TestDetectMigrationRequest_RejectsBadSchemaWithFailedLedgerEntry(t *testing.T) {
	isvc := minimalISVC("llama-70b", "prod", 1)
	uuid := addMigrationAnnotation(t, isvc, "bad-schema", `{"schemaVersion":"v99","component":"engine","instance":0,"from_node":"n"}`)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	_, _, _, found, err := detectMigrationRequest(context.Background(), p, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if found {
		t.Errorf("unsupported schema must not be dispatched")
	}
	// Ledger should record a Failed entry so we don't keep retrying.
	ledger, _ := internalsource.LoadAuditLedgerForISVC(context.Background(), c, isvc)
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger should record failed UUID %q to suppress re-attempts", uuid)
	}
}
