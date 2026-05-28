package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
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

	ledger, err := audit.LoadLedger(context.Background(), c, isvc)
	if err != nil {
		t.Fatalf("load empty ledger: %v", err)
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("expected empty ledger, got %d entries", len(ledger.Entries))
	}

	ledger.UpsertEntry(audit.Entry{RequestUUID: "abc", Phase: audit.PhaseCompleted})
	if err := audit.PersistLedger(context.Background(), c, isvc, ledger); err != nil {
		t.Fatalf("persist: %v", err)
	}

	reloaded, err := audit.LoadLedger(context.Background(), c, isvc)
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
	if err := audit.PersistLedger(context.Background(), c, isvc, ledger); err != nil {
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
	ledger, _ := audit.LoadLedger(context.Background(), c, isvc)
	if !ledger.HasCompletedOrFailedRequest(uuid) {
		t.Errorf("ledger should record failed UUID %q to suppress re-attempts", uuid)
	}
}

// --- Migrate state machine ---

func TestMigrate_NilClient(t *testing.T) {
	_, err := Migrate(context.Background(), core.ReconcileParams{}, core.ComponentPlan{Component: v1beta1.EngineComponent}, 0, "uuid", &audit.MigrationRequest{})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestMigrate_AlreadyCompleted_NoOp(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	ledger := &audit.Ledger{Entries: []audit.Entry{{RequestUUID: "done", Phase: audit.PhaseCompleted}}}
	if err := audit.PersistLedger(context.Background(), c, isvc, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Migrate(context.Background(), p, plan, 0, "done", &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "n"})
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if !done {
		t.Errorf("completed UUID should short-circuit done=true")
	}
}

func TestMigrate_RefusesWhenSourcePhaseNotReady(t *testing.T) {
	// Migration arriving during initial Create must NOT stamp
	// Phase=Migrating — that would overwrite the in-flight Create's
	// Operation and deadlock the Component. Returns done=false with
	// no status mutation; reconciler will retry when source reaches
	// Ready.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Force source into Phase=Creating with an active Create Operation
	// (typical state during initial bring-up).
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceCreating
	isvc.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type: v1beta1.InstanceOperationCreate,
		Step: "CreatePods",
	}
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	req := &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "node5"}

	done, err := Migrate(context.Background(), p, plan, 0, "req-mid-create", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: migration must wait for source to reach Ready")
	}

	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase == v1beta1.OMENativeInstanceMigrating {
		t.Errorf("source must NOT be stamped Migrating while another op owns it; got Phase=%q", s.Phase)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationCreate {
		t.Errorf("source Operation must be preserved; got %+v", s.Operation)
	}
	// No surge should have been allocated.
	if len(fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses) != 1 {
		t.Errorf("no surge InstanceStatus should exist; got %+v",
			fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses)
	}
}

func TestMigrate_RefusesWhenSourceRunningRevisionEmpty(t *testing.T) {
	// Migration arriving on a Ready source that has never recorded a
	// RunningRevision (e.g., a race where the post-create promote step
	// is pending) must also defer — otherwise surgeRevisionAndSpec
	// returns nil forever.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// RunningRevision is unset by default in isvcReadyAtIncarnation.
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	req := &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "node5"}

	done, err := Migrate(context.Background(), p, plan, 0, "req-no-rev", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: missing RunningRevision must defer migration")
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase == v1beta1.OMENativeInstanceMigrating {
		t.Errorf("source must NOT be stamped Migrating without a recorded RunningRevision; got Phase=%q", s.Phase)
	}
}

func TestMigrate_MissingSourceMarksFailed(t *testing.T) {
	// Alfred asks to migrate Instance 7 but no InstanceStatus[7]
	// exists. Permanent failure — record a Failed ledger row so the
	// reconciler doesn't keep re-dispatching the UUID.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	req := &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 7, FromNode: "node5"}

	done, err := Migrate(context.Background(), p, plan, 7, "req-missing", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if !done {
		t.Errorf("missing source should mark Failed and return done=true")
	}
	ledger, _ := audit.LoadLedger(context.Background(), c, isvc)
	if !ledger.HasCompletedOrFailedRequest("req-missing") {
		t.Errorf("ledger should record Failed entry for the missing-source request")
	}
}

// TestMigrate_MultiPodRejectedWithAuditEntry pins that Migrate refuses a
// multi-pod Instance migration with a Failed audit row. Multi-pod migration
// requires per-Runner surge ordinal allocation that the OEP defers; without
// this guard a multi-pod ISVC's migration would silently create a single-pod
// surge that can never satisfy the gang readiness gate, leaving the
// migration wedged until the deadline.
func TestMigrate_MultiPodRejectedWithAuditEntry(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	sourceSpec := targetSpecImage("llama:v1")
	sourcePod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	sourcePod.Spec.NodeName = "node5"
	c := newFakeClientWithDiscovery(t, isvc, sourcePod)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, sourceSpec)

	// Multi-pod plan: leader + 1 worker. Use BuildPlan with MultiPod=true
	// so the source Instance's Runners reflect a real multi-pod layout.
	p := minimalParams(t, isvc, c)
	p.MultiPod = true
	p.WorkerSize = 1
	p.WorkerPodSpec = &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "worker", Image: "llama:v1"}},
	}
	plan, err := core.BuildPlan(p)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     "engine",
		Instance:      0,
		FromNode:      "node5",
	}

	done, err := Migrate(context.Background(), p, plan, 0, "req-multipod", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if !done {
		t.Errorf("expected done=true (rejection is terminal)")
	}

	// Source InstanceStatus must NOT be stamped Phase=Migrating — the
	// rejection happened up-front, before any status mutation.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	s := fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses[0]
	if s.Phase == v1beta1.OMENativeInstanceMigrating {
		t.Errorf("source must NOT be stamped Migrating for multi-pod rejection; got %q", s.Phase)
	}

	// Audit ledger must carry the Failed entry naming the reason.
	ledger, _ := audit.LoadLedger(context.Background(), c, isvc)
	if !ledger.HasCompletedOrFailedRequest("req-multipod") {
		t.Fatalf("ledger should record Failed entry for the multi-pod request")
	}
	failed := false
	for _, e := range ledger.Entries {
		if e.RequestUUID == "req-multipod" && e.Phase == audit.PhaseFailed {
			failed = true
			if e.Outcome == "" {
				t.Errorf("Failed entry should carry a non-empty Outcome reason")
			}
		}
	}
	if !failed {
		t.Errorf("expected a Failed entry for req-multipod; got %+v", ledger.Entries)
	}

	// No surge InstanceStatus should be allocated.
	if len(fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses) != 1 {
		t.Errorf("no surge should be allocated; got %+v",
			fresh.Status.Components[v1beta1.EngineComponent].OMENative.InstanceStatuses)
	}
}

func TestMigrate_FirstPassAllocatesSurgeAndStampsBothStatuses(t *testing.T) {
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Source pod is on the running revision; surge needs a CR to point at.
	sourceSpec := targetSpecImage("llama:v1")
	sourcePod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	sourcePod.Spec.NodeName = "node5" // matches req.FromNode (validateFromNode gate)
	c := newFakeClientWithDiscovery(t, isvc, sourcePod)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, sourceSpec)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	req := &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "node5", HintTargetNodes: []string{"node3"}}

	done, err := Migrate(context.Background(), p, plan, 0, "req-1", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if done {
		t.Fatalf("expected done=false on first pass (surge pod just created)")
	}

	// Status: source Migrating + surge Creating.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	cs := fresh.Status.Components[v1beta1.EngineComponent]
	var src, srg *v1beta1.OMENativeInstanceStatus
	for i := range cs.OMENative.InstanceStatuses {
		s := &cs.OMENative.InstanceStatuses[i]
		if s.Index == 0 {
			src = s
		} else {
			srg = s
		}
	}
	if src == nil || src.Phase != v1beta1.OMENativeInstanceMigrating {
		t.Errorf("source should be Phase=Migrating; got %+v", src)
	}
	if src.Operation == nil || src.Operation.SurgeIndex == nil {
		t.Errorf("source Operation.SurgeIndex must be set")
	}
	if srg == nil || srg.Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("surge should be Phase=Creating; got %+v", srg)
	}
	if srg != nil && srg.Operation == nil {
		t.Errorf("surge Operation must be set")
	}

	// Audit ledger should record Started.
	ledger, _ := audit.LoadLedger(context.Background(), c, isvc)
	if ledger.InFlightEntry("req-1") == nil {
		t.Errorf("ledger should carry in-flight entry for req-1; got %+v", ledger.Entries)
	}

	// Surge pod should exist with the anti-affinity overlay.
	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	surgePodName := query.PodName(isvc.Name, v1beta1.EngineComponent, *src.Operation.SurgeIndex, "default", 0)
	var surgePod *corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].Name == surgePodName {
			surgePod = &pods.Items[i]
		}
	}
	if surgePod == nil {
		t.Fatalf("surge pod %s should exist; got %v", surgePodName, podListNames(pods.Items))
	}
	if surgePod.Spec.Affinity == nil || surgePod.Spec.Affinity.NodeAffinity == nil {
		t.Errorf("surge pod should have NodeAffinity from migration overlay")
	}
	if surgePod.Spec.Affinity != nil && surgePod.Spec.Affinity.NodeAffinity != nil {
		req := surgePod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if req == nil || len(req.NodeSelectorTerms) == 0 {
			t.Errorf("required anti-affinity from FromNode should be set")
		}
	}
}

func TestMigrate_WaitsForSurgeRotationBeforeDrainingSource(t *testing.T) {
	// Surge pods are ContainersReady and serving=True, but no slice
	// publishes them as Ready=true yet. Migrate must NOT mark source
	// not-serving — doing so would land a zero-routable-endpoint
	// window.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	sourceSpec := targetSpecImage("llama:v1")
	sourcePod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	sourcePod.Spec.NodeName = "node5" // matches req.FromNode (validateFromNode gate)
	c := newFakeClientWithDiscovery(t, isvc, sourcePod)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, sourceSpec)
	req := &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "node5"}

	// Pass 1: allocates surge, creates surge pod. Pass 2 onward: synthesize
	// ContainersReady on surge but DO NOT seed a rotation slice. Migrate
	// should never advance past Step 7b.
	for pass := 0; pass < 4; pass++ {
		fresh := &v1beta1.InferenceService{}
		_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
		pPass := minimalParams(t, fresh, c)
		planPass, _ := core.BuildPlan(pPass)
		// Surge pods get ContainersReady but no rotation slice.
		surgePods := &corev1.PodList{}
		_ = c.List(context.Background(), surgePods, client.InNamespace("prod"))
		for i := range surgePods.Items {
			if surgePods.Items[i].Name == sourcePod.Name {
				continue
			}
			synthesizeContainersReady(t, c, &surgePods.Items[i])
		}
		core.DefaultExpectations.Forget("prod", "llama-70b", v1beta1.EngineComponent, 0)
		if src := query.FindInstanceStatus(fresh, v1beta1.EngineComponent, 0); src != nil && src.Operation != nil && src.Operation.SurgeIndex != nil {
			core.DefaultExpectations.Forget("prod", "llama-70b", v1beta1.EngineComponent, *src.Operation.SurgeIndex)
		}
		done, err := Migrate(context.Background(), pPass, planPass, 0, "req-no-rotation", req)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if done {
			t.Fatalf("pass %d: migration must not complete without surge rotation", pass)
		}
	}
	// Source pod must STILL be serving — the drain shouldn't have started.
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(sourcePod), got)
	if !podreadiness.IsServing(got) {
		t.Errorf("source pod must remain serving until surge is in rotation; got conditions=%+v", got.Status.Conditions)
	}
}

func TestMigrate_ResumeFromLedgerWhenSourceStatusLost(t *testing.T) {
	// Torn-write recovery: source InstanceStatus was wiped between
	// patchInstanceStatusMigrating and persistAuditLedger (or a later
	// reconcile manually edited it away). The Started ledger entry
	// still holds the surge index; Migrate must pick that up via the
	// inFlight-branch of the switch rather than re-allocating.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	// Pre-seed ledger with Started entry pointing at surge index 2.
	ledger := &audit.Ledger{Entries: []audit.Entry{{
		RequestUUID:   "req-resume",
		Phase:         audit.PhaseStarted,
		SurgeInstance: 2,
		StartedAt:     metav1.Now().UTC().Format(time.RFC3339),
	}}}
	if err := audit.PersistLedger(context.Background(), c, isvc, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	// Simulate the lost-source-status state by wiping isvc.Status.
	isvc.Status.Components = nil
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	req := &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "node5"}

	done, err := Migrate(context.Background(), p, plan, 0, "req-resume", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// Source revision lookup will fail (no source status); Migrate must
	// bail without error and without re-allocating a new surge index.
	if done {
		t.Fatalf("expected done=false: no source revision available, no progress possible")
	}
	// Ledger entry must still point at surge 2 (not re-allocated).
	reloaded, _ := audit.LoadLedger(context.Background(), c, isvc)
	if entry := reloaded.InFlightEntry("req-resume"); entry == nil || entry.SurgeInstance != 2 {
		t.Errorf("ledger surge index must remain 2; got %+v", entry)
	}
}

func TestMigrate_DedupAfterCompletedLedgerEntry(t *testing.T) {
	// After step 11 persists Completed, even a subsequent reconcile that
	// finds source and surge in any state must short-circuit. This is
	// the post-fix safety: regardless of whether the destructive step
	// fully succeeded, hasCompletedOrFailedRequest blocks re-dispatch.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	ledger := &audit.Ledger{Entries: []audit.Entry{{
		RequestUUID: "req-completed",
		Phase:       audit.PhaseCompleted,
		StartedAt:   metav1.Now().UTC().Format(time.RFC3339),
		CompletedAt: metav1.Now().UTC().Format(time.RFC3339),
	}}}
	if err := audit.PersistLedger(context.Background(), c, isvc, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)

	done, err := Migrate(context.Background(), p, plan, 0, "req-completed",
		&audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "node5"})
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if !done {
		t.Errorf("Completed ledger entry must short-circuit Migrate")
	}
}

func TestMigrate_ConvergesAcrossPasses(t *testing.T) {
	// End-to-end happy path: drive Migrate until done=true, then verify
	// source is gone and surge is Ready at the source's revision.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	sourceSpec := targetSpecImage("llama:v1")
	sourcePod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	sourcePod.Spec.NodeName = "node5" // matches req.FromNode (validateFromNode gate)
	c := newFakeClientWithDiscovery(t, isvc, sourcePod)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, sourceSpec)
	p := minimalParams(t, isvc, c)
	_ = p
	req := &audit.MigrationRequest{SchemaVersion: audit.SchemaV1, Component: "engine", Instance: 0, FromNode: "node5"}

	const maxPasses = 10
	for pass := 0; pass < maxPasses; pass++ {
		// Refresh params; core.BuildPlan must observe the latest status.
		fresh := &v1beta1.InferenceService{}
		_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
		pPass := minimalParams(t, fresh, c)
		planPass, _ := core.BuildPlan(pPass)

		// Help convergence the way kubelet would in a real cluster:
		// after Migrate creates the surge pod, synthesize ContainersReady,
		// and after the drain step, ensure the source pod's EndpointSlice
		// looks drained.
		surgePods := &corev1.PodList{}
		_ = c.List(context.Background(), surgePods, client.InNamespace("prod"))
		for i := range surgePods.Items {
			pod := &surgePods.Items[i]
			if pod.Name == sourcePod.Name {
				continue
			}
			synthesizeContainersReady(t, c, pod)
			// kube-proxy mock: once surge containers are Ready, the
			// endpointslice publishes the surge endpoint as Ready=true.
			ensureInRotationSlice(t, c, isvc, pod)
		}
		// Drain check uses EndpointSlices; seed one that reports the
		// source pod as Ready=false (drained).
		ensureDrainedSlice(t, c, isvc, sourcePod)
		// Simulate the pod-watch observing the prior pass's writes.
		core.DefaultExpectations.Forget("prod", "llama-70b", v1beta1.EngineComponent, 0)
		if src := query.FindInstanceStatus(fresh, v1beta1.EngineComponent, 0); src != nil && src.Operation != nil && src.Operation.SurgeIndex != nil {
			core.DefaultExpectations.Forget("prod", "llama-70b", v1beta1.EngineComponent, *src.Operation.SurgeIndex)
		}

		done, err := Migrate(context.Background(), pPass, planPass, 0, "req-end-to-end", req)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		// Mimic the reconciler's deferred AggregateAndWriteStatus so per-
		// Instance PodCount / ReadyPodCount / AvailablePodCount counters
		// refresh between passes — Migrate's surge-Available gate reads
		// surgeStatus.AvailablePodCount. Inlined here (rather than calling
		// the top-level omenative.AggregateAndWriteStatus) to avoid an
		// ops/ → omenative import cycle.
		if serr := refreshInstanceCounters(context.Background(), pPass, planPass); serr != nil {
			t.Fatalf("pass %d: refreshInstanceCounters: %v", pass, serr)
		}
		if done {
			final := &v1beta1.InferenceService{}
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), final)
			cs := final.Status.Components[v1beta1.EngineComponent]
			for _, s := range cs.OMENative.InstanceStatuses {
				if s.Index == 0 {
					t.Errorf("source InstanceStatus should have been removed; got %+v", s)
				}
				if s.Index > 0 && s.Phase != v1beta1.OMENativeInstanceReady {
					t.Errorf("surge should be Phase=Ready; got %+v", s)
				}
			}
			ledger, _ := audit.LoadLedger(context.Background(), c, final)
			if !ledger.HasCompletedOrFailedRequest("req-end-to-end") {
				t.Errorf("ledger should record Completed entry; got %+v", ledger.Entries)
			}
			return
		}
	}
	t.Fatalf("Migrate did not converge after %d passes", maxPasses)
}

// (TestApplyMigrationOverlay_* moved to workload/ops/render_test.go
// in commit 5; the overlay function lives in workload/ops/render.go now.)

// --- test-only helpers ---

func synthesizeContainersReady(t *testing.T, c client.Client, pod *corev1.Pod) {
	t.Helper()
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
		return
	}
	for _, cond := range fresh.Status.Conditions {
		if cond.Type == corev1.ContainersReady && cond.Status == corev1.ConditionTrue {
			return
		}
	}
	fresh.Status.Conditions = append(fresh.Status.Conditions, corev1.PodCondition{
		Type:   corev1.ContainersReady,
		Status: corev1.ConditionTrue,
	})
	if err := c.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("synthesize ContainersReady: %v", err)
	}
}

// ensureDrainedSlice mimics kube-proxy publishing the pod as drained
// (Ready=false) on both the per-revision routed Service (which the
// drain path reads after the headless-drain-hang fix) and the headless
// Service (which status_aggregate's availability rollup still reads).
// A real cluster has the same pod appearing in both slices.
func ensureDrainedSlice(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, pod *corev1.Pod) {
	t.Helper()
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" {
		t.Fatalf("ensureDrainedSlice: pod %s missing ome.io/revision-hash label", pod.Name)
	}
	perRev := sliceWithEndpoint(isvc.Namespace, fmt.Sprintf("%s-drained-rev", pod.Name), coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, hash), pod, false)
	_ = c.Create(context.Background(), perRev)
	headless := sliceWithEndpoint(isvc.Namespace, fmt.Sprintf("%s-drained-hd", pod.Name), query.HeadlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, false)
	_ = c.Create(context.Background(), headless)
}

func ensureInRotationSlice(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, pod *corev1.Pod) {
	t.Helper()
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" {
		t.Fatalf("ensureInRotationSlice: pod %s missing ome.io/revision-hash label", pod.Name)
	}
	perRev := sliceWithEndpoint(isvc.Namespace, fmt.Sprintf("%s-rotation-rev", pod.Name), coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, hash), pod, true)
	_ = c.Create(context.Background(), perRev)
	headless := sliceWithEndpoint(isvc.Namespace, fmt.Sprintf("%s-rotation-hd", pod.Name), query.HeadlessServiceName(isvc.Name, v1beta1.EngineComponent), pod, true)
	_ = c.Create(context.Background(), headless)
}

func podListNames(pods []corev1.Pod) []string {
	out := make([]string, len(pods))
	for i, p := range pods {
		out[i] = p.Name
	}
	return out
}

// refreshInstanceCounters reproduces the per-Instance counter slice
// of AggregateAndWriteStatus: list live pods, list EndpointSlices for
// the headless Service, and stamp PodCount / ReadyPodCount /
// AvailablePodCount onto each InstanceStatus. Used by
// TestMigrate_ConvergesAcrossPasses to simulate the reconciler's
// deferred status write between Migrate passes — without it, surge
// instances stay at PodCount=0 forever and Migrate never advances
// past the surge-Available gate.
func refreshInstanceCounters(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan) error {
	pods, err := query.ListOMENativePods(ctx, p.Client, p.ISVC, plan.Component)
	if err != nil {
		return fmt.Errorf("refreshInstanceCounters: list pods: %w", err)
	}
	byIndex := query.BucketPodsByInstanceIdx(pods)

	availableByPod := map[string]struct{}{}
	slices, err := drain.EndpointSlicesForService(ctx, p.Reader(), p.ISVC.Namespace, query.HeadlessServiceName(p.ISVC.Name, plan.Component))
	if err != nil {
		return fmt.Errorf("refreshInstanceCounters: list slices: %w", err)
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if ep.TargetRef == nil || (ep.TargetRef.Kind != "" && ep.TargetRef.Kind != "Pod") {
				continue
			}
			if !drain.EndpointAvailable(ep) {
				continue
			}
			availableByPod[ep.TargetRef.Name] = struct{}{}
		}
	}

	fresh := &v1beta1.InferenceService{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
		return fmt.Errorf("refreshInstanceCounters: re-read ISVC: %w", err)
	}
	cs := fresh.Status.Components[plan.Component]
	if cs.OMENative == nil {
		return nil
	}
	for i := range cs.OMENative.InstanceStatuses {
		s := &cs.OMENative.InstanceStatuses[i]
		instPods := byIndex[s.Index]
		s.PodCount = int32(len(instPods))
		var ready, avail int32
		for _, pod := range instPods {
			if podreadiness.IsContainersReady(pod) {
				ready++
			}
			if _, ok := availableByPod[pod.Name]; ok {
				avail++
			}
		}
		s.ReadyPodCount = ready
		s.AvailablePodCount = avail
	}
	fresh.Status.Components[plan.Component] = cs
	return p.Client.Status().Update(ctx, fresh)
}

// Silence "imported and not used" if a referenced test helper becomes unused.
var _ = appsv1.ControllerRevision{}
var _ = metav1.Now

// --- Migrate rejects unsatisfiable NodeAffinity ---

func TestMigrate_RejectsNodeAffinityConflict(t *testing.T) {
	// Source PodSpec has a required NodeAffinity that pins
	// kubernetes.io/hostname=node5; the migration request asks to move
	// off node5. The overlay's NotIn[node5] would render the surge
	// unschedulable. Migrate must reject up-front (Failed audit row +
	// MigrationNodeAffinityConflict event) instead of letting the surge
	// pod sit Pending until instanceReadyTimeout.
	resetExpectations(t)
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	conflictingSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node5"},
						}},
					}},
				},
			},
		},
	}
	sourcePod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	sourcePod.Spec.NodeName = "node5" // matches req.FromNode + spec pin
	c := newFakeClientWithDiscovery(t, isvc, sourcePod)
	seedRunningRevision(t, c, isvc, v1beta1.EngineComponent, 0, conflictingSpec)
	p := minimalParams(t, isvc, c)
	plan, _ := core.BuildPlan(p)
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     "engine",
		Instance:      0,
		FromNode:      "node5",
	}

	done, err := Migrate(context.Background(), p, plan, 0, "req-conflict", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if !done {
		t.Errorf("conflict should terminate the migration on first pass; got done=false")
	}

	// Failed audit row must record the conflict so a re-delivered UUID
	// short-circuits via hasCompletedOrFailedRequest.
	ledger, _ := audit.LoadLedger(context.Background(), c, isvc)
	if !ledger.HasCompletedOrFailedRequest("req-conflict") {
		t.Errorf("ledger should carry a terminal entry for the rejected request")
	}

	// Rejection must run BEFORE patchInstanceStatusMigrating — otherwise
	// source wedges in Phase=Migrating until tryAbandonStuckMigration
	// fires (~instanceReadyTimeout). Assert source is still steady-Ready
	// with no in-flight Operation.
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	src := query.FindInstanceStatus(fresh, v1beta1.EngineComponent, 0)
	if src == nil {
		t.Fatal("source InstanceStatus should still exist after rejection")
	}
	if src.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("source Phase = %q; want Ready (rejection must not stamp Migrating)", src.Phase)
	}
	if src.Operation != nil {
		t.Errorf("source Operation = %+v; want nil (rejection must leave no in-flight op)", src.Operation)
	}
	// No surge InstanceStatus should exist either.
	if surge := query.FindInstanceStatus(fresh, v1beta1.EngineComponent, 1); surge != nil {
		t.Errorf("surge InstanceStatus exists with Phase=%q; want absent (no stamp before rejection)", surge.Phase)
	}
}

// --- validateFromNode ---

func TestValidateFromNode_MatchOK(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Spec.NodeName = "node5"
	c := newFakeClientWithDiscovery(t, isvc, pod)
	p := minimalParams(t, isvc, c)

	mismatch, defer_, err := validateFromNode(context.Background(), p, v1beta1.EngineComponent, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ || mismatch != "" {
		t.Errorf("expected pass; got mismatch=%q defer=%v", mismatch, defer_)
	}
}

func TestValidateFromNode_MismatchRejected(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Spec.NodeName = "node7" // pod actually on node7
	c := newFakeClientWithDiscovery(t, isvc, pod)
	p := minimalParams(t, isvc, c)

	mismatch, defer_, err := validateFromNode(context.Background(), p, v1beta1.EngineComponent, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("scheduled pod on different node must be a rejection, not a defer")
	}
	if mismatch == "" {
		t.Errorf("expected mismatch reason")
	}
}

func TestValidateFromNode_UnscheduledDefers(t *testing.T) {
	// Pod exists but Spec.NodeName is empty. Defer rather than reject —
	// the next reconcile re-polls.
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := runningPodAtRevision(isvc, 0, 1, "llama:v1") // NodeName left ""
	c := newFakeClientWithDiscovery(t, isvc, pod)
	p := minimalParams(t, isvc, c)

	mismatch, defer_, err := validateFromNode(context.Background(), p, v1beta1.EngineComponent, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if !defer_ {
		t.Errorf("unscheduled pod must defer; got mismatch=%q defer=%v", mismatch, defer_)
	}
	if mismatch != "" {
		t.Errorf("defer must not carry a rejection reason; got %q", mismatch)
	}
}

func TestValidateFromNode_NoLivePodsRejects(t *testing.T) {
	// Source instance has no pods at all — that's a rejection (Alfred
	// can't legitimately request a migration of a non-existent instance).
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	mismatch, defer_, err := validateFromNode(context.Background(), p, v1beta1.EngineComponent, 0, "node5")
	if err != nil {
		t.Fatalf("validateFromNode: %v", err)
	}
	if defer_ {
		t.Errorf("no live pods must reject, not defer")
	}
	if mismatch == "" {
		t.Errorf("expected mismatch reason for no-live-pods")
	}
}

// --- clearMigrationRequestAnnotation ---

func TestClearMigrationRequestAnnotation_RemovesKey(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	uuid := "req-clear-1"
	addMigrationAnnotation(t, isvc, uuid, migrationRequestJSON("engine", 0, "node5"))
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	if err := clearMigrationRequestAnnotation(context.Background(), p, uuid); err != nil {
		t.Fatalf("clearMigrationRequestAnnotation: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	if _, present := fresh.Annotations[audit.MigrationRequestAnnotationPrefix+uuid]; present {
		t.Errorf("annotation must be removed; got %v", fresh.Annotations)
	}
}

func TestClearMigrationRequestAnnotation_IdempotentWhenAbsent(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	// No annotation set — call should succeed as a no-op.
	if err := clearMigrationRequestAnnotation(context.Background(), p, "never-present"); err != nil {
		t.Fatalf("no-op clear must not error: %v", err)
	}
}

func TestClearMigrationRequestAnnotation_LeavesOtherAnnotations(t *testing.T) {
	isvc := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	uuid := "req-target"
	addMigrationAnnotation(t, isvc, uuid, migrationRequestJSON("engine", 0, "node5"))
	addMigrationAnnotation(t, isvc, "req-other", migrationRequestJSON("engine", 1, "node6"))
	isvc.Annotations["unrelated/key"] = "keep-me"
	c := newFakeClientWithDiscovery(t, isvc)
	p := minimalParams(t, isvc, c)

	if err := clearMigrationRequestAnnotation(context.Background(), p, uuid); err != nil {
		t.Fatalf("clearMigrationRequestAnnotation: %v", err)
	}
	fresh := &v1beta1.InferenceService{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
	if _, present := fresh.Annotations[audit.MigrationRequestAnnotationPrefix+uuid]; present {
		t.Errorf("target annotation must be removed")
	}
	if _, present := fresh.Annotations[audit.MigrationRequestAnnotationPrefix+"req-other"]; !present {
		t.Errorf("unrelated migration annotation must survive")
	}
	if fresh.Annotations["unrelated/key"] != "keep-me" {
		t.Errorf("non-migration annotation must survive")
	}
}
