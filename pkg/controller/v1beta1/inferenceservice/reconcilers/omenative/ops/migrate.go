// Dispatch shim for Migrate — the per-Instance Migrate surge state
// machine itself lives in pkg/controller/v1beta1/workload/ops after the
// workload-extraction refactor (part 2 slice 4d of N).
//
// The shim's only job is to build the workload.ReconcileInput +
// workload.Deps pair from the legacy core.ReconcileParams (wiring the
// ClearMigrationRequest callback to the local
// clearMigrationRequestAnnotation helper) and delegate. The
// dispatcher-facing entry points (Migrate, detectMigrationRequest,
// nextInFlightFromLedger, clearMigrationRequestAnnotation,
// migrateRequeueInterval) stay here because dispatch.go and
// timeouts.go consume them directly without going through workload.
package ops

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/ops"
)

// Migrate is the ISVC-adapter entry point. It projects the legacy
// core.ReconcileParams onto workload.Deps + workload.ReconcileInput
// (wiring the local clearMigrationRequestAnnotation helper into
// ReconcileInput.ClearMigrationRequest) and delegates to
// workloadops.Migrate — the source-agnostic body of the per-Instance
// Migrate surge state machine.
//
// Returns (done, accepted, err) — see workloadops.Migrate for the
// contract. The accepted bit signals the dispatcher whether Migrate
// took ownership; done=false && accepted=false means the dispatcher
// should fall through to other ops so an in-flight Update/Restart/
// Create can converge (otherwise the dispatcher loops indefinitely
// in the Migrate-defer branch — affinity-trigger-not-detected
// deadlock).
//
// Wires deps.RenderHook to core.ISVCRenderHook(p.ISVC) so the surge
// pod's Render path runs coordination.InjectPeerEnv — matches the
// legacy core.RenderWithRevisionForISVC behavior used by the in-package
// migrate body before the extraction.
func Migrate(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, sourceIdx int32, requestUUID string, req *audit.MigrationRequest) (bool, bool, error) {
	deps := workload.Deps{
		Client:       p.Client,
		APIReader:    p.APIReader,
		Recorder:     p.Recorder,
		Expectations: p.ExpectationsCache(),
		RenderHook:   core.ISVCRenderHook(p.ISVC),
	}
	input := internalsource.BuildReconcileInput(internalsource.BuildInputDeps{
		ISVC:          p.ISVC,
		Component:     p.Component,
		ObjectMeta:    p.ObjectMeta,
		PodSpec:       p.PodSpec,
		WorkerPodSpec: p.WorkerPodSpec,
		Client:        p.Client,
		Expectations:  p.ExpectationsCache(),
		ClearMigrationRequestAnnotation: func(ctx context.Context, uuid string) error {
			return clearMigrationRequestAnnotation(ctx, p, uuid)
		},
	})
	return workloadops.Migrate(ctx, deps, input, plan, sourceIdx, requestUUID, req)
}

// clearMigrationRequestAnnotation removes the
// ome.io/migration-request-v1-<uuid> annotation. The requester contract
// requires accept/reject within 30s by annotation removal; without this
// they re-deliver the same UUID indefinitely. retry.RetryOnConflict so
// concurrent reconcile-pass writes don't lose the removal.
//
// Kept in the omenative/ops package because timeouts.go (which has not
// migrated to workload/ops yet) and the workload.Migrate
// ClearMigrationRequest closure both call it. Once timeouts.go moves to
// workload/ops the helper can shrink to a workload-side equivalent on
// internalsource.
func clearMigrationRequestAnnotation(ctx context.Context, p core.ReconcileParams, uuid string) error {
	if p.Client == nil || p.ISVC == nil || uuid == "" {
		return nil
	}
	key := audit.MigrationRequestAnnotationPrefix + uuid
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(p.ISVC), fresh); err != nil {
			return fmt.Errorf("clearMigrationRequestAnnotation: get ISVC: %w", err)
		}
		if fresh.Annotations == nil {
			return nil
		}
		if _, present := fresh.Annotations[key]; !present {
			return nil
		}
		delete(fresh.Annotations, key)
		if err := p.Client.Update(ctx, fresh); err != nil {
			return fmt.Errorf("clearMigrationRequestAnnotation: update ISVC: %w", err)
		}
		return nil
	})
}

// nextInFlightFromLedger returns the oldest Started ledger entry for the
// given component whose UUID has no terminal counterpart. Returns empty
// uuid when nothing is in flight. The audit.MigrationRequest is reconstructed
// from the ledger entry; HintTargetNodes and Reason are lost on this
// path but neither is needed for second-pass Migrate calls (the surge
// already exists with the overlay applied, Operation.Reason already
// carries the reason on the InstanceStatus).
func nextInFlightFromLedger(ledger *audit.Ledger, component v1beta1.ComponentType) (string, *audit.MigrationRequest, int32) {
	if ledger == nil {
		return "", nil, 0
	}
	// Index Started + terminal entries by UUID for O(N) selection.
	terminal := map[string]struct{}{}
	for _, e := range ledger.Entries {
		if e.Phase == audit.PhaseCompleted || e.Phase == audit.PhaseFailed {
			terminal[e.RequestUUID] = struct{}{}
		}
	}
	var oldestStartedAt string
	var picked *audit.Entry
	for i := range ledger.Entries {
		e := &ledger.Entries[i]
		if e.Phase != audit.PhaseStarted {
			continue
		}
		if e.Component != string(component) {
			continue
		}
		if _, done := terminal[e.RequestUUID]; done {
			continue
		}
		if picked == nil || e.StartedAt < oldestStartedAt {
			picked = e
			oldestStartedAt = e.StartedAt
		}
	}
	if picked == nil {
		return "", nil, 0
	}
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     picked.Component,
		Instance:      picked.SourceInstance,
		FromNode:      picked.FromNode,
	}
	return picked.RequestUUID, req, picked.SourceInstance
}

// detectMigrationRequest finds the next migration to drive. Two sources:
//
//  1. ISVC annotations carrying `ome.io/migration-request-v1-<uuid>`.
//     These are the entry-point trigger written by the migration
//     requester.
//
//  2. The audit ledger's in-flight Started entries. Once Migrate accepts
//     a request it clears the annotation immediately (the requester's
//     contract is 30s response-or-clear), but the migration itself takes
//     many reconcile passes — surge create, drain, source delete,
//     promote. Without ledger-driven detection, the very next reconcile
//     after annotation-clear would skip the Migration pass entirely,
//     fall through to Create, and reconcileInstance would re-promote
//     the still-Migrating source to Ready. That mis-classification
//     causes the post-migration scale-down pass to delete the surge as
//     "extra" (Phase=Ready + Operation=nil at a sparse index).
//
// Schema-violating / wrong-component annotations get a Failed ledger
// entry but don't surface as Go errors.
func detectMigrationRequest(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType) (string, *audit.MigrationRequest, int32, bool, error) {
	if p.ISVC == nil {
		return "", nil, 0, false, nil
	}

	ledger, err := internalsource.LoadAuditLedgerForISVC(ctx, p.Reader(), p.ISVC)
	if err != nil {
		return "", nil, 0, false, err
	}

	// Source 2 first: an in-flight ledger entry means an accepted
	// migration is still mid-flow. Drive it forward regardless of
	// annotation state. Pick the oldest Started entry (smallest
	// StartedAt) for deterministic dispatch when multiple are in flight.
	if uuid, req, srcIdx := nextInFlightFromLedger(ledger, component); uuid != "" {
		return uuid, req, srcIdx, true, nil
	}

	// Source 1: annotation-driven new requests.
	if p.ISVC.Annotations == nil {
		return "", nil, 0, false, nil
	}
	keys := make([]string, 0, len(p.ISVC.Annotations))
	for k := range p.ISVC.Annotations {
		if strings.HasPrefix(k, audit.MigrationRequestAnnotationPrefix) {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return "", nil, 0, false, nil
	}
	sort.Strings(keys)

	for _, k := range keys {
		uuid := audit.ExtractRequestUUID(k)
		if uuid == "" {
			continue
		}
		if ledger.HasCompletedOrFailedRequest(uuid) {
			continue
		}
		raw := p.ISVC.Annotations[k]
		req, perr := audit.ParseMigrationRequest(raw)
		if perr != nil {
			// Terminal Failed so we don't re-parse every reconcile.
			ledger.UpsertEntry(audit.Entry{
				RequestUUID: uuid,
				Phase:       audit.PhaseFailed,
				StartedAt:   metav1.Now().UTC().Format(time.RFC3339),
				CompletedAt: metav1.Now().UTC().Format(time.RFC3339),
				Outcome:     perr.Error(),
			})
			if err := internalsource.PersistAuditLedgerForISVC(ctx, p.Client, p.ISVC, ledger); err != nil {
				return "", nil, 0, false, fmt.Errorf("persist Failed ledger after parse error: %w", err)
			}
			if errors.Is(perr, audit.ErrUnsupportedSchemaVersion) {
				status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonUnsupportedSchemaVersion,
					"OMENative migration uuid=%s rejected: %v", uuid, perr)
			} else {
				status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonMigrationRequestRejected,
					"OMENative migration uuid=%s rejected: %v", uuid, perr)
			}
			continue
		}
		if req.Component != string(component) {
			continue
		}
		return uuid, req, req.Instance, true, nil
	}
	return "", nil, 0, false, nil
}
