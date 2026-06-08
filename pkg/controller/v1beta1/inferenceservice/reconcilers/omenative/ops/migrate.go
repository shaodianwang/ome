// Migration-request detection for the omenative/ops package.
//
// The per-Instance Migrate surge state machine lives in
// pkg/controller/v1beta1/workload/ops; the IR-managed path drives it via
// workload.Reconcile. What remains here are the two ISVC-shaped detection
// helpers (detectMigrationRequest, nextInFlightFromLedger) the IR
// controller reaches through irbridge.go: they read the user-facing
// ISVC's migration-request annotation + the ISVC-owned audit ledger,
// which are inherently v1beta1-typed and ISVC-scoped.
package ops

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

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
		Reason:        picked.Reason,
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
