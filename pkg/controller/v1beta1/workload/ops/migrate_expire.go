package ops

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// migrationExpiredReason is the LastFailure.Reason stamped on a source
// Instance whose pods are observed unhealthy when its migration record
// expires. Distinct from the instance-op DeadlineExceeded reason: the
// timeout authority here is the record's Deadline, not an
// Operation.Deadline.
const migrationExpiredReason = "MigrationExpired"

// ExpireMigrations is the deadline consumer for Manual migration
// records: every non-terminal Manual record in ObservedState.Migrations
// whose Deadline has passed is consumed — not just the one the
// dispatcher would drive, so a queued record can never sit expired
// behind an executing one. Returns how many records reached a terminal
// phase this pass; the dispatcher requeues when > 0 so the next pass
// rebuilds plan + ObservedState from the post-expiry status.
//
// Precedence — a migration that is actually completing wins over
// expiry. Two carve-outs, both decided from live observation:
//
//   - Completed-tail crash window: the completion tail already promoted
//     the surge and removed the source status, but died before the
//     record's terminal write. The migration SUCCEEDED — the record is
//     closed Completed and the surge is untouched. (The drive pass
//     cannot finish this window itself: with the source status gone it
//     can no longer resolve the source's RunningRevision and retries
//     forever.)
//   - Draining with the source fully drained AND the surge passing the
//     drive pass's full completion gates (runtime-ready + in-rotation +
//     Available — the SAME gates, via surgeTailGatesPassed): completion
//     is one idempotent drive tail away (delete drained source pods →
//     promote → remove → Completed), so the drive pass runs instead.
//     This cannot strand the record: the moment any of those
//     observations regresses — drain incomplete, or any surge gate the
//     drive itself would block on forever — the next expiry pass fires.
//
// Everything else past Deadline expires in one transition:
//
//  1. Clear the Migrate op on the surge (it KEEPS its status slot;
//     unpinned, the next pass's plan drops it and the ordinary
//     scale-down Delete pipeline tears it down — drain gate,
//     escalation, RemoveInstance — no bespoke teardown).
//  2. Clear the source's Migrate op, remove the drive's source-drain
//     serving key from every live source pod (expiry must undo every
//     reversible mutation the drive made — the key's normal removal is
//     pod deletion, which an expired-but-kept source never gets), and
//     restore the source's Phase from OBSERVATION: live source pods
//     all runtime-ready → Ready (RunningRevision untouched); else
//     Failed with a LastFailure. Never a blind stamp. The source stays
//     in the plan (its index is within the replica budget) so it can
//     never become a scale-down extra.
//  3. Mirror a terminal Failed row into the audit ledger — a HARD
//     step: a mirror failure aborts the pass before the terminal
//     record write, so the still-non-terminal record retries next pass
//     until the mirror lands. Without the row, the status entry's
//     eventual trim would let the upgrade import re-synthesize the
//     UUID from its Started row as fresh Accepted work.
//  4. Terminal record write LAST — the crash anchor: a crash anywhere
//     above leaves the record non-terminal and the next pass re-runs
//     the idempotent steps. (Record-first would strand: a terminal
//     record with still-pinned ops has no consumer.) Because this pass
//     runs BEFORE the drive pass, the drive can never re-stamp ops
//     between a partial expiry and its retry.
//  5. Warning event naming the uuid + blocker + surge teardown.
//
// An Accepted record (surge never allocated — SurgeInstance nil or the
// legacy -1 sentinel) has no instance ops to clear and no surge to tear
// down: op stamps only ever happen after the allocation write, so the
// expiry is record + ledger + event only and the source is not touched.
func ExpireMigrations(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan) (int, error) {
	now := input.Now()
	expired := 0
	for i := range input.ObservedState.Migrations {
		rec := &input.ObservedState.Migrations[i]
		if !isExpiredMigrationCandidate(rec, now) {
			continue
		}
		if input.MutateMigration == nil {
			return expired, fmt.Errorf("ExpireMigrations: MutateMigration not wired (uuid=%s)", rec.RequestUUID)
		}
		terminal, err := expireMigrationRecord(ctx, deps, input, plan, rec)
		if err != nil {
			return expired, fmt.Errorf("ExpireMigrations: expire uuid=%s: %w", rec.RequestUUID, err)
		}
		if terminal {
			expired++
		}
	}
	return expired, nil
}

// isExpiredMigrationCandidate reports whether rec is an expiry
// candidate: a non-terminal Manual record whose Deadline has passed. A
// zero Deadline never expires (mirrors the instance-op rule).
func isExpiredMigrationCandidate(rec *workload.MigrationRecord, now time.Time) bool {
	if rec.Trigger != workload.MigrationTriggerManual || rec.Phase.Terminal() {
		return false
	}
	return !rec.Deadline.IsZero() && now.After(rec.Deadline.Time)
}

// HasExpiredMigrationCandidate reports whether any record is an expiry
// candidate ExpireMigrations would consume. The dispatcher's decision
// layer consults this so the (pure) selection and the (effectful)
// consumption share one filter.
func HasExpiredMigrationCandidate(records []workload.MigrationRecord, now time.Time) bool {
	for i := range records {
		if isExpiredMigrationCandidate(&records[i], now) {
			return true
		}
	}
	return false
}

// expireMigrationRecord consumes one expired record. Returns
// terminal=true when the record was closed (Failed, or Completed via
// the completed-tail edge); false when the drive pass should finish it
// instead (Draining, one idempotent tail away).
func expireMigrationRecord(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, rec *workload.MigrationRecord) (bool, error) {
	uuid := rec.RequestUUID
	source := findInstanceStatus(input.ObservedState.InstanceStatuses, rec.SourceInstance)
	allocated := rec.SurgeInstance != nil && *rec.SurgeInstance >= 0
	var surge *workload.InstanceStatus
	if allocated {
		surge = findInstanceStatus(input.ObservedState.InstanceStatuses, *rec.SurgeInstance)
	}

	// Completed-tail crash edge: source status gone + surge promoted
	// Ready means the migration actually succeeded. Close Completed,
	// touch nothing else.
	if allocated && source == nil && surge != nil && surge.Phase == workload.InstancePhaseReady {
		msg := fmt.Sprintf("migrated to instance=%d", *rec.SurgeInstance)
		if err := mirrorTerminalMigrationLedger(ctx, deps, input, plan, rec, audit.PhaseCompleted, "migrated"); err != nil {
			return false, err
		}
		if err := closeMigrationRecord(ctx, input, uuid, workload.MigrationPhaseCompleted, msg); err != nil {
			return false, err
		}
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonMigrationCompleted,
			"OMENative migration uuid=%s complete: %s -> instance=%d (record closed at expiry after the completion tail)",
			uuid, instanceKey(plan.Component, rec.SourceInstance), *rec.SurgeInstance)
		return true, nil
	}

	// Draining precedence: when the source is fully drained AND the
	// surge is fully runtime-ready, completion is one idempotent drive
	// tail away — prefer driving to completion over expiry.
	if allocated && rec.Phase == workload.MigrationPhaseDraining {
		tailReady, err := migrationTailReady(ctx, deps, input, plan, rec.SourceInstance, *rec.SurgeInstance)
		if err != nil {
			return false, err
		}
		if tailReady {
			return false, nil
		}
	}

	blocker := migrationExpiryBlocker(rec.Phase)

	if allocated {
		// Surge: clear the pin only. Its status slot (Phase=Creating,
		// or whatever it reached) stays — unpinned, the next pass's
		// plan drops the index and the standard Delete pipeline tears
		// it down.
		if err := clearMigrateOperation(ctx, input, *rec.SurgeInstance, uuid); err != nil {
			return false, fmt.Errorf("clear surge Migrate op (instance=%d): %w", *rec.SurgeInstance, err)
		}
		// Source: clear the pin and restore the phase from live
		// observation.
		if source != nil {
			if err := restoreSourceFromObservation(ctx, deps, input, plan, rec.SourceInstance, uuid); err != nil {
				return false, fmt.Errorf("restore source (instance=%d): %w", rec.SourceInstance, err)
			}
		}
	}

	if err := mirrorTerminalMigrationLedger(ctx, deps, input, plan, rec, audit.PhaseFailed, blocker); err != nil {
		return false, err
	}

	if err := closeMigrationRecord(ctx, input, uuid, workload.MigrationPhaseFailed, blocker); err != nil {
		return false, err
	}

	if allocated {
		recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationExpired,
			"OMENative migration uuid=%s expired: %s; tearing down surge instance=%d",
			uuid, blocker, *rec.SurgeInstance)
	} else {
		recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonMigrationExpired,
			"OMENative migration uuid=%s expired: %s", uuid, blocker)
	}
	return true, nil
}

// migrationTailReady reports whether an expired Draining record is one
// idempotent drive tail away from Completed: every live source pod is
// drained out of its routed Service AND the surge passes the drive
// pass's OWN completion gates — runtime-ready plus the shared
// rotation + availability gates (surgeTailGatesPassed). The surge
// gates must be exactly the drive's: any state the drive blocks on
// (dead surge pod, ready-but-never-in-rotation, not Available) must
// expire, not defer — a weaker check here would park the record
// non-terminal past its deadline forever.
func migrationTailReady(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx, surgeIdx int32) (bool, error) {
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list surge pods: %w", err)
	}
	if !query.AllPodsRuntimeReady(surgePods) {
		return false, nil
	}
	if ok, gerr := surgeTailGatesPassed(ctx, deps, input, plan, surgeIdx, surgePods); gerr != nil || !ok {
		return false, gerr
	}
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("list source pods: %w", err)
	}
	for _, pod := range sourcePods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		drained, derr := drain.IsPodDrained(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
		if derr != nil {
			return false, fmt.Errorf("check drain on source pod %s: %w", pod.Name, derr)
		}
		if !drained {
			return false, nil
		}
	}
	return true, nil
}

// restoreSourceFromObservation clears the source's Migrate pin,
// removes the drive's source-drain serving key from every live source
// pod, and sets the source's Phase from what actually runs: live
// source pods all runtime-ready → Ready (RunningRevision untouched —
// it served throughout); else Failed with a LastFailure so the
// source's own escalation machinery takes over. Never a blind stamp.
//
// The un-drain is unconditional — keyed on live pod state, not the
// record's phase, and applied whichever Phase the observation decides.
// Expiry must undo every reversible mutation the drive made: the
// drain key's normal removal is source-pod deletion at the end of a
// SUCCESSFUL migration, which an expired-but-kept source never gets.
// Without it a Draining-expired source (or one caught in the
// SurgeReady→Draining crash window, keys applied but phase not yet
// advanced) reads Ready while every pod carries serving=False —
// permanently out of the routed Service and invisible to the
// availability counters. A Failed-observed source is un-drained too,
// so a later recovery serves. MarkPodServing is idempotent (no-op
// when the key is absent).
func restoreSourceFromObservation(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx int32, uuid string) error {
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return fmt.Errorf("list source pods: %w", err)
	}
	for _, pod := range sourcePods {
		if err := podreadiness.MarkPodServing(ctx, deps.Client, pod, podreadiness.WriterMigrateSourceDrain, uuid); err != nil {
			return fmt.Errorf("remove source-drain key from pod %s: %w", pod.Name, err)
		}
	}
	healthy := query.AllPodsRuntimeReady(sourcePods)
	now := metav1.NewTime(input.Now())
	return input.MutateInstance(ctx, sourceIdx, func(s *workload.InstanceStatus) bool {
		if s.Phase == "" {
			// Fresh-empty slot from the append path: the status was
			// deleted out from under us — don't resurrect.
			return false
		}
		changed := false
		if s.Operation != nil && s.Operation.Type == workload.InstanceOperationMigrate && s.Operation.RequestUUID == uuid {
			s.Operation = nil
			changed = true
		}
		want := workload.InstancePhaseReady
		if !healthy {
			want = workload.InstancePhaseFailed
		}
		if s.Phase != want {
			s.Phase = want
			if !healthy {
				s.LastFailure = &workload.InstanceTermination{
					Reason:  migrationExpiredReason,
					Message: "migration expired; source pods unhealthy",
					Time:    now,
				}
			}
			changed = true
		}
		return changed
	})
}

// clearMigrateOperation drops the Migrate Operation pin for uuid from
// the instance at idx, leaving the rest of the status untouched. No-op
// when the slot is gone or the op is absent / a different type / a
// different request.
func clearMigrateOperation(ctx context.Context, input workload.ReconcileInput, idx int32, uuid string) error {
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		if s.Operation == nil || s.Operation.Type != workload.InstanceOperationMigrate || s.Operation.RequestUUID != uuid {
			return false
		}
		s.Operation = nil
		return true
	})
}

// closeMigrationRecord writes the record's terminal phase + outcome
// message + CompletedAt. Idempotent: an already-terminal record is
// untouched.
func closeMigrationRecord(ctx context.Context, input workload.ReconcileInput, uuid string, phase workload.MigrationPhase, message string) error {
	return input.MutateMigration(ctx, uuid, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() {
			return false
		}
		m.Phase = phase
		m.Message = message
		now := metav1.NewTime(input.Now())
		m.CompletedAt = &now
		return true
	})
}

// mirrorTerminalMigrationLedger upserts the terminal audit row for rec.
// A HARD step on the expiry path: callers return the error BEFORE the
// record's terminal write, so the still-non-terminal record simply
// retries next pass until the mirror lands (the crash-anchor ordering
// makes re-entry idempotent — same discipline as the drive path's
// terminal ledger writes). The row is what blocks the trim + upgrade
// import from resurrecting the UUID after the status entry ages out.
func mirrorTerminalMigrationLedger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, rec *workload.MigrationRecord, phase, outcome string) error {
	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), ledgerOwnerObject(input))
	if err != nil {
		return fmt.Errorf("load ledger for terminal mirror (uuid=%s): %w", rec.RequestUUID, err)
	}
	req := &audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(plan.Component),
		Instance:        rec.SourceInstance,
		FromNode:        rec.FromNode,
		HintTargetNodes: append([]string(nil), rec.HintTargetNodes...),
		Reason:          rec.Reason,
	}
	surgeIdx := int32(-1)
	if rec.SurgeInstance != nil {
		surgeIdx = *rec.SurgeInstance
	}
	ledger.UpsertEntry(audit.NewTerminalEntry(*ledger.InFlightEntryOrSeed(rec.RequestUUID, req, surgeIdx), phase, outcome))
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, ledgerOwnerObject(input), ledgerOwnerGVK(input), ledger); err != nil {
		return fmt.Errorf("persist terminal ledger mirror (uuid=%s): %w", rec.RequestUUID, err)
	}
	return nil
}

// migrationExpiryBlocker names what the migration was stuck on when its
// Deadline passed, derived deterministically from the record's phase.
func migrationExpiryBlocker(p workload.MigrationPhase) string {
	switch p {
	case workload.MigrationPhaseAccepted:
		return "deadline exceeded in phase Accepted: surge never allocated"
	case workload.MigrationPhaseSurgePending:
		return "deadline exceeded in phase SurgePending: surge pods never became ready"
	case workload.MigrationPhaseSurgeReady:
		return "deadline exceeded in phase SurgeReady: source drain never started"
	case workload.MigrationPhaseDraining:
		return "deadline exceeded in phase Draining: source drain incomplete"
	default:
		return fmt.Sprintf("deadline exceeded in phase %s", p)
	}
}
