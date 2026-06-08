// Auto-migration trigger for stuck Instances.
//
// When an Instance has been in Phase=Updating or Phase=Creating longer
// than MigrationPolicy.ProgressDeadlineSeconds, the controller writes a
// synthetic migration request directly to the audit ledger (bypassing
// the annotation surface — this is controller-internal). The existing
// surge → drain → promote machinery picks the request up via Source-2
// of detectMigrationRequest and drives the surge on a different node
// via the migration anti-affinity overlay.
//
// Bounded by MaxAutoRetries: counts ledger entries for this
// (component, instance) whose Reason == AutoRecover (any phase). When
// the cap is reached, the controller refuses to write a new request
// and stamps the LifecycleConditionProgressDeadlineExceeded condition
// on the InstanceStatus.
package ops

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

const (
	// AutoMigrateReason is the Reason string stamped on synthetic
	// MigrationRequest + ledger Entry written by the auto-trigger
	// detector. Search for this constant to find every site that
	// reads or writes auto-recovery requests.
	AutoMigrateReason = "AutoRecover"

	// DefaultMaxAutoRetries caps the controller's self-trigger count
	// per (component, instance) when MigrationPolicy.MaxAutoRetries is
	// unset. Bounded so a misconfigured pod can't infinite-loop the
	// migration machinery.
	DefaultMaxAutoRetries int32 = 3

	// ConditionProgressDeadlineExceeded is the Instance-level
	// condition type stamped when the auto-trigger has exhausted its
	// retry budget. Operator intervention required.
	ConditionProgressDeadlineExceeded = "ProgressDeadlineExceeded"

	// ReasonProgressDeadlineExceeded is the condition reason paired
	// with ConditionProgressDeadlineExceeded.
	ReasonProgressDeadlineExceeded = "ProgressDeadlineExceeded"

	// EventReasonAutoMigrationTriggered is the K8s event reason fired
	// when the controller writes a synthetic auto-recovery migration
	// request to the audit ledger.
	EventReasonAutoMigrationTriggered = "AutoMigrationTriggered"

	// EventReasonAutoMigrationCapReached is the K8s event reason fired
	// when the controller refuses to self-trigger another migration
	// because MaxAutoRetries has been reached.
	EventReasonAutoMigrationCapReached = "AutoMigrationCapReached"
)

// detectStuckInstancesForAutoMigration finds Instances that have been
// in Phase=Updating or Phase=Creating longer than
// MigrationPolicy.ProgressDeadlineSeconds and writes synthetic ledger
// entries (Reason=AutoRecover) so the existing Migrate dispatch path
// drives surge-on-different-node recovery.
//
// Idempotent across reconcile passes — skips Instances that already
// have an in-flight migration ledger entry (detectMigrationRequest will
// pick those up via its Source-2 path).
//
// Gated by:
//   - MigrationPolicy != nil
//   - MigrationPolicy.Mode != Never
//   - MigrationPolicy.ProgressDeadlineSeconds != nil
//   - per-Instance auto-retry count < MaxAutoRetries (default 3)
//
// On retry exhaustion: stamps LifecycleConditionProgressDeadlineExceeded
// on the Instance and does NOT write a new migration request.
//
// Returns nil on success (whether or not any synthetic requests were
// written); errors are surfaced for ledger persistence / status write
// failures so the caller can requeue.
func detectStuckInstancesForAutoMigration(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType) error {
	if p.ISVC == nil || p.Client == nil {
		return nil
	}

	policy := migrationPolicyForComponentExt(p.ComponentExt)
	if policy == nil || policy.Mode == v1beta1.MigrationPolicyModeNever {
		return nil
	}
	if policy.ProgressDeadlineSeconds == nil {
		return nil
	}
	deadline := time.Duration(*policy.ProgressDeadlineSeconds) * time.Second
	if deadline <= 0 {
		return nil
	}
	maxRetries := DefaultMaxAutoRetries
	if policy.MaxAutoRetries != nil {
		maxRetries = *policy.MaxAutoRetries
	}

	cs, ok := p.ISVC.Status.Components[component]
	if !ok || cs.Lifecycle == nil {
		return nil
	}

	now := time.Now()

	// Load the ledger once and reuse across all Instances. Reduces
	// apiserver round-trips when multiple Instances of one Component
	// are stuck simultaneously (e.g., a bad image rolled out to N
	// replicas).
	ledger, err := internalsource.LoadAuditLedgerForISVC(ctx, p.Reader(), p.ISVC)
	if err != nil {
		return fmt.Errorf("detectStuckInstancesForAutoMigration: load ledger: %w", err)
	}

	mutated := false
	for i := range cs.Lifecycle.InstanceStatuses {
		s := &cs.Lifecycle.InstanceStatuses[i]
		if !isStuckPhase(s.Phase) {
			continue
		}
		// Skip Instances that already have an in-flight migration
		// ledger entry — Source-2 of detectMigrationRequest will pick
		// them up on the next pass. Defers to the existing flow rather
		// than racing with it.
		if instanceHasInFlightMigration(ledger, component, s.Index) {
			continue
		}
		stuckSince, ok := stuckSinceForInstance(s)
		if !ok {
			// No Operation.StartedAt to anchor on — skip rather than
			// fall back to a now() guess that would never trigger.
			continue
		}
		if now.Sub(stuckSince) < deadline {
			continue
		}

		retries := countAutoMigrationRetries(ledger, component, s.Index)
		if retries >= maxRetries {
			if err := stampProgressDeadlineExceeded(ctx, p, component, s.Index, retries, deadline); err != nil {
				return fmt.Errorf("detectStuckInstancesForAutoMigration: stamp condition (instance=%d): %w", s.Index, err)
			}
			status.RecordWarning(p.Recorder, p.ISVC, EventReasonAutoMigrationCapReached,
				"OMENative auto-migration cap reached: %s phase=%s stuck %.0fs deadline=%.0fs retries=%d/%d",
				status.InstanceKey(component, s.Index), s.Phase, now.Sub(stuckSince).Seconds(),
				deadline.Seconds(), retries, maxRetries)
			continue
		}

		// Resolve FromNode from the live source pod so the workload-side
		// validateFromNode check passes (it cross-references
		// request.FromNode against pod.Spec.NodeName). Skip when no pod
		// is yet scheduled — there's nothing to migrate off of, and the
		// existing stuck-pod escalation in timeouts.go handles the
		// never-scheduled case via a different path.
		fromNode, err := resolveLiveSourceNode(ctx, p, component, s.Index)
		if err != nil {
			return fmt.Errorf("detectStuckInstancesForAutoMigration: resolve source node (instance=%d): %w", s.Index, err)
		}
		if fromNode == "" {
			continue
		}

		// Write a synthetic Started entry directly. The detector
		// itself does not need to clear an annotation (there isn't one
		// for synthetic requests); detectMigrationRequest Source-2
		// will pick this up on the next pass and dispatch Migrate.
		reqUUID := uuid.NewString()
		entry := audit.Entry{
			RequestUUID:    reqUUID,
			Component:      string(component),
			SourceInstance: s.Index,
			Phase:          audit.PhaseStarted,
			Reason:         AutoMigrateReason,
			FromNode:       fromNode,
			StartedAt:      metav1.NewTime(now).UTC().Format(time.RFC3339),
		}
		ledger.UpsertEntry(entry)
		mutated = true

		status.RecordNormal(p.Recorder, p.ISVC, EventReasonAutoMigrationTriggered,
			"OMENative auto-migration triggered: %s phase=%s stuck %.0fs deadline=%.0fs attempt=%d/%d uuid=%s",
			status.InstanceKey(component, s.Index), s.Phase, now.Sub(stuckSince).Seconds(),
			deadline.Seconds(), retries+1, maxRetries, reqUUID)
		coordination.RecordAutoMigrationTriggered(p.ISVC.Name, string(component), AutoMigrateReason)
	}

	if !mutated {
		return nil
	}
	if err := internalsource.PersistAuditLedgerForISVC(ctx, p.Client, p.ISVC, ledger); err != nil {
		return fmt.Errorf("detectStuckInstancesForAutoMigration: persist ledger: %w", err)
	}
	return nil
}

// isStuckPhase reports whether the Phase is one the auto-migration
// detector watches for stuckness. Only Updating + Creating count —
// Migrating already triggered migration recovery, Restarting is a
// short-lived recovery itself, Pending hasn't yet allocated the
// Operation anchor.
func isStuckPhase(phase v1beta1.OMENativeInstancePhase) bool {
	switch phase {
	case v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceUpdating:
		return true
	}
	return false
}

// stuckSinceForInstance returns the time when the Instance entered its
// current phase (which is what we measure "stuck" against). Uses the
// Operation.StartedAt anchor — this is set when the Phase=Creating /
// Updating transition writes the Operation, and is cleared when the
// Phase progresses to Ready. While an Operation is in flight, its
// StartedAt is the authoritative "current phase started at" timestamp.
//
// Returns ok=false when no Operation is present — the detector skips
// rather than guessing now() (which would prevent the trigger from
// ever firing).
func stuckSinceForInstance(s *v1beta1.OMENativeInstanceStatus) (time.Time, bool) {
	if s == nil || s.Operation == nil {
		return time.Time{}, false
	}
	if s.Operation.StartedAt.IsZero() {
		return time.Time{}, false
	}
	return s.Operation.StartedAt.Time, true
}

// instanceHasInFlightMigration reports whether the ledger has a Started
// entry for this (component, instance) without a terminal counterpart.
// Used to suppress fresh auto-trigger requests when an existing one is
// still being driven by Source-2 of detectMigrationRequest.
func instanceHasInFlightMigration(ledger *audit.Ledger, component v1beta1.ComponentType, idx int32) bool {
	if ledger == nil {
		return false
	}
	terminal := map[string]struct{}{}
	for _, e := range ledger.Entries {
		if e.Phase == audit.PhaseCompleted || e.Phase == audit.PhaseFailed {
			terminal[e.RequestUUID] = struct{}{}
		}
	}
	for _, e := range ledger.Entries {
		if e.Phase != audit.PhaseStarted {
			continue
		}
		if e.Component != string(component) {
			continue
		}
		if e.SourceInstance != idx {
			continue
		}
		if _, done := terminal[e.RequestUUID]; done {
			continue
		}
		return true
	}
	return false
}

// countAutoMigrationRetries returns the number of ledger entries (any
// phase) for this (component, instance) whose Reason == AutoRecover.
// Used to gate against the MaxAutoRetries cap.
//
// Counts ALL entries (Started + terminal) — a Failed prior attempt
// still counts toward the cap, so a misconfigured pod doesn't
// repeatedly chew through the migration budget.
func countAutoMigrationRetries(ledger *audit.Ledger, component v1beta1.ComponentType, idx int32) int32 {
	if ledger == nil {
		return 0
	}
	count := int32(0)
	for _, e := range ledger.Entries {
		if e.Reason != AutoMigrateReason {
			continue
		}
		if e.Component != string(component) {
			continue
		}
		if e.SourceInstance != idx {
			continue
		}
		count++
	}
	return count
}

// stampProgressDeadlineExceeded writes the
// ConditionProgressDeadlineExceeded Instance-level condition. Uses the
// status.MutateInstance helper to round-trip through the apiserver
// under retry.RetryOnConflict.
func stampProgressDeadlineExceeded(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32, retries int32, deadline time.Duration) error {
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		// Escalate Phase to Failed so the rest of the controller (and
		// operators) see the Instance as terminally stuck.
		if s.Phase != v1beta1.OMENativeInstanceFailed {
			s.Phase = v1beta1.OMENativeInstanceFailed
		}
		message := fmt.Sprintf("auto-migration retry cap %d reached; instance phase=%s stuck past ProgressDeadlineSeconds=%.0fs",
			retries, s.Phase, deadline.Seconds())
		cond := metav1.Condition{
			Type:               ConditionProgressDeadlineExceeded,
			Status:             metav1.ConditionTrue,
			Reason:             ReasonProgressDeadlineExceeded,
			Message:            message,
			ObservedGeneration: p.ISVC.Generation,
			LastTransitionTime: metav1.Now(),
		}
		return upsertInstanceCondition(s, cond)
	})
}

// upsertInstanceCondition merges cond into s.Conditions in-place,
// preserving LastTransitionTime when Status didn't change. Returns true
// when the slice was mutated (a real condition write happened).
func upsertInstanceCondition(s *v1beta1.OMENativeInstanceStatus, cond metav1.Condition) bool {
	for i := range s.Conditions {
		if s.Conditions[i].Type != cond.Type {
			continue
		}
		existing := &s.Conditions[i]
		if existing.Status == cond.Status &&
			existing.Reason == cond.Reason &&
			existing.Message == cond.Message &&
			existing.ObservedGeneration == cond.ObservedGeneration {
			return false
		}
		if existing.Status == cond.Status {
			// Preserve LastTransitionTime when Status is unchanged so
			// consumers can tell "stuck since X" from "just stamped".
			cond.LastTransitionTime = existing.LastTransitionTime
		}
		s.Conditions[i] = cond
		return true
	}
	s.Conditions = append(s.Conditions, cond)
	return true
}

// migrationPolicyForComponentExt extracts the MigrationPolicy from the
// Component's Lifecycle block. Returns nil when any link in the chain
// is missing — the detector treats that as "auto-migration disabled".
func migrationPolicyForComponentExt(ext *v1beta1.ComponentExtensionSpec) *v1beta1.MigrationPolicy {
	if ext == nil || ext.Lifecycle == nil {
		return nil
	}
	return ext.Lifecycle.MigrationPolicy
}

// resolveLiveSourceNode returns the node the source Instance's pods
// are running on, or "" when no pod is yet scheduled or pods span
// multiple nodes. The workload-side Migrate validateFromNode helper
// cross-references this against pod.Spec.NodeName, so we must populate
// it with the actually-observed node rather than relying on the
// requester (synthetic requests have no requester).
func resolveLiveSourceNode(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx int32) (string, error) {
	reader := p.Reader()
	if reader == nil {
		return "", nil
	}
	pods, err := query.LiveListPodsForInstance(ctx, reader, p.ISVC.Namespace, p.ISVC.Name, v1beta1convert.ComponentTypeToWorkload(component), idx)
	if err != nil {
		return "", fmt.Errorf("list source pods: %w", err)
	}
	if len(pods) == 0 {
		return "", nil
	}
	var observed string
	for _, pod := range pods {
		node := pod.Spec.NodeName
		if node == "" {
			return "", nil
		}
		if observed == "" {
			observed = node
			continue
		}
		if observed != node {
			// Source pods span multiple nodes — single-pod migration
			// can't legitimately handle that. Defer; the next pass
			// re-evaluates after one pod terminates.
			return "", nil
		}
	}
	return observed, nil
}
