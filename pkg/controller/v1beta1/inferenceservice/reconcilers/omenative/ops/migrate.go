package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/revision"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/status"
)

// migrateRequeueInterval is the wait between Migrate passes while a
// surge migration is in flight.
const migrateRequeueInterval = 5 * time.Second

// Migrate drives one surge migration toward completion. Triggered by an
// ISVC annotation; multi-pass and crash-safe via the audit ConfigMap.
//
// Sequence (surge-only):
//
//  1. Audit-ledger dedup; reject schema-invalid / capacity-exceeded.
//  2. Allocate surge index = lowest unused (sparse-safe). Stamp source
//     Phase=Migrating, surge Phase=Creating; persist Started entry.
//  3. Reuse source's RunningRevision as the surge template; create surge
//     pods with the migration anti-affinity overlay.
//  4. Wait surge ContainersReady + in-rotation + Available; flip serving.
//  5. Drain source (serving=False, wait drain.IsPodDrained), then delete.
//  6. Promote surge Ready+RunningRevision, drop source InstanceStatus,
//     persist Completed entry — in that order (crash window if reversed).
func Migrate(ctx context.Context, p core.ReconcileParams, plan core.ComponentPlan, sourceIdx int32, requestUUID string, req *audit.MigrationRequest) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Migrate: nil client")
	}
	if req == nil {
		return false, fmt.Errorf("Migrate: nil request (instance=%d)", sourceIdx)
	}

	ledger, err := audit.LoadLedger(ctx, p.Reader(), p.ISVC)
	if err != nil {
		return false, fmt.Errorf("Migrate: load audit ledger: %w", err)
	}
	if ledger.HasCompletedOrFailedRequest(requestUUID) {
		return true, nil
	}

	// Recover surge index from source status (post-stamp) or ledger
	// (post-Started, pre-stamp crash); allocate on a fresh request.
	source := query.FindInstanceStatus(p.ISVC, plan.Component, sourceIdx)
	inFlight := ledger.InFlightEntry(requestUUID)
	var surgeIdx int32
	switch {
	case source != nil && source.Operation != nil && source.Operation.Type == v1beta1.InstanceOperationMigrate && source.Operation.SurgeIndex != nil:
		surgeIdx = *source.Operation.SurgeIndex
	case inFlight != nil:
		surgeIdx = inFlight.SurgeInstance
	default:
		// Fresh request: refuse to stamp Migrating unless source is a
		// steady Ready with a recorded revision. Without these guards a
		// mid-Create stamp deadlocks Create and a mid-Update / Restart
		// stamp silently steamrolls the in-flight op.
		if source == nil {
			return failMigration(ctx, p, ledger, req, requestUUID, "source InstanceStatus missing")
		}
		if source.Phase != v1beta1.OMENativeInstanceReady || source.Operation != nil || source.RunningRevision == "" {
			return false, nil
		}

		// Validate FromNode against where source pods actually run. The
		// requester's view may be stale; if source has since moved, the
		// surge's NotIn[FromNode] could land on the SAME node as the
		// post-move source — silently no-op'ing the migration.
		switch mismatch, defer_, err := validateFromNode(ctx, p, plan.Component, sourceIdx, req.FromNode); {
		case err != nil:
			return false, fmt.Errorf("Migrate: validate from-node (instance=%d): %w", sourceIdx, err)
		case defer_:
			return false, nil
		case mismatch != "":
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonMigrationFromNodeMismatch,
				"OMENative migration uuid=%s rejected: %s", requestUUID, mismatch)
			return failMigration(ctx, p, ledger, req, requestUUID, mismatch)
		}

		// Cluster-wide capacity / rate-limit gate (in-flight + last-hour
		// counts across the audit ledger).
		if ok, reason := audit.ValidateCapacity(ledger, time.Now()); !ok {
			status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonRateLimited,
				"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
			return failMigration(ctx, p, ledger, req, requestUUID, reason)
		}

		surgeIdx = core.AllocateSurgeIndex(p.ISVC, plan.Component)
		if err := patchInstanceStatusMigrating(ctx, p, plan.Component, sourceIdx, surgeIdx, requestUUID, req, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("Migrate: stamp source status (instance=%d): %w", sourceIdx, err)
		}
		if err := patchInstanceStatusMigrationSurge(ctx, p, plan.Component, surgeIdx, sourceIdx, requestUUID, req, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("Migrate: stamp surge status (instance=%d): %w", surgeIdx, err)
		}
		ledger.UpsertEntry(audit.NewStartedEntry(req, requestUUID, surgeIdx))
		if err := audit.PersistLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
			return false, fmt.Errorf("Migrate: persist Started ledger: %w", err)
		}
		// Clear the annotation after stamping Started so a re-delivery
		// within the requester's retry window dedups via the ledger.
		if err := clearMigrationRequestAnnotation(ctx, p, requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: clear request annotation: %w", err)
		}
		status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonMigrationRequestAccepted,
			"OMENative %s migration accepted (uuid=%s, source-node=%s, surge-index=%d)",
			status.InstanceKey(plan.Component, sourceIdx), requestUUID, req.FromNode, surgeIdx)
	}

	sourcePods, err := query.ListPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: list source pods: %w", err)
	}
	surgePods, err := query.ListPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: list surge pods: %w", err)
	}

	// Migration moves placement, not template — surge runs at the source's
	// RunningRevision.
	surgeRev, surgeRevSpec, err := surgeRevisionAndSpec(ctx, p, plan.Component, sourceIdx, sourcePods)
	if err != nil {
		return false, fmt.Errorf("Migrate: resolve surge revision: %w", err)
	}
	if surgeRev == nil || surgeRevSpec == nil {
		// Source not yet promoted with a RunningRevision — retry.
		return false, nil
	}

	// Migration overlay tells Render to stamp anti-affinity vs FromNode.
	surgeInst := core.InstancePlan{
		Index:       surgeIdx,
		Incarnation: 1,
		Runners:     []core.RunnerPlan{{Name: "default", Size: 1}},
		MigrationOverlay: &core.MigrationOverlay{
			FromNode:        req.FromNode,
			HintTargetNodes: req.HintTargetNodes,
		},
	}

	// Refuse a surge whose hard NodeAffinity would collapse to
	// "hostname=FromNode AND hostname!=FromNode". Without this gate the
	// surge sits Pending until timeout — silent rejection vs. early fail.
	if WouldOverlayConflictWithNodeAffinity(surgeRevSpec, surgeInst.MigrationOverlay) {
		reason := fmt.Sprintf("source PodSpec NodeAffinity requires kubernetes.io/hostname=%s; overlay's NotIn[%s] would make scheduling impossible", req.FromNode, req.FromNode)
		status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonMigrationNodeAffinityConflict,
			"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
		return failMigration(ctx, p, ledger, req, requestUUID, reason)
	}

	desired := expectedPodNamesForInstance(p.ISVC.Name, plan.Component, surgeInst)
	existingByName := query.IndexPodsByName(surgePods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, surgeIdx) {
			return false, nil
		}
		// Render against the surge revision's PodSpec, not p.PodSpec.
		// core.Expectations bucket on surgeIdx (not sourceIdx) so surge
		// creates track separately from source-side deletes.
		surgeP := p
		surgeP.PodSpec = surgeRevSpec
		if _, cerr := createMissingPods(ctx, surgeP, plan, surgeInst, surgeIdx, missing, revisionHashFromTarget(surgeRev)); cerr != nil {
			return false, fmt.Errorf("Migrate: create surge pod set: %w", cerr)
		}
		return false, nil
	}

	if !query.AllPodsRuntimeReady(surgePods) {
		return false, nil
	}
	for _, pod := range surgePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, p.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("Migrate: serving=True on surge pod %s: %w", pod.Name, err)
		}
	}

	// Wait for surge endpoints in rotation before draining source — without
	// this gate the swap can hit a zero-routable-endpoint window. Live
	// reader so a cold informer doesn't falsely report zero slices.
	serviceName := query.HeadlessServiceName(p.ISVC.Name, plan.Component)
	for _, pod := range surgePods {
		inRotation, rerr := drain.IsPodInRotation(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
		if rerr != nil {
			return false, fmt.Errorf("Migrate: check rotation on surge pod %s: %w", pod.Name, rerr)
		}
		if !inRotation {
			return false, nil
		}
	}

	// Wait for surge to be Available (Ready ≥ minReadySeconds) before
	// draining. At alpha minReadySeconds defaults to 0 so this collapses
	// onto Ready, but the gate stays for when a non-zero value is added.
	surgeStatus := query.FindInstanceStatus(p.ISVC, plan.Component, surgeIdx)
	if surgeStatus == nil || surgeStatus.PodCount == 0 ||
		surgeStatus.AvailablePodCount < surgeStatus.PodCount {
		return false, nil
	}

	for _, pod := range sourcePods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		// Migrate-source-drain key; cleared by pod deletion at the end.
		if err := podreadiness.MarkPodNotServing(ctx, p.Client, pod, podreadiness.WriterMigrateSourceDrain, requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: serving=False on source pod %s: %w", pod.Name, err)
		}
	}
	for _, pod := range sourcePods {
		drained, derr := drain.IsPodDrained(ctx, p.Reader(), p.ISVC.Namespace, serviceName, pod)
		if derr != nil {
			return false, fmt.Errorf("Migrate: check drain on source pod %s: %w", pod.Name, derr)
		}
		if !drained {
			return false, nil
		}
	}

	// Live read before deletes — a stale-empty cache view would skip the
	// delete loop and orphan source pods at source-status removal.
	sourcePods, err = query.LiveListPodsForInstance(ctx, p, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: live-list source pods: %w", err)
	}

	if len(sourcePods) > 0 {
		if !p.ExpectationsCache().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			p.ExpectationsCache().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx, 1)
			if err := p.Client.Delete(ctx, pod); err != nil {
				p.ExpectationsCache().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("Migrate: delete source pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Persist Completed AFTER the surge promote + source-status removal
	// succeed. The inverse order opens a crash window where the ledger
	// says Completed but source is still Migrating and surge still
	// Creating; the next reconcile dedups via hasCompletedOrFailedRequest
	// and never finishes cleanup. Both operations below are idempotent so
	// re-delivery between them is safe.
	if err := patchInstanceStatusReadyOnRevision(ctx, p, plan.Component, surgeIdx, surgeRev.Name); err != nil {
		return false, fmt.Errorf("Migrate: promote surge to Ready: %w", err)
	}
	if _, rerr := removeInstanceStatus(ctx, p, plan.Component, sourceIdx); rerr != nil {
		return false, fmt.Errorf("Migrate: remove source InstanceStatus: %w", rerr)
	}
	ledger.UpsertEntry(audit.NewTerminalEntry(*ledger.InFlightEntryOrSeed(requestUUID, req, surgeIdx), audit.PhaseCompleted, "migrated"))
	if err := audit.PersistLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Completed ledger: %w", err)
	}
	// Re-clear in case a re-delivery slipped in between the accept-time
	// clear and now. Idempotent.
	if err := clearMigrationRequestAnnotation(ctx, p, requestUUID); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on completion: %w", err)
	}
	status.RecordNormal(p.Recorder, p.ISVC, status.EventReasonMigrationCompleted,
		"OMENative migration uuid=%s complete: %s -> instance=%d (revision=%s)",
		requestUUID, status.InstanceKey(plan.Component, sourceIdx), surgeIdx, surgeRev.Name)
	return true, nil
}

// validateFromNode live-reads source pods, returning three states:
//
//   - rejectionReason="" defer_=false err=nil — valid, proceed
//   - defer_=true — transient (pod not scheduled yet); caller requeues
//   - rejectionReason != "" — permanent mismatch; caller fails
//
// Source pods on multiple nodes is a rejection — alpha single-pod can't
// legitimately span nodes.
func validateFromNode(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, sourceIdx int32, fromNode string) (string, bool, error) {
	pods, err := query.LiveListPodsForInstance(ctx, p, component, sourceIdx)
	if err != nil {
		return "", false, fmt.Errorf("list source pods: %w", err)
	}
	if len(pods) == 0 {
		return "source instance has no live pods", false, nil
	}
	var observed string
	for _, pod := range pods {
		node := pod.Spec.NodeName
		if node == "" {
			// Unscheduled — defer; next reconcile re-validates.
			return "", true, nil
		}
		if observed == "" {
			observed = node
			continue
		}
		if observed != node {
			return "source pods span multiple nodes (" + observed + ", " + node + ")", false, nil
		}
	}
	if observed != fromNode {
		return "request.FromNode=" + fromNode + " does not match observed source node=" + observed, false, nil
	}
	return "", false, nil
}

// failMigration writes a terminal Failed audit row; the ledger blocks
// re-attempts on this UUID. Returns done=true so the reconciler stops.
func failMigration(ctx context.Context, p core.ReconcileParams, ledger *audit.Ledger, req *audit.MigrationRequest, uuid, reason string) (bool, error) {
	now := metav1.Now().UTC().Format(time.RFC3339)
	entry := audit.Entry{
		RequestUUID: uuid,
		Phase:       audit.PhaseFailed,
		Component:   req.Component,
		FromNode:    req.FromNode,
		StartedAt:   now,
		CompletedAt: now,
		Outcome:     reason,
	}
	ledger.UpsertEntry(entry)
	if err := audit.PersistLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Failed ledger (%s): %w", reason, err)
	}
	// Clear on rejection too — the requester retries against the same
	// UUID forever if the annotation lingers.
	if err := clearMigrationRequestAnnotation(ctx, p, uuid); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on rejection: %w", err)
	}
	status.RecordWarning(p.Recorder, p.ISVC, status.EventReasonMigrationRequestRejected,
		"OMENative migration uuid=%s rejected: %s", uuid, reason)
	return true, nil
}

// clearMigrationRequestAnnotation removes the
// ome.io/migration-request-v1-<uuid> annotation. The requester contract
// requires accept/reject within 30s by annotation removal; without this
// they re-deliver the same UUID indefinitely. retry.RetryOnConflict so
// concurrent reconcile-pass writes don't lose the removal.
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

	ledger, err := audit.LoadLedger(ctx, p.Reader(), p.ISVC)
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
			if err := audit.PersistLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
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

// surgeRevisionAndSpec returns the source's RunningRevision CR + PodSpec.
// (nil, nil, nil) means no recorded RunningRevision yet OR the CR was
// deleted — caller retries.
func surgeRevisionAndSpec(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, sourceIdx int32, sourcePods []*corev1.Pod) (*appsv1.ControllerRevision, *corev1.PodSpec, error) {
	source := query.FindInstanceStatus(p.ISVC, component, sourceIdx)
	if source == nil || source.RunningRevision == "" {
		return nil, nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	if err := p.Reader().Get(ctx, client.ObjectKey{Namespace: p.ISVC.Namespace, Name: source.RunningRevision}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("get source CR %s: %w", source.RunningRevision, err)
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, nil, fmt.Errorf("unmarshal source CR data: %w", err)
	}
	if payload.PodSpec == nil {
		return nil, nil, fmt.Errorf("source CR %s missing podSpec payload", source.RunningRevision)
	}
	return cr, payload.PodSpec, nil
}

// migrationRole picks which side of the pair stampMigrationStatus is
// writing — source goes to Phase=Migrating with sibling=surge; surge
// goes to Phase=Creating with sibling=source and Incarnation seeded.
type migrationRole int

const (
	migrationRoleSource migrationRole = iota
	migrationRoleSurge
)

// stampMigrationStatus writes one side of the migration pair. Idempotent
// on (Phase, Operation.Type, RequestUUID).
func stampMigrationStatus(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx, siblingIdx int32, role migrationRole, uuid string, req *audit.MigrationRequest, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	siblingPtr := siblingIdx
	wantPhase := v1beta1.OMENativeInstanceMigrating
	idSuffix := ""
	if role == migrationRoleSurge {
		wantPhase = v1beta1.OMENativeInstanceCreating
		idSuffix = "-surge"
	}
	return status.MutateInstance(ctx, p.Client, p.ISVC, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
		if s.Phase == wantPhase &&
			s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationMigrate &&
			s.Operation.RequestUUID == uuid {
			return false
		}
		if role == migrationRoleSurge && s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Phase = wantPhase
		s.Operation = &v1beta1.InstanceOperation{
			ID:              fmt.Sprintf("migrate-%s%s-%d", uuid, idSuffix, now.Unix()),
			Type:            v1beta1.InstanceOperationMigrate,
			Step:            "CreateSurge",
			RequestUUID:     uuid,
			FromNode:        req.FromNode,
			HintTargetNodes: append([]string(nil), req.HintTargetNodes...),
			SurgeIndex:      &siblingPtr,
			Reason:          req.Reason,
			StartedAt:       now,
			LastProgressAt:  now,
			Deadline:        metav1.NewTime(now.Add(timeout)),
		}
		return true
	})
}

// patchInstanceStatusMigrating stamps source Phase=Migrating; SurgeIndex
// on Operation points forward at the surge.
func patchInstanceStatusMigrating(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, idx, surgeIdx int32, uuid string, req *audit.MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, p, component, idx, surgeIdx, migrationRoleSource, uuid, req, timeout)
}

// patchInstanceStatusMigrationSurge stamps surge Phase=Creating; SurgeIndex
// points back at source (field reused as a sibling pointer so observers
// can correlate the pair from either side).
func patchInstanceStatusMigrationSurge(ctx context.Context, p core.ReconcileParams, component v1beta1.ComponentType, surgeIdx, sourceIdx int32, uuid string, req *audit.MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, p, component, surgeIdx, sourceIdx, migrationRoleSurge, uuid, req, timeout)
}
