package omenative

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
//  5. Drain source (serving=False, wait IsPodDrained), then delete.
//  6. Promote surge Ready+RunningRevision, drop source InstanceStatus,
//     persist Completed entry — in that order (crash window if reversed).
func Migrate(ctx context.Context, p ReconcileParams, plan ComponentPlan, sourceIdx int32, requestUUID string, req *MigrationRequest) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Migrate: nil client")
	}
	if req == nil {
		return false, fmt.Errorf("Migrate: nil request (instance=%d)", sourceIdx)
	}

	ledger, err := loadAuditLedger(ctx, p.reader(), p.ISVC)
	if err != nil {
		return false, fmt.Errorf("Migrate: load audit ledger: %w", err)
	}
	if ledger.hasCompletedOrFailedRequest(requestUUID) {
		return true, nil
	}

	// Recover surge index from source status (post-stamp) or ledger
	// (post-Started, pre-stamp crash); allocate on a fresh request.
	source := findInstanceStatus(p.ISVC, plan.Component, sourceIdx)
	inFlight := ledger.inFlightEntry(requestUUID)
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
			recordWarning(p, EventReasonMigrationFromNodeMismatch,
				"OMENative migration uuid=%s rejected: %s", requestUUID, mismatch)
			return failMigration(ctx, p, ledger, req, requestUUID, mismatch)
		}

		// Cluster-wide capacity / rate-limit gate (in-flight + last-hour
		// counts across the audit ledger).
		if ok, reason := validateMigrationCapacity(ledger, time.Now()); !ok {
			recordWarning(p, EventReasonRateLimited,
				"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
			return failMigration(ctx, p, ledger, req, requestUUID, reason)
		}

		used := existingInstanceIndices(p.ISVC, plan.Component)
		surgeIdx = lowestUnusedIndex(used)
		if err := patchInstanceStatusMigrating(ctx, p, plan.Component, sourceIdx, surgeIdx, requestUUID, req, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("Migrate: stamp source status (instance=%d): %w", sourceIdx, err)
		}
		if err := patchInstanceStatusMigrationSurge(ctx, p, plan.Component, surgeIdx, sourceIdx, requestUUID, req, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("Migrate: stamp surge status (instance=%d): %w", surgeIdx, err)
		}
		ledger.upsertEntry(auditStartedEntry(req, requestUUID, surgeIdx))
		if err := persistAuditLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
			return false, fmt.Errorf("Migrate: persist Started ledger: %w", err)
		}
		// Clear the annotation after stamping Started so a re-delivery
		// within the requester's retry window dedups via the ledger.
		if err := clearMigrationRequestAnnotation(ctx, p, requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: clear request annotation: %w", err)
		}
		recordNormal(p, EventReasonMigrationRequestAccepted,
			"OMENative %s migration accepted (uuid=%s, source-node=%s, surge-index=%d)",
			instanceKey(plan.Component, sourceIdx), requestUUID, req.FromNode, surgeIdx)
	}

	sourcePods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: list source pods: %w", err)
	}
	surgePods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, surgeIdx)
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
	surgeInst := InstancePlan{
		Index:       surgeIdx,
		Incarnation: 1,
		Runners:     []RunnerPlan{{Name: "default", Size: 1}},
		MigrationOverlay: &MigrationOverlay{
			FromNode:        req.FromNode,
			HintTargetNodes: req.HintTargetNodes,
		},
	}

	// Refuse a surge whose hard NodeAffinity would collapse to
	// "hostname=FromNode AND hostname!=FromNode". Without this gate the
	// surge sits Pending until timeout — silent rejection vs. early fail.
	if wouldOverlayConflictWithNodeAffinity(surgeRevSpec, surgeInst.MigrationOverlay) {
		reason := fmt.Sprintf("source PodSpec NodeAffinity requires kubernetes.io/hostname=%s; overlay's NotIn[%s] would make scheduling impossible", req.FromNode, req.FromNode)
		recordWarning(p, EventReasonMigrationNodeAffinityConflict,
			"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
		return failMigration(ctx, p, ledger, req, requestUUID, reason)
	}

	desired := expectedPodNamesForInstance(p.ISVC.Name, plan.Component, surgeInst)
	existingByName := indexPodsByName(surgePods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, surgeIdx) {
			return false, nil
		}
		// Render against the surge revision's PodSpec, not p.PodSpec.
		// Expectations bucket on surgeIdx (not sourceIdx) so surge
		// creates track separately from source-side deletes.
		surgeP := p
		surgeP.PodSpec = surgeRevSpec
		if _, cerr := createMissingPods(ctx, surgeP, plan, surgeInst, surgeIdx, missing); cerr != nil {
			return false, fmt.Errorf("Migrate: create surge pod set: %w", cerr)
		}
		return false, nil
	}

	if !allPodsRuntimeReady(surgePods) {
		return false, nil
	}
	for _, pod := range surgePods {
		if IsServing(pod) {
			continue
		}
		if err := MarkPodServing(ctx, p.Client, pod, WriterLifecycle, KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("Migrate: serving=True on surge pod %s: %w", pod.Name, err)
		}
	}

	// Wait for surge endpoints in rotation before draining source — without
	// this gate the swap can hit a zero-routable-endpoint window. Live
	// reader so a cold informer doesn't falsely report zero slices.
	serviceName := headlessServiceName(p.ISVC.Name, plan.Component)
	for _, pod := range surgePods {
		inRotation, rerr := IsPodInRotation(ctx, p.reader(), p.ISVC.Namespace, serviceName, pod)
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
	surgeStatus := findInstanceStatus(p.ISVC, plan.Component, surgeIdx)
	if surgeStatus == nil || surgeStatus.PodCount == 0 ||
		surgeStatus.AvailablePodCount < surgeStatus.PodCount {
		return false, nil
	}

	for _, pod := range sourcePods {
		if !IsServing(pod) {
			continue
		}
		// Migrate-source-drain key; cleared by pod deletion at the end.
		if err := MarkPodNotServing(ctx, p.Client, pod, WriterMigrateSourceDrain, requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: serving=False on source pod %s: %w", pod.Name, err)
		}
	}
	for _, pod := range sourcePods {
		drained, derr := IsPodDrained(ctx, p.reader(), p.ISVC.Namespace, serviceName, pod)
		if derr != nil {
			return false, fmt.Errorf("Migrate: check drain on source pod %s: %w", pod.Name, derr)
		}
		if !drained {
			return false, nil
		}
	}

	// Live read before deletes — a stale-empty cache view would skip the
	// delete loop and orphan source pods at source-status removal.
	sourcePods, err = liveListPodsForInstance(ctx, p, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: live-list source pods: %w", err)
	}

	if len(sourcePods) > 0 {
		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			p.expectations().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx, 1)
			if err := p.Client.Delete(ctx, pod); err != nil {
				p.expectations().ObservedDelete(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx)
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
	ledger.upsertEntry(auditTerminalEntry(*ledger.inFlightEntryOrSeed(requestUUID, req, surgeIdx), auditPhaseCompleted, "migrated"))
	if err := persistAuditLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Completed ledger: %w", err)
	}
	// Re-clear in case a re-delivery slipped in between the accept-time
	// clear and now. Idempotent.
	if err := clearMigrationRequestAnnotation(ctx, p, requestUUID); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on completion: %w", err)
	}
	recordNormal(p, EventReasonMigrationCompleted,
		"OMENative migration uuid=%s complete: %s -> instance=%d (revision=%s)",
		requestUUID, instanceKey(plan.Component, sourceIdx), surgeIdx, surgeRev.Name)
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
func validateFromNode(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, sourceIdx int32, fromNode string) (string, bool, error) {
	pods, err := liveListPodsForInstance(ctx, p, component, sourceIdx)
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
func failMigration(ctx context.Context, p ReconcileParams, ledger *auditLedger, req *MigrationRequest, uuid, reason string) (bool, error) {
	now := metav1.Now().UTC().Format(time.RFC3339)
	entry := AuditEntry{
		RequestUUID: uuid,
		Phase:       auditPhaseFailed,
		Component:   req.Component,
		FromNode:    req.FromNode,
		StartedAt:   now,
		CompletedAt: now,
		Outcome:     reason,
	}
	ledger.upsertEntry(entry)
	if err := persistAuditLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Failed ledger (%s): %w", reason, err)
	}
	// Clear on rejection too — the requester retries against the same
	// UUID forever if the annotation lingers.
	if err := clearMigrationRequestAnnotation(ctx, p, uuid); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on rejection: %w", err)
	}
	recordWarning(p, EventReasonMigrationRequestRejected,
		"OMENative migration uuid=%s rejected: %s", uuid, reason)
	return true, nil
}

// clearMigrationRequestAnnotation removes the
// ome.io/migration-request-v1-<uuid> annotation. The requester contract
// requires accept/reject within 30s by annotation removal; without this
// they re-deliver the same UUID indefinitely. retry.RetryOnConflict so
// concurrent reconcile-pass writes don't lose the removal.
func clearMigrationRequestAnnotation(ctx context.Context, p ReconcileParams, uuid string) error {
	if p.Client == nil || p.ISVC == nil || uuid == "" {
		return nil
	}
	key := migrationRequestAnnotationPrefix + uuid
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

// inFlightEntryOrSeed returns the in-flight Started entry, synthesizing
// one if missing — covers the case where the ledger was lost between
// writes. Keeps the terminal append in Migrate's tail simple.
func (l *auditLedger) inFlightEntryOrSeed(uuid string, req *MigrationRequest, surgeIdx int32) *AuditEntry {
	if e := l.inFlightEntry(uuid); e != nil {
		return e
	}
	e := auditStartedEntry(req, uuid, surgeIdx)
	return &e
}

// detectMigrationRequest scans annotations for the first non-terminal
// migration request. Schema-violating / wrong-component requests get a
// Failed ledger entry but don't surface as Go errors.
func detectMigrationRequest(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType) (string, *MigrationRequest, int32, bool, error) {
	if p.ISVC == nil || p.ISVC.Annotations == nil {
		return "", nil, 0, false, nil
	}
	// Deterministic order when multiple requests are pending.
	keys := make([]string, 0, len(p.ISVC.Annotations))
	for k := range p.ISVC.Annotations {
		if strings.HasPrefix(k, migrationRequestAnnotationPrefix) {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return "", nil, 0, false, nil
	}
	sort.Strings(keys)

	ledger, err := loadAuditLedger(ctx, p.reader(), p.ISVC)
	if err != nil {
		return "", nil, 0, false, err
	}

	for _, k := range keys {
		uuid := extractRequestUUID(k)
		if uuid == "" {
			continue
		}
		if ledger.hasCompletedOrFailedRequest(uuid) {
			continue
		}
		raw := p.ISVC.Annotations[k]
		req, perr := parseMigrationRequest(raw)
		if perr != nil {
			// Terminal Failed so we don't re-parse every reconcile.
			ledger.upsertEntry(AuditEntry{
				RequestUUID: uuid,
				Phase:       auditPhaseFailed,
				StartedAt:   metav1.Now().UTC().Format(time.RFC3339),
				CompletedAt: metav1.Now().UTC().Format(time.RFC3339),
				Outcome:     perr.Error(),
			})
			if err := persistAuditLedger(ctx, p.Client, p.ISVC, ledger); err != nil {
				return "", nil, 0, false, fmt.Errorf("persist Failed ledger after parse error: %w", err)
			}
			if errors.Is(perr, errUnsupportedSchemaVersion) {
				recordWarning(p, EventReasonUnsupportedSchemaVersion,
					"OMENative migration uuid=%s rejected: %v", uuid, perr)
			} else {
				recordWarning(p, EventReasonMigrationRequestRejected,
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
func surgeRevisionAndSpec(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, sourceIdx int32, sourcePods []*corev1.Pod) (*appsv1.ControllerRevision, *corev1.PodSpec, error) {
	source := findInstanceStatus(p.ISVC, component, sourceIdx)
	if source == nil || source.RunningRevision == "" {
		return nil, nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	if err := p.reader().Get(ctx, client.ObjectKey{Namespace: p.ISVC.Namespace, Name: source.RunningRevision}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("get source CR %s: %w", source.RunningRevision, err)
	}
	var payload revisionDataPayload
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
func stampMigrationStatus(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx, siblingIdx int32, role migrationRole, uuid string, req *MigrationRequest, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	siblingPtr := siblingIdx
	wantPhase := v1beta1.OMENativeInstanceMigrating
	idSuffix := ""
	if role == migrationRoleSurge {
		wantPhase = v1beta1.OMENativeInstanceCreating
		idSuffix = "-surge"
	}
	return mutateInstanceStatus(ctx, p, component, idx, func(s *v1beta1.OMENativeInstanceStatus) bool {
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
func patchInstanceStatusMigrating(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx, surgeIdx int32, uuid string, req *MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, p, component, idx, surgeIdx, migrationRoleSource, uuid, req, timeout)
}

// patchInstanceStatusMigrationSurge stamps surge Phase=Creating; SurgeIndex
// points back at source (field reused as a sibling pointer so observers
// can correlate the pair from either side).
func patchInstanceStatusMigrationSurge(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, surgeIdx, sourceIdx int32, uuid string, req *MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, p, component, surgeIdx, sourceIdx, migrationRoleSurge, uuid, req, timeout)
}
