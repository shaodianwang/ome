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

// Migrate drives one surge migration toward completion. Triggered by a
// MigrationRequest written into an ISVC annotation; orchestrated as a
// multi-pass state machine that survives controller restarts via the
// audit ConfigMap.
//
// Sequence (per the design — surge-only in v1):
//
//  1. Audit-ledger dedup: if this UUID already terminally completed,
//     no-op.
//  2. Validate request; reject UnsupportedSchemaVersion.
//  3. Allocate surge Instance index = lowest unused (sparse-safe).
//  4. Patch status: source -> Migrating, surge -> Creating, both with
//     Operation=Migrate carrying RequestUUID / FromNode / SurgeIndex.
//  5. Persist a Started audit-ledger row.
//  6. Reuse the running CR as the surge revision; create the surge
//     pod set at that revision with the migration anti-affinity
//     overlay.
//  7. Wait until surge pods are ContainersReady; flip serving on the
//     surge.
//  8. Drain the source: serving=False, wait IsPodDrained against the
//     headless Service.
//  9. Delete source pods; wait until the live list is empty.
//  10. Remove source InstanceStatus; promote surge to Ready with
//     RunningRevision pointing at the source's revision.
//  11. Persist a Completed audit-ledger row.
//
// done=true means the migration has finished (step 10+11). done=false
// means the caller should requeue.
func Migrate(ctx context.Context, p ReconcileParams, plan ComponentPlan, sourceIdx int32, requestUUID string, req *MigrationRequest) (bool, error) {
	if p.Client == nil {
		return false, fmt.Errorf("Migrate: nil client")
	}
	if req == nil {
		return false, fmt.Errorf("Migrate: nil request (instance=%d)", sourceIdx)
	}

	// Step 1: dedup against the audit ledger.
	ledger, err := loadAuditLedger(ctx, p.reader(), p.ISVC)
	if err != nil {
		return false, fmt.Errorf("Migrate: load audit ledger: %w", err)
	}
	if ledger.hasCompletedOrFailedRequest(requestUUID) {
		return true, nil
	}

	// Step 3 / 4 / 5 — only when this is the first reconcile of this
	// request (no Started entry yet). The Operation on the source
	// InstanceStatus carries the SurgeIndex once allocated, so later
	// passes can re-read it without re-allocating.
	source := findInstanceStatus(p.ISVC, plan.Component, sourceIdx)
	inFlight := ledger.inFlightEntry(requestUUID)
	var surgeIdx int32
	switch {
	case source != nil && source.Operation != nil && source.Operation.Type == v1beta1.InstanceOperationMigrate && source.Operation.SurgeIndex != nil:
		surgeIdx = *source.Operation.SurgeIndex
	case inFlight != nil:
		// Ledger remembers the surge index across a crash mid-stamp.
		surgeIdx = inFlight.SurgeInstance
	default:
		// Fresh request: refuse to stamp Migrating until the source
		// is a steady Ready Instance with a recorded revision. Without
		// these guards a request mid-Create overwrites Create's
		// Operation and deadlocks, and a request mid-Update / Restart
		// silently steamrolls the in-flight op.
		if source == nil {
			return failMigration(ctx, p, ledger, req, requestUUID, "source InstanceStatus missing")
		}
		if source.Phase != v1beta1.OMENativeInstanceReady || source.Operation != nil || source.RunningRevision == "" {
			return false, nil
		}

		// MIGRATE-5: validate the request's FromNode matches the
		// node the source pods actually run on. Alfred may have read
		// stale data; if the source has since moved (e.g., a prior
		// eviction relocated it), surge anti-affinity NotIn[FromNode]
		// could place the surge on the SAME node as the post-move
		// source — silently turning the migration into a no-op.
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

		// MIGRATE-4: cluster-wide capacity / rate-limit check. Counts
		// in-flight Started entries and last-hour Started entries
		// across the audit ledger; rejects with RateLimited if
		// either cap is exceeded.
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
		// MIGRATE-3: clear the annotation now that we've stamped
		// Started. Alfred observes acceptance via either annotation
		// removal or the status update; clearing here means a
		// re-delivered request within Alfred's retry window finds the
		// audit ledger and dedups via hasCompletedOrFailedRequest /
		// inFlightEntry.
		if err := clearMigrationRequestAnnotation(ctx, p, requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: clear request annotation: %w", err)
		}
		recordNormal(p, EventReasonMigrationRequestAccepted,
			"OMENative %s migration accepted (uuid=%s, source-node=%s, surge-index=%d)",
			instanceKey(plan.Component, sourceIdx), requestUUID, req.FromNode, surgeIdx)
	}

	// Step 6: create the surge pod set.
	sourcePods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: list source pods: %w", err)
	}
	surgePods, err := listPodsForInstance(ctx, p.Client, p.ISVC, plan.Component, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: list surge pods: %w", err)
	}

	// Determine the revision the surge should run at. Same as the
	// source's RunningRevision — migration moves placement, not
	// template.
	surgeRev, surgeRevSpec, err := surgeRevisionAndSpec(ctx, p, plan.Component, sourceIdx, sourcePods)
	if err != nil {
		return false, fmt.Errorf("Migrate: resolve surge revision: %w", err)
	}
	if surgeRev == nil || surgeRevSpec == nil {
		// Source has no recorded RunningRevision yet — happens on the
		// very first reconcile after Create lands but before the
		// status promote step writes RunningRevision. Bail back and
		// retry; Update / Create will fill it in.
		return false, nil
	}

	// Build the surge InstancePlan with the migration overlay so Render
	// stamps the anti-affinity.
	surgeInst := InstancePlan{
		Index:       surgeIdx,
		Incarnation: 1,
		Runners:     []RunnerPlan{{Name: "default", Size: 1}},
		MigrationOverlay: &MigrationOverlay{
			FromNode:        req.FromNode,
			HintTargetNodes: req.HintTargetNodes,
		},
	}

	// REV-6: refuse to render a surge whose hard NodeAffinity would
	// collapse to "hostname=FromNode AND hostname!=FromNode" once the
	// overlay is applied. Without this gate the surge pod sits Pending
	// until instanceReadyTimeout and the migration eventually times out
	// — a silent rejection that an early Failed entry surfaces clearly.
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
		// Use the surge revision's PodSpec as the create template, not
		// the rendered p.PodSpec — migration keeps the running shape.
		surgeP := p
		surgeP.PodSpec = surgeRevSpec
		created, cerr := createMissingPods(ctx, surgeP, plan, surgeInst, missing)
		if cerr != nil {
			return false, fmt.Errorf("Migrate: create surge pod set: %w", cerr)
		}
		if created > 0 {
			p.expectations().ExpectCreates(p.ISVC.Namespace, p.ISVC.Name, plan.Component, surgeIdx, created)
		}
		return false, nil
	}

	// Step 7: wait surge ContainersReady, then flip serving.
	if !allPodsRuntimeReady(surgePods) {
		return false, nil
	}
	for _, pod := range surgePods {
		if IsServing(pod) {
			continue
		}
		// Surge pods are fresh — use the Lifecycle key.
		if err := MarkPodServing(ctx, p.Client, pod, WriterLifecycle, KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("Migrate: serving=True on surge pod %s: %w", pod.Name, err)
		}
	}

	// Step 7b: wait for kube-proxy to publish surge endpoints as Ready
	// before initiating the source drain. Without this gate the swap
	// can land in a zero-routable-endpoint window: serving flips on
	// surge, drain flips on source, but neither has propagated to the
	// dataplane yet. Live reader so a cold informer doesn't falsely
	// report zero slices.
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

	// MIGRATE-6: per OEP §7.5.2 step 8, wait until the surge is fully
	// Available (Ready for >= minReadySeconds) before draining the
	// source. At v1 alpha minReadySeconds defaults to 0 so this
	// collapses onto Ready, but the gate stays so the addition of a
	// non-zero minReadySeconds doesn't reopen the regression. A
	// flapping-ready surge would otherwise trigger source drain
	// against a surge that isn't stable.
	surgeStatus := findInstanceStatus(p.ISVC, plan.Component, surgeIdx)
	if surgeStatus == nil || surgeStatus.PodCount == 0 ||
		surgeStatus.AvailablePodCount < surgeStatus.PodCount {
		return false, nil
	}

	// Step 8: drain the source — serving=False, then wait IsPodDrained.
	for _, pod := range sourcePods {
		if !IsServing(pod) {
			continue
		}
		// Source pods get the Migrate-source-drain key. The key is
		// removed by virtue of source pod deletion at the end of
		// the migration; no explicit RemoveNotReadyKey needed.
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

	// OPS-1: refresh source pods via live read before issuing
	// deletes. The earlier sourcePods read at the top of Migrate
	// was cached; a stale-empty view here would skip the delete
	// loop and proceed to source-status removal while the API
	// server still holds the pods (Merged review MIGRATE-2 / OPS-1
	// orphan scenario).
	sourcePods, err = liveListPodsForInstance(ctx, p, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: live-list source pods: %w", err)
	}

	// Step 9: delete source pods (expectations-gated, same pattern as Delete).
	if len(sourcePods) > 0 {
		if !p.expectations().Satisfied(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx) {
			return false, nil
		}
		deleted := 0
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			if err := p.Client.Delete(ctx, pod); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("Migrate: delete source pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			deleted++
		}
		if deleted > 0 {
			p.expectations().ExpectDeletes(p.ISVC.Namespace, p.ISVC.Name, plan.Component, sourceIdx, deleted)
		}
		return false, nil
	}

	// MIGRATE-2: persist Completed AFTER the surge promote +
	// source-status removal succeed (was: write Completed first,
	// then mutate). The prior order created a crash window where a
	// crash between ledger-write and surge-promote left the ledger
	// saying Completed while source still had Phase=Migrating and
	// surge still Phase=Creating — the next reconcile would dedup-
	// short-circuit and never finish the cleanup. Now the dedup
	// short-circuit only fires after the cluster state actually
	// matches "migration done."
	//
	// Re-delivery during these two steps is safe — both operations
	// are idempotent (surge index already allocated, surge promote
	// is patchInstanceStatusReadyOnRevision which is skip-write
	// when already in the target shape, source removal is no-op if
	// already removed).
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
	// MIGRATE-3: ensure the annotation is gone on completion too —
	// the accept-time clear at the top of this function may have
	// missed a re-delivery that arrived after acceptance but before
	// completion. Idempotent: no-op when the annotation is already
	// absent.
	if err := clearMigrationRequestAnnotation(ctx, p, requestUUID); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on completion: %w", err)
	}
	recordNormal(p, EventReasonMigrationCompleted,
		"OMENative migration uuid=%s complete: %s -> instance=%d (revision=%s)",
		requestUUID, instanceKey(plan.Component, sourceIdx), surgeIdx, surgeRev.Name)
	return true, nil
}

// validateFromNode live-reads the source pods and reports whether
// req.FromNode matches the node they actually run on. Three-state
// return: (rejectionReason, defer_, err).
//
//   - rejectionReason="" defer_=false err=nil — valid, caller proceeds
//   - defer_=true — transient state (pod not yet scheduled); caller
//     returns (false, nil) and re-polls
//   - rejectionReason != "" — permanent mismatch; caller fails the
//     migration with the human-readable reason
//
// Treats source pods on multiple distinct nodes as a rejection —
// single-pod alpha can't represent that legitimately.
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
			// Pod not yet scheduled — defer rather than reject. A
			// transient unscheduled window shouldn't fail an otherwise
			// valid request; the next reconcile re-validates.
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

// failMigration writes a terminal Failed audit row and returns done=true
// so the reconciler stops re-dispatching the UUID. The annotation stays
// on the ISVC; the ledger blocks re-attempts.
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
	// MIGRATE-3: clear the annotation on the rejection path too —
	// Alfred contract says the controller responds within 30s by
	// either accepting or rejecting; rejection without clearing
	// would have Alfred retry every 5m against the same UUID.
	if err := clearMigrationRequestAnnotation(ctx, p, uuid); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on rejection: %w", err)
	}
	recordWarning(p, EventReasonMigrationRequestRejected,
		"OMENative migration uuid=%s rejected: %s", uuid, reason)
	return true, nil
}

// clearMigrationRequestAnnotation removes the
// ome.io/migration-request-v1-<uuid> annotation from the parent
// ISVC, implementing MIGRATE-3 from the OEP-0007 review. Per OEP
// §"Migration API and Mechanics" the controller must clear the
// annotation within 30s of accepting or rejecting a request — Alfred's
// retry contract assumes the annotation disappears once observed.
//
// Wrapped in retry.RetryOnConflict to ride concurrent ISVC writes
// (per-op status mutations from the same reconcile pass). No-op when
// the annotation is already absent.
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

// inFlightEntryOrSeed returns the existing Started entry for uuid if
// present, otherwise synthesizes a Started entry (used when the
// audit ledger was lost between writes). Keeps the terminal append in
// Migrate's tail simple.
func (l *auditLedger) inFlightEntryOrSeed(uuid string, req *MigrationRequest, surgeIdx int32) *AuditEntry {
	if e := l.inFlightEntry(uuid); e != nil {
		return e
	}
	e := auditStartedEntry(req, uuid, surgeIdx)
	return &e
}

// detectMigrationRequest scans the ISVC's annotations for the first
// migration request whose UUID hasn't already completed terminally.
// Returns (uuid, request, true) when one is found. Schema-violating
// or component-mismatched requests are rejected via a Failed ledger
// entry but do not surface as Go errors.
func detectMigrationRequest(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType) (string, *MigrationRequest, int32, bool, error) {
	if p.ISVC == nil || p.ISVC.Annotations == nil {
		return "", nil, 0, false, nil
	}
	// Sort keys for deterministic dispatch when multiple migration
	// requests are pending. Go map iteration is randomized.
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
			// Schema violation: record a terminal Failed row so we
			// don't re-attempt every reconcile.
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
			// Skip — request targets a different Component.
			continue
		}
		return uuid, req, req.Instance, true, nil
	}
	return "", nil, 0, false, nil
}

// surgeRevisionAndSpec returns the CR and its embedded PodSpec that
// the surge Instance should run at — the source's RunningRevision.
// Returns (nil, nil, nil) when no RunningRevision is recorded yet OR
// the named CR has been deleted (retention race, operator cleanup);
// the caller bails and retries on a later reconcile.
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

// patchInstanceStatusMigrating moves the source InstanceStatus to
// migrationRole tells stampMigrationStatus which side of the pair it's
// writing. Source -> Phase=Migrating, sibling pointer is the surge;
// surge -> Phase=Creating, sibling pointer is the source and
// Incarnation is initialized.
type migrationRole int

const (
	migrationRoleSource migrationRole = iota
	migrationRoleSurge
)

// stampMigrationStatus is the shared write for both sides of a
// migration pair. Idempotent on (Phase, Operation.Type, RequestUUID).
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

// patchInstanceStatusMigrating moves the source InstanceStatus to
// Phase=Migrating. SurgeIndex on the source's Operation points
// FORWARD at the surge.
func patchInstanceStatusMigrating(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, idx, surgeIdx int32, uuid string, req *MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, p, component, idx, surgeIdx, migrationRoleSource, uuid, req, timeout)
}

// patchInstanceStatusMigrationSurge stamps the surge InstanceStatus at
// Phase=Creating. SurgeIndex on the surge's Operation points BACK at
// the source (the field is reused as a generic sibling pointer so an
// external observer can correlate the two entries by either side).
func patchInstanceStatusMigrationSurge(ctx context.Context, p ReconcileParams, component v1beta1.ComponentType, surgeIdx, sourceIdx int32, uuid string, req *MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, p, component, surgeIdx, sourceIdx, migrationRoleSurge, uuid, req, timeout)
}
