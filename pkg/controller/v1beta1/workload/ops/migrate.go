package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
)

// MigrateRequeueInterval is the wait between Migrate passes while a
// surge migration is in flight. Exported so the ISVC dispatcher (which
// still owns the per-Component requeue calculus) can reach the same
// pacing without redeclaring it.
const MigrateRequeueInterval = 5 * time.Second

// Migrate drives one surge migration toward completion. Triggered by an
// adapter-side annotation (or other source-of-truth carrier) that the
// caller has already parsed into (uuid, req). Multi-pass and crash-safe
// via the audit ConfigMap.
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
//
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types. The ISVC-OMENative dispatcher delegates
// via internalsource.BuildReconcileInput; the IR controller will call
// the same entry point with its own ReconcileInput.
//
// SINGLE-POD ONLY: Multi-pod migration is OEP-deferred — surge sizing
// must mirror the source's full Runner layout (1 leader + N workers),
// and the surge must satisfy the gang readiness gate before drain
// proceeds. The single-pod surge path the rest of this function
// implements would create a single "default" surge pod that can never
// satisfy a multi-pod gang, leaving the migration stuck until the
// deadline. We reject up-front with a Failed audit row so the requester
// observes the rejection within its SLA. When multi-pod migration lands
// (future OEP-0007 amendment), drop the guard and extend the surge path
// to emit the full Runner layout.
func Migrate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx int32, requestUUID string, req *audit.MigrationRequest) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Migrate: nil client")
	}
	if req == nil {
		return false, fmt.Errorf("Migrate: nil request (instance=%d)", sourceIdx)
	}

	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), input.OwnerObject)
	if err != nil {
		return false, fmt.Errorf("Migrate: load audit ledger: %w", err)
	}
	if ledger.HasCompletedOrFailedRequest(requestUUID) {
		return true, nil
	}

	// Multi-pod migration is OEP-deferred (see the SINGLE-POD ONLY
	// note on Migrate above). Reject up-front with a Failed audit row.
	if isMultiPodInstance(plan, sourceIdx) {
		return failMigration(ctx, deps, input, ledger, req, requestUUID,
			"multi-pod Instance migration not yet supported (single-pod surge cannot satisfy a multi-pod gang)")
	}

	// Recover surge index from source status (post-stamp) or ledger
	// (post-Started, pre-stamp crash); allocate on a fresh request.
	source := findInstanceStatus(input.ObservedState.InstanceStatuses, sourceIdx)
	inFlight := ledger.InFlightEntry(requestUUID)
	var surgeIdx int32
	switch {
	case source != nil && source.Operation != nil && source.Operation.Type == workload.InstanceOperationMigrate && source.Operation.SurgeIndex != nil:
		surgeIdx = *source.Operation.SurgeIndex
	case inFlight != nil:
		surgeIdx = inFlight.SurgeInstance
	default:
		// Fresh request: refuse to stamp Migrating unless source is a
		// steady Ready with a recorded revision. Without these guards a
		// mid-Create stamp deadlocks Create and a mid-Update / Restart
		// stamp silently steamrolls the in-flight op.
		if source == nil {
			return failMigration(ctx, deps, input, ledger, req, requestUUID, "source InstanceStatus missing")
		}
		if source.Phase != workload.InstancePhaseReady || source.Operation != nil || source.RunningRevision == "" {
			return false, nil
		}

		// Validate FromNode against where source pods actually run. The
		// requester's view may be stale; if source has since moved, the
		// surge's NotIn[FromNode] could land on the SAME node as the
		// post-move source — silently no-op'ing the migration.
		switch mismatch, defer_, err := validateFromNode(ctx, deps, input, plan, sourceIdx, req.FromNode); {
		case err != nil:
			return false, fmt.Errorf("Migrate: validate from-node (instance=%d): %w", sourceIdx, err)
		case defer_:
			return false, nil
		case mismatch != "":
			recordWarning(deps.Recorder, eventTarget(input), EventReasonMigrationFromNodeMismatch,
				"OMENative migration uuid=%s rejected: %s", requestUUID, mismatch)
			return failMigration(ctx, deps, input, ledger, req, requestUUID, mismatch)
		}

		// Cluster-wide capacity / rate-limit gate (in-flight + last-hour
		// counts across the audit ledger).
		if ok, reason := audit.ValidateCapacity(ledger, time.Now()); !ok {
			recordWarning(deps.Recorder, eventTarget(input), EventReasonRateLimited,
				"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
			return failMigration(ctx, deps, input, ledger, req, requestUUID, reason)
		}

		// Resolve the source's RunningRevision and check the overlay BEFORE
		// stamping. A post-stamp rejection leaves source Phase=Migrating
		// for ~instanceReadyTimeout until the timeout cleanup fires —
		// operators see a wedged status and the requester can't observe
		// the rejection within its SLA. (nil, nil) means the CR was GC'd
		// or RunningRevision isn't set yet — defer.
		_, surgeRevSpec, err := surgeRevisionAndSpec(ctx, deps, input, sourceIdx)
		if err != nil {
			return false, fmt.Errorf("Migrate: resolve surge revision: %w", err)
		}
		if surgeRevSpec == nil {
			return false, nil
		}
		if WouldOverlayConflictWithNodeAffinity(surgeRevSpec, &workload.MigrationOverlay{
			FromNode:        req.FromNode,
			HintTargetNodes: req.HintTargetNodes,
		}) {
			reason := fmt.Sprintf("source PodSpec NodeAffinity requires kubernetes.io/hostname=%s; overlay's NotIn[%s] would make scheduling impossible", req.FromNode, req.FromNode)
			recordWarning(deps.Recorder, eventTarget(input), EventReasonMigrationNodeAffinityConflict,
				"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
			return failMigration(ctx, deps, input, ledger, req, requestUUID, reason)
		}

		surgeIdx = workload.AllocateSurgeIndex(input.ObservedState.InstanceStatuses)
		if err := patchInstanceStatusMigrating(ctx, input, sourceIdx, surgeIdx, requestUUID, req, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("Migrate: stamp source status (instance=%d): %w", sourceIdx, err)
		}
		if err := patchInstanceStatusMigrationSurge(ctx, input, surgeIdx, sourceIdx, requestUUID, req, plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("Migrate: stamp surge status (instance=%d): %w", surgeIdx, err)
		}
		ledger.UpsertEntry(audit.NewStartedEntry(req, requestUUID, surgeIdx))
		if err := audit.PersistLedgerForOwner(ctx, deps.Client, input.OwnerObject, input.OwnerGVK, ledger); err != nil {
			return false, fmt.Errorf("Migrate: persist Started ledger: %w", err)
		}
		// Clear the annotation after stamping Started so a re-delivery
		// within the requester's retry window dedups via the ledger.
		if err := clearMigrationRequest(ctx, input, requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: clear request annotation: %w", err)
		}
		recordNormal(deps.Recorder, eventTarget(input), EventReasonMigrationRequestAccepted,
			"OMENative %s migration accepted (uuid=%s, source-node=%s, surge-index=%d)",
			instanceKey(input.Key.Component, sourceIdx), requestUUID, req.FromNode, surgeIdx)
	}

	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: list source pods: %w", err)
	}
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: list surge pods: %w", err)
	}

	// Migration moves placement, not template — surge runs at the source's
	// RunningRevision.
	surgeRev, surgeRevSpec, err := surgeRevisionAndSpec(ctx, deps, input, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: resolve surge revision: %w", err)
	}
	if surgeRev == nil || surgeRevSpec == nil {
		// Source not yet promoted with a RunningRevision — retry.
		return false, nil
	}

	// Migration overlay tells Render to stamp anti-affinity vs FromNode.
	surgeInst := workload.InstancePlan{
		Index:       surgeIdx,
		Incarnation: 1,
		Runners:     []workload.RunnerPlan{{Name: "default", Size: 1}},
		MigrationOverlay: &workload.MigrationOverlay{
			FromNode:        req.FromNode,
			HintTargetNodes: req.HintTargetNodes,
		},
	}

	// Refuse a surge whose hard NodeAffinity would collapse to
	// "hostname=FromNode AND hostname!=FromNode". Without this gate the
	// surge sits Pending until timeout — silent rejection vs. early fail.
	if WouldOverlayConflictWithNodeAffinity(surgeRevSpec, surgeInst.MigrationOverlay) {
		reason := fmt.Sprintf("source PodSpec NodeAffinity requires kubernetes.io/hostname=%s; overlay's NotIn[%s] would make scheduling impossible", req.FromNode, req.FromNode)
		recordWarning(deps.Recorder, eventTarget(input), EventReasonMigrationNodeAffinityConflict,
			"OMENative migration uuid=%s rejected: %s", requestUUID, reason)
		return failMigration(ctx, deps, input, ledger, req, requestUUID, reason)
	}

	desired := expectedPodNamesForInstance(input, plan, surgeInst)
	existingByName := query.IndexPodsByName(surgePods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, surgeIdx) {
			return false, nil
		}
		// Render against the surge revision's PodSpec, not input.DesiredSpec.PodSpec.
		// Use a shallow ReconcileInput copy with the surge revision's PodSpec so
		// renderHook + per-Component metadata stay intact. Expectations bucket on
		// surgeIdx (not sourceIdx) so surge creates track separately from
		// source-side deletes.
		surgeInput := input
		surgeInput.DesiredSpec.PodSpec = surgeRevSpec
		if _, cerr := createMissingPods(ctx, deps, surgeInput, plan, surgeInst, surgeIdx, missing, revisionHashFromTarget(surgeRev)); cerr != nil {
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
		if err := podreadiness.MarkPodServing(ctx, deps.Client, pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("Migrate: serving=True on surge pod %s: %w", pod.Name, err)
		}
	}

	// Wait for surge endpoints in rotation before draining source — without
	// this gate the swap can hit a zero-routable-endpoint window. Live
	// reader so a cold informer doesn't falsely report zero slices.
	for _, pod := range surgePods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		inRotation, rerr := drain.IsPodInRotation(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
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
	surgeStatus := findInstanceStatus(input.ObservedState.InstanceStatuses, surgeIdx)
	if surgeStatus == nil || surgeStatus.PodCount == 0 ||
		surgeStatus.AvailablePodCount < surgeStatus.PodCount {
		return false, nil
	}

	for _, pod := range sourcePods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		// Migrate-source-drain key; cleared by pod deletion at the end.
		if err := podreadiness.MarkPodNotServing(ctx, deps.Client, pod, podreadiness.WriterMigrateSourceDrain, requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: serving=False on source pod %s: %w", pod.Name, err)
		}
	}
	for _, pod := range sourcePods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		drained, derr := drain.IsPodDrained(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
		if derr != nil {
			return false, fmt.Errorf("Migrate: check drain on source pod %s: %w", pod.Name, derr)
		}
		if !drained {
			return false, nil
		}
	}

	// Live read before deletes — a stale-empty cache view would skip the
	// delete loop and orphan source pods at source-status removal.
	sourcePods, err = query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: live-list source pods: %w", err)
	}

	if len(sourcePods) > 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, sourceIdx) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, sourceIdx, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, sourceIdx)
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
	// Creating; the next reconcile dedups via HasCompletedOrFailedRequest
	// and never finishes cleanup. Both operations below are idempotent so
	// re-delivery between them is safe.
	if err := patchInstanceStatusReadyOnRevision(ctx, input, surgeIdx, surgeRev.Name); err != nil {
		return false, fmt.Errorf("Migrate: promote surge to Ready: %w", err)
	}
	if _, rerr := input.RemoveInstance(ctx, sourceIdx); rerr != nil {
		return false, fmt.Errorf("Migrate: remove source InstanceStatus: %w", rerr)
	}
	ledger.UpsertEntry(audit.NewTerminalEntry(*ledger.InFlightEntryOrSeed(requestUUID, req, surgeIdx), audit.PhaseCompleted, "migrated"))
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, input.OwnerObject, input.OwnerGVK, ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Completed ledger: %w", err)
	}
	// Re-clear in case a re-delivery slipped in between the accept-time
	// clear and now. Idempotent.
	if err := clearMigrationRequest(ctx, input, requestUUID); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on completion: %w", err)
	}
	recordNormal(deps.Recorder, eventTarget(input), EventReasonMigrationCompleted,
		"OMENative migration uuid=%s complete: %s -> instance=%d (revision=%s)",
		requestUUID, instanceKey(input.Key.Component, sourceIdx), surgeIdx, surgeRev.Name)
	return true, nil
}

// validateFromNode live-reads source pods, returning three states:
//
//   - rejectionReason="" defer_=false err=nil — valid, proceed
//   - defer_=true — transient (pod not scheduled yet); caller requeues
//   - rejectionReason != "" — permanent mismatch; caller fails
//
// Source pods on multiple nodes is a rejection — single-pod migration
// can't legitimately span nodes.
func validateFromNode(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx int32, fromNode string) (string, bool, error) {
	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
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
// Emits a Warning event so operators see the rejection in `kubectl
// describe`. Clears the trigger annotation (via input.ClearMigrationRequest)
// so the requester does not re-deliver against the same UUID indefinitely.
func failMigration(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, ledger *audit.Ledger, req *audit.MigrationRequest, uuid, reason string) (bool, error) {
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
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, input.OwnerObject, input.OwnerGVK, ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Failed ledger (%s): %w", reason, err)
	}
	// Clear on rejection too — the requester retries against the same
	// UUID forever if the annotation lingers.
	if err := clearMigrationRequest(ctx, input, uuid); err != nil {
		return false, fmt.Errorf("Migrate: clear request annotation on rejection: %w", err)
	}
	recordWarning(deps.Recorder, eventTarget(input), EventReasonMigrationRequestRejected,
		"OMENative migration uuid=%s rejected: %s", uuid, reason)
	return true, nil
}

// isMultiPodInstance reports whether the Instance at sourceIdx in plan
// is multi-pod (TotalPodsForInstance > 1). Used by Migrate to reject a
// multi-pod migration with a Failed audit row up-front; the rest of the
// Migrate state machine assumes single-pod surge semantics.
//
// Returns false when no Instance with that index exists in the plan —
// the rest of Migrate handles missing-source via failMigration("source
// InstanceStatus missing"). Either way, this guard short-circuits ONLY
// when the Component is actively multi-pod.
func isMultiPodInstance(plan workload.ComponentPlan, sourceIdx int32) bool {
	for _, inst := range plan.Instances {
		if inst.Index == sourceIdx {
			return inst.TotalPods() > 1
		}
	}
	return false
}

// surgeRevisionAndSpec returns the source's RunningRevision CR + PodSpec.
// (nil, nil, nil) means no recorded RunningRevision yet OR the CR was
// deleted — caller retries.
//
// Reads the source's running revision from input.ObservedState; fetches
// the CR via deps.Reader() (live read) so a stale cache doesn't return
// a deleted CR as still-present.
func surgeRevisionAndSpec(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, sourceIdx int32) (*appsv1.ControllerRevision, *corev1.PodSpec, error) {
	source := findInstanceStatus(input.ObservedState.InstanceStatuses, sourceIdx)
	if source == nil || source.RunningRevision == "" {
		return nil, nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	if err := deps.Reader().Get(ctx, client.ObjectKey{Namespace: input.Key.Namespace, Name: source.RunningRevision}, cr); err != nil {
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
func stampMigrationStatus(ctx context.Context, input workload.ReconcileInput, idx, siblingIdx int32, role migrationRole, uuid string, req *audit.MigrationRequest, timeout time.Duration) error {
	now := metav1.NewTime(time.Now())
	siblingPtr := siblingIdx
	wantPhase := workload.InstancePhaseMigrating
	idSuffix := ""
	if role == migrationRoleSurge {
		wantPhase = workload.InstancePhaseCreating
		idSuffix = "-surge"
	}
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == wantPhase &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationMigrate &&
			s.Operation.RequestUUID == uuid {
			return false
		}
		if role == migrationRoleSurge && s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Phase = wantPhase
		s.Operation = &workload.InstanceOperation{
			ID:              fmt.Sprintf("migrate-%s%s-%d", uuid, idSuffix, now.Unix()),
			Type:            workload.InstanceOperationMigrate,
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
func patchInstanceStatusMigrating(ctx context.Context, input workload.ReconcileInput, idx, surgeIdx int32, uuid string, req *audit.MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, input, idx, surgeIdx, migrationRoleSource, uuid, req, timeout)
}

// patchInstanceStatusMigrationSurge stamps surge Phase=Creating; SurgeIndex
// points back at source (field reused as a sibling pointer so observers
// can correlate the pair from either side).
func patchInstanceStatusMigrationSurge(ctx context.Context, input workload.ReconcileInput, surgeIdx, sourceIdx int32, uuid string, req *audit.MigrationRequest, timeout time.Duration) error {
	return stampMigrationStatus(ctx, input, surgeIdx, sourceIdx, migrationRoleSurge, uuid, req, timeout)
}

// drainServiceForPod returns the per-revision *routed* Service name to
// gate drain against for pod. Read from the pod's ome.io/revision-hash
// label (stamped by Render). Empty string when the label is missing —
// caller treats that as "no routed Service to check" and proceeds
// straight to delete.
//
// We deliberately do NOT use the Component's headless Service here.
// That Service sets PublishNotReadyAddresses=true so peer-discovery DNS
// resolves before pods are Ready; kube-proxy then publishes the
// endpoint with Conditions.Ready=true regardless of the pod's actual
// Ready state, which would make drain.IsPodDrained wait forever. The
// per-revision routed Service (created by the coordination layer with
// PublishNotReadyAddresses=false) reflects the controller-owned
// ome.io/serving gate correctly.
//
// plan.Component type flows from the caller so migrate.go can address
// per-revision Services without importing v1beta1 itself.
func drainServiceForPod(input workload.ReconcileInput, plan workload.ComponentPlan, pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" {
		return ""
	}
	return query.PerRevisionServiceName(input.Key.OwnerName, workload.ComponentType(plan.Component), hash)
}

// clearMigrationRequest forwards to input.ClearMigrationRequest.
// input.ClearMigrationRequest is required by contract — callers that
// don't wire a removal flow set an explicit no-op closure (see
// workload.ReconcileInput docs). Empty uuid is a no-op: nothing to
// clear.
func clearMigrationRequest(ctx context.Context, input workload.ReconcileInput, uuid string) error {
	if uuid == "" {
		return nil
	}
	return input.ClearMigrationRequest(ctx, uuid)
}
