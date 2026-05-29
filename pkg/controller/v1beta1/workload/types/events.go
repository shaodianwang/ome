package types

// EventReason is the workload-internal event-reason identifier the ops
// state machines stamp on K8s Events emitted against the owner object.
// The distinct Go type prevents accidental coupling with reason strings
// owned by other packages (e.g. omenative/status retains its own copies
// for legacy callers); call sites pass an EventReason value, and the
// record.EventRecorder API absorbs the implicit string conversion.
//
// Values match the legacy omenative/status reason strings byte-for-byte
// so existing operator dashboards and `kubectl describe` output keep
// matching after the workload extraction. The byte-equality is enforced
// by these typed constants being the single source-of-truth across the
// workload subpackages (ops/, gang/, …) — drift can only occur inside
// this file, not across siblings.
type EventReason string

// String returns the underlying string form. Convenience for call
// sites that need to interpolate the reason in a free-form message or
// pass it to a logger.
func (r EventReason) String() string { return string(r) }

// Workload-emitted event reasons. Defined once in the workload root so
// every ops / gang subpackage shares the same identifiers; the legacy
// `omenative/status` package keeps its own copies (typed as plain
// strings) for callers still writing through the v1beta1-typed helpers.
const (
	// --- Create / scale-up lifecycle (workload/ops/create.go) -----------

	// EventReasonInstanceCreated fires the first time an Instance's
	// pods are materialized (first transition into Phase=Creating).
	EventReasonInstanceCreated EventReason = "InstanceCreated"

	// EventReasonInstanceReady fires when an Instance transitions from
	// any non-Ready phase to Phase=Ready (every pod runtime-ready and
	// ome.io/serving=True).
	EventReasonInstanceReady EventReason = "InstanceReady"

	// --- In-place update lifecycle (workload/ops/update.go) -------------

	// EventReasonInPlaceUpdateStarted fires the first time an Instance
	// transitions to Phase=Updating with Step=InPlace.
	EventReasonInPlaceUpdateStarted EventReason = "InPlaceUpdateStarted"

	// EventReasonInPlaceUpdateCompleted fires when an in-place rollout
	// converges to Phase=Ready with the new RunningRevision recorded.
	EventReasonInPlaceUpdateCompleted EventReason = "InPlaceUpdateCompleted"

	// EventReasonInPlaceUpdateNotPossible fires when InPlaceOnly is
	// configured but the diff exceeds container images.
	EventReasonInPlaceUpdateNotPossible EventReason = "InPlaceUpdateNotPossible"

	// --- Recreate / surge update lifecycle (workload/ops/update.go) -----

	// EventReasonRecreateUpdateStarted fires the first time an Instance
	// transitions to Phase=Updating with Step=Drain (recreate or surge).
	EventReasonRecreateUpdateStarted EventReason = "RecreateUpdateStarted"

	// EventReasonRecreateUpdateCompleted fires when a recreate/surge
	// rollout converges to Phase=Ready with the new RunningRevision.
	EventReasonRecreateUpdateCompleted EventReason = "RecreateUpdateCompleted"

	// --- Restart lifecycle (workload/ops/restart.go) --------------------

	// EventReasonRestartTriggered fires the first time an Instance
	// transitions into Phase=Restarting from a non-Restarting baseline.
	EventReasonRestartTriggered EventReason = "RestartTriggered"

	// EventReasonRestartCompleted fires when an Instance transitions
	// from Phase=Restarting back to Phase=Ready (Operation=nil).
	EventReasonRestartCompleted EventReason = "RestartCompleted"

	// --- Orphan-pod safety (workload/ops/restart.go, update.go) ---------

	// EventReasonFoundOrphan fires when Restart or recreate-Update finds
	// a pod under the OMENative selector but missing the
	// ome.io/instance-incarnation label. The reconciler refuses to
	// delete it; the operator must re-classify or remove the pod
	// manually. Shared between Restart and recreate-Update because both
	// surface the same orphan-pod condition.
	EventReasonFoundOrphan EventReason = "FoundOrphan"

	// --- Migration request lifecycle (workload/ops/migrate.go) ----------

	// EventReasonMigrationRequestAccepted fires when a migration request
	// passes validation and the source Instance is stamped Migrating.
	EventReasonMigrationRequestAccepted EventReason = "MigrationRequestAccepted"

	// EventReasonMigrationRequestRejected fires when a migration request
	// fails validation (capacity, node-affinity conflict, multi-pod
	// guard, etc.). A terminal Failed audit row is persisted.
	EventReasonMigrationRequestRejected EventReason = "MigrationRequestRejected"

	// EventReasonMigrationCompleted fires when the surge has been
	// promoted Ready and the source InstanceStatus has been removed.
	EventReasonMigrationCompleted EventReason = "MigrationCompleted"

	// EventReasonRateLimited fires when the cluster-wide migration
	// capacity / rate-limit gate rejects a request.
	EventReasonRateLimited EventReason = "RateLimited"

	// EventReasonMigrationFromNodeMismatch fires when the request's
	// FromNode does not match where the source's pods are observed to
	// be running.
	EventReasonMigrationFromNodeMismatch EventReason = "MigrationFromNodeMismatch"

	// EventReasonMigrationNodeAffinityConflict fires when the surge's
	// hard NodeAffinity would collapse to "hostname=FromNode AND
	// hostname!=FromNode" — rejected before rendering.
	EventReasonMigrationNodeAffinityConflict EventReason = "MigrationNodeAffinityConflict"

	// --- Gang-scheduling heuristic (workload/gang/gang.go) --------------

	// EventReasonMaybeNoGangScheduler is a soft Warning fired the first
	// time a multi-pod Instance's PodGroup is created under a pod
	// template whose `spec.schedulerName` is unset OR equals the
	// upstream default ("default-scheduler"). The PodGroup itself was
	// created successfully — but a stock kube-scheduler does NOT read
	// `scheduling.x-k8s.io/v1alpha1` PodGroup objects, so the gang
	// contract degrades to per-pod scheduling silently. Operators must
	// either install scheduler-plugins as a secondary scheduler and set
	// `runtime.spec.schedulerName=scheduler-plugins-scheduler`, OR
	// install it as a default-scheduler plugin (in which case the
	// warning is a false positive — the controller cannot detect that
	// arrangement from inside the cluster, so we err on the side of
	// surfacing the heuristic). De-duplicated per (owner, Component)
	// per controller process so reconciles do not spam the event log.
	EventReasonMaybeNoGangScheduler EventReason = "MaybeNoGangScheduler"
)
