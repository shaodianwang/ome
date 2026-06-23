package types

// EventReason is the workload-internal event-reason identifier the ops
// state machines stamp on K8s Events. Values match the legacy
// omenative/status reason strings byte-for-byte so existing operator
// dashboards and `kubectl describe` output keep matching.
type EventReason string

func (r EventReason) String() string { return string(r) }

const (
	// Create / scale-up (workload/ops/create.go).
	EventReasonInstanceCreated EventReason = "InstanceCreated"
	EventReasonInstanceReady   EventReason = "InstanceReady"

	// In-place update (workload/ops/update.go).
	EventReasonInPlaceUpdateStarted     EventReason = "InPlaceUpdateStarted"
	EventReasonInPlaceUpdateCompleted   EventReason = "InPlaceUpdateCompleted"
	EventReasonInPlaceUpdateNotPossible EventReason = "InPlaceUpdateNotPossible"

	// Recreate / surge update (workload/ops/update.go).
	EventReasonRecreateUpdateStarted   EventReason = "RecreateUpdateStarted"
	EventReasonRecreateUpdateCompleted EventReason = "RecreateUpdateCompleted"

	// Restart (workload/ops/restart.go).
	EventReasonRestartTriggered EventReason = "RestartTriggered"
	EventReasonRestartCompleted EventReason = "RestartCompleted"

	// EventReasonFoundOrphan fires when Restart or recreate-Update
	// finds a pod under the OMENative selector but missing the
	// ome.io/instance-incarnation label. The reconciler refuses to
	// delete it; the operator must re-classify or remove the pod
	// manually.
	EventReasonFoundOrphan EventReason = "FoundOrphan"

	// Migration (workload/ops/migrate.go).
	EventReasonMigrationRequestAccepted      EventReason = "MigrationRequestAccepted"
	EventReasonMigrationRequestRejected      EventReason = "MigrationRequestRejected"
	EventReasonMigrationCompleted            EventReason = "MigrationCompleted"
	EventReasonRateLimited                   EventReason = "RateLimited"
	EventReasonMigrationFromNodeMismatch     EventReason = "MigrationFromNodeMismatch"
	EventReasonMigrationNodeAffinityConflict EventReason = "MigrationNodeAffinityConflict"

	// EventReasonMaybeNoGangScheduler is a soft Warning fired the first
	// time a multi-pod Instance's PodGroup is created under a pod
	// template whose `spec.schedulerName` is unset or equals the
	// upstream default ("default-scheduler"). A stock kube-scheduler
	// does NOT read scheduling.x-k8s.io/v1alpha1 PodGroup objects, so
	// the gang contract degrades to per-pod scheduling silently.
	// Operators install scheduler-plugins as a secondary scheduler
	// (`scheduler-plugins-scheduler`) or as a default-scheduler plugin
	// (in which case the warning is a false positive the controller
	// can't detect from inside the cluster). Dedup'd per (owner,
	// Component) per process.
	EventReasonMaybeNoGangScheduler EventReason = "MaybeNoGangScheduler"

	// EventReasonGangSplitRisk is a soft Warning fired the first time a
	// multi-node gang WORKER pod is created with no co-location
	// podAffinity at all — neither an OME-injected topologyKey term nor a
	// user-declared one. Such a gang may schedule across separate
	// network / NVLink / TPU topology domains, which breaks the
	// tightly-coupled collectives a multi-node runtime needs (NCCL/RCCL/
	// NIXL all-reduce, multi-host TPU sessions). The operator sets
	// engine.topologyKey / decoder.topologyKey (e.g. a NVLink/RDMA domain
	// label, or the GKE TPU topology label) or declares a worker
	// podAffinity. Advisory only — never blocks the create. Dedup'd per
	// (owner, Component) per process.
	EventReasonGangSplitRisk EventReason = "GangSplitRisk"
)
