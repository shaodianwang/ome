package recorder

// ClassSummary aggregates time-to-Ready for one size class. Times are
// measured from the moment the generator created the InferenceService to
// the moment the recorder first observed the class's InferenceReplica
// report ReadyReplicas == Replicas (every Instance Ready).
type ClassSummary struct {
	// Count is the number of InferenceServices in this class that reached
	// Ready and were recorded.
	Count int32 `json:"count"`

	// totalTimeToReadyMs is the running sum used to compute the average;
	// excluded from the marshaled summary (the average is what the checker
	// gates on).
	totalTimeToReadyMs int64 `json:"-"`

	// AverageTimeToReadyMs is the mean time-to-Ready across the class. This
	// is the value the checker compares against the rangespec.
	AverageTimeToReadyMs int64 `json:"averageTimeToReadyMs"`

	// MaxTimeToReadyMs is the slowest single InferenceService in the class.
	// Informational; surfaces tail latency the average hides.
	MaxTimeToReadyMs int64 `json:"maxTimeToReadyMs"`

	// PodsPerISVC is the pod fan-out one InferenceService in this class
	// produced (Replicas Instances x leader/worker gang size). Carried in
	// the summary so a reader can relate a class's convergence latency to
	// the pod count it drives — the OMENative cost axis.
	PodsPerISVC int32 `json:"podsPerISVC"`
}

// StatusUpdateSummary reports the controller-runtime status-write outcome
// counters (ome_isvc_status_update_total) read from the prometheus
// registry after the run. Total is the sum across all result labels;
// Conflict is the 409-retry-exhausted subset — the OMENative status-churn
// hotspot the harness exists to watch. Broken out per controller (isvc,
// ir) so a regression localizes to the writer that caused it.
type StatusUpdateSummary struct {
	// Total is the sum of status-update attempts (all results) for this
	// controller.
	Total int64 `json:"total"`
	// Conflict is the count of status writes that exhausted their
	// RetryOnConflict budget on a 409. The ratio Conflict/Total is the
	// conflict rate the checker holds to a ceiling.
	Conflict int64 `json:"conflict"`
}

// ChurnSummary reports controller-runtime workqueue + reconcile counters
// for one controller, read from the prometheus registry after the run. A
// healthy run shows reconciles roughly proportional to the work done and a
// drained queue; a storm shows reconcile/add counts far above the object
// count with no convergence.
type ChurnSummary struct {
	// ReconcileTotal is controller_runtime_reconcile_total summed across
	// result labels for this controller.
	ReconcileTotal int64 `json:"reconcileTotal"`
	// WorkqueueAdds is workqueue_adds_total for this controller's queue.
	WorkqueueAdds int64 `json:"workqueueAdds"`
	// WorkqueueDepth is the instantaneous workqueue_depth sampled at
	// summary time. After convergence this should be ~0; a non-trivial
	// residual depth means the controller is still churning.
	WorkqueueDepth int64 `json:"workqueueDepth"`
}

// Summary is the run report written to <out>/summary.yaml and read by the
// checker.
type Summary struct {
	// Classes maps class name to its time-to-Ready aggregate.
	Classes map[string]*ClassSummary `json:"classes"`

	// TotalReady is the number of InferenceServices that reached Ready
	// (IR ReadyReplicas == Replicas) across all classes. The checker
	// compares this against the config's expected total to detect a run
	// that never converged.
	TotalReady int `json:"totalReady"`

	// ExpectedTotal is the number of InferenceServices the config asked
	// the generator to create. TotalReady < ExpectedTotal means the run
	// timed out before everything converged.
	ExpectedTotal int `json:"expectedTotal"`

	// ExpectedPods is the total pod count the config drives to Ready (sum
	// over classes of Count x Replicas x gang size). Surfaced so the
	// apiserver op counts and conflict rate below can be read against the
	// pod count they scale with — the distinguishing axis of the OMENative
	// path vs. the constant-cost ISVC path.
	ExpectedPods int `json:"expectedPods"`

	// DroppedEvents counts intake events (created or ready) the recorder
	// dropped because its bounded channel was full when the observer tried
	// to enqueue. A non-zero value means the recorder was saturated and the
	// numbers may understate reality — the checker treats any drop as a
	// failure so it can't silently corrupt a calibration baseline.
	DroppedEvents uint64 `json:"droppedEvents"`

	// PodCreateToReady aggregates per-pod create-to-Ready latency across
	// every harness pod the observer saw flip Ready (independent of the
	// per-ISVC class roll-up above). Best-effort: a pod whose create stamp
	// the observer missed is excluded. Informational — the checker does
	// not gate on it — but it isolates pod-lifecycle latency from the
	// control-plane reconcile latency the class roll-up measures.
	PodCreateToReady PodLatencySummary `json:"podCreateToReady"`

	// APIOps is the per-(verb, resource-kind) apiserver op count the
	// reconcilers issued through the opcount-wrapped client + reader,
	// rendered as "verb/Kind" -> count (opcount.Flat()). Reports reconcile
	// op-amplification — the pod-LIST fan-out and status-write churn the
	// OMENative path is sensitive to. The checker holds the grand total to
	// a (generous, uncalibrated) ceiling.
	APIOps map[string]int64 `json:"apiOps"`

	// APIOpsTotal is the grand total of every counted apiserver op
	// (sum of APIOps values). The checker's op-count ceiling gates on this.
	APIOpsTotal int64 `json:"apiOpsTotal"`

	// StatusUpdates reports the status-write success/conflict counters per
	// controller (isvc, ir), read from the controller-runtime prometheus
	// registry.
	StatusUpdates map[string]*StatusUpdateSummary `json:"statusUpdates"`

	// Churn reports per-controller workqueue + reconcile counters read from
	// the controller-runtime prometheus registry. Keyed by controller name
	// as controller-runtime labels it (e.g. "inferenceservice",
	// "inferencereplica").
	Churn map[string]*ChurnSummary `json:"churn"`

	// IdleReconcileRatePerSec is the steady-state reconcile rate measured
	// AFTER convergence: total controller_runtime_reconcile_total deltas
	// over a short sampling window, divided by the window seconds. This is
	// the storm guard — a converged OMENative fleet must settle to ~0
	// reconciles/sec; a non-trivial idle rate means a hot-loop requeue bug.
	IdleReconcileRatePerSec float64 `json:"idleReconcileRatePerSec"`
}

// PodLatencySummary aggregates pod-level create-to-Ready latency.
type PodLatencySummary struct {
	// Count is the number of pods that flipped Ready and had a recorded
	// create stamp.
	Count int32 `json:"count"`
	// totalMs is the running sum used to compute the average; excluded from
	// the marshaled summary.
	totalMs int64 `json:"-"`
	// AverageMs is the mean pod create-to-Ready latency.
	AverageMs int64 `json:"averageMs"`
	// MaxMs is the slowest single pod.
	MaxMs int64 `json:"maxMs"`
}
