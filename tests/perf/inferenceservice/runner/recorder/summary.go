package recorder

// ClassSummary aggregates time-to-Ready for one size class. Times are
// measured from the moment the generator created the InferenceService to
// the moment the recorder first observed its Ready condition flip True.
type ClassSummary struct {
	// Count is the number of InferenceServices in this class that reached
	// Ready and were recorded.
	Count int32 `json:"count"`

	// totalTimeToReadyMs is the running sum used to compute the average;
	// excluded from the marshaled summary (the average is what the
	// checker gates on).
	totalTimeToReadyMs int64 `json:"-"`

	// AverageTimeToReadyMs is the mean time-to-Ready across the class.
	// This is the value the checker compares against the rangespec.
	AverageTimeToReadyMs int64 `json:"averageTimeToReadyMs"`

	// MaxTimeToReadyMs is the slowest single InferenceService in the
	// class. Informational; surfaces tail latency the average hides.
	MaxTimeToReadyMs int64 `json:"maxTimeToReadyMs"`
}

// Summary is the run report written to <out>/summary.yaml and read by
// the checker.
type Summary struct {
	// Classes maps class name to its time-to-Ready aggregate.
	Classes map[string]*ClassSummary `json:"classes"`

	// TotalReady is the number of InferenceServices that reached Ready
	// across all classes. The checker compares this against the config's
	// expected total to detect a run that never converged.
	TotalReady int `json:"totalReady"`

	// ExpectedTotal is the number of InferenceServices the config asked
	// the generator to create. TotalReady < ExpectedTotal means the run
	// timed out before everything converged.
	ExpectedTotal int `json:"expectedTotal"`

	// DroppedEvents counts intake events (created or ready) the recorder
	// dropped because its bounded channel was full when the
	// executor/observer tried to enqueue. A non-zero value means the
	// recorder was saturated and the numbers may understate reality — the
	// checker treats any drop as a failure so it can't silently corrupt a
	// calibration baseline.
	DroppedEvents uint64 `json:"droppedEvents"`
}
