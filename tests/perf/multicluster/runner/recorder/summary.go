package recorder

// LatencyStats is a latency distribution in milliseconds. Percentiles let
// the checker gate on the tail (p95/p99) rather than only the mean, which
// hides the slow placements that matter most in a fan-out race.
type LatencyStats struct {
	// Count is the number of samples folded into this distribution.
	Count int `json:"count"`
	// P50 / P95 / P99 are the median and tail latencies in milliseconds.
	P50 int64 `json:"p50Ms"`
	P95 int64 `json:"p95Ms"`
	P99 int64 `json:"p99Ms"`
	// Max is the slowest single sample in milliseconds.
	Max int64 `json:"maxMs"`
	// Mean is the arithmetic mean in milliseconds.
	Mean int64 `json:"meanMs"`
}

// ClassSummary aggregates placement latency for one size class. Placement
// latency is measured from the moment the generator created the source
// InferenceService to the moment the recorder first observed its
// status.placement.Phase==Placed.
type ClassSummary struct {
	// Placement is the create -> Placed latency distribution for this class.
	Placement LatencyStats `json:"placement"`
}

// CandidateBucketSummary aggregates placement latency by the number of
// candidate clusters a source fanned out to. Fan-out cost scales with the
// candidate count (more Creates, more findWinner Gets, more loser Deletes),
// so bucketing by it surfaces how placement latency grows with fleet width.
type CandidateBucketSummary struct {
	// Candidates is the number of candidate clusters for the sources in
	// this bucket.
	Candidates int `json:"candidates"`
	// Placement is the create -> Placed latency distribution for the
	// sources that fanned out to exactly Candidates clusters.
	Placement LatencyStats `json:"placement"`
}

// ConvergenceSummary is the headline funnel-vs-poll comparison for ONE
// pass (funnel enabled or disabled). Status-convergence latency is measured
// from the moment a member admits the winner's derived to the moment the
// source's status.placement first reflects the win — i.e. how long the
// cross-cluster status takes to flow back to the control plane.
type ConvergenceSummary struct {
	// FunnelEnabled records which pass this is: true for the event-driven
	// funnel pass, false for the poll-only pass.
	FunnelEnabled bool `json:"funnelEnabled"`
	// Convergence is the member-admits-winner -> source-reflects-it latency
	// distribution for this pass.
	Convergence LatencyStats `json:"convergence"`
	// Placement is the create -> Placed latency distribution for this pass,
	// kept alongside convergence so the two passes can be compared on total
	// placement latency too (the poll-only pass is slower end-to-end because
	// the final status flow-back waits on the poll).
	Placement LatencyStats `json:"placement"`
	// RemoteOps is the per-verb/kind remote-client op count accumulated over
	// this pass (fan-out Creates/Updates, findWinner Gets, loser Deletes,
	// status reads on the MEMBER apiservers). Keyed "verb/Kind" so a
	// diff/baseline is stable.
	RemoteOps map[string]int64 `json:"remoteOps"`
	// RemoteOpsTotal is the sum of every remote op in this pass — the single
	// number that captures fan-out op-amplification across the fleet.
	RemoteOpsTotal int64 `json:"remoteOpsTotal"`
	// LocalOpsTotal is the sum of the placer's CONTROL-PLANE-side ops this pass
	// (source ISVC reads, status writes, WorkloadCluster lists). Kept distinct
	// from RemoteOpsTotal so the cross-cluster fan-out cost is not conflated with
	// local bookkeeping; the funnel reduces local status re-reads, so a drop here
	// between the poll and funnel passes is part of the funnel's win.
	LocalOpsTotal int64 `json:"localOpsTotal"`
}

// Summary is the run report written to <out>/summary.yaml and read by the
// checker. It carries BOTH passes (funnel on and off) plus the headline
// convergence delta between them — the number that proves the funnel's
// value.
type Summary struct {
	// Members is the number of member clusters the run stood up.
	Members int `json:"members"`
	// DispatcherMode is the fan-out breadth policy the run used (AllAtOnce
	// or Incremental). Recorded so a summary is self-describing when two
	// runs are compared.
	DispatcherMode string `json:"dispatcherMode"`

	// FanOutCreatesPerSource is the average number of derived-ISVC Creates the
	// placer issued per source on the MEMBER apiservers (funnel pass): the
	// remote create count divided by the sources placed. It is the blast-radius
	// proxy for the dispatcher tradeoff — AllAtOnce clones onto every candidate
	// (≈ members), while Incremental clones onto only the batches that ran before
	// a winner emerged (< members). Compare it against placement latency across
	// an AllAtOnce vs an Incremental run to see the tradeoff: Incremental trades
	// (usually) higher placement latency for a smaller simultaneous blast radius.
	FanOutCreatesPerSource float64 `json:"fanOutCreatesPerSource"`

	// Classes maps class name to its placement-latency aggregate. Folded
	// from the funnel-ENABLED pass (the production default), so the per-class
	// thresholds the checker gates on reflect the shipped configuration.
	Classes map[string]*ClassSummary `json:"classes"`

	// CandidateBuckets aggregates placement latency by candidate-cluster
	// count, from the funnel-ENABLED pass.
	CandidateBuckets []*CandidateBucketSummary `json:"candidateBuckets"`

	// FirstFanOut is the create -> derived-exists-on-a-candidate latency
	// distribution (time-to-first-fan-out), from the funnel-ENABLED pass.
	FirstFanOut LatencyStats `json:"firstFanOut"`
	// RaceToWin is the derived-created -> winner-admitted latency
	// distribution, from the funnel-ENABLED pass. Reflects the simulated
	// member admit delays plus the placer's findWinner cadence.
	RaceToWin LatencyStats `json:"raceToWin"`

	// FunnelEnabled / FunnelDisabled are the two convergence passes.
	FunnelEnabled  ConvergenceSummary `json:"funnelEnabled"`
	FunnelDisabled ConvergenceSummary `json:"funnelDisabled"`

	// ConvergenceDeltaMeanMs / ConvergenceDeltaP95Ms are the headline
	// numbers: (poll-only convergence) - (funnel convergence). A large
	// positive value is the funnel's win — event-driven convergence is that
	// much faster than the poll backstop.
	ConvergenceDeltaMeanMs int64 `json:"convergenceDeltaMeanMs"`
	ConvergenceDeltaP95Ms  int64 `json:"convergenceDeltaP95Ms"`

	// ExpectedTotalPerPass is the number of source InferenceServices each
	// pass asked the generator to create.
	ExpectedTotalPerPass int `json:"expectedTotalPerPass"`
	// TotalPlacedFunnel / TotalPlacedPoll are the number of sources that
	// reached Placed in each pass. Below ExpectedTotalPerPass means that
	// pass timed out before converging.
	TotalPlacedFunnel int `json:"totalPlacedFunnel"`
	TotalPlacedPoll   int `json:"totalPlacedPoll"`

	// DroppedEvents counts intake events the recorder dropped because its
	// bounded channel was full. A non-zero value means the recorder was
	// saturated and the numbers may understate reality — the checker treats
	// any drop as a failure so it can't silently corrupt a baseline.
	DroppedEvents uint64 `json:"droppedEvents"`
}
