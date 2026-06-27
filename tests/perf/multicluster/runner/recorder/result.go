package recorder

import (
	"os"
	"sort"

	"sigs.k8s.io/yaml"
)

// PassResult is the raw, unaggregated outcome of one pass (funnel enabled or
// disabled): the latency samples (in ms) the Recorder collected, plus the
// per-class and per-candidate-count breakdowns. The runner turns two
// PassResults (one per pass) into the final two-pass Summary via BuildSummary.
type PassResult struct {
	// ExpectedTotal / TotalPlaced bound convergence for this pass.
	ExpectedTotal int
	TotalPlaced   int

	// Placement is every source's create -> Placed latency (ms).
	Placement []int64
	// FirstFanOut is every source's create -> derived-on-a-candidate latency.
	FirstFanOut []int64
	// RaceToWin is every source's derived-created -> winner-admitted latency.
	RaceToWin []int64
	// Convergence is every source's winner-admitted -> source-reflects-it
	// latency — the headline funnel-vs-poll metric.
	Convergence []int64

	// ByClass / ByCandidates break Placement latency down by size class and by
	// candidate-cluster count.
	ByClass      map[string][]int64
	ByCandidates map[int][]int64
}

// statsOf computes the latency distribution of a sample set. An empty set
// yields a zero-valued LatencyStats (Count 0), which the checker treats as
// "this pass produced no samples".
func statsOf(samples []int64) LatencyStats {
	if len(samples) == 0 {
		return LatencyStats{}
	}
	sorted := make([]int64, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum int64
	for _, v := range sorted {
		sum += v
	}
	return LatencyStats{
		Count: len(sorted),
		P50:   percentile(sorted, 0.50),
		P95:   percentile(sorted, 0.95),
		P99:   percentile(sorted, 0.99),
		Max:   sorted[len(sorted)-1],
		Mean:  sum / int64(len(sorted)),
	}
}

// percentile returns the p-quantile (0..1) of an already-sorted slice using
// the nearest-rank method. Stable and dependency-free; exact percentile
// algorithm choice does not matter for a perf signal as long as it is
// consistent across runs.
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	// nearest-rank: ceil(p*N) - 1, clamped into range.
	rank := int(p*float64(len(sorted)) + 0.9999999)
	idx := rank - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// PassOps bundles one pass's op-count totals: the per-verb/kind remote map plus
// the remote and local totals. Kept as a struct so BuildSummary's signature does
// not balloon as more op breakdowns are added.
type PassOps struct {
	Remote      map[string]int64
	RemoteTotal int64
	LocalTotal  int64
}

// convergenceSummaryOf folds one pass's convergence + placement samples and its
// op-counts into a ConvergenceSummary.
func convergenceSummaryOf(funnelEnabled bool, p PassResult, ops PassOps) ConvergenceSummary {
	return ConvergenceSummary{
		FunnelEnabled:  funnelEnabled,
		Convergence:    statsOf(p.Convergence),
		Placement:      statsOf(p.Placement),
		RemoteOps:      ops.Remote,
		RemoteOpsTotal: ops.RemoteTotal,
		LocalOpsTotal:  ops.LocalTotal,
	}
}

// BuildSummary combines the funnel-ENABLED and funnel-DISABLED pass results
// into the final Summary. Per-class / per-candidate / fan-out / race-to-win
// aggregates are taken from the funnel-enabled pass (the production default),
// while the convergence delta is computed across both passes — the number
// that proves the funnel's value.
func BuildSummary(
	members int,
	dispatcherMode string,
	funnel PassResult, funnelOps PassOps,
	poll PassResult, pollOps PassOps,
	dropped uint64,
) *Summary {
	s := &Summary{
		Members:                members,
		DispatcherMode:         dispatcherMode,
		FanOutCreatesPerSource: createsPerSource(funnelOps.Remote, funnel.TotalPlaced),
		Classes:                map[string]*ClassSummary{},
		FirstFanOut:            statsOf(funnel.FirstFanOut),
		RaceToWin:              statsOf(funnel.RaceToWin),
		FunnelEnabled:          convergenceSummaryOf(true, funnel, funnelOps),
		FunnelDisabled:         convergenceSummaryOf(false, poll, pollOps),

		ExpectedTotalPerPass: funnel.ExpectedTotal,
		TotalPlacedFunnel:    funnel.TotalPlaced,
		TotalPlacedPoll:      poll.TotalPlaced,
		DroppedEvents:        dropped,
	}

	for class, samples := range funnel.ByClass {
		s.Classes[class] = &ClassSummary{Placement: statsOf(samples)}
	}

	// Candidate buckets in ascending candidate-count order for a stable,
	// diffable summary.
	cands := make([]int, 0, len(funnel.ByCandidates))
	for c := range funnel.ByCandidates {
		cands = append(cands, c)
	}
	sort.Ints(cands)
	for _, c := range cands {
		s.CandidateBuckets = append(s.CandidateBuckets, &CandidateBucketSummary{
			Candidates: c,
			Placement:  statsOf(funnel.ByCandidates[c]),
		})
	}

	// Headline delta: poll-only convergence minus funnel convergence. Positive
	// means the funnel is faster (the expected, valuable result).
	s.ConvergenceDeltaMeanMs = s.FunnelDisabled.Convergence.Mean - s.FunnelEnabled.Convergence.Mean
	s.ConvergenceDeltaP95Ms = s.FunnelDisabled.Convergence.P95 - s.FunnelEnabled.Convergence.P95

	return s
}

// derivedCreateOpKey is the opcount key for a derived-ISVC Create on a member
// (verb "create", kind "InferenceService"). The blast-radius proxy reads it out
// of the per-pass remote op map.
const derivedCreateOpKey = "create/InferenceService"

// createsPerSource returns the average number of derived-ISVC Creates per placed
// source — the dispatcher blast-radius proxy. Zero placed sources yields 0 (no
// division by zero); rounded to two decimals for a stable, readable summary.
func createsPerSource(remoteOps map[string]int64, placed int) float64 {
	if placed <= 0 {
		return 0
	}
	creates := remoteOps[derivedCreateOpKey]
	ratio := float64(creates) / float64(placed)
	// Round to 2 decimals so the summary is stable across runs that differ only
	// in floating-point noise.
	return float64(int64(ratio*100+0.5)) / 100
}

// WriteSummary marshals the summary to path as YAML.
func WriteSummary(path string, s *Summary) error {
	b, err := yaml.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
