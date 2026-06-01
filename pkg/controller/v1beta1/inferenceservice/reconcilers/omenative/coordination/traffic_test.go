package coordination

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestBuildTrafficTargets_FillsPerRevisionServiceName(t *testing.T) {
	got := BuildTrafficTargets("llama", v1beta1.EngineComponent, []RevisionWeight{
		{RevisionHash: "newhash", Percent: 25, Tag: "latest", LatestRevision: true},
		{RevisionHash: "oldhash", Percent: 75, Tag: "prev"},
	})
	want := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "llama-engine-rev-newhash", Percent: 25, Tag: "latest", LatestRevision: true},
		{RevisionName: "llama-engine-rev-oldhash", Percent: 75, Tag: "prev"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("BuildTrafficTargets diff (-want +got):\n%s", diff)
	}
}

func TestBuildTrafficTargets_SkipsZeroOrEmpty(t *testing.T) {
	got := BuildTrafficTargets("llama", v1beta1.EngineComponent, []RevisionWeight{
		{RevisionHash: "", Percent: 25},
		{RevisionHash: "hash", Percent: 0},
		{RevisionHash: "hash2", Percent: 100},
	})
	if len(got) != 1 {
		t.Fatalf("filter: got %d entries want 1: %+v", len(got), got)
	}
	if got[0].RevisionName != "llama-engine-rev-hash2" {
		t.Errorf("kept entry: got %q", got[0].RevisionName)
	}
}

func TestSortedWeights_DescendingByPercent(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "a", Percent: 25},
		{RevisionHash: "b", Percent: 75},
		{RevisionHash: "c", Percent: 75},
	}
	out := SortedWeights(in)
	if out[0].Percent != 75 || out[1].Percent != 75 || out[2].Percent != 25 {
		t.Errorf("descending: got %+v", out)
	}
	if out[0].RevisionHash != "b" || out[1].RevisionHash != "c" {
		t.Errorf("tie-break by hash: got %+v", out)
	}
}

func TestNormalizeWeights_AbsorbsResidualOnMax(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "big", Percent: 70},
		{RevisionHash: "small", Percent: 29},
	}
	out := NormalizeWeights(in)
	if out[0].Percent != 71 {
		t.Errorf("biggest absorbs +1: got %d want 71", out[0].Percent)
	}
	if out[1].Percent != 29 {
		t.Errorf("smaller unchanged: got %d want 29", out[1].Percent)
	}
}

func TestNormalizeWeights_NoOpWhenSumIs100(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "a", Percent: 60},
		{RevisionHash: "b", Percent: 40},
	}
	out := NormalizeWeights(in)
	if diff := cmp.Diff(in, out); diff != "" {
		t.Errorf("100-sum should be unchanged (-want +got):\n%s", diff)
	}
}

func TestNormalizeWeights_ZeroSumLeavesAsIs(t *testing.T) {
	in := []RevisionWeight{
		{RevisionHash: "a", Percent: 0},
		{RevisionHash: "b", Percent: 0},
	}
	out := NormalizeWeights(in)
	if diff := cmp.Diff(in, out); diff != "" {
		t.Errorf("zero-sum: got diff (-want +got):\n%s", diff)
	}
}

func TestComputeWeightsFromPods_SingleRevisionIs100(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{"hash1": 3}, 3, "hash1")
	if len(got) != 1 {
		t.Fatalf("got %d entries want 1", len(got))
	}
	if got[0].Percent != 100 {
		t.Errorf("Percent: got %d want 100", got[0].Percent)
	}
	if !got[0].LatestRevision {
		t.Errorf("LatestRevision: should be true when hash matches latestHash")
	}
}

func TestComputeWeightsFromPods_MultiRevisionApportions(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{
		"oldhash": 3,
		"newhash": 1,
	}, 4, "newhash")
	// 3/4 = 75%, 1/4 = 25%. Sum = 100, no normalization needed.
	weights := map[string]int32{}
	latest := map[string]bool{}
	for _, w := range got {
		weights[w.RevisionHash] = w.Percent
		latest[w.RevisionHash] = w.LatestRevision
	}
	if weights["oldhash"] != 75 || weights["newhash"] != 25 {
		t.Errorf("apportion: got %+v want oldhash=75 newhash=25", weights)
	}
	if !latest["newhash"] || latest["oldhash"] {
		t.Errorf("LatestRevision: got %+v", latest)
	}
}

func TestComputeWeightsFromPods_ZeroTotalReturnsNil(t *testing.T) {
	got := ComputeWeightsFromPods(map[string]int32{}, 0, "")
	if got != nil {
		t.Errorf("zero total: got %+v want nil", got)
	}
}

func TestComputeWeightsFromPods_NormalizesRoundoff(t *testing.T) {
	// 3 revisions at 1 pod each, total 3: 1/3 = 33% each → sum 99.
	// NormalizeWeights bumps the largest entry by 1 to land on 100.
	got := ComputeWeightsFromPods(map[string]int32{
		"a": 1,
		"b": 1,
		"c": 1,
	}, 3, "")
	sum := int32(0)
	for _, w := range got {
		sum += w.Percent
	}
	if sum != 100 {
		t.Errorf("normalized sum: got %d want 100; got entries=%+v", sum, got)
	}
}

func TestTrafficDiffersMeaningfully_LenMismatch(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100}}
	if !TrafficDiffersMeaningfully(desired, nil) {
		t.Errorf("different length: should report diff")
	}
}

func TestTrafficDiffersMeaningfully_PercentMismatch(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 99}}
	if !TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("percent drift: should report diff")
	}
}

func TestTrafficDiffersMeaningfully_SetEqualityIgnoresOrder(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "a", Percent: 25},
		{RevisionName: "b", Percent: 75},
	}
	observed := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "b", Percent: 75},
		{RevisionName: "a", Percent: 25},
	}
	if TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("same content reordered: should not report diff")
	}
}

func TestTrafficDiffersMeaningfully_TagDriftIgnored(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, Tag: "latest"}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, Tag: ""}}
	if TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("tag-only drift: should not report diff")
	}
}

// TestTrafficDiffersMeaningfully_LatestRevisionFlip pins the regression
// from PD-disaggregated A-6 testing: when the engine's traffic[0] was
// initially written with LatestRevision=false (because the IR Status had
// not yet populated LifecycleStatus.UpdateRevision on the first
// reconcile that observed pods), subsequent reconciles computed the
// correct LatestRevision=true value but the no-op short-circuit treated
// the flag as cosmetic and suppressed the write — leaving the engine
// entry permanently stuck without the flag while decoder/router (which
// won the IR-Status race on that cluster) had it set correctly.
//
// The fix: a LatestRevision flip is meaningful drift (operator-visible
// and consumed by downstream HTTPRoute reconcilers) and MUST trigger a
// write.
func TestTrafficDiffersMeaningfully_LatestRevisionFlipReportsDiff(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: false}}
	if !TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("LatestRevision flip false->true: should report diff (otherwise the no-op short-circuit strands the stale flag)")
	}
	// Symmetric case: flipping true->false also matters (e.g., when the
	// rollout advances and the prior LatestRolledoutRevision is demoted).
	if !TrafficDiffersMeaningfully(observed, desired) {
		t.Errorf("LatestRevision flip true->false: should report diff")
	}
}

// TestTrafficDiffersMeaningfully_LatestRevisionSameNoDiff pins the
// no-op fast path: when LatestRevision matches across desired and
// observed (most reconciles), the no-op short-circuit still kicks in
// and the writer skips the apiserver round-trip.
func TestTrafficDiffersMeaningfully_LatestRevisionSameNoDiff(t *testing.T) {
	desired := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	observed := []v1beta1.ComponentTrafficTarget{{RevisionName: "a", Percent: 100, LatestRevision: true}}
	if TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("identical LatestRevision: should not report diff")
	}
}
