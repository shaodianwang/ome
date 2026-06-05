package canary

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

func TestCanaryWeights(t *testing.T) {
	w := canaryWeights("new", "old", 10)
	if len(w) != 2 {
		t.Fatalf("want 2 weights, got %d", len(w))
	}
	// canary first (LatestRevision), then stable.
	if w[0].RevisionHash != "new" || w[0].Percent != 10 || !w[0].LatestRevision {
		t.Fatalf("canary weight wrong: %+v", w[0])
	}
	if w[1].RevisionHash != "old" || w[1].Percent != 90 {
		t.Fatalf("stable weight wrong: %+v", w[1])
	}
}

func TestApplyTraffic(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Name = "svc"
	isvc.Status.Canary = &v1beta1.CanaryStatus{}

	// 10% to canary → both per-revision Services present.
	applyTraffic(isvc, v1beta1.EngineComponent, "new", "old", 10)
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 2 {
		t.Fatalf("want 2 targets at 10%%, got %d (%+v)", len(tr), tr)
	}
	wantNew := coordination.PerRevisionServiceName("svc", v1beta1.EngineComponent, "new")
	wantOld := coordination.PerRevisionServiceName("svc", v1beta1.EngineComponent, "old")
	got := map[string]int32{}
	for _, x := range tr {
		got[x.RevisionName] = x.Percent
	}
	if got[wantNew] != 10 || got[wantOld] != 90 {
		t.Fatalf("weights wrong: %+v", got)
	}
	if isvc.Status.Canary.ObservedTrafficWeight != 10 {
		t.Fatalf("observed weight not recorded: %d", isvc.Status.Canary.ObservedTrafficWeight)
	}

	// 100% → only the canary Service (stable 0 is filtered out).
	applyTraffic(isvc, v1beta1.EngineComponent, "new", "old", 100)
	tr = isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 1 || tr[0].RevisionName != wantNew || tr[0].Percent != 100 {
		t.Fatalf("at 100%% want only canary 100, got %+v", tr)
	}

	// 0% → only the stable Service.
	applyTraffic(isvc, v1beta1.EngineComponent, "new", "old", 0)
	tr = isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(tr) != 1 || tr[0].RevisionName != wantOld || tr[0].Percent != 100 {
		t.Fatalf("at 0%% want only stable 100, got %+v", tr)
	}
}
