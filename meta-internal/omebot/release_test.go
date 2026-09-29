//go:build meta_internal

package omebot

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func kargoObj(kind, name, created string, body map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "kargo.akuity.io/v1alpha1",
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": "kargo-proj-a", "creationTimestamp": created},
	}
	for k, v := range body {
		obj[k] = v
	}
	return &unstructured.Unstructured{Object: obj}
}

func chart(name, version string) map[string]any {
	return map[string]any{"repoURL": "oci://registry.example.com/charts", "name": name, "version": version}
}

func fakeRelease(t *testing.T) *Release {
	t.Helper()
	objs := []runtime.Object{
		kargoObj("Stage", "stage-rc", "2026-09-01T00:00:00Z", map[string]any{"status": map[string]any{
			"freightHistory": []any{map[string]any{"items": map[string]any{
				"Warehouse/control-plane": map[string]any{"name": "f2", "charts": []any{chart("ome-release", "1.0.0-2")}},
			}}},
			"health":        map[string]any{"status": "Healthy"},
			"lastPromotion": map[string]any{"name": "stage-rc.p2", "status": map[string]any{"phase": "Succeeded"}, "finishedAt": "2026-09-27T10:00:00Z"},
		}}),
		kargoObj("Stage", "stage-prod", "2026-09-01T00:00:00Z", map[string]any{"status": map[string]any{}}),
		kargoObj("Freight", "f1", "2026-09-25T00:00:00Z", map[string]any{"alias": "old-otter", "charts": []any{chart("ome-release", "1.0.0-1")}}),
		kargoObj("Freight", "f2", "2026-09-26T00:00:00Z", map[string]any{"alias": "busy-bee", "charts": []any{chart("ome-release", "1.0.0-2")},
			"status": map[string]any{"verifiedIn": map[string]any{"stage-rc": map[string]any{}}}}),
		kargoObj("Freight", "f3", "2026-09-27T00:00:00Z", map[string]any{"alias": "new-newt", "charts": []any{chart("ome-release", "1.0.0-3")}}),
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		stagesGVR:  "StageList",
		freightGVR: "FreightList",
	}, objs...)
	return &Release{Client: client, Namespace: "kargo-proj-a", Recent: 2}
}

func TestReleaseReport(t *testing.T) {
	rel := fakeRelease(t)
	rep, err := rel.Report(context.Background(), []ClusterStatus{
		{Name: "member-a", KargoStage: "stage-rc", Reachable: true, OMEInstalled: true, OMEVersion: "1.0.0-2"},
		{Name: "hub-a", Error: "dial tcp: timeout"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Stages) != 2 || rep.Stages[0].Name != "stage-prod" || rep.Stages[1].Name != "stage-rc" {
		t.Fatalf("stages not sorted by name: %+v", rep.Stages)
	}
	rc := rep.Stages[1]
	if rc.Freight != "f2" || rc.Charts != "ome-release@1.0.0-2" || rc.Health != "Healthy" || rc.PromotionPhase != "Succeeded" || rc.FinishedAt != "2026-09-27T10:00:00Z" {
		t.Fatalf("stage-rc row: %+v", rc)
	}
	if p := rep.Stages[0]; p.Freight != "-" || p.Health != "-" || p.PromotionPhase != "-" {
		t.Fatalf("empty stage should render dashes: %+v", p)
	}
	if len(rep.Freight) != 2 || rep.Freight[0].Name != "f3" || rep.Freight[1].Name != "f2" {
		t.Fatalf("freight not newest-first capped at 2: %+v", rep.Freight)
	}
	if strings.Join(rep.Freight[1].VerifiedIn, ",") != "stage-rc" || rep.Freight[1].Alias != "busy-bee" {
		t.Fatalf("freight row: %+v", rep.Freight[1])
	}
	text := RenderRelease(rep)
	for _, want := range []string{"stage-rc", "f2", "ome-release@1.0.0-2", "new-newt", "member-a", "1.0.0-2", "hub-a", "unreachable"} {
		if !strings.Contains(text, want) {
			t.Errorf("render lacks %q:\n%s", want, text)
		}
	}
}

func TestNewRelease(t *testing.T) {
	cfg, err := Parse([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	prober := &Prober{Config: cfg, Paths: map[string]string{}}
	if rel, err := NewRelease(cfg, prober); rel != nil || err != nil {
		t.Fatalf("no kargo block: rel %v err %v", rel, err)
	}
	cfg.Kargo = &KargoConfig{Cluster: "nope", Namespace: "kargo-proj-a", RecentFreight: 3}
	if _, err := NewRelease(cfg, prober); err == nil {
		t.Fatal("unregistered kargo cluster must fail")
	}
	// Outside a pod the in-cluster reader has no ServiceAccount to load.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	cfg.Kargo = &KargoConfig{InCluster: true, Namespace: "kargo-proj-a", RecentFreight: 3}
	if _, err := NewRelease(cfg, prober); err == nil || !strings.Contains(err.Error(), "in-cluster") {
		t.Fatalf("in-cluster kargo outside a pod: err %v", err)
	}
}
