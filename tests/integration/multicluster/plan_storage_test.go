package multicluster

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/plan"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing"
)

func TestPlanStorageCompatibility(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "full", "ome.io_inferenceservices.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		single bool
		move   bool
		prune  func(map[string]apiextensionsv1.JSONSchemaProps)
		want   error
	}{
		{name: "current schema"},
		{name: "Single authority survives storage", single: true},
		{name: "Single movement survives storage", single: true, move: true},
		{name: "schema without Single movement", single: true, move: true, prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["plan"].Properties, "singleMove")
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without replacement selection", single: true, move: true, prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["plan"].Properties["singleMove"].Properties, "selected")
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without replacement cursor", single: true, move: true, prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["plan"].Properties["singleMove"].Properties, "cursor")
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without replacement timeout origin", single: true, move: true, prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["candidates"].Items.Schema.Properties["allocation"].Properties, "replacementStartedAt")
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without Single winner", single: true, prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["plan"].Properties, "winner")
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without race cleanup permission", single: true, prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["candidates"].Items.Schema.Properties["allocation"].Properties, "raceCandidate")
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without surge coordination", prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			stored := fields["plan"]
			delete(stored.Properties, "pauseSurge")
			stored.Required = slices.DeleteFunc(stored.Required, func(name string) bool { return name == "pauseSurge" })
			fields["plan"] = stored
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without plan authority", prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) { delete(fields, "plan") }, want: plan.ErrSchemaUnsupported},
		{name: "schema without candidate allocations", prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["candidates"].Items.Schema.Properties, "allocation")
		}, want: plan.ErrSchemaUnsupported},
		{name: "schema without rendering contracts", prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["candidates"].Items.Schema.Properties["allocation"].Properties["capacity"].Properties, "demandContract")
		}, want: plan.ErrSchemaUnsupported},
	} {
		t.Run(tt.name, func(t *testing.T) {
			installed := crd.DeepCopy()
			if tt.prune != nil {
				for i := range installed.Spec.Versions {
					tt.prune(installed.Spec.Versions[i].Schema.OpenAPIV3Schema.Properties["status"].Properties["placement"].Properties)
				}
			}
			env := ometesting.EtcdForBusyHosts(&envtest.Environment{CRDs: []*apiextensionsv1.CustomResourceDefinition{installed}})
			cfg, err := env.Start()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := env.Stop(); err != nil {
					t.Error(err)
				}
			})
			scheme := runtime.NewScheme()
			if err := v1beta1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			cl, err := client.New(cfg, client.Options{Scheme: scheme})
			if err != nil {
				t.Fatal(err)
			}
			if err := cl.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}); err != nil {
				t.Fatal(err)
			}
			store := plan.Store{Client: cl, Reader: cl}
			for _, participant := range []bool{false, true} {
				name := "local-service"
				if participant {
					name = "source-service"
				}
				t.Run(name, func(t *testing.T) {
					source := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"}}
					if participant {
						source.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSplit}
						if tt.single {
							source.Spec.Placement.Mode = v1beta1.PlacementModeSingle
						}
					}
					if err := cl.Create(t.Context(), source); err != nil {
						t.Fatal(err)
					}
					before := source.DeepCopy()
					proposal := plan.Proposal{InputDigest: "intent-a", PauseSurge: true, UnassignedReplicas: 2, Assignments: map[string]v1beta1.CandidateAllocationStatus{
						"member-a": {ClusterUID: "registration-a", OriginalReplicas: 6, CurrentReplicas: 6, DesiredReplicas: 4, MatchingTerms: []int32{0}},
						"member-b": {ClusterUID: "registration-b", OriginalReplicas: 0, CurrentReplicas: 0, DesiredReplicas: 0},
					}}
					assignment := proposal.Assignments["member-b"]
					assignment.Capacity = &v1beta1.PlacementCapacitySample{DemandFingerprint: strings.Repeat("a", 64), DemandContract: &v1beta1.PlacementDemandContract{
						Fingerprint: strings.Repeat("a", 64), Components: []v1beta1.PlacementComponentDemand{{Component: v1beta1.EngineComponent, RenderingHash: strings.Repeat("b", 64)}},
					}, Replicas: 0, Pools: []v1beta1.PlacementCapacityPool{{
						ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a", Demand: 2, Allocatable: 0,
						ObservedAt: metav1.NewTime(time.Unix(100, 0).UTC()), ReportUID: "report-a", ReportResourceVersion: "1",
						Attribution: v1beta1.AcceleratorCapacityAttribution{FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", Complete: true, NodeLabels: map[string]string{"accelerator": "gpu-a"}},
					}}}
					proposal.Assignments["member-b"] = assignment
					if tt.single {
						home := &v1beta1.PlacementHomePolicy{InputDigest: "intent-a", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}}
						proposal = plan.Proposal{Mode: v1beta1.PlacementModeSingle, Winner: "member-a", InputDigest: "intent-a", Assignments: map[string]v1beta1.CandidateAllocationStatus{
							"member-a": {ClusterUID: "registration-a", OriginalReplicas: 2, CurrentReplicas: 2, DesiredReplicas: 2, CurrentHome: home.DeepCopy(), DesiredHome: home.DeepCopy()},
							"member-b": {ClusterUID: "registration-b", RaceCandidate: true, DrainRequested: true},
						}}
					}
					if tt.move {
						proposal.PauseSurge = true
						proposal.SingleMove = &v1beta1.PlacementSingleMoveStatus{Selected: "member-b", Cursor: "member-b"}
						a := proposal.Assignments["member-a"]
						b := proposal.Assignments["member-b"]
						b.CurrentReplicas, b.DesiredReplicas = a.CurrentReplicas, a.DesiredReplicas
						b.CurrentHome, b.DesiredHome = a.CurrentHome.DeepCopy(), a.DesiredHome.DeepCopy()
						b.RaceCandidate, b.DrainRequested = false, false
						start := metav1.NewMicroTime(time.Unix(100, 123000).UTC())
						b.ReplacementStartedAt = &start
						a.DesiredHome, a.DesiredReplicas = nil, 0
						proposal.Assignments["member-a"], proposal.Assignments["member-b"] = a, b
					}
					got, err := store.Persist(t.Context(), source, proposal)
					if !participant {
						if err == nil || got != nil {
							t.Fatal("local service accepted a placement plan")
						}
						stored := &v1beta1.InferenceService{}
						if err := cl.Get(t.Context(), client.ObjectKeyFromObject(source), stored); err != nil {
							t.Fatal(err)
						}
						if diff := cmp.Diff(before, stored); diff != "" {
							t.Fatalf("local service changed:\n%s", diff)
						}
						return
					}
					if !errors.Is(err, tt.want) {
						t.Fatalf("want %v, got %v", tt.want, err)
					}
					if tt.want != nil {
						if got != nil {
							t.Fatal("pruned plan authorized member writes")
						}
						return
					}
					want := &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
						ID: got.Status.Placement.Plan.ID, Revision: 1, SourceUID: source.UID, ObservedGeneration: source.Generation,
						InputDigest: "intent-a", PauseSurge: true, RequestedReplicas: 6, AssignedReplicas: 4, UnassignedReplicas: 2,
					}}
					if tt.single {
						want.Cluster = "member-a"
						want.Plan.Mode, want.Plan.Winner = v1beta1.PlacementModeSingle, "member-a"
						want.Plan.PauseSurge = false
						want.Plan.RequestedReplicas, want.Plan.AssignedReplicas, want.Plan.UnassignedReplicas = 2, 2, 0
					}
					if tt.move {
						want.Plan.PauseSurge = true
						want.Plan.SingleMove = proposal.SingleMove.DeepCopy()
					}
					for _, cluster := range []string{"member-a", "member-b"} {
						allocation := proposal.Assignments[cluster]
						want.Candidates = append(want.Candidates, v1beta1.CandidatePlacement{Cluster: cluster, Allocation: &allocation})
					}
					if diff := cmp.Diff(want, got.Status.Placement); diff != "" {
						t.Fatal(diff)
					}
					retried, err := store.Persist(t.Context(), got, proposal)
					if err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(got, retried); diff != "" {
						t.Fatalf("restart changed plan:\n%s", diff)
					}
				})
			}
		})
	}
}
