package multicluster

import (
	"errors"
	"os"
	"path/filepath"
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
		name  string
		prune func(map[string]apiextensionsv1.JSONSchemaProps)
		want  error
	}{
		{name: "current schema"},
		{name: "schema without plan authority", prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) { delete(fields, "plan") }, want: plan.ErrSchemaUnsupported},
		{name: "schema without candidate allocations", prune: func(fields map[string]apiextensionsv1.JSONSchemaProps) {
			delete(fields["candidates"].Items.Schema.Properties, "allocation")
		}, want: plan.ErrSchemaUnsupported},
	} {
		t.Run(tt.name, func(t *testing.T) {
			installed := crd.DeepCopy()
			if tt.prune != nil {
				for i := range installed.Spec.Versions {
					tt.prune(installed.Spec.Versions[i].Schema.OpenAPIV3Schema.Properties["status"].Properties["placement"].Properties)
				}
			}
			env := &envtest.Environment{CRDs: []*apiextensionsv1.CustomResourceDefinition{installed}}
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
						source.Spec.Placement = &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSplit}
					}
					if err := cl.Create(t.Context(), source); err != nil {
						t.Fatal(err)
					}
					before := source.DeepCopy()
					proposal := plan.Proposal{InputDigest: "intent-a", UnassignedReplicas: 2, Assignments: map[string]v1beta1.CandidateAllocationStatus{
						"member-a": {ClusterUID: "registration-a", OriginalReplicas: 6, CurrentReplicas: 6, DesiredReplicas: 4, MatchingTerms: []int32{0}},
						"member-b": {ClusterUID: "registration-b", OriginalReplicas: 0, CurrentReplicas: 0, DesiredReplicas: 0},
					}}
					assignment := proposal.Assignments["member-b"]
					assignment.Capacity = &v1beta1.PlacementCapacitySample{DemandFingerprint: "demand-a", Replicas: 0, Pools: []v1beta1.PlacementCapacityPool{{
						ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a", Demand: 2, Allocatable: 0,
						ObservedAt: metav1.NewTime(time.Unix(100, 0).UTC()), ReportUID: "report-a", ReportResourceVersion: "1",
						Attribution: v1beta1.AcceleratorCapacityAttribution{FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", Complete: true, NodeLabels: map[string]string{"accelerator": "gpu-a"}},
					}}}
					proposal.Assignments["member-b"] = assignment
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
						InputDigest: "intent-a", RequestedReplicas: 6, AssignedReplicas: 4, UnassignedReplicas: 2,
					}}
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
