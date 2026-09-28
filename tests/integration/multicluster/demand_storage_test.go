package multicluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestDemandContractStorage(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "full", "ome.io_inferencereplicas.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	for _, schema := range []struct {
		name  string
		prune bool
	}{{name: "current schema"}, {name: "schema without demand", prune: true}} {
		t.Run(schema.name, func(t *testing.T) {
			installed := crd.DeepCopy()
			if schema.prune {
				for i := range installed.Spec.Versions {
					delete(installed.Spec.Versions[i].Schema.OpenAPIV3Schema.Properties["spec"].Properties["placementExecution"].Properties, "demand")
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
			for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, corev1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			cl, err := client.New(cfg, client.Options{Scheme: scheme})
			if err != nil {
				t.Fatal(err)
			}
			if err := cl.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}); err != nil {
				t.Fatal(err)
			}
			for _, tt := range []struct {
				name               string
				edit               func(*v1beta1.PlacementExecutionPolicy)
				local, wantInvalid bool
			}{
				{name: "member"},
				{name: "local", local: true},
				{name: "static", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Demand = nil }},
				{name: "invalid-fingerprint", wantInvalid: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Demand.Fingerprint = "invalid" }},
				{name: "invalid-rendering", wantInvalid: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Demand.Components[0].RenderingHash = "invalid" }},
				{name: "missing-components", wantInvalid: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Demand.Components = []v1beta1.PlacementComponentDemand{} }},
				{name: "duplicate-components", wantInvalid: true, edit: func(p *v1beta1.PlacementExecutionPolicy) {
					p.Demand.Components = append(p.Demand.Components, p.Demand.Components[0])
				}},
				{name: "router-component", wantInvalid: true, edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Demand.Components[0].Component = v1beta1.RouterComponent }},
			} {
				t.Run(tt.name, func(t *testing.T) {
					policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", Demand: &v1beta1.PlacementDemandContract{
						Fingerprint: strings.Repeat("a", 64), Components: []v1beta1.PlacementComponentDemand{{Component: v1beta1.EngineComponent, RenderingHash: strings.Repeat("b", 64)}},
					}}
					if tt.edit != nil {
						tt.edit(policy)
					}
					if tt.local {
						policy = nil
					}
					ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: tt.name, Namespace: "team-a"}, Spec: v1beta1.InferenceReplicaSpec{
						ParentRef: v1beta1.ParentReference{Name: "service"}, Component: v1beta1.EngineComponent, PlacementExecution: policy,
						Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "example.com/serving:v1"}}}}}},
					}}
					err := cl.Create(t.Context(), ir)
					if tt.wantInvalid && !schema.prune {
						if !apierrors.IsInvalid(err) {
							t.Fatalf("invalid contract accepted: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					stored := &v1beta1.InferenceReplica{}
					if err := cl.Get(t.Context(), client.ObjectKeyFromObject(ir), stored); err != nil {
						t.Fatal(err)
					}
					want := policy.DeepCopy()
					if schema.prune && want != nil {
						want.Demand = nil
					}
					if diff := cmp.Diff(want, stored.Spec.PlacementExecution); diff != "" {
						t.Fatalf("stored authority (-want +got):\n%s", diff)
					}
					if tt.local {
						if diff := cmp.Diff(ir, stored); diff != "" {
							t.Fatalf("local round trip changed object:\n%s", diff)
						}
					}
				})
			}
		})
	}
}
