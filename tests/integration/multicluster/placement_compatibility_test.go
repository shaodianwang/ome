package multicluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing"
	"github.com/ome-projects/ome/pkg/validation"
)

func TestStoredLegacyPlacementSurvivesSchemaUpgrade(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "full", "ome.io_inferenceservices.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	current := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(data, current); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join("testdata", "legacy-placement-schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	legacyPlacement := apiextensionsv1.JSONSchemaProps{}
	if err := json.Unmarshal(data, &legacyPlacement); err != nil {
		t.Fatal(err)
	}
	previous := current.DeepCopy()
	spec := previous.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
	spec.Properties["placement"] = legacyPlacement
	spec.XValidations = nil
	previous.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"] = spec
	env := ometesting.EtcdForBusyHosts(&envtest.Environment{CRDs: []*apiextensionsv1.CustomResourceDefinition{previous}})
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
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, corev1.AddToScheme, apiextensionsv1.AddToScheme} {
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
	cases := []struct {
		name                   string
		placement, annotations string
		derived                bool
	}{
		{name: "member-local"},
		{name: "annotation source", annotations: `{"ome.io/accelerator-requirements":"accelerator=gpu-a","ome.io/cluster-selector":"region=west"}`},
		{name: "implicit Single", placement: `{"requirements":"accelerator=gpu-a"}`},
		{name: "empty typed intent", placement: `{}`},
		{name: "explicit Single", placement: `{"mode":"Single","clusterSelector":"metadata.name=member-a"}`},
		{name: "All", placement: `{"mode":"All","requirements":"accelerator=gpu-a"}`},
		{name: "Packed", placement: `{"mode":"Split","requirements":"accelerator=gpu-a","split":{"replicas":3,"spread":false}}`},
		{name: "spread and anti-sliver", placement: `{"mode":"Split","requirements":"accelerator=gpu-a","split":{"replicas":3,"spread":true,"minReplicasPerCluster":2,"maxReplicasPerCluster":4}}`},
		{name: "routing alias", placement: `{"mode":"All","requirements":"accelerator=gpu-a","capacityFactors":{"member-a":"2"}}`},
		{name: "typed overrides annotation", placement: `{"clusterSelector":"region=east"}`, annotations: `{"ome.io/cluster-selector":"region=west"}`},
		{name: "old derived copy", derived: true, placement: `{"mode":"Split","requirements":"accelerator=gpu-a","split":{"replicas":3,"spread":false}}`},
	}
	stored := make([]*v1beta1.InferenceService, len(cases))
	for i, tt := range cases {
		object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": v1beta1.SchemeGroupVersion.String(), "kind": "InferenceService", "metadata": map[string]any{"name": fmt.Sprintf("service-%d", i), "namespace": "team-a"}, "spec": map[string]any{"engine": map[string]any{"minReplicas": int64(2), "maxReplicas": int64(5)}}}}
		if tt.placement != "" {
			var p map[string]any
			if err := json.Unmarshal([]byte(tt.placement), &p); err != nil {
				t.Fatal(err)
			}
			object.Object["spec"].(map[string]any)["placement"] = p
		}
		if tt.annotations != "" {
			var a map[string]any
			if err := json.Unmarshal([]byte(tt.annotations), &a); err != nil {
				t.Fatal(err)
			}
			object.Object["metadata"].(map[string]any)["annotations"] = a
		}
		if tt.derived {
			object.SetLabels(map[string]string{"ome.io/placement-origin": "source-uid"})
		}
		if err := cl.Create(t.Context(), object); err != nil {
			t.Fatalf("seed %s: %v", tt.name, err)
		}
		stored[i] = &v1beta1.InferenceService{}
		if err := cl.Get(t.Context(), client.ObjectKeyFromObject(object), stored[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{CRDs: []*apiextensionsv1.CustomResourceDefinition{current}}); err != nil {
		t.Fatal(err)
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			before := stored[i]
			got := &v1beta1.InferenceService{}
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(before), got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, got); diff != "" {
				t.Fatalf("schema upgrade changed stored object:\n%s", diff)
			}
			if err := validation.ValidatePlacementIntent(got); err != nil {
				t.Fatal(err)
			}
			if got.Annotations == nil {
				got.Annotations = map[string]string{}
			}
			got.Annotations["example.com/owner"] = "team-a"
			if err := cl.Update(t.Context(), got); err != nil {
				t.Fatalf("metadata update: %v", err)
			}
			if diff := cmp.Diff(before.Spec, got.Spec); diff != "" {
				t.Fatalf("metadata update changed spec:\n%s", diff)
			}
			*got.Spec.Engine.MinReplicas = 3
			if err := cl.Update(t.Context(), got); err != nil {
				t.Fatalf("replica update: %v", err)
			}
			if err := validation.ValidatePlacementIntent(got); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("policy handoff survives admission without webhooks", func(t *testing.T) {
		source := &v1beta1.InferenceService{}
		if err := cl.Get(t.Context(), client.ObjectKeyFromObject(stored[2]), source); err != nil {
			t.Fatal(err)
		}
		source.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}
		if err := cl.Update(t.Context(), source); err != nil {
			t.Fatal(err)
		}
		for _, p := range []*v1beta1.PlacementSpec{nil, {Mode: v1beta1.PlacementModeSingle}, {Policy: v1beta1.PlacementPolicyLegacy, Mode: v1beta1.PlacementModeSingle}} {
			changed := source.DeepCopy()
			changed.Spec.Placement = p
			if err := cl.Update(t.Context(), changed, client.DryRunAll); err == nil {
				t.Fatal("schema allowed ClusterAffinity policy removal")
			}
		}
	})
}
