package multicluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/pkg/validation"
)

func TestPlacementIntentStorage(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "crd", "full", "ome.io_inferenceservices.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{CRDs: []*apiextensionsv1.CustomResourceDefinition{&crd}}
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
	for i, tt := range []struct {
		name, spec                        string
		local, rejectSchema, rejectIntent bool
		rejectUpdateSchema                bool
	}{
		{name: "ordinary local has no placement default", spec: `{}`, local: true},
		{name: "local component bounds remain independent", spec: `{"engine":{"minReplicas":2,"maxReplicas":5}}`, local: true},
		{name: "single can omit affinity", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single"}}`},
		{name: "legacy replacement timeout requires opt-in", spec: `{"placement":{"mode":"Single","replacementTimeout":"5m"}}`, rejectSchema: true},
		{name: "replacement timeout without mode requires opt-in", spec: `{"placement":{"replacementTimeout":"5m"}}`, rejectSchema: true},
		{name: "single replacement timeout round trips", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","replacementTimeout":"5m0s"}}`},
		{name: "zero replacement timeout is invalid", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","replacementTimeout":"0s"}}`, rejectSchema: true},
		{name: "negative replacement timeout is invalid", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","replacementTimeout":"-1s"}}`, rejectSchema: true},
		{name: "invalid replacement duration is rejected", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","replacementTimeout":"pending"}}`, rejectSchema: true},
		{name: "replacement timeout requires Single", spec: `{"placement":{"policy":"ClusterAffinity","mode":"All","replacementTimeout":"5m"}}`, rejectSchema: true},
		{name: "split retains explicit weights", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Split","clusterAffinity":[{"weight":3,"matchExpressions":[{"key":"region","operator":"In","values":["west"]}]}],"split":{"replicas":5},"maxSurge":0}}`},
		{name: "placement requires explicit mode", spec: `{"placement":{"policy":"ClusterAffinity"}}`, rejectSchema: true},
		{name: "empty affinity remains invalid", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","clusterAffinity":[]}}`, rejectSchema: true},
		{name: "null affinity cannot become match all", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","clusterAffinity":null}}`, rejectIntent: true, rejectUpdateSchema: true},
		{name: "legacy empty requirements remain visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","requirements":""}}`, rejectIntent: true},
		{name: "legacy null requirements remain visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","requirements":null}}`, rejectIntent: true},
		{name: "legacy selector remains visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Single","clusterSelector":"region=west"}}`, rejectIntent: true},
		{name: "legacy false spread remains visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Split","split":{"replicas":2,"spread":false}}}`, rejectIntent: true},
		{name: "legacy zero minimum remains visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Split","split":{"replicas":2,"minReplicasPerCluster":0}}}`, rejectIntent: true},
		{name: "legacy null spread remains visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Split","split":{"replicas":2,"spread":null}}}`, rejectIntent: true},
		{name: "legacy empty factors remain visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Split","split":{"replicas":2},"capacityFactors":{}}}`, rejectIntent: true},
		{name: "legacy null factors remain visible", spec: `{"placement":{"policy":"ClusterAffinity","mode":"Split","split":{"replicas":2},"capacityFactors":null}}`, rejectIntent: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var spec map[string]any
			if err := json.Unmarshal([]byte(tt.spec), &spec); err != nil {
				t.Fatal(err)
			}
			object := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": v1beta1.SchemeGroupVersion.String(), "kind": "InferenceService",
				"metadata": map[string]any{"name": fmt.Sprintf("service-%d", i), "namespace": "team-a"}, "spec": spec,
			}}
			err := cl.Create(t.Context(), object)
			if diff := cmp.Diff(tt.rejectSchema, err != nil); diff != "" {
				t.Fatalf("schema outcome (-want +got):\n%s; %v", diff, err)
			}
			if tt.rejectSchema {
				return
			}
			stored := &v1beta1.InferenceService{}
			key := client.ObjectKeyFromObject(object)
			if err := cl.Get(t.Context(), key, stored); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.rejectIntent, validation.ValidatePlacementIntent(stored) != nil); diff != "" {
				t.Fatalf("stored intent (-want +got):\n%s", diff)
			}
			if tt.local {
				before := stored.DeepCopy()
				r := &placement.Reconciler{Client: cl, APIReader: cl, Log: logr.Discard()}
				if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				if err := cl.Get(t.Context(), key, stored); err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(before, stored); diff != "" {
					t.Fatalf("local object changed:\n%s", diff)
				}
				if stored.Spec.Placement != nil {
					t.Fatal("local service acquired placement intent")
				}
				return
			}
			// A typed client update must preserve restriction presence, including
			// legacy zero values that ordinary omitempty encoding would discard.
			err = cl.Update(t.Context(), stored)
			if diff := cmp.Diff(tt.rejectUpdateSchema, err != nil); diff != "" {
				t.Fatalf("typed update outcome (-want +got):\n%s; %v", diff, err)
			}
			if err := cl.Get(t.Context(), key, stored); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.rejectIntent, validation.ValidatePlacementIntent(stored) != nil); diff != "" {
				t.Errorf("typed round trip changed intent:\n%s", diff)
			}
		})
	}
}
