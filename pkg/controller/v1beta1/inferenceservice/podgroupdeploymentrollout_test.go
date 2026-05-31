package inferenceservice

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func TestBuildPodGroupDeploymentRollout(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	r := &InferenceServiceReconciler{Scheme: scheme}

	minPrefill := 8
	minDecode := 4
	tp := int32(8)
	replicasPerGroup := int32(2)
	maxUnavailablePercent := int32(25)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama",
			Namespace: "sandbox",
			UID:       types.UID("uid-1"),
		},
	}
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			MinReplicas: &minPrefill,
			Annotations: map[string]string{constants.DeploymentMode: string(constants.PodGroupDeploymentRollout)},
			PodGroupDeployment: &v1beta1.PodGroupDeploymentComponentSpec{
				ReplicasPerGroup: &replicasPerGroup,
				TopologyKey:      "ds.coreweave.com/nvlink.domain",
				Rollout: &v1beta1.PodGroupDeploymentRolloutStrategy{
					MaxUnavailablePercent: &maxUnavailablePercent,
				},
				MultiNode: &v1beta1.PodGroupDeploymentMultiNodeSpec{
					Enabled:            true,
					TensorParallelSize: &tp,
				},
			},
		},
	}
	decoder := &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			MinReplicas: &minDecode,
			Annotations: map[string]string{constants.DeploymentMode: string(constants.PodGroupDeploymentRollout)},
		},
	}
	template := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"component": "engine"}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "worker", Image: "image"}},
		},
	}

	obj, err := r.buildPodGroupDeploymentRollout(isvc, engine, decoder, template, template.DeepCopy())
	if err != nil {
		t.Fatalf("build PodGroupDeploymentRollout: %v", err)
	}

	if got := obj.GetName(); got != "llama" {
		t.Fatalf("name = %q, want llama", got)
	}
	if len(obj.GetOwnerReferences()) != 1 || obj.GetOwnerReferences()[0].Kind != "InferenceService" {
		t.Fatalf("owner refs = %#v, want InferenceService owner", obj.GetOwnerReferences())
	}
	mode, _, _ := unstructured.NestedString(obj.Object, "spec", "mode")
	if mode != "disagg" {
		t.Fatalf("mode = %q, want disagg", mode)
	}
	prefillGroups, _, _ := unstructured.NestedInt64(obj.Object, "spec", "prefill", "groups")
	if prefillGroups != 8 {
		t.Fatalf("prefill groups = %d, want 8", prefillGroups)
	}
	decodeGroups, _, _ := unstructured.NestedInt64(obj.Object, "spec", "decode", "groups")
	if decodeGroups != 4 {
		t.Fatalf("decode groups = %d, want 4", decodeGroups)
	}
	prefillReplicas, _, _ := unstructured.NestedInt64(obj.Object, "spec", "prefill", "replicasPerGroup")
	if prefillReplicas != 2 {
		t.Fatalf("prefill replicasPerGroup = %d, want 2", prefillReplicas)
	}
	component, _, _ := unstructured.NestedString(obj.Object, "spec", "prefill", "template", "metadata", "labels", "app.kubernetes.io/component")
	if component != "prefill" {
		t.Fatalf("prefill app.kubernetes.io/component = %q, want prefill", component)
	}
	rollout, _, _ := unstructured.NestedInt64(obj.Object, "spec", "strategy", "maxUnavailablePercent")
	if rollout != 25 {
		t.Fatalf("maxUnavailablePercent = %d, want 25", rollout)
	}
	multiNode, _, _ := unstructured.NestedBool(obj.Object, "spec", "prefill", "multiNode", "enabled")
	if !multiNode {
		t.Fatalf("prefill multiNode.enabled = false, want true")
	}
}
