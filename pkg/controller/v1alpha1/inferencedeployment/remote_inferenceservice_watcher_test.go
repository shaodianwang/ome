package inferencedeployment

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	omev1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestInferenceDeploymentKeyFromRemoteObject(t *testing.T) {
	isvc := remoteInferenceService("llama", "serving", map[string]string{
		labelManagedBy:           managedByValue,
		labelDeployment:          "llama",
		labelDeploymentNamespace: "control",
		labelTarget:              "cluster-a",
	})

	key, ok := inferenceDeploymentKeyFromRemoteObject("cluster-a", isvc)
	if !ok {
		t.Fatalf("inferenceDeploymentKeyFromRemoteObject() ok = false, want true")
	}
	if key != (types.NamespacedName{Namespace: "control", Name: "llama"}) {
		t.Fatalf("key = %s, want control/llama", key)
	}
}

func TestInferenceDeploymentKeyFromRemoteObjectIgnoresUnmanagedObjects(t *testing.T) {
	isvc := remoteInferenceService("llama", "serving", map[string]string{
		labelDeployment:          "llama",
		labelDeploymentNamespace: "control",
		labelTarget:              "cluster-a",
	})

	if key, ok := inferenceDeploymentKeyFromRemoteObject("cluster-a", isvc); ok {
		t.Fatalf("inferenceDeploymentKeyFromRemoteObject() = %s, true; want false", key)
	}
}

func TestInferenceDeploymentKeyFromRemoteObjectIgnoresWrongTarget(t *testing.T) {
	isvc := remoteInferenceService("llama", "serving", map[string]string{
		labelManagedBy:           managedByValue,
		labelDeployment:          "llama",
		labelDeploymentNamespace: "control",
		labelTarget:              "cluster-b",
	})

	if key, ok := inferenceDeploymentKeyFromRemoteObject("cluster-a", isvc); ok {
		t.Fatalf("inferenceDeploymentKeyFromRemoteObject() = %s, true; want false", key)
	}
}

func TestInferenceDeploymentKeyFromRemoteObjectHandlesDeleteTombstone(t *testing.T) {
	isvc := remoteInferenceService("llama", "serving", map[string]string{
		labelManagedBy:           managedByValue,
		labelDeployment:          "llama",
		labelDeploymentNamespace: "control",
		labelTarget:              "cluster-a",
	})
	tombstone := cache.DeletedFinalStateUnknown{Key: "serving/llama", Obj: isvc}

	key, ok := inferenceDeploymentKeyFromRemoteObject("cluster-a", tombstone)
	if !ok {
		t.Fatalf("inferenceDeploymentKeyFromRemoteObject() ok = false, want true")
	}
	if key != (types.NamespacedName{Namespace: "control", Name: "llama"}) {
		t.Fatalf("key = %s, want control/llama", key)
	}
}

func TestRemoteInferenceDeploymentKeysFromObjectsUsesOldAndNewLabels(t *testing.T) {
	oldISVC := remoteInferenceService("llama", "serving", map[string]string{
		labelManagedBy:           managedByValue,
		labelDeployment:          "llama",
		labelDeploymentNamespace: "control",
		labelTarget:              "cluster-a",
	})
	newISVCWithLabelsRemoved := remoteInferenceService("llama", "serving", nil)

	keys := remoteInferenceDeploymentKeysFromObjects("cluster-a", oldISVC, newISVCWithLabelsRemoved)
	if len(keys) != 1 {
		t.Fatalf("keys = %#v, want one key", keys)
	}
	if keys[0] != (types.NamespacedName{Namespace: "control", Name: "llama"}) {
		t.Fatalf("key = %s, want control/llama", keys[0])
	}
}

func TestRemoteInferenceDeploymentKeysFromObjectsDeduplicatesUpdate(t *testing.T) {
	oldISVC := remoteInferenceService("llama", "serving", map[string]string{
		labelManagedBy:           managedByValue,
		labelDeployment:          "llama",
		labelDeploymentNamespace: "control",
		labelTarget:              "cluster-a",
	})
	newISVC := oldISVC.DeepCopy()

	keys := remoteInferenceDeploymentKeysFromObjects("cluster-a", oldISVC, newISVC)
	if len(keys) != 1 {
		t.Fatalf("keys = %#v, want one deduplicated key", keys)
	}
}

func remoteInferenceService(name, namespace string, labels map[string]string) *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
	}
}
