package v1alpha1

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestEnqueuePGDFromChild(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox",
			Name:      "sglang-prefill-0-0",
			Labels: map[string]string{
				labelManagedBy: managerName,
				labelInstance:  "sglang-prefill",
			},
		},
	}

	requests := enqueuePGDFromChild(context.Background(), pod)
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if got, want := requests[0].NamespacedName, (client.ObjectKey{Namespace: "sandbox", Name: "sglang-prefill"}); got != want {
		t.Fatalf("request = %v, want %v", got, want)
	}
}

func TestEnqueuePGDFromChildIgnoresUnmanagedObject(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox",
			Name:      "other",
			Labels: map[string]string{
				labelInstance: "sglang-prefill",
			},
		},
	}

	if requests := enqueuePGDFromChild(context.Background(), pod); len(requests) != 0 {
		t.Fatalf("requests = %v, want empty", requests)
	}
}

func TestEnqueuePGDRFromChildPGD(t *testing.T) {
	pgd := &PodGroupDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox",
			Name:      "sglang-prefill",
			Labels: map[string]string{
				labelManagedBy: managerName,
				labelRollout:   "sglang",
			},
		},
	}

	requests := enqueuePGDRFromChildPGD(context.Background(), pgd)
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if got, want := requests[0].NamespacedName, (client.ObjectKey{Namespace: "sandbox", Name: "sglang"}); got != want {
		t.Fatalf("request = %v, want %v", got, want)
	}
}

func TestEnqueuePGDRFromManagedChildObjectUsesRolloutLabel(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox",
			Name:      "sglang-decode-new-0-0",
			Labels: map[string]string{
				labelManagedBy: managerName,
				labelRollout:   "sglang",
			},
		},
	}

	requests := (&PodGroupDeploymentRolloutReconciler{}).enqueuePGDRFromChildObject(context.Background(), pod)
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if got, want := requests[0].NamespacedName, (client.ObjectKey{Namespace: "sandbox", Name: "sglang"}); got != want {
		t.Fatalf("request = %v, want %v", got, want)
	}
}

func TestEnqueuePGDRFromManagedChildObject(t *testing.T) {
	pgd := &PodGroupDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: operatorAPIVersion,
			Kind:       operatorKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox",
			Name:      "sglang-decode",
			Labels: map[string]string{
				labelManagedBy: managerName,
				labelRollout:   "sglang",
			},
		},
	}
	obj, err := podGroupDeploymentToUnstructured(pgd)
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reconciler := &PodGroupDeploymentRolloutReconciler{
		controller: New(nil, fake.NewSimpleDynamicClient(scheme, obj)),
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sandbox",
			Name:      "sglang-decode-new-0-0",
			Labels: map[string]string{
				labelManagedBy: managerName,
				labelInstance:  "sglang-decode",
			},
		},
	}
	requests := reconciler.enqueuePGDRFromChildObject(context.Background(), pod)
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if got, want := requests[0].NamespacedName, (client.ObjectKey{Namespace: "sandbox", Name: "sglang"}); got != want {
		t.Fatalf("request = %v, want %v", got, want)
	}
}
