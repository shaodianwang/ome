package util

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateNamespaceFromPrefix creates a Namespace whose name starts with
// the given prefix and a unique suffix assigned by the apiserver via
// metav1.GenerateName. Returns the created Namespace (with Name set).
//
// Per-test namespaces are the primary isolation mechanism — cluster-
// scoped resources (ClusterBaseModel) still need per-test cleanup, but
// anything namespaced gets dropped (logically) when the namespace is
// marked Terminating.
func CreateNamespaceFromPrefix(ctx context.Context, c client.Client, prefix string) (*corev1.Namespace, error) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix}}
	if err := c.Create(ctx, ns); err != nil {
		return nil, fmt.Errorf("create namespace with prefix %q: %w", prefix, err)
	}
	return ns, nil
}

// DeleteNamespace runs every registered CleanupFunc (which strip
// finalizers from each CRD) and then issues a Delete on the namespace
// itself. Does NOT wait for the namespace to fully disappear — envtest
// has no kube-controller-manager to run the namespace controller, so
// a Deleted namespace stays in Terminating phase forever. Per-test
// isolation comes from GenerateName-assigned unique names, not from
// actual garbage collection of stale namespaces.
//
// This is the central place where per-CRD cleanup logic plugs in via
// RegisterCleanup. The pattern means adding a new CRD with a
// finalizer is a one-line init() in its builder package, not an edit
// to a central walker.
//
// Idempotent: passing a namespace that doesn't exist is success.
func DeleteNamespace(ctx context.Context, c client.Client, namespace string) error {
	if namespace == "" {
		return nil
	}

	if err := runRegisteredCleanups(ctx, c, namespace); err != nil {
		return fmt.Errorf("namespace %q cleanup hook: %w", namespace, err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if err := c.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete namespace %q: %w", namespace, err)
	}
	return nil
}
