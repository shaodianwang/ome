package util

import (
	"context"
	"fmt"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ownedNamespaces are the namespaces this process created through
// CreateNamespaceFromPrefix and has not yet deleted through DeleteNamespace.
// Suite-wide observers read them to find the objects the running spec owns.
var (
	ownedMu         sync.Mutex
	ownedNamespaces = map[string]struct{}{}
)

// OwnedNamespaces returns, in name order, the namespaces this process created
// and still owns.
func OwnedNamespaces() []string {
	ownedMu.Lock()
	defer ownedMu.Unlock()
	out := make([]string, 0, len(ownedNamespaces))
	for name := range ownedNamespaces {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func ownNamespace(name string) {
	ownedMu.Lock()
	defer ownedMu.Unlock()
	ownedNamespaces[name] = struct{}{}
}

func disownNamespace(name string) {
	ownedMu.Lock()
	defer ownedMu.Unlock()
	delete(ownedNamespaces, name)
}

// CreateNamespaceFromPrefix creates a Namespace whose name starts with
// the given prefix and a unique suffix assigned by the apiserver via
// metav1.GenerateName. Returns the created Namespace (with Name set) and
// records it as owned by this process until DeleteNamespace.
//
// Per-test namespaces are the primary isolation mechanism — cluster-
// scoped resources (ClusterBaseModel, AcceleratorClass) still need
// per-test cleanup, but anything namespaced gets dropped (logically)
// when the namespace is marked Terminating.
func CreateNamespaceFromPrefix(ctx context.Context, c client.Client, prefix string) (*corev1.Namespace, error) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix}}
	if err := c.Create(ctx, ns); err != nil {
		return nil, fmt.Errorf("create namespace with prefix %q: %w", prefix, err)
	}
	ownNamespace(ns.Name)
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
	disownNamespace(namespace)

	if err := runRegisteredCleanups(ctx, c, namespace); err != nil {
		return fmt.Errorf("namespace %q cleanup hook: %w", namespace, err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if err := c.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete namespace %q: %w", namespace, err)
	}
	return nil
}
