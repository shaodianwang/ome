package util

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
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

// cleanupRetry bounds how long a cleanup step keeps retrying an apiserver
// answer that a short wait usually clears, and how long it waits between
// attempts. The window opens at the first attempt.
type cleanupRetry struct {
	window   time.Duration
	interval time.Duration
}

// defaultCleanupRetry rides out an apiserver or etcd stall of the usual
// length without holding a spec for long once the cluster is really gone.
var defaultCleanupRetry = cleanupRetry{window: MediumTimeout, interval: time.Second}

// DeleteNamespace runs every registered CleanupFunc (which strip
// finalizers from each CRD) and then issues a Delete on the namespace
// itself. Each step retries a transient apiserver answer — an internal
// error such as an etcd timeout, a server timeout, rate limiting, a brief
// outage — for a bounded window and reports it only once the window closes.
// A namespace that is already gone or already Terminating counts as
// deleted, because an etcd timeout can follow a write that was applied.
//
// Does NOT wait for the namespace to fully disappear — envtest
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
	return deleteNamespace(ctx, c, namespace, defaultCleanupRetry)
}

func deleteNamespace(ctx context.Context, c client.Client, namespace string, retry cleanupRetry) error {
	if namespace == "" {
		return nil
	}
	disownNamespace(namespace)

	err := retry.run(ctx, "cleanup hooks for namespace "+namespace, func() error {
		return runRegisteredCleanups(ctx, c, namespace)
	})
	if err != nil {
		return fmt.Errorf("namespace %q cleanup hook: %w", namespace, err)
	}
	err = retry.run(ctx, "delete namespace "+namespace, func() error {
		return deleteUnlessTerminating(ctx, c, namespace)
	})
	if err != nil {
		return fmt.Errorf("delete namespace %q: %w", namespace, err)
	}
	return nil
}

// deleteUnlessTerminating issues the namespace Delete. A failed call still
// counts as done when the namespace is gone or already carries a deletion
// timestamp: the delete reached the store even though its answer did not
// reach us.
func deleteUnlessTerminating(ctx context.Context, c client.Client, namespace string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	err := c.Delete(ctx, ns)
	if err == nil || apierrors.IsNotFound(err) || namespaceTerminating(ctx, c, namespace) {
		return nil
	}
	return err
}

// namespaceTerminating reports whether the namespace is gone or being
// deleted. A failed read reports false so the caller keeps its own error.
func namespaceTerminating(ctx context.Context, c client.Client, namespace string) bool {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, ns); err != nil {
		return apierrors.IsNotFound(err)
	}
	return ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating
}

// run calls fn until it succeeds, returns an error that waiting will not
// clear, the window closes, or ctx ends. Each retry is written to the spec
// output so a lane log shows the stall the cleanup rode out.
func (r cleanupRetry) run(ctx context.Context, what string, fn func() error) error {
	start := time.Now()
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || !transientAPIError(err) || time.Since(start) >= r.window {
			return err
		}
		fmt.Fprintf(ginkgo.GinkgoWriter, "%s: attempt %d got a transient apiserver error, retrying in %s: %v\n",
			what, attempt, r.interval, err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(r.interval):
		}
	}
}

// transientAPIError reports whether an apiserver answer usually clears on
// its own: an internal error such as an etcd timeout, a server or gateway
// timeout, rate limiting, a brief outage, or a connection that timed out.
func transientAPIError(err error) bool {
	if apierrors.IsInternalError(err) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) || apierrors.IsUnexpectedServerError(err) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
