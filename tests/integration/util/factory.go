package util

import (
	"context"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CleanupFunc removes finalizers (or otherwise unblocks deletion) from
// any of the registered type's instances in the given namespace.
// Returning an error fails the namespace cleanup; nil-error means
// "nothing to do or already clean".
//
// Implementations must be idempotent and tolerate the namespace not
// existing (Kubernetes 404 is success for cleanup).
type CleanupFunc func(ctx context.Context, c client.Client, namespace string) error

var (
	cleanupMu sync.Mutex
	cleanups  []CleanupFunc
)

// RegisterCleanup adds a finalizer-removal hook called by
// DeleteNamespace before the namespace itself is deleted. Each
// builder package's init() should register its CRD's cleanup so
// adding a new CRD = one init() block, not a central edit.
//
// Safe for init-time concurrent calls (sync.Mutex). The order in
// which cleanups run is not guaranteed; order-sensitive cleanups
// (e.g., child-before-parent) should be expressed by a single
// CleanupFunc that calls them in order.
func RegisterCleanup(fn CleanupFunc) {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	cleanups = append(cleanups, fn)
}

// runRegisteredCleanups invokes every registered CleanupFunc against
// the given namespace, collecting errors. Stops on first error so the
// caller can surface a useful failure point.
func runRegisteredCleanups(ctx context.Context, c client.Client, namespace string) error {
	cleanupMu.Lock()
	snapshot := append([]CleanupFunc(nil), cleanups...)
	cleanupMu.Unlock()
	for _, fn := range snapshot {
		if err := fn(ctx, c, namespace); err != nil {
			return err
		}
	}
	return nil
}
