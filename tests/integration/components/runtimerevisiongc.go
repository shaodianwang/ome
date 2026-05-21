package components

import (
	"context"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/constants"
	v1beta1runtimerevisioncontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/runtimerevision"
)

// RuntimeRevisionGCOption tunes the GC controller for tests that want
// short retention / grace periods so they can observe the state
// machine in seconds rather than hours.
type RuntimeRevisionGCOption func(*v1beta1runtimerevisioncontroller.GCReconciler)

// WithRetention overrides RetentionPerRuntime.
func WithRetention(n int) RuntimeRevisionGCOption {
	return func(r *v1beta1runtimerevisioncontroller.GCReconciler) { r.RetentionPerRuntime = n }
}

// WithGracePeriod overrides GracePeriod.
func WithGracePeriod(d time.Duration) RuntimeRevisionGCOption {
	return func(r *v1beta1runtimerevisioncontroller.GCReconciler) { r.GracePeriod = d }
}

// WithResync overrides Resync.
func WithResync(d time.Duration) RuntimeRevisionGCOption {
	return func(r *v1beta1runtimerevisioncontroller.GCReconciler) { r.Resync = d }
}

// SetupRuntimeRevisionGC registers the GC controller. Defaults match
// production (retention 10, grace 24h, resync 5m); tests pass options
// to compress time.
func SetupRuntimeRevisionGC(_ context.Context, mgr ctrl.Manager, opts ...RuntimeRevisionGCOption) error {
	r := &v1beta1runtimerevisioncontroller.GCReconciler{
		Client:              mgr.GetClient(),
		Log:                 ctrl.Log.WithName("RuntimeRevisionGC"),
		Scheme:              mgr.GetScheme(),
		OMENamespace:        constants.OMENamespace,
		RetentionPerRuntime: 10,
		GracePeriod:         24 * time.Hour,
		Resync:              5 * time.Minute,
	}
	for _, opt := range opts {
		opt(r)
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RuntimeRevision GC controller: %w", err)
	}
	return nil
}
