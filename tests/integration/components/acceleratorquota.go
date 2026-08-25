package components

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/acceleratorquota"
	kueuebackend "github.com/ome-projects/ome/pkg/quota/backend/kueue"
	"github.com/ome-projects/ome/pkg/quota/tree"
)

// AcceleratorQuotaOption tunes the reconciler for a suite.
type AcceleratorQuotaOption func(*acceleratorquota.Reconciler)

// WithQuotaMaxDepth sets the greatest permitted distance from the root, in
// edges. Zero disables the depth check.
func WithQuotaMaxDepth(d int) AcceleratorQuotaOption {
	return func(r *acceleratorquota.Reconciler) { r.Options.MaxDepth = d }
}

// WithQuotaResync sets the idle re-check cadence. Specs compress it so a
// violation that only a clock can clear is observable in seconds.
func WithQuotaResync(d time.Duration) AcceleratorQuotaOption {
	return func(r *acceleratorquota.Reconciler) { r.ResyncInterval = d }
}

// WithQuotaCapacity turns on deriving the cluster's own accelerator capacity
// onto the reserved root. Off unless a suite asks, matching the binary: absent
// config disables derivation rather than guessing which resources are
// accelerators.
func WithQuotaCapacity(resources []string, hysteresisPercent int32) AcceleratorQuotaOption {
	return func(r *acceleratorquota.Reconciler) {
		r.Capacity = acceleratorquota.CapacityOptions{
			Resources: resources, HysteresisPercent: hysteresisPercent,
		}
	}
}

// WithQuotaMaterialization turns on rendering the tree into Kueue. Off unless a
// suite asks, matching the binary: absent cover resources disable it rather
// than inventing a ceiling that would surface as a real quota.
//
// The writer is the manager's client and the reader its uncached APIReader,
// exactly as cmd/ome-quota-manager wires it — the ownership check before every
// apply is a read-then-write, and a cached read would let an object created
// moments earlier be adopted as if it were not there.
func WithQuotaMaterialization(mgr ctrl.Manager, fieldManager string, cover map[corev1.ResourceName]resource.Quantity) AcceleratorQuotaOption {
	return func(r *acceleratorquota.Reconciler) {
		r.Materialize = acceleratorquota.MaterializeOptions{
			Backend: &kueuebackend.Backend{
				Writer: mgr.GetClient(),
				Reader: mgr.GetAPIReader(),
				Options: kueuebackend.Options{
					FieldManager:   fieldManager,
					CoverResources: cover,
				},
			},
		}
	}
}

// SetupAcceleratorQuota registers the quota status controller.
//
// The root name is the compiled-in reserved name, not an option: it is a
// wire-contract identity rather than a tunable, and a suite that overrode it
// would be testing a tree no real cluster can author.
func SetupAcceleratorQuota(_ context.Context, mgr ctrl.Manager, opts ...AcceleratorQuotaOption) error {
	r := &acceleratorquota.Reconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Log:       ctrl.Log.WithName("AcceleratorQuota"),
		APIReader: mgr.GetAPIReader(),
		Options: tree.Options{
			RootName: v1beta1.AcceleratorQuotaRootName,
			MaxDepth: 5,
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup AcceleratorQuota controller: %w", err)
	}
	return nil
}
