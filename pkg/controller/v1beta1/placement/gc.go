package placement

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

const defaultGCInterval = 5 * time.Minute

// GCReconciler periodically sweeps workload clusters for orphaned derived
// InferenceServices — copies this control plane created whose source ISVC no
// longer exists (source force-deleted before its finalizer ran, or a cluster
// was unreachable during deletion). Safety net on top of the finalizer cleanup
// in the placement Reconciler.
type GCReconciler struct {
	client.Client // control-plane client
	Log           logr.Logger
	Clusters      ClusterClients
	// Interval is the sweep cadence; defaults to defaultGCInterval.
	Interval time.Duration
}

// OrphanedDeriveds returns the derived ISVCs whose origin-UID is not in the set
// of live control-plane ISVC UIDs.
func OrphanedDeriveds(liveUIDs map[string]bool, derived []v1beta1.InferenceService) []v1beta1.InferenceService {
	var orphans []v1beta1.InferenceService
	for i := range derived {
		uid := derived[i].Annotations[PlacementOriginUIDAnnotation]
		if uid == "" {
			continue // not a placement-derived object we own
		}
		if !liveUIDs[uid] {
			orphans = append(orphans, derived[i])
		}
	}
	return orphans
}

// sweep lists live CP ISVC UIDs, then for each connected cluster deletes
// origin-labeled derived ISVCs whose source is gone.
func (g *GCReconciler) sweep(ctx context.Context) error {
	cpList := &v1beta1.InferenceServiceList{}
	if err := g.List(ctx, cpList); err != nil {
		return fmt.Errorf("gc: list control-plane ISVCs: %w", err)
	}
	live := make(map[string]bool, len(cpList.Items))
	for i := range cpList.Items {
		live[string(cpList.Items[i].UID)] = true
	}

	for _, cluster := range g.Clusters.Connected() {
		cl, ok := g.Clusters.ClientFor(cluster)
		if !ok {
			continue
		}
		derivedList := &v1beta1.InferenceServiceList{}
		if err := cl.List(ctx, derivedList, client.HasLabels{PlacementOriginLabel}); err != nil {
			g.Log.Error(err, "gc: list derived ISVCs", "cluster", cluster)
			continue
		}
		for _, orphan := range OrphanedDeriveds(live, derivedList.Items) {
			o := orphan
			if err := cl.Delete(ctx, &o); err != nil && !apierrors.IsNotFound(err) {
				g.Log.Error(err, "gc: delete orphan derived", "cluster", cluster, "isvc", o.Namespace+"/"+o.Name)
				continue
			}
			g.Log.Info("gc: deleted orphan derived ISVC", "cluster", cluster, "isvc", o.Namespace+"/"+o.Name)
		}
	}
	return nil
}

// Start runs the periodic sweep until ctx is cancelled. Implements
// manager.Runnable.
func (g *GCReconciler) Start(ctx context.Context) error {
	interval := g.Interval
	if interval <= 0 {
		interval = defaultGCInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := g.sweep(ctx); err != nil {
				g.Log.Error(err, "gc: sweep failed")
			}
		}
	}
}
