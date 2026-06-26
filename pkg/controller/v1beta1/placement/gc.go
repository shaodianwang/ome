package placement

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// DefaultGCInterval is the fallback orphan-sweep cadence used when
// GCReconciler.Interval is unset. The operative value is supplied by the
// manager flag / chart; this is the graceful-degradation default.
const DefaultGCInterval = 5 * time.Minute

// defaultGCPageSize bounds the per-request item count for the paginated
// control-plane sweep list. Fallback only; override via GCReconciler.PageSize.
const defaultGCPageSize = 500

// isvcMetadataList returns an empty metadata-only list typed as ome.io
// InferenceServices, so List deserializes object metadata only (no spec/status).
func isvcMetadataList() *metav1.PartialObjectMetadataList {
	l := &metav1.PartialObjectMetadataList{}
	l.SetGroupVersionKind(v1beta1.SchemeGroupVersion.WithKind("InferenceServiceList"))
	return l
}

// GCReconciler periodically sweeps workload clusters for orphaned derived
// InferenceServices — copies this control plane created whose source ISVC no
// longer exists (source force-deleted before its finalizer ran, or a cluster
// was unreachable during deletion). Safety net on top of the finalizer cleanup
// in the placement Reconciler.
type GCReconciler struct {
	// APIReader reads control-plane InferenceServices UNCACHED. sweep() only
	// reads the control plane (to build the live-UID set) and deletes on remote
	// clusters — it never writes the control plane, so a Reader suffices. Using
	// the uncached reader is a safety guarantee: a cold/empty informer cache must
	// never make sweep classify every derived ISVC as an orphan and delete them.
	APIReader client.Reader
	Log       logr.Logger
	Clusters  ClusterClients
	// Interval is the sweep cadence; defaults to DefaultGCInterval.
	Interval time.Duration
	// PageSize bounds the per-request item count for the paginated control-plane
	// list; defaults to defaultGCPageSize.
	PageSize int64
}

// OrphanedDeriveds returns the derived ISVCs whose origin-UID is not in the set
// of live control-plane ISVC UIDs. Operates on metadata only — orphan
// classification and deletion need just the annotations and name/namespace.
func OrphanedDeriveds(liveUIDs map[string]bool, derived []metav1.PartialObjectMetadata) []metav1.PartialObjectMetadata {
	var orphans []metav1.PartialObjectMetadata
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
// origin-labeled derived ISVCs whose source is gone. Both lists are
// metadata-only so full specs/status are never deserialized.
func (g *GCReconciler) sweep(ctx context.Context) error {
	pageSize := g.PageSize
	if pageSize <= 0 {
		pageSize = defaultGCPageSize
	}
	live := map[string]bool{}
	continueToken := ""
	for {
		cpList := isvcMetadataList()
		if err := g.APIReader.List(ctx, cpList, client.Limit(pageSize), client.Continue(continueToken)); err != nil {
			return fmt.Errorf("gc: list control-plane ISVCs: %w", err)
		}
		for i := range cpList.Items {
			live[string(cpList.Items[i].UID)] = true
		}
		continueToken = cpList.Continue
		if continueToken == "" {
			break
		}
	}

	for _, cluster := range g.Clusters.Connected() {
		cl, ok := g.Clusters.ClientFor(cluster)
		if !ok {
			continue
		}
		derivedList := isvcMetadataList()
		if err := cl.List(ctx, derivedList, client.HasLabels{PlacementOriginLabel}); err != nil {
			g.Log.Error(err, "gc: list derived ISVCs", "cluster", cluster)
			continue
		}
		for _, orphan := range OrphanedDeriveds(live, derivedList.Items) {
			o := orphan
			o.SetGroupVersionKind(v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
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
		interval = DefaultGCInterval
	}
	// Sweep once on startup (APIReader is uncached, so this is safe immediately)
	// rather than waiting a full interval to reap orphans after a restart.
	if err := g.sweep(ctx); err != nil {
		g.Log.Error(err, "gc: initial sweep failed")
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
