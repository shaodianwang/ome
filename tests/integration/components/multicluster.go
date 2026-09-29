package components

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/acceleratorquota"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
	"github.com/ome-projects/ome/pkg/quota/tree"
)

// ClusterManager is the shared cross-cluster transport. Both the
// WorkloadCluster reconciler (which connects/disconnects clusters) and the
// placement reconciler (which reads the live per-cluster clients) operate on the
// SAME instance — exactly as cmd/manager wires them — so the placer sees the
// connections the registry establishes. SetupWorkloadCluster constructs one and
// returns it for SetupPlacement to consume.
type ClusterManager = workloadcluster.Manager

// WorkloadClusterOption mutates the WorkloadCluster reconciler setup before it is
// registered. Functional-options pattern keeps SetupWorkloadCluster's signature
// stable as the reconciler accumulates dependencies (matches the other
// components/* Setup funcs). Every operative value is supplied by the option;
// there is no hardcoded magic baked in here — unset durations fall through to the
// reconciler's own graceful-degradation defaults.
type WorkloadClusterOption func(*workloadClusterOpts)

type workloadClusterOpts struct {
	healthInterval        time.Duration
	connectionGracePeriod time.Duration
	execPolicy            workloadcluster.ExecCredentialPolicy
	probe                 func(ctx context.Context, kubeconfig []byte) error
	// cacheControlPlaneID, when non-nil, enables the selectively-caching remote
	// client for derived InferenceServices scoped to that control-plane identity.
	cacheControlPlaneID *string
}

// WithClusterHealthInterval sets the WorkloadCluster re-probe cadence; suites
// pass a small value for a tight loop. Unset uses the reconciler default.
func WithClusterHealthInterval(d time.Duration) WorkloadClusterOption {
	return func(o *workloadClusterOpts) { o.healthInterval = d }
}

// WithClusterConnectionGracePeriod sets how long a previously-reachable cluster
// tolerates transient probe failures before it is flipped to Ready=False; a
// non-positive value disables grace. Unset uses the reconciler default.
func WithClusterConnectionGracePeriod(d time.Duration) WorkloadClusterOption {
	return func(o *workloadClusterOpts) { o.connectionGracePeriod = d }
}

// WithExecCredentialPolicy sets the exec-credential policy applied during
// kubeconfig validation. The zero value (the default) denies exec plugins — the
// safe default the production validator also uses.
func WithExecCredentialPolicy(p workloadcluster.ExecCredentialPolicy) WorkloadClusterOption {
	return func(o *workloadClusterOpts) { o.execPolicy = p }
}

// WithClusterProbe overrides the reachability probe. Unset uses the reconciler's
// real probe (a /version discovery call), which works against envtest member
// clusters. Suites inject a fake here only when they need deterministic
// reachable/unreachable transitions without a live endpoint.
func WithClusterProbe(probe func(ctx context.Context, kubeconfig []byte) error) WorkloadClusterOption {
	return func(o *workloadClusterOpts) { o.probe = probe }
}

// WithDerivedISVCCache enables the selectively-caching remote client for derived
// InferenceServices (origin/control-plane-scoped, Pods banned), mirroring the
// production --workloadcluster-cache flag. It is the status watch funnel's hard
// dependency: the funnel registers a cache event handler on this informer, so a
// suite testing event-driven convergence enables it here and wires
// WithStatusFunnel on the placer with the same control-plane id.
func WithDerivedISVCCache(controlPlaneID string) WorkloadClusterOption {
	return func(o *workloadClusterOpts) { o.cacheControlPlaneID = &controlPlaneID }
}

// SetupWorkloadCluster registers the WorkloadCluster registry controller on the
// supplied manager and returns the shared cluster Manager it wired. The Manager
// is added as a controller-runtime Runnable so it captures the manager's
// long-lived context (the base for remote-client contexts) and disconnects every
// cluster on shutdown — no leaked background watches. Mirrors the cmd/manager
// wiring; the returned Manager MUST be passed to SetupPlacement so the placer
// reads the same live connections.
// SetupQuotaTransport wires the management plane's OWN cross-cluster transport
// and the connector that keeps it in step with the registry.
//
// A second Manager rather than the one SetupWorkloadCluster returns, mirroring
// production: ome-quota-manager is a separate binary, so it cannot share an
// in-process struct with ome-manager, and a suite that shared one would be
// testing a wiring nothing ships. Two Managers is also what surfaces the real
// costs — a second informer set per member, and a second client to keep in step.
//
// Deliberately NO second workloadcluster.Reconciler. That controller writes the
// registry's Ready condition from in-memory grace and backoff state, so a second
// one would flap the condition between two processes and lose the argument
// silently. The connector only reads.
func SetupQuotaTransport(_ context.Context, mgr ctrl.Manager) (*ClusterManager, error) {
	clusterManager := workloadcluster.NewManager(mgr.GetScheme())
	// Cached, and before the Connector: SetCacheOptions only reaches clients
	// built after it, so a member connected first would keep a never-caching
	// one and never deliver an event. Unfiltered because the hub reads two
	// disjoint sets off a member -- its own origin-labelled projections, and
	// the member's unlabelled root -- and one selector covers every cached kind.
	clusterManager.SetCacheOptions(workloadcluster.CacheOptions{
		CachedKinds: sets.New(v1beta1.SchemeGroupVersion.WithKind("AcceleratorQuota").GroupKind()),
	})
	if err := mgr.Add(clusterManager); err != nil {
		return nil, fmt.Errorf("SetupQuotaTransport: add cluster manager runnable: %w", err)
	}
	connector := &acceleratorquota.Connector{
		Client:   mgr.GetClient(),
		Log:      ctrl.Log.WithName("AcceleratorQuotaConnector"),
		Clusters: clusterManager,
	}
	if err := connector.SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("SetupQuotaTransport: SetupWithManager: %w", err)
	}
	return clusterManager, nil
}

// SetupQuotaMemberFunnel watches AcceleratorQuota on every connected member and
// returns the channel its changes arrive on. Wire it into SetupQuotaProjection
// so the hub's fleet view follows the members rather than a clock.
func SetupQuotaMemberFunnel(_ context.Context, mgr ctrl.Manager, clusters *ClusterManager,
) (<-chan event.GenericEvent, error) {
	funnel := workloadcluster.NewStatusFunnel(clusters, workloadcluster.FunnelConfig{
		NewList:   func() client.ObjectList { return &v1beta1.AcceleratorQuotaList{} },
		NewObject: func() client.Object { return &v1beta1.AcceleratorQuota{} },
		// Every change resolves to the root: the pass rebuilds the whole tree
		// whatever woke it, so one key lets a burst collapse into one request.
		Resolve: func(client.Object) (types.NamespacedName, bool) {
			return types.NamespacedName{Name: v1beta1.AcceleratorQuotaRootName}, true
		},
	})
	if err := mgr.Add(funnel); err != nil {
		return nil, fmt.Errorf("SetupQuotaMemberFunnel: add funnel runnable: %w", err)
	}
	return funnel.Events(), nil
}

// SetupQuotaProjection registers the management-mode quota controller: the half
// that splits an authored fleet tree and writes each member's copy through the
// transport SetupQuotaTransport built.
//
// Capacity derivation and Kueue materialization are both left off, which is
// what makes this management mode rather than workload mode -- the hub measures
// no hardware of its own and writes no Kueue object anywhere.
func SetupQuotaProjection(_ context.Context, mgr ctrl.Manager, clusters *ClusterManager,
	origin, fieldManager string, resync time.Duration, memberEvents <-chan event.GenericEvent,
) error {
	r := &acceleratorquota.Reconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Log:       ctrl.Log.WithName("AcceleratorQuotaProjector"),
		APIReader: mgr.GetAPIReader(),
		Options: tree.Options{
			RootName: v1beta1.AcceleratorQuotaRootName,
			MaxDepth: 5,
		},
		ResyncInterval: resync,
		Project: acceleratorquota.ProjectOptions{
			Clusters:      clusters,
			Origin:        origin,
			FieldManager:  fieldManager,
			DefaultPolicy: v1beta1.AcceleratorQuotaDistributionProportional,
		},
	}
	var opts []acceleratorquota.Option
	if memberEvents != nil {
		opts = append(opts, acceleratorquota.WithMemberEvents(memberEvents))
	}
	if err := r.SetupWithManager(mgr, opts...); err != nil {
		return fmt.Errorf("SetupQuotaProjection: SetupWithManager: %w", err)
	}
	return nil
}

func SetupWorkloadCluster(_ context.Context, mgr ctrl.Manager, opts ...WorkloadClusterOption) (*ClusterManager, error) {
	cfg := &workloadClusterOpts{}
	for _, o := range opts {
		o(cfg)
	}

	clusterManager := workloadcluster.NewManager(mgr.GetScheme())
	clusterManager.SetExecCredentialPolicy(cfg.execPolicy)
	if cfg.cacheControlPlaneID != nil {
		// Scope the cached derived-InferenceService informer to exactly the set the
		// status funnel watches/resolves, mirroring cmd/manager.
		clusterManager.SetCacheOptions(workloadcluster.CacheOptions{
			CachedKinds:     sets.New(v1beta1.SchemeGroupVersion.WithKind("InferenceService").GroupKind()),
			DefaultSelector: placement.FunnelConfigFor(*cfg.cacheControlPlaneID).WatchSelector,
		})
	}
	if err := mgr.Add(clusterManager); err != nil {
		return nil, fmt.Errorf("SetupWorkloadCluster: add cluster manager runnable: %w", err)
	}

	r := &workloadcluster.Reconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		Log:                   ctrl.Log.WithName("WorkloadCluster"),
		Manager:               clusterManager,
		ExecPolicy:            cfg.execPolicy,
		HealthInterval:        cfg.healthInterval,
		ConnectionGracePeriod: cfg.connectionGracePeriod,
		Probe:                 cfg.probe,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("SetupWorkloadCluster: SetupWithManager: %w", err)
	}
	return clusterManager, nil
}

// PlacementOption mutates the placement reconciler setup before it is registered.
type PlacementOption func(*placementOpts)

type placementOpts struct {
	capacity       *placement.CapacityConfig
	requeue        time.Duration
	controlPlaneID string
	// funnelControlPlaneID, when non-nil, wires the cross-cluster status watch
	// funnel (requires the derived-ISVC cache enabled on the cluster Manager) so
	// status converges on events rather than the poll.
	funnelControlPlaneID *string
	statusSafetyRequeue  time.Duration
	statusBatchPeriod    time.Duration
	winnerLostGrace      time.Duration
}

// WithCapacityPlacement supplies the hardware placement configuration explicitly.
func WithCapacityPlacement(config *placement.CapacityConfig) PlacementOption {
	return func(o *placementOpts) { o.capacity = config }
}

// WithPlacementRequeue sets the placement poll cadence; suites pass a small
// value. Unset uses the reconciler default.
func WithPlacementRequeue(d time.Duration) PlacementOption {
	return func(o *placementOpts) { o.requeue = d }
}

// WithControlPlaneID stamps the control-plane identity onto every derived ISVC
// so a shared-workload-cluster GC sweep reaps only this control plane's
// deriveds. Empty keeps single-control-plane behavior (no stamping).
func WithControlPlaneID(id string) PlacementOption {
	return func(o *placementOpts) { o.controlPlaneID = id }
}

// WithStatusFunnel wires the cross-cluster status watch funnel over the shared
// cluster Manager, so a derived's status change on a member re-reconciles its
// local source on an event. The Manager MUST have the derived-ISVC cache enabled
// (WithDerivedISVCCache) — the funnel registers a cache event handler. Pass the
// same control-plane id used for the cache so the watch/resolve scoping matches.
func WithStatusFunnel(controlPlaneID string) PlacementOption {
	return func(o *placementOpts) { o.funnelControlPlaneID = &controlPlaneID }
}

// WithStatusSafetyRequeue sets the steady-state status re-read backstop. With the
// funnel on, suites leave this long (the default) so the assertion can only pass
// via events; with the funnel off, suites set it short so the poll fallback
// converges within the test window.
func WithStatusSafetyRequeue(d time.Duration) PlacementOption {
	return func(o *placementOpts) { o.statusSafetyRequeue = d }
}

// WithStatusBatchPeriod sets the funnel-event debounce window. Suites pass a
// small value so a derived-status event enqueues a reconcile promptly.
func WithStatusBatchPeriod(d time.Duration) PlacementOption {
	return func(o *placementOpts) { o.statusBatchPeriod = d }
}

// WithWinnerLostGracePeriod sets how long the placer holds before re-placing once
// the sticky winner's derived is observed absent on a still-connected winner.
// Suites that assert re-placement within the test window must set this below that
// window — the production default (1m) exceeds a typical Eventually timeout.
func WithWinnerLostGracePeriod(d time.Duration) PlacementOption {
	return func(o *placementOpts) { o.winnerLostGrace = d }
}

// SetupPlacement registers the control-plane fan-out (placement) controller on
// the supplied manager, reading the live per-cluster clients from the shared
// cluster Manager returned by SetupWorkloadCluster. Mirrors the cmd/manager
// wiring on the control-plane role. This is the control-plane-only half of the
// pair; member clusters run SetupWorkloadCluster alone (or nothing).
func SetupPlacement(ctx context.Context, mgr ctrl.Manager, clusters *ClusterManager, opts ...PlacementOption) error {
	if clusters == nil {
		return fmt.Errorf("SetupPlacement: nil cluster manager (call SetupWorkloadCluster first)")
	}
	cfg := &placementOpts{}
	for _, o := range opts {
		o(cfg)
	}

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("SetupPlacement: build clientset: %w", err)
	}
	// Placement reads member InferenceReplica rows through the decoder; an
	// unwired decoder fails closed on ColumnarV2 objects.
	instanceStatusDecoder, err := InstanceStatusDecoderFromConfig(clientset)
	if err != nil {
		return fmt.Errorf("SetupPlacement: load omenativeStatus configuration: %w", err)
	}

	r := &placement.Reconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		Log:                   ctrl.Log.WithName("Placement"),
		InstanceStatusDecoder: instanceStatusDecoder,
		Clusters:              clusters,
		Requeue:               cfg.requeue,
		ControlPlaneID:        cfg.controlPlaneID,
		WinnerLostGracePeriod: cfg.winnerLostGrace,
		Capacity:              cfg.capacity,
	}

	var convergeOpts []placement.ConvergeOption
	if cfg.statusBatchPeriod > 0 {
		convergeOpts = append(convergeOpts, placement.WithStatusBatchPeriod(cfg.statusBatchPeriod))
	}
	if cfg.statusSafetyRequeue > 0 {
		convergeOpts = append(convergeOpts, placement.WithStatusSafetyRequeue(cfg.statusSafetyRequeue))
	}
	if cfg.funnelControlPlaneID != nil {
		funnel := workloadcluster.NewStatusFunnel(clusters, placement.FunnelConfigFor(*cfg.funnelControlPlaneID))
		if err := mgr.Add(funnel); err != nil {
			return fmt.Errorf("SetupPlacement: add status funnel: %w", err)
		}
		convergeOpts = append(convergeOpts, placement.WithStatusEvents(funnel.Events()))
	}

	if err := r.SetupWithManager(mgr, convergeOpts...); err != nil {
		return fmt.Errorf("SetupPlacement: SetupWithManager: %w", err)
	}
	return nil
}

// SetupPlacementGC registers the placement orphan-GC Runnable on the manager. It
// periodically sweeps the connected workload clusters for derived ISVCs whose
// source no longer exists, scoped to controlPlaneID so it never reaps another
// control plane's deriveds. Reads the control plane UNCACHED (mgr.GetAPIReader)
// exactly as cmd/manager wires it. interval is the sweep cadence (suites pass a
// small value); a non-positive value falls back to the reconciler default.
func SetupPlacementGC(_ context.Context, mgr ctrl.Manager, clusters *ClusterManager, controlPlaneID string, interval time.Duration) error {
	if clusters == nil {
		return fmt.Errorf("SetupPlacementGC: nil cluster manager (call SetupWorkloadCluster first)")
	}
	if err := mgr.Add(&placement.GCReconciler{
		APIReader:      mgr.GetAPIReader(),
		Log:            ctrl.Log.WithName("PlacementGC"),
		Clusters:       clusters,
		Interval:       interval,
		ControlPlaneID: controlPlaneID,
	}); err != nil {
		return fmt.Errorf("SetupPlacementGC: add GC runnable: %w", err)
	}
	return nil
}
