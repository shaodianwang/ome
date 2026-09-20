package main

import (
	"fmt"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	placementrouting "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
	workloadcluster "github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
)

// mcWiring is the multi-cluster wiring resolved from a MultiClusterConfig: the
// plain values that flow into the WorkloadCluster transport, the placement
// controller, and the endpoint publisher. It is split out of setupMultiCluster
// so the config->values mapping — the part where a mis-wired field would
// silently mis-tune the control plane — is unit-testable without a manager.
//
// Durations are already resolved (the accessors yield 0 on an omitted key, and
// each consuming option then applies its own in-package default).
type mcWiring struct {
	clientTuning      workloadcluster.ClientTuning
	cacheEnabled      bool
	healthInterval    time.Duration
	connectionGrace   time.Duration
	eventsBatchPeriod time.Duration
	reconnectBackoff  workloadcluster.ReconnectBackoffConfig

	// Placement + its status convergence and GC (control plane only).
	requeue                time.Duration
	gcInterval             time.Duration
	maxConcurrent          int
	placeTimeout           time.Duration
	winnerLostGrace        time.Duration
	statusBatchPeriod      time.Duration
	statusSafetyRequeue    time.Duration
	dispatcherMode         placement.DispatcherMode
	dispatcherStepSize     int
	dispatcherRoundTimeout time.Duration
	localQueue             string
	funnelResyncInterval   time.Duration
	funnelBufferSize       int

	endpoint placementendpoint.Config
	routing  placementrouting.Config
}

// resolveMCWiring maps a loaded MultiClusterConfig to the wiring values used to
// build the multi-cluster controllers.
func resolveMCWiring(mc *controllerconfig.MultiClusterConfig) mcWiring {
	wc, pl, ep, rt := mc.WorkloadCluster, mc.Placement, mc.Endpoint, mc.Routing

	// Status-convergence backstop: with the cache (and thus the watch funnel) on,
	// events drive freshness and the safety requeue only recovers a missed event;
	// with it off there is no event source, so the poll cadence (requeueInterval)
	// is the backstop.
	safetyRequeue := pl.StatusSafetyRequeueDuration()
	if !wc.CacheEnabled {
		safetyRequeue = pl.RequeueIntervalDuration()
	}

	return mcWiring{
		clientTuning: workloadcluster.ClientTuning{
			QPS:            float32(wc.ClientQPS),
			Burst:          wc.ClientBurst,
			PerCallTimeout: wc.PerCallTimeoutDuration(),
		},
		cacheEnabled:      wc.CacheEnabled,
		healthInterval:    wc.HealthIntervalDuration(),
		connectionGrace:   wc.ConnectionGraceDuration(),
		eventsBatchPeriod: wc.EventsBatchPeriodDuration(),
		reconnectBackoff: workloadcluster.ReconnectBackoffConfig{
			EstablishInitial: wc.EstablishInitialDuration(),
			EstablishMax:     wc.EstablishMaxDuration(),
			RetryMax:         wc.ReconnectRetryMaxDuration(),
		},
		requeue:                pl.RequeueIntervalDuration(),
		gcInterval:             pl.GCIntervalDuration(),
		maxConcurrent:          pl.MaxConcurrentReconciles,
		placeTimeout:           pl.FanoutTimeoutDuration(),
		winnerLostGrace:        pl.WinnerLostGraceDuration(),
		statusBatchPeriod:      pl.StatusBatchPeriodDuration(),
		statusSafetyRequeue:    safetyRequeue,
		dispatcherMode:         placement.DispatcherMode(pl.DispatcherMode),
		dispatcherStepSize:     pl.DispatcherStepSize,
		dispatcherRoundTimeout: pl.DispatcherRoundTimeoutDuration(),
		localQueue:             pl.LocalQueue,
		funnelResyncInterval:   wc.FunnelResyncIntervalDuration(),
		funnelBufferSize:       wc.FunnelBufferSize,
		endpoint: placementendpoint.Config{
			GlobalHostTemplate: ep.GlobalHostTemplate,
			GlobalGateway:      ep.GlobalGateway,
			RouteNamespace:     ep.RouteNamespace,
			BackendPort:        int32(ep.BackendPort),
			GatewayBackend: placementendpoint.GatewayBackendConfig{
				RewriteHostname: ep.GatewayBackend.RewriteHostname,
				TLS: placementendpoint.GatewayBackendTLSConfig{
					Enabled:                 ep.GatewayBackend.TLS.Enabled,
					WellKnownCACertificates: ep.GatewayBackend.TLS.WellKnownCACertificates,
				},
				EndpointSlices: placementendpoint.GatewayBackendEndpointSliceConfig{
					Enabled:                ep.GatewayBackend.EndpointSlices.Enabled,
					AddressRefreshInterval: ep.GatewayBackend.EndpointSlices.AddressRefreshIntervalDuration(),
				},
			},
		},
		routing: placementrouting.Config{
			Enabled: rt.Enabled,
			Observer: placementrouting.ObserverConfig{
				MaxConcurrentReconciles: rt.Observer.MaxConcurrentReconciles,
				MaxConcurrentRequests:   rt.Observer.MaxConcurrentRequests,
				MaxResponseBytes:        rt.Observer.MaxResponseBytes,
				MinPeriod:               rt.Observer.MinPeriodDuration(),
				MaxSamples:              rt.Observer.MaxSamples,
			},
			Publisher: placementrouting.PublisherConfig{
				Name:           rt.Publisher.Name,
				Options:        rt.Publisher.Options,
				ResyncInterval: rt.Publisher.ResyncIntervalDuration(),
			},
			Probe: placementrouting.ProbeConfig{
				Path:             rt.Probe.Path,
				Method:           rt.Probe.Method,
				AcceptStatuses:   rt.Probe.AcceptStatuses,
				GateStatuses:     rt.Probe.GateStatuses,
				Period:           rt.Probe.PeriodDuration(),
				Timeout:          rt.Probe.TimeoutDuration(),
				FailureThreshold: rt.Probe.FailureThreshold,
				SuccessThreshold: rt.Probe.SuccessThreshold,
				AllFailedPolicy:  placementrouting.AllFailedPolicy(rt.Probe.AllFailedPolicy),
			},
			Capacity: placementrouting.CapacityConfig{
				Path:    rt.Capacity.Path,
				Method:  rt.Capacity.Method,
				Format:  placementrouting.CapacityFormat(rt.Capacity.Format),
				Options: rt.Capacity.Options,
				Samples: rt.Capacity.Samples,
				Quorum:  rt.Capacity.Quorum,
				Period:  rt.Capacity.PeriodDuration(),
				Timeout: rt.Capacity.TimeoutDuration(),
				MaxAge:  rt.Capacity.MaxAgeDuration(),
			},
		},
	}
}

// validateDispatcherMode rejects an unrecognized fan-out policy. The dispatcher
// itself falls back to AllAtOnce for anything it does not recognize, so a typo
// ("incremental" for "Incremental") would otherwise silently select a different
// breadth policy than the operator asked for.
func validateDispatcherMode(mode placement.DispatcherMode) error {
	switch mode {
	case "", placement.DispatcherModeAllAtOnce, placement.DispatcherModeIncremental:
		return nil
	default:
		return fmt.Errorf("invalid multi-cluster configuration: placement.dispatcherMode %q must be %q or %q",
			mode, placement.DispatcherModeAllAtOnce, placement.DispatcherModeIncremental)
	}
}

func newTrafficMapPublisherReconciler(
	kubeClient client.Client,
	apiReader client.Reader,
	publisher *placementrouting.TrafficMapPublisher,
	active bool,
	leaderElectionEnabled bool,
) *placementendpoint.TrafficMapPublisherReconciler {
	return &placementendpoint.TrafficMapPublisherReconciler{
		Client:                kubeClient,
		APIReader:             apiReader,
		Log:                   ctrl.Log.WithName("controllers").WithName("TrafficMapPublisher"),
		Publisher:             publisher.Publisher,
		GlobalOptions:         publisher.GlobalOptions,
		RequeueAfter:          publisher.ResyncInterval,
		Active:                active,
		LeaderElectionEnabled: leaderElectionEnabled,
	}
}

// setupMultiCluster wires the multi-cluster control/transport layer onto mgr:
// the WorkloadCluster registry/transport always, plus — on the control plane —
// the placement (fan-out) controller, its orphan GC, and the global endpoint
// publisher, all sharing the one WorkloadCluster Manager so the placement
// controllers read the live per-cluster clients it connects. Tunables load from
// the inferenceservice-config ConfigMap; topology, identity, and security come
// from options (flags).
func setupMultiCluster(mgr manager.Manager, clientSet kubernetes.Interface, options Options, isControlPlane bool, instanceStatusDecoder irstatus.Decoder) error {
	mcConfig, err := controllerconfig.NewMultiClusterConfig(clientSet)
	if err != nil {
		return fmt.Errorf("load multi-cluster configuration: %w", err)
	}
	// Loading is forgiving by contract (an unusable knob reads as "unset" and the
	// owning package applies its default). Refuse to start on one instead: a
	// silently-ignored knob means the control plane runs with timings the operator
	// did not choose.
	if err := mcConfig.Validate(); err != nil {
		return fmt.Errorf("invalid multi-cluster configuration: %w", err)
	}
	w := resolveMCWiring(mcConfig)
	if err := validateDispatcherMode(w.dispatcherMode); err != nil {
		return err
	}
	// Validate after resolution so the check sees parsed durations. A disabled
	// installation may retain staged observer settings without loading their
	// optional plugins until routing is enabled again.
	if err := w.routing.Validate(); err != nil {
		return fmt.Errorf("invalid multi-cluster configuration: %w", err)
	}

	clusterManager := workloadcluster.NewManager(mgr.GetScheme())
	execPolicy := workloadcluster.ExecCredentialPolicy{
		Allowed:         options.allowExecCredentials,
		AllowedCommands: splitAndTrim(options.execCredentialAllowedCmds),
	}
	clusterManager.SetExecCredentialPolicy(execPolicy)
	clusterManager.SetClientTuning(w.clientTuning)
	if w.cacheEnabled {
		// Scope the cached derived-InferenceService informer to exactly the set the
		// watch funnel watches and resolves: this control plane's deriveds (origin
		// marker, control-plane-scoped when an identity is configured). Sharing one
		// selector keeps the cache from holding another control plane's deriveds on
		// a shared workload cluster and guarantees every object the funnel's cache
		// handler resolves is actually cached.
		clusterManager.SetCacheOptions(workloadcluster.CacheOptions{
			CachedKinds:     sets.New(v1beta1.SchemeGroupVersion.WithKind("InferenceService").GroupKind()),
			DefaultSelector: placement.FunnelConfigFor(options.placementControlPlaneID).WatchSelector,
		})
	}
	// Run the cluster Manager as a Runnable so it captures the controller-manager's
	// long-lived context as the base for remote-client (and cache-informer)
	// contexts, and disconnects everything on shutdown (no leaked watches).
	if err := mgr.Add(clusterManager); err != nil {
		return fmt.Errorf("add WorkloadCluster manager runnable: %w", err)
	}
	setupLog.Info("Setting up WorkloadCluster controller")
	if err := (&workloadcluster.Reconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		Log:                   ctrl.Log.WithName("controllers").WithName("WorkloadCluster"),
		Manager:               clusterManager,
		ExecPolicy:            execPolicy,
		HealthInterval:        w.healthInterval,
		ConnectionGracePeriod: w.connectionGrace,
		ProbeTimeout:          w.clientTuning.PerCallTimeout,
	}).SetupWithManager(mgr,
		workloadcluster.WithEventsBatchPeriod(w.eventsBatchPeriod),
		workloadcluster.WithReconnectBackoff(w.reconnectBackoff),
	); err != nil {
		return fmt.Errorf("create WorkloadCluster controller: %w", err)
	}

	if !isControlPlane {
		return nil
	}
	trafficMapPublisher, err := placementrouting.NewTrafficMapPublisher(
		w.routing.Publisher,
		mgr.GetClient(),
		mgr.GetAPIReader(),
	)
	if err != nil {
		return fmt.Errorf("invalid multi-cluster configuration: %w", err)
	}

	setupLog.Info("Setting up multi-cluster placement (fan-out) controller")
	// Cross-cluster status convergence. The status batch period and safety requeue
	// always apply; when the cache is enabled the watch funnel additionally feeds
	// events so a derived's status change re-reconciles its source on an event
	// rather than waiting for the safety requeue (which is then only the
	// missed-event backstop). Without the cache there is no event source, so
	// convergence stays on the poll cadence (already folded into statusSafetyRequeue).
	convergeOpts := []placement.ConvergeOption{
		placement.WithStatusBatchPeriod(w.statusBatchPeriod),
		placement.WithStatusSafetyRequeue(w.statusSafetyRequeue),
	}
	if w.cacheEnabled {
		funnelCfg := placement.FunnelConfigFor(options.placementControlPlaneID)
		funnelCfg.ResyncInterval = w.funnelResyncInterval
		funnelCfg.BufferSize = w.funnelBufferSize
		funnel := workloadcluster.NewStatusFunnel(clusterManager, funnelCfg)
		if err := mgr.Add(funnel); err != nil {
			return fmt.Errorf("add multi-cluster status funnel runnable: %w", err)
		}
		convergeOpts = append(convergeOpts, placement.WithStatusEvents(funnel.Events()))
	}

	// Derived workloads are only Kueue-gated when they carry a queue label, so
	// an unset queue means placement bypasses quota entirely — worth saying out
	// loud rather than discovering it from admitted pods.
	if w.localQueue == "" {
		setupLog.Info("placement: no localQueue configured; derived workloads will not carry a Kueue queue label and will not be quota-gated")
	}

	if err := (&placement.Reconciler{
		Client:                  mgr.GetClient(),
		APIReader:               mgr.GetAPIReader(),
		InstanceStatusDecoder:   instanceStatusDecoder,
		Scheme:                  mgr.GetScheme(),
		Log:                     ctrl.Log.WithName("controllers").WithName("Placement"),
		Clusters:                clusterManager,
		Requeue:                 w.requeue,
		ControlPlaneID:          options.placementControlPlaneID,
		MaxConcurrentReconciles: w.maxConcurrent,
		PlaceTimeout:            w.placeTimeout,
		WinnerLostGracePeriod:   w.winnerLostGrace,
		DispatcherMode:          w.dispatcherMode,
		DispatcherStepSize:      w.dispatcherStepSize,
		DispatcherRoundTimeout:  w.dispatcherRoundTimeout,
		LocalQueue:              w.localQueue,
	}).SetupWithManager(mgr, convergeOpts...); err != nil {
		return fmt.Errorf("create Placement controller: %w", err)
	}
	if err := mgr.Add(&placement.GCReconciler{
		APIReader:      mgr.GetAPIReader(),
		Log:            ctrl.Log.WithName("controllers").WithName("PlacementGC"),
		Clusters:       clusterManager,
		Interval:       w.gcInterval,
		ControlPlaneID: options.placementControlPlaneID,
	}); err != nil {
		return fmt.Errorf("add placement GC runnable: %w", err)
	}

	// A selected TrafficMap publisher owns the TrafficMap lifecycle directly.
	// Otherwise, retain the default Gateway API path that publishes from the
	// InferenceService placement status.
	if trafficMapPublisher != nil {
		setupLog.Info("Setting up multi-cluster TrafficMap publisher", "publisher", trafficMapPublisher.Publisher.Name(),
			"availableTrafficMapPublishers", placementrouting.RegisteredTrafficMapPublishers())
		if err := newTrafficMapPublisherReconciler(
			mgr.GetClient(),
			mgr.GetAPIReader(),
			trafficMapPublisher,
			w.routing.Enabled,
			options.enableLeaderElection,
		).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("create TrafficMap publisher controller: %w", err)
		}
	} else {
		// The Gateway API scheme is needed only by the default HTTPRoute backend.
		utilruntime.Must(gatewayapiv1.Install(mgr.GetScheme()))
		endpointPublisher := placementendpoint.NewGatewayAPIPublisher(
			mgr.GetClient(),
			w.endpoint,
			placementendpoint.WithBackendAddressResolver(
				placementendpoint.NewGatewayAddressResolver(clusterManager),
			),
		)
		setupLog.Info("Setting up multi-cluster endpoint publisher", "publisher", endpointPublisher.Name(),
			"availableTrafficMapPublishers", placementrouting.RegisteredTrafficMapPublishers())
		if err := (&placementendpoint.Reconciler{
			Client:    mgr.GetClient(),
			Log:       ctrl.Log.WithName("controllers").WithName("PlacementEndpoint"),
			Publisher: endpointPublisher,
			Config:    w.endpoint,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("create PlacementEndpoint controller: %w", err)
		}
	}

	var (
		prober   *placementrouting.Prober
		capacity *placementrouting.CapacityPoller
	)
	if w.routing.IsEnabled() {
		// One process-wide executor bounds all probe and capacity HTTP work. Its
		// queue is derived from the same operator-supplied request limit, keeping
		// both running and waiting observations explicitly bounded.
		executor, err := placementrouting.NewObserverExecutor(
			w.routing.Observer.MaxConcurrentRequests,
			w.routing.Observer.MaxConcurrentRequests,
		)
		if err != nil {
			return fmt.Errorf("create routing observer executor: %w", err)
		}
		if err := mgr.Add(executor); err != nil {
			return fmt.Errorf("add routing observer executor: %w", err)
		}

		// Both observed inputs talk to the same homes with the same credentials,
		// so they share one redirect-refusing HTTP client and transport.
		observerClient := placementrouting.NewObserverClient()
		prober, err = placementrouting.NewProber(
			executor,
			observerClient,
			clock.RealClock{},
			w.routing.Observer.MaxResponseBytes,
			ctrl.Log.WithName("controllers").WithName("PlacementRoutingProbe"),
		)
		if err != nil {
			return fmt.Errorf("create routing endpoint prober: %w", err)
		}
		capacity, err = placementrouting.NewCapacityPoller(
			executor,
			observerClient,
			clock.RealClock{},
			w.routing.Observer.MaxResponseBytes,
			ctrl.Log.WithName("controllers").WithName("PlacementRoutingCapacity"),
		)
		if err != nil {
			return fmt.Errorf("create routing capacity poller: %w", err)
		}
	}

	// Project placement into the capacity-aware TrafficMap the publisher consumes.
	// Always wired: when disabled the controller is a no-op that reaps any
	// TrafficMap it previously created, so toggling the feature (a restart, since
	// config loads once) reverses cleanly without stranding routing tables.
	setupLog.Info("Setting up multi-cluster TrafficMap routing controller", "enabled", w.routing.Enabled,
		// Optional capacity formats are compiled in, so log what this build
		// carries: a binary missing one should be obvious here rather than at
		// the first poll.
		"capacityFormats", placementrouting.RegisteredCapacityFormats())
	if err := (&placementrouting.Reconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Log:       ctrl.Log.WithName("controllers").WithName("PlacementRouting"),
		Config:    w.routing,
		Prober:    prober,
		Capacity:  capacity,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create PlacementRouting controller: %w", err)
	}
	return nil
}
