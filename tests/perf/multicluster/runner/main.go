// Command runner drives the multi-cluster PLACEMENT performance harness.
//
// It stands up one control-plane envtest instance plus N member (workload)
// envtest instances IN A SINGLE PROCESS, wires the real WorkloadCluster +
// placement controllers on the control plane and a member-side admitter on
// each member (standing in for Kueue + the worker reconciler), then generates
// a data-driven load of source InferenceServices that fan out across the
// members, race, pick a winner, sweep losers, and flow status back.
//
// The headline measurement is the cross-cluster status-convergence delta
// between the watch-FUNNEL (event-driven) and POLL-only paths: the runner runs
// the SAME scenario twice — once with the funnel enabled (+ derived-ISVC cache,
// long safety requeue) and once with it disabled (short safety requeue,
// poll-only) — and reports both convergence latencies plus their delta. It also
// records placement latency (create -> Placed) bucketed by candidate count,
// time-to-first-fan-out, race-to-win, and per-pass remote round-trips
// (op-counted on the per-cluster clients). On completion it writes summary.yaml
// + cmd.stats.yaml, which the checker asserts against.
//
// Everything behavioral is a flag or comes from the generator YAML; there are
// no magic in-code defaults for member counts, admit delays, intervals,
// dispatcher mode, or the funnel comparison windows.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	zaplog "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workloadcluster"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/util"
	"github.com/ome-projects/ome/tests/perf/internal/opcount"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/admitter"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/clusters"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/config"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/countingclusters"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/generator"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/observer"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/recorder"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/stats"
)

var (
	outputDir       = flag.String("o", "", "output directory for summary.yaml + cmd.stats.yaml (required)")
	generatorConfig = flag.String("generator-config", "", "path to the generator config YAML (required)")
	loadNamespace   = flag.String("namespace", "perf-mc", "namespace the harness creates source InferenceServices in")

	timeout = flag.Duration("timeout", 10*time.Minute, "max time to wait for ONE pass to converge")

	// Member admit-loop and admitter concurrency. The admit DELAY is per-member
	// config (it drives the race outcome); these are throughput knobs for the
	// fake admitter, not OME behavior.
	admitterConcurrency = flag.Int("admitter-concurrency", 10, "MaxConcurrentReconciles for each member's admitter")

	// Recorder intake buffer. Bounded so a saturated recorder drops (and counts)
	// events rather than back-pressuring the observer.
	recorderBuffer = flag.Int("recorder-buffer", 4096, "bounded intake-channel depth for the recorder")

	// Funnel event-buffer slack: multiplier on (sources x members) for the
	// funnel's event-channel depth, so a creation burst's per-derived add/update
	// events (and a few re-fires) are buffered rather than dropped. A dropped
	// funnel event falls back to the long safety requeue and would understate the
	// funnel's convergence win, so the harness sizes the buffer generously rather
	// than relying on the production default (which is tuned for steady state,
	// not a synthetic burst). This is a measurement-fidelity sizing knob, not OME
	// behavior.
	funnelBufferSlack = flag.Int("funnel-buffer-slack", 8, "funnel event-channel depth multiplier over sources*members")

	// How long to wait for every member's WorkloadCluster to reach Ready=True at
	// the start of each pass before generating load.
	clusterReadyTimeout = flag.Duration("cluster-ready-timeout", 2*time.Minute, "max wait for all WorkloadClusters Ready before a pass")
)

func main() {
	opts := zap.Options{
		TimeEncoder: zapcore.RFC3339NanoTimeEncoder,
		ZapOpts:     []zaplog.Option{zaplog.AddCaller()},
		Development: true,
		Level:       zaplog.NewAtomicLevelAt(zapcore.InfoLevel),
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	log := zap.New(zap.UseFlagOptions(&opts))
	ctrl.SetLogger(log)

	if err := run(ctrl.LoggerInto(context.Background(), log)); err != nil {
		log.Error(err, "runner failed")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx)

	if *outputDir == "" || *generatorConfig == "" {
		return fmt.Errorf("both -o and -generator-config are required")
	}
	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		return fmt.Errorf("creating output dir: %w", err)
	}

	cfg, err := config.LoadConfig(*generatorConfig)
	if err != nil {
		return err
	}
	log.Info("loaded generator config",
		"members", len(cfg.Members), "classes", len(cfg.Classes),
		"sourcesPerPass", cfg.Total(), "dispatcher", cfg.Placement.DispatcherMode)

	memberNames := make([]string, len(cfg.Members))
	for i, m := range cfg.Members {
		memberNames[i] = m.Name
	}

	startWall := time.Now()

	// Stand up the control plane + members (two-phase scheme guard) and seed the
	// member WorkloadClusters/Secrets on the control plane.
	topo, err := clusters.BringUp(ctx, memberNames, cfg.Placement.AcceleratorLabel, cfg.Placement.AcceleratorValue)
	if err != nil {
		return err
	}
	defer topo.Teardown()

	if err := topo.EnsureNamespaceEverywhere(ctx, *loadNamespace); err != nil {
		return err
	}

	// Start each member's admitter once (it persists across both passes). The
	// admit timestamps feed BOTH passes' recorders via a swappable sink so the
	// admitter does not need restarting per pass.
	sinkRouter := &admitSinkRouter{}
	if err := startMemberAdmitters(ctx, topo, cfg, sinkRouter); err != nil {
		return err
	}

	// Pass 1: funnel ENABLED (event-driven convergence, long safety requeue).
	funnelRes, funnelOps, err := runPass(ctx, topo, cfg, sinkRouter, true)
	if err != nil {
		return fmt.Errorf("funnel-enabled pass: %w", err)
	}
	log.Info("funnel-enabled pass complete",
		"placed", funnelRes.TotalPlaced, "remoteOps", funnelOps.RemoteTotal, "localOps", funnelOps.LocalTotal)

	// Pass 2: funnel DISABLED (poll-only convergence, short safety requeue).
	pollRes, pollOps, err := runPass(ctx, topo, cfg, sinkRouter, false)
	if err != nil {
		return fmt.Errorf("funnel-disabled pass: %w", err)
	}
	log.Info("funnel-disabled pass complete",
		"placed", pollRes.TotalPlaced, "remoteOps", pollOps.RemoteTotal, "localOps", pollOps.LocalTotal)

	wallMs := time.Since(startWall).Milliseconds()

	summary := recorder.BuildSummary(
		len(cfg.Members), string(cfg.Placement.DispatcherMode),
		funnelRes, funnelOps,
		pollRes, pollOps,
		sinkRouter.dropped(),
	)

	if err := recorder.WriteSummary(filepath.Join(*outputDir, "summary.yaml"), summary); err != nil {
		return fmt.Errorf("writing summary: %w", err)
	}
	if err := writeCmdStats(filepath.Join(*outputDir, "cmd.stats.yaml"), wallMs); err != nil {
		return fmt.Errorf("writing cmd stats: %w", err)
	}

	log.Info("run complete",
		"wallMs", wallMs,
		"convergenceDeltaMeanMs", summary.ConvergenceDeltaMeanMs,
		"convergenceDeltaP95Ms", summary.ConvergenceDeltaP95Ms,
		"funnelConvergenceMeanMs", summary.FunnelEnabled.Convergence.Mean,
		"pollConvergenceMeanMs", summary.FunnelDisabled.Convergence.Mean,
		"summary", filepath.Join(*outputDir, "summary.yaml"))
	return nil
}

// startMemberAdmitters wires one admitter onto each member's manager, with that
// member's configured admit delay. The admitters run for the whole process; the
// sink they report admit timestamps to is the swappable router so both passes
// can collect convergence data without restarting the members.
func startMemberAdmitters(ctx context.Context, topo *clusters.Topology, cfg *config.Config, sink admitter.AdmitSink) error {
	for i, m := range cfg.Members {
		member := topo.Members[i]
		delay := time.Duration(m.AdmitDelayMs) * time.Millisecond
		conc := *admitterConcurrency
		err := member.StartManager(ctx, func(_ context.Context, mgr ctrl.Manager) error {
			return admitter.New(mgr.GetClient(), delay, conc, time.Now, sink).SetupWithManager(mgr)
		})
		if err != nil {
			return fmt.Errorf("member %q: start admitter: %w", m.Name, err)
		}
	}
	return nil
}

// runPass runs ONE measured pass against the already-stood-up topology: it
// starts a fresh control-plane manager (WorkloadCluster reconciler + placement
// reconciler, the latter funnel-on or funnel-off per funnelEnabled), waits for
// every WorkloadCluster to reconnect Ready, points the admit-sink router at this
// pass's recorder, generates the source ISVCs, waits for convergence, then stops
// the control-plane manager. It returns the pass's recorder result and the
// per-cluster remote op counts accumulated during the pass.
func runPass(ctx context.Context, topo *clusters.Topology, cfg *config.Config, router *admitSinkRouter, funnelEnabled bool) (recorder.PassResult, recorder.PassOps, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("funnelEnabled", funnelEnabled)

	rec := recorder.New(*timeout, cfg.Total(), *recorderBuffer)
	// Separate ledgers: remote (member-apiserver) round-trips are the headline
	// fan-out metric; local (control-plane) ops are bookkeeping kept distinct.
	remoteCounts := opcount.NewCounts()
	localCounts := opcount.NewCounts()
	ops := func() recorder.PassOps {
		return recorder.PassOps{
			Remote:      remoteCounts.Flat(),
			RemoteTotal: remoteCounts.Total(),
			LocalTotal:  localCounts.Total(),
		}
	}

	// Route this pass's admit timestamps into this pass's recorder. The members'
	// admitters keep running; only the sink target changes.
	router.set(rec)
	defer router.clear()

	// Start the control-plane manager for this pass.
	if err := startControlPlane(ctx, topo, cfg, rec, remoteCounts, localCounts, funnelEnabled); err != nil {
		return recorder.PassResult{}, recorder.PassOps{}, fmt.Errorf("start control plane: %w", err)
	}
	defer func() {
		if err := topo.Control.StopManager(context.Background()); err != nil {
			log.Error(err, "stopping control-plane manager (non-fatal)")
		}
	}()

	// Wait for every member's WorkloadCluster to (re)connect Ready before
	// generating, so every member is an eligible candidate from the first source.
	if err := topo.WaitWorkloadClustersReady(ctx, *clusterReadyTimeout, 200*time.Millisecond); err != nil {
		return recorder.PassResult{}, recorder.PassOps{}, err
	}
	log.Info("all WorkloadClusters Ready; generating load")

	// Recorder loop on a cancellable child context so an early generate error can
	// stop it without waiting out the convergence timeout. Returns when every
	// source is Placed or the timeout trips.
	recCtx, cancelRec := context.WithCancel(ctx)
	defer cancelRec()
	recDone := make(chan error, 1)
	go func() { recDone <- rec.Run(recCtx) }()

	// Generate this pass's source ISVCs (name-suffixed so the two passes don't
	// collide on the shared control apiserver).
	suffix := passSuffix(funnelEnabled)
	gen := generator.New(topo.Control.Client(), cfg, *loadNamespace, suffix, rec)
	if err := gen.Generate(ctx); err != nil {
		cancelRec()
		<-recDone
		return recorder.PassResult{}, ops(), fmt.Errorf("generating load: %w", err)
	}

	runErr := <-recDone
	res := rec.Result()
	if runErr != nil {
		return res, ops(), fmt.Errorf("pass did not converge: %w", runErr)
	}
	return res, ops(), nil
}

// startControlPlane wires + starts the control-plane manager for one pass. It
// reuses components.SetupWorkloadCluster for the cluster Manager + remote cache
// (enabling the derived-ISVC cache only when the funnel is on, since the funnel
// registers a cache event handler), then wires the placement Reconciler
// DIRECTLY so the harness can: (a) wrap the local client AND the per-cluster
// remote clients with op-counting, and (b) drive the dispatcher / requeue /
// funnel knobs from config. This mirrors cmd/manager's wiring shape while
// adding only measurement.
func startControlPlane(ctx context.Context, topo *clusters.Topology, cfg *config.Config, obs *recorder.Recorder, remoteCounts, localCounts *opcount.Counts, funnelEnabled bool) error {
	id := cfg.Placement.ControlPlaneID

	return topo.Control.StartManager(ctx, func(setupCtx context.Context, mgr ctrl.Manager) error {
		// Cluster Manager + WorkloadCluster reconciler. The derived-ISVC cache is
		// the funnel's hard dependency, so enable it only in the funnel pass.
		wcOpts := []components.WorkloadClusterOption{
			components.WithClusterHealthInterval(durMs(cfg.Placement.ClusterHealthIntervalMs)),
		}
		if funnelEnabled {
			wcOpts = append(wcOpts, components.WithDerivedISVCCache(id))
		}
		clusterMgr, err := components.SetupWorkloadCluster(setupCtx, mgr, wcOpts...)
		if err != nil {
			return fmt.Errorf("SetupWorkloadCluster: %w", err)
		}

		// Placement Reconciler, wired directly. Local (control-plane) client
		// op-counted into localCounts; the per-cluster remote clients op-counted
		// into remoteCounts via the counting ClusterClients wrapper.
		// The control plane carries the same inferenceservice-config the manager
		// binary requires; placement decodes member rows under its bound. The
		// manager cache is not started yet, so the seed uses a direct client.
		directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
		if err != nil {
			return fmt.Errorf("build direct client: %w", err)
		}
		if err := util.SeedDefaultConfigMaps(setupCtx, directClient); err != nil {
			return fmt.Errorf("seed inferenceservice-config: %w", err)
		}
		clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("build clientset: %w", err)
		}
		instanceStatusDecoder, err := components.InstanceStatusDecoderFromConfig(clientset)
		if err != nil {
			return fmt.Errorf("load omenativeStatus configuration: %w", err)
		}
		r := &placement.Reconciler{
			Client:                  opcount.NewClient(mgr.GetClient(), localCounts),
			InstanceStatusDecoder:   instanceStatusDecoder,
			Scheme:                  mgr.GetScheme(),
			Log:                     ctrl.Log.WithName("Placement"),
			Clusters:                countingclusters.New(clusterMgr, remoteCounts),
			Requeue:                 durMs(cfg.Placement.RequeueMs),
			ControlPlaneID:          id,
			MaxConcurrentReconciles: cfg.Placement.MaxConcurrentReconciles,
			PlaceTimeout:            durMs(cfg.Placement.PlaceTimeoutMs),
			DispatcherMode:          placement.DispatcherMode(cfg.Placement.DispatcherMode),
			DispatcherStepSize:      cfg.Placement.DispatcherStepSize,
			DispatcherRoundTimeout:  durMs(cfg.Placement.DispatcherRoundTimeoutMs),
		}

		var convergeOpts []placement.ConvergeOption
		convergeOpts = append(convergeOpts, placement.WithStatusBatchPeriod(durMs(cfg.Funnel.BatchPeriodMs)))
		if funnelEnabled {
			// Funnel ON: long safety requeue so convergence is attributable to
			// events, plus the funnel channel itself. Size the funnel's event
			// buffer to the whole fan-out fleet (sources x members) so a creation
			// burst's add/update events are not dropped — a dropped event would fall
			// back to the (long) safety requeue and understate the funnel's win.
			convergeOpts = append(convergeOpts, placement.WithStatusSafetyRequeue(durMs(cfg.Funnel.EnabledSafetyRequeueMs)))
			fcfg := placement.FunnelConfigFor(id)
			fcfg.BufferSize = cfg.Total() * len(cfg.Members) * *funnelBufferSlack
			// Notice newly-connected members promptly: they connect just after this
			// manager starts, so a long resync would leave the funnel blind to the
			// first members (and convergence would fall to the safety requeue) for
			// that long.
			fcfg.ResyncInterval = durMs(cfg.Funnel.ResyncIntervalMs)
			funnel := workloadcluster.NewStatusFunnel(clusterMgr, fcfg)
			if err := mgr.Add(funnel); err != nil {
				return fmt.Errorf("add status funnel: %w", err)
			}
			convergeOpts = append(convergeOpts, placement.WithStatusEvents(funnel.Events()))
		} else {
			// Funnel OFF: no funnel channel; the (short) safety requeue IS the
			// convergence cadence (poll-only).
			convergeOpts = append(convergeOpts, placement.WithStatusSafetyRequeue(durMs(cfg.Funnel.DisabledSafetyRequeueMs)))
		}

		if err := r.SetupWithManager(mgr, convergeOpts...); err != nil {
			return fmt.Errorf("placement SetupWithManager: %w", err)
		}

		// Observer: watch the control-plane sources and time their placement
		// transitions. Every member is a candidate for every source in this
		// harness, so the candidate count is the member count.
		if err := observer.New(obs, len(cfg.Members)).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("observer SetupWithManager: %w", err)
		}
		return nil
	})
}

// admitSinkRouter forwards admit timestamps to the recorder of the pass that is
// currently running. The members' admitters hold a single sink for the whole
// process, but each pass has its own recorder; the router swaps the live target
// between passes and counts any stamp that arrives with no active recorder (a
// late admit from the previous pass) so those are not silently lost. It also
// accumulates each pass's recorder drop count so the summary can report a single
// saturation number across both passes.
type admitSinkRouter struct {
	mu        sync.Mutex
	target    *recorder.Recorder
	orphans   uint64
	totalDrop uint64
}

func (s *admitSinkRouter) set(r *recorder.Recorder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.target = r
}

func (s *admitSinkRouter) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.target != nil {
		s.totalDrop += s.target.DroppedEvents()
	}
	s.target = nil
}

// RecordAdmitted implements admitter.AdmitSink: forward to the active pass's
// recorder, or count an orphan when a stamp arrives between passes.
func (s *admitSinkRouter) RecordAdmitted(key types.NamespacedName, at time.Time) {
	s.mu.Lock()
	target := s.target
	if target == nil {
		s.orphans++
	}
	s.mu.Unlock()
	if target != nil {
		target.RecordAdmitted(key, at)
	}
}

// dropped reports recorder-side drops across both passes plus router orphans, so
// the checker's saturation gate sees a single number.
func (s *admitSinkRouter) dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalDrop + s.orphans
}

func passSuffix(funnelEnabled bool) string {
	if funnelEnabled {
		return "funnel"
	}
	return "poll"
}

// durMs converts a millisecond count to a Duration. Zero stays zero so the
// reconciler's own graceful-degradation fallback applies (no magic literal).
func durMs(ms uint) time.Duration { return time.Duration(ms) * time.Millisecond }

// writeCmdStats records the run's wall time plus best-effort user/sys CPU and
// peak RSS (via getrusage in rusage_*.go).
func writeCmdStats(path string, wallMs int64) error {
	cs := cmdResourceUsage()
	cs.WallMs = wallMs
	return stats.Write(path, cs)
}
