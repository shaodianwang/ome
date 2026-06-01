// Package irmanaged_kind exercises the IR-MANAGED OMENative path
// end-to-end against a real KIND cluster (kubelet + scheduler +
// containerd). This is the path that is the production default since
// #320 (IsIRManagedComponent → true): the ISVC controller projects
// each OMENative Component to an InferenceReplica CR, and the
// inferencereplica controller reconciles that CR into pods + status.
//
// It is deliberately a SEPARATE suite/cluster from coordination_kind,
// which disables IR-managed (SetGlobalDisableIRManaged(true)) to
// exercise the legacy direct path. The two paths aggregate status
// differently, so they get different harnesses. Tests here use the
// fake-serving container image built and kind-loaded by RunKindSuite.
package irmanaged_kind

import (
	"context"
	"testing"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	mcfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	inferencereplica "github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Per-spec timeouts tuned for real kubelet latency (mirrors
// coordination_kind/timeouts_test.go).
const (
	kindShortTimeout  = 30 * time.Second
	kindMediumTimeout = 2 * time.Minute
	kindLongTimeout   = 5 * time.Minute
	kindPollInterval  = 100 * time.Millisecond
)

// Suite-wide state populated by RunKindSuite + BeforeSuite.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

// TestIRManagedKindController is the Go entrypoint Ginkgo hooks into.
func TestIRManagedKindController(t *testing.T) {
	// The ISVC controller emits a per-Component PodMonitor on every
	// OMENative reconcile (reconcileOMENativeSubresources runs for BOTH
	// the IR-managed and the legacy path); without the CRD + scheme the
	// apiserver rejects the Create and every reconcile errors.
	//
	// KEDA ScaledObject is now Owns()'d unconditionally by the ISVC
	// controller (PR #337, OEP-0013 Slice 2, decision Q6 — KEDA is a
	// hard cluster-side dependency). Without the scheme + CRD pair in
	// place the controller-runtime cache-sync errors out with "kind must
	// be registered to the Scheme" and the suite never reaches its first
	// spec — matching the coordination_kind suite (PR #349). Without
	// this, irmanaged_kind specs see the controller wedged on a
	// per-reconcile error log ("no kind is registered for the type
	// v1alpha1.ScaledObject in scheme") and the IR aggregator never
	// reaches steady-state Ready.
	scheme := kindutil.BaseScheme()
	utilruntime.Must(monitoringv1.AddToScheme(scheme))
	utilruntime.Must(kedav1.AddToScheme(scheme))

	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: "ome-irmanaged-kind",
		// 1 worker (+ tainted control-plane) = 1 schedulable node. Single-pod
		// surge peaks at 2 pods (old ordinal-0 + surge ordinal-1) with NO
		// anti-node-affinity, so they co-locate on the one worker — fine.
		Workers: 1,
		BuildImages: []kindutil.ImageBuildSpec{
			{
				ContextDir: "tests/integration/kindutil/fake-serving",
				Tag:        "fake-serving:v1",
				BuildArgs:  map[string]string{"VERSION": "v1"},
			},
			{
				ContextDir: "tests/integration/kindutil/fake-serving",
				Tag:        "fake-serving:v2",
				BuildArgs:  map[string]string{"VERSION": "v2"},
			},
		},
		// Default CRDPaths (config/crd/full) + the prometheus-operator
		// PodMonitor CRD + the KEDA ScaledObject CRD. KEDA is a hard
		// cluster-side dependency for the ISVC controller as of #337
		// (OEP-0013 Slice 2, decision Q6) — Owns(&kedav1.ScaledObject{})
		// is unconditional, so the CRD must be installed for cache-sync
		// to succeed even when individual IR-managed specs don't
		// exercise the autoscaling path. Mirrors coordination_kind's
		// CRDPaths exactly so both suites stay in lockstep on this
		// shared cluster-side prerequisite.
		CRDPaths: []string{
			"config/crd/full",
			"tests/integration/dep-crds/prometheus-operator",
			"tests/integration/dep-crds/keda",
		},
		Scheme: scheme,
		Setup:  mgrSetup,
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "IR-Managed KIND Suite")
}

// mgrSetup wires up the BaseModel + InferenceService + InferenceReplica
// reconcilers against the KIND apiserver, with IR-managed ENABLED.
func mgrSetup(ctx context.Context, mgr ctrl.Manager) error {
	// Explicitly ENABLE IR-managed (the production default). The atomic
	// is process-wide; set it to false defensively so a sibling suite
	// that disabled it can't leak its state into this binary. With IR
	// enabled, irprojector.IsIRManagedComponent(OMENative, false) → true,
	// so the ISVC controller projects each OMENative Component to an
	// InferenceReplica CR instead of running the legacy direct path.
	irprojector.SetGlobalDisableIRManaged(false)

	// Seed the inferenceservice-config ConfigMap so the reconciler can
	// read it at first reconcile. Direct (uncached) client — the manager
	// cache isn't started during mgrSetup.
	directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		return err
	}
	if err := util.SeedDefaultConfigMaps(ctx, directClient); err != nil {
		return err
	}

	// Fake model-cache backend — every source comes back Reachable with a
	// llama-shape config.json (same as coordination_kind; the IR path
	// asserts on controller-side state, not model-cache fidelity).
	fakeMC := &mcfake.Client{
		CheckSourceFunc: func(_ context.Context, _ sharded.Source) (*sharded.SourceStatus, error) {
			return &sharded.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
		},
		InspectSourceFunc: func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			return &sharded.SourceInspection{
				Objects: []sharded.Object{{Source: src, Path: src.URI + "/config.json"}},
				Metadata: sharded.ModelMetadataInputs{
					ConfigJSON: []byte(`{"model_type":"llama","architectures":["LlamaForCausalLM"]}`),
				},
			}, nil
		},
	}
	provider := sharded.NewProvider(
		sharded.ProviderClusterCache,
		fakeMC,
		sharded.ProviderConfig{
			Provider: sharded.ProviderClusterCache,
			Endpoint: "http://cluster-cache.clustercache.svc.cluster.local:10090",
		},
	)

	if err := components.SetupBaseModel(ctx, mgr,
		components.WithModelCacheProvider(provider)); err != nil {
		return err
	}
	if err := components.SetupInferenceService(ctx, mgr,
		components.WithISVCModelCacheProvider(provider)); err != nil {
		return err
	}

	// The IR-managed path's second half: the inferencereplica controller
	// reconciles the projected IR CRs into pods + status. Nothing under
	// tests/ registers this today — mirror cmd/manager/main.go:393-402.
	return (&inferencereplica.Reconciler{
		Client:    mgr.GetClient(),
		Log:       ctrl.Log.WithName("InferenceReplica"),
		APIReader: mgr.GetAPIReader(),
		Recorder:  mgr.GetEventRecorderFor("InferenceReplica"),
	}).SetupWithManager(mgr)
}
