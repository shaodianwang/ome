// Package coordination hosts integration tests for the OEP-0007.1
// cross-Component rollout coordination layer (per-revision Services,
// generation-scoped peer discovery, Status.Components.<c>.Traffic[]
// producer, four coordination policies + two pacing types).
//
// Tests register the full InferenceService reconciler (which dispatches
// into the omenative subpackage when the ome.io/deploymentMode
// annotation selects OMENative, then invokes coordination.Reconcile in
// Step 6a) and assert behavior end-to-end against envtest.
//
// envtest has no kubelet or endpointslice-controller, so the suite
// shares the same kubelet + EndpointSlice + binder simulators the
// OMENative integration suite uses — they live in
// tests/integration/util/simulators.go.
//
// Test-case naming follows the plan in
// .claude/specs/2026-05-24-oep-0007.1-integration-test-spec.md: each
// case carries a short code (W-1, SVC-3, T-11, D-2, A-4, PM-5, S-9,
// I-1, R-1, PC-1, P-1, M-1, MUT-1, CON-1, UH-1, EC-1, CLEAN-1,
// LIFE-1, MULTI-1, CF-1, E2E-1, REG-1) so failures map directly to a
// spec row and (where applicable) a wiring-gap fix commit on this PR.
package coordination

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	mcfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestCoordinationController is the Go entrypoint Ginkgo hooks into.
func TestCoordinationController(t *testing.T) {
	util.RunSuite(t, "OEP-0007.1 Coordination Integration Suite")
}

// Suite-wide state populated by BeforeSuite, used by every Describe.
var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
	// kubeletSim ticks NodeName (via binder) + ContainersReady onto
	// OMENative-owned pods so the gate pipeline can converge against
	// envtest's no-kubelet world. Required for any coordination test
	// that asserts on Ready / Available transitions (most of them).
	kubeletSim *util.PodKubeletSimulator
	// sliceSim mirrors pod state into EndpointSlices so drain waits +
	// IsPodInRotation converge. Required for coordination tests that
	// assert on Status.Components.<c>.Traffic[] availability counts.
	sliceSim *util.EndpointSliceSimulator
	// suiteBinder is the clientset-backed binding helper. Available to
	// specs that need to pin a pod to a specific node rather than
	// letting the kubelet simulator's round-robin pick.
	suiteBinder *util.PodBinder
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// Match the inferenceservice + omenative suites' optional CRD
	// bundle so controller wiring is identical across all three.
	// PrometheusOperator is required for PodMonitor scheme
	// registration; the others are no-ops for coordination-only tests
	// but keep the controller's SetupWithManager path identical to the
	// production suite.
	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{
			framework.LWS,
			framework.KEDA,
			framework.EnvoyGateway,
			framework.GatewayAPI,
			framework.PrometheusOperator,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	ginkgo.By("seeding inferenceservice-config so the reconciler can read it at reconcile time")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())

	ginkgo.By("registering BaseModel + InferenceService reconcilers with a cluster_cache provider")
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
	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		if err := components.SetupBaseModel(ctx, mgr,
			components.WithModelCacheProvider(provider)); err != nil {
			return err
		}
		return components.SetupInferenceService(ctx, mgr,
			components.WithISVCModelCacheProvider(provider))
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())

	ginkgo.By("starting the kubelet simulator for OMENative-owned pods")
	cs, err := kubernetes.NewForConfig(s.Cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "kubernetes.NewForConfig")
	suiteBinder = util.NewPodBinder(cs)
	kubeletSim = util.NewPodKubeletSimulator(k8sClient, suiteBinder)
	// Coordination specs assert on full bump-cycle convergence (E2E-1,
	// REG-1 update path, etc.), which requires podRuntimeImagesMatch to
	// eventually flip True. Without the image-status mirror, in-place
	// updates stall at that gate until the InstanceReadyTimeout (30m
	// default) trips fallback — way past any spec timeout. The OMENative
	// timeouts suite deliberately leaves this off to exercise the
	// deadline-fallback path.
	kubeletSim.SetImageStatusMirror(true)
	kubeletSim.Start(suiteCtx)

	ginkgo.By("starting the EndpointSlice simulator to mirror pod state")
	sliceSim = util.NewEndpointSliceSimulator(k8sClient)
	sliceSim.Start(suiteCtx)
})

var _ = ginkgo.AfterSuite(func() {
	if sliceSim != nil {
		sliceSim.Stop()
	}
	if kubeletSim != nil {
		kubeletSim.Stop()
	}
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})
