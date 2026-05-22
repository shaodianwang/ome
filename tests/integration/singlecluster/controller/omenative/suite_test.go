// Package omenative hosts integration tests for the OMENative
// deployment-mode path of the InferenceService reconciler. Tests
// register the full ISVC reconciler (which dispatches into the
// omenative subpackage when the ome.io/deploymentMode annotation
// selects OMENative) and assert behavior end-to-end against envtest.
//
// envtest has no kubelet, so pods created by OMENative never reach
// ContainersReady on their own. The kubelet-simulator helper in
// helpers_test.go watches OMENative-owned pods and synthesizes
// PodScheduled + ContainersReady so the readiness-gate pipeline can
// converge. Without it, every assertion on Ready / Available status
// would time out.
package omenative

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

// TestOMENativeController is the Go entrypoint Ginkgo hooks into.
func TestOMENativeController(t *testing.T) {
	util.RunSuite(t, "OMENative Integration Suite")
}

// Suite-wide state populated by BeforeSuite, used by every Describe.
var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
	// kubeletSim ticks NodeName (via binder) + ContainersReady onto
	// OMENative-owned pods so the gate pipeline can converge against
	// envtest's no-kubelet world. Started in BeforeSuite, stopped in
	// AfterSuite.
	kubeletSim *podKubeletSimulator
	// sliceSim mirrors pod state into EndpointSlices so drain waits +
	// IsPodInRotation converge against envtest's missing
	// endpointslice-controller.
	sliceSim *endpointSliceSimulator
	// suiteBinder is the clientset-backed binding helper. Exported to
	// specs that need to pin a pod to a specific node (e.g. Migrate's
	// FromNode validation) rather than letting the kubelet sim pick.
	suiteBinder *podBinder
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// Match the inferenceservice suite's optional CRD bundle exactly
	// so the controller wiring is identical. PrometheusOperator is
	// required for PodMonitor scheme registration; the others are
	// no-ops for OMENative-only tests but keep the controller's
	// SetupWithManager path identical to the production suite.
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
	// Build a clientset for the binding subresource — controller-
	// runtime's typed client doesn't expose it. Used by the kubelet
	// simulator to assign Spec.NodeName via Bind on otherwise-
	// unscheduled pods.
	cs, err := kubernetes.NewForConfig(s.Cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "kubernetes.NewForConfig")
	suiteBinder = newPodBinder(cs)
	kubeletSim = newPodKubeletSimulator(k8sClient, suiteBinder)
	kubeletSim.Start(suiteCtx)

	ginkgo.By("starting the EndpointSlice simulator to mirror pod state")
	sliceSim = newEndpointSliceSimulator(k8sClient)
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
