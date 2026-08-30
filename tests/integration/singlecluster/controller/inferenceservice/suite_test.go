// Package inferenceservice hosts integration tests for the
// InferenceService reconciler driven against envtest via the shared
// framework.
package inferenceservice

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	mcfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestInferenceServiceController is the Go entrypoint Ginkgo hooks into.
func TestInferenceServiceController(t *testing.T) {
	util.RunSuite(t, "InferenceService Controller Integration Suite")
}

// Suite-wide state populated by BeforeSuite, used by every Describe.
var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
	// suiteFakeMC is exposed package-private so Sharded-using specs can
	// re-script CheckSourceFunc / InspectSourceFunc per-test. PVC and
	// per-node specs never invoke this — their CBMs don't dispatch
	// through the sharded backend.
	suiteFakeMC *mcfake.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// Optional bundles allow each deployment mode's spec to run in the
	// same suite. Bundles are mode-orthogonal: installing LWS does not
	// change RawDeployment behavior (annotation + Leader/Worker presence
	// are the controller-side mode triggers).
	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{
			framework.LWS,
			framework.KEDA,
			// EnvoyGateway gives the traffic translator a
			// real BackendTrafficPolicy CRD to write against. Tests
			// without spec.traffic / ome.io/* annotations see
			// HasIntent=false and the translator is never invoked,
			// so other suites in this directory are unaffected.
			framework.EnvoyGateway,
			// GatewayAPI registers HTTPRoute (scheme + CRD) so specs
			// that apply HTTPRoute objects produced by the builder
			// can read them back from envtest.
			framework.GatewayAPI,
			// PrometheusOperator registers PodMonitor (scheme + CRD).
			// The ISVC RawDeployment path unconditionally reconciles a
			// PodMonitor — without this the SetControllerReference
			// call fails with "no kind is registered for the type v1.PodMonitor".
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
	// Single provider instance shared by both reconcilers:
	//   - BaseModel reconciler uses the Client to drive Sharded CBMs
	//     through CheckSource/InspectSource → Ready.
	//   - ISVC reconciler uses Name (→ runtimeselector.Config.ModelCacheProvider)
	//     and Config (→ InferenceServicesConfig.ModelCache, which
	//     components/base.go reads to inject MODEL_CACHE_PROVIDER /
	//     MODEL_CACHE_ENDPOINT / CLUSTER_CACHE_HEADLESS_SERVICE /
	//     MODEL_CACHE_OPTIONS on Sharded pods).
	// Endpoint is a benign URL so the env-injection assertions in the
	// swap tests have something to check against.
	suiteFakeMC = &mcfake.Client{}
	setSuiteFakeMCDefaults()
	provider := sharded.NewProvider(
		sharded.ProviderClusterCache,
		suiteFakeMC,
		sharded.ProviderConfig{
			Provider: sharded.ProviderClusterCache,
			Endpoint: "http://cluster-cache.clustercache.svc.cluster.local:10090",
		},
	)
	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// Register the OMENative pod field-index on the manager cache,
		// exactly as cmd/manager does. Without it every OMENative ISVC
		// reconcile fails at the coordination step's indexed pod lookup
		// and status never converges.
		if err := query.RegisterOMENativePodIndex(ctx, mgr.GetFieldIndexer()); err != nil {
			return err
		}
		// SetupBaseModel without WithOmeAgentConfig is fine — PVC tests
		// pre-bind their PVCs by hand and never invoke the metadata Job.
		if err := components.SetupBaseModel(ctx, mgr,
			components.WithModelCacheProvider(provider)); err != nil {
			return err
		}
		return components.SetupInferenceService(ctx, mgr,
			components.WithISVCModelCacheProvider(provider))
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

// setSuiteFakeMCDefaults restores the suite's fake mc client to a
// reachable+Llama-config-returning baseline. Idempotent. Sharded specs
// call this in BeforeEach to undo any per-test scripting from prior
// specs (Ginkgo specs in the same Ordered Describe share package state
// since the fake is a suite-level singleton).
func setSuiteFakeMCDefaults() {
	suiteFakeMC.CheckSourceFunc = func(_ context.Context, _ sharded.Source) (*sharded.SourceStatus, error) {
		return &sharded.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
	}
	suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
		return &sharded.SourceInspection{
			Objects: []sharded.Object{
				{Source: src, Path: src.URI + "/config.json"},
			},
			Metadata: sharded.ModelMetadataInputs{
				// Minimal config.json the modelparser accepts.
				// Architecture matches the runtime fixtures the swap
				// tests build.
				ConfigJSON: []byte(`{"model_type":"llama","architectures":["LlamaForCausalLM"]}`),
			},
		}, nil
	}
}

func updateInferenceService(key types.NamespacedName, mutate func(*v1beta1.InferenceService)) {
	gomega.Eventually(func(g gomega.Gomega) {
		latest := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, key, latest)).To(gomega.Succeed())
		mutate(latest)
		g.Expect(k8sClient.Update(suiteCtx, latest)).To(gomega.Succeed())
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(gomega.Succeed())
}

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})
