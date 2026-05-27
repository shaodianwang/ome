// Package coordination_kind is the KIND-backed twin of the envtest
// coordination/ suite. Tests here exercise the OEP-0007.1
// cross-Component rollout coordination layer end-to-end against a
// real kubelet + scheduler + containerd, rather than the
// PodKubeletSimulator the envtest sibling carries.
//
// Test-case naming follows the same code system as coordination/
// (W-1, SVC-3, T-11, D-2, A-4, PM-5, S-9, I-1, R-1, PC-1, P-1, M-1,
// MUT-1, CON-1, UH-1, EC-1, CLEAN-1, LIFE-1, MULTI-1, CF-1, E2E-1,
// REG-1) so failures map to the same spec rows as the envtest suite.
//
// Tests use the fake-serving container image (built and kind-loaded
// by RunKindSuite) instead of the envtest sim's no-op image.
package coordination_kind

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	mcfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Suite-wide state populated by RunKindSuite + BeforeSuite. Specs
// access these directly the way the envtest sibling does — keeps the
// per-spec porting delta minimal.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

// TestCoordinationKindController is the Go entrypoint Ginkgo hooks
// into. Brings up the KIND cluster + OME reconcilers + fake-serving
// image, then runs every Describe in this package.
func TestCoordinationKindController(t *testing.T) {
	// Stash a SuiteState by calling RunKindSuite up front. The
	// BeforeSuite below mirrors suiteCtx / k8sClient out of it into
	// package-level vars so fixture helpers can reference them with
	// the same name the envtest suite uses.
	// OMENative emits a per-Component PodMonitor (added in #231) on
	// every reconcile. Without the prometheus-operator CRD installed
	// the apiserver rejects the Create and the reconciler errors out
	// on every pass; without the scheme registered the controller
	// can't even build the object. Both have to be in place for any
	// OMENative test to make progress.
	scheme := kindutil.BaseScheme()
	utilruntime.Must(monitoringv1.AddToScheme(scheme))

	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: "ome-coord-kind",
		Workers:     0,
		BuildImages: []kindutil.ImageBuildSpec{
			{
				ContextDir: "tests/integration/kindutil/fake-serving",
				Tag:        "fake-serving:v1",
				BuildArgs:  map[string]string{"VERSION": "v1"},
			},
			{
				// Distinct VERSION build-arg makes v2 a different image
				// digest from v1 (same binary, different LABEL layer).
				// In-place tests need this — kubelet short-circuits the
				// Image-equality check when two tags map to the same
				// sha256 (the previous ExtraTags alias approach), leaving
				// Status.ContainerStatuses[*].Image at the v1 string and
				// hanging ops.podRuntimeImagesMatch.
				ContextDir: "tests/integration/kindutil/fake-serving",
				Tag:        "fake-serving:v2",
				BuildArgs:  map[string]string{"VERSION": "v2"},
			},
		},
		// Default CRDPaths (config/crd/full) + the prometheus-operator
		// PodMonitor CRD. The PodMonitor is the only optional CRD the
		// OMENative reconciler hard-requires today; other optional
		// schemes (LWS, KEDA, Gateway API, Volcano) remain unwired
		// because the coordination tests don't exercise them.
		CRDPaths: []string{
			"config/crd/full",
			"tests/integration/dep-crds/prometheus-operator",
		},
		Scheme: scheme,
		Setup:  mgrSetup,
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "OEP-0007.1 Coordination KIND Suite")
}

// mgrSetup mirrors the envtest coordination suite's controller
// wiring as closely as possible. The reconcilers don't know they're
// talking to KIND vs envtest — same APIs, different cluster behind
// the manager's RestConfig.
func mgrSetup(ctx context.Context, mgr ctrl.Manager) error {
	// Seed the inferenceservice-config ConfigMap so the reconciler
	// can read it at first reconcile (matches the envtest suite's
	// BeforeSuite seed step). Use a direct apiserver client here —
	// mgr.GetClient() is cached and the cache isn't started yet
	// during mgrSetup.
	directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		return err
	}
	if err := util.SeedDefaultConfigMaps(ctx, directClient); err != nil {
		return err
	}

	// Fake model-cache backend — every source comes back Reachable
	// with a llama-shape config.json. Production-tests would point at
	// a real cluster_cache; KIND coordination tests don't need that
	// fidelity because they assert on the controller-side state
	// machine, not the model-cache itself.
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
	return components.SetupInferenceService(ctx, mgr,
		components.WithISVCModelCacheProvider(provider))
}
