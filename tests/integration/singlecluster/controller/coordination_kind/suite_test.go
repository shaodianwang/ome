// Package coordination_kind exercises the OMENative cross-Component
// rollout coordination layer end-to-end against a real KIND cluster
// (kubelet + scheduler + containerd). Tests use the fake-serving
// container image built and kind-loaded by RunKindSuite.
package coordination_kind

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	mcfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Suite-wide state populated by RunKindSuite + BeforeSuite. Specs
// reference these directly.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

// TestCoordinationKindController is the Go entrypoint Ginkgo hooks
// into. Brings up the KIND cluster + OME reconcilers + fake-serving
// image, then runs every Describe in this package.
func TestCoordinationKindController(t *testing.T) {
	// OMENative emits a per-Component PodMonitor on every reconcile.
	// Without the prometheus-operator CRD installed the apiserver
	// rejects the Create and the reconciler errors out on every pass;
	// without the scheme registered the controller can't even build
	// the object. Both have to be in place for any OMENative test to
	// make progress.
	//
	// KEDA ScaledObject is now Owns()'d unconditionally by the ISVC
	// controller (PR #337, OEP-0013 Slice 2, decision Q6 — KEDA is a
	// hard cluster-side dependency). Without the scheme + CRD pair in
	// place the controller-runtime cache-sync errors out 2 min into
	// startup with "kind must be registered to the Scheme" and the
	// suite never reaches its first spec.
	scheme := kindutil.BaseScheme()
	utilruntime.Must(monitoringv1.AddToScheme(scheme))
	utilruntime.Must(kedav1.AddToScheme(scheme))

	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: "ome-coord-kind",
		// 2 workers. Migration tests (PD-5, PDA-1, PDX-5, INT-*) move a pod
		// OFF its source node: the surge pod gets a required NodeAffinity
		// excluding the source node, so it needs another SCHEDULABLE node
		// to land on. The kind control-plane node carries the default
		// node-role.kubernetes.io/control-plane:NoSchedule taint (and the
		// rendered pods don't tolerate it), so it does NOT count — with
		// Workers=1 the source lands on the single worker and the surge has
		// nowhere to go (worker excluded by anti-affinity, control-plane by
		// taint) → Pending forever → 300s timeout. Two workers give the
		// surge a real off-source home. (Workers=0 single-node fails for
		// the same reason; the old "1 worker = 2 nodes" reasoning ignored
		// the control-plane taint.)
		Workers: 2,
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
		// PodMonitor CRD + the KEDA ScaledObject CRD. KEDA is a hard
		// cluster-side dependency for the ISVC controller as of #337
		// (OEP-0013 Slice 2, decision Q6) — Owns(&kedav1.ScaledObject{})
		// is unconditional, so the CRD must be installed for cache-sync
		// to succeed even when individual coordination specs don't
		// exercise the autoscaling path. Other optional schemes (LWS,
		// Gateway API, Volcano) remain unwired because the coordination
		// tests don't exercise them.
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

	ginkgo.RunSpecs(t, "Coordination KIND Suite")
}

// mgrSetup wires up the OMENative + BaseModel reconcilers against the
// KIND apiserver. Same controller code as production — just a different
// cluster behind the manager's RestConfig.
func mgrSetup(ctx context.Context, mgr ctrl.Manager) error {
	// Route OMENative-mode Components through the legacy omenative direct
	// path, NOT the IR-managed projection. As of #320 IR-managed is the
	// default for OMENative components, but the cross-Component rollout
	// COORDINATION this suite exercises (Sequential / BlueGreen / RatioBalanced
	// / soak) lives ONLY on the omenative direct path — the IR controller
	// leaves UpdateGate nil (inferencereplica/convert.go) and does no
	// coordination. Without this, the ISVC controller would project every
	// Component to an InferenceReplica that no reconciler in this suite
	// manages, so no pods are ever created and every spec times out. This
	// mirrors the production `--disable-ir-managed-omenative` escape hatch.
	// (IR-managed-path coverage is a separate suite — see TODO/Goal 2.)
	irprojector.SetGlobalDisableIRManaged(true)

	// Seed the inferenceservice-config ConfigMap so the reconciler can
	// read it at first reconcile. Use a direct apiserver client here —
	// mgr.GetClient() is cached and the cache isn't started yet during
	// mgrSetup.
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
