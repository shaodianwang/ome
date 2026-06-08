// Package kindfix holds the OME-specific scaffolding shared across the KIND
// integration suites (omenative_coordination_kind, irmanaged_kind, omenative_perm_kind):
// the manager Setup, scheme, CRD set, fake-serving images, cluster names, and
// the KIND timeout tier. kindutil stays the generic KIND plumbing (cluster /
// image / CRD lifecycle); kindfix is the OME layer above it. Nothing in
// kindutil / components / util imports kindfix, so there is no import cycle.
package kindfix

import (
	"context"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	mcfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	inferencereplica "github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KIND cluster names — one named cluster per behavioral path. The perm suite
// deliberately REUSES the coordination cluster (CoordClusterName), so the two
// are bound by this symbol rather than a copy-pasted literal.
const (
	CoordClusterName     = "ome-coord-kind"
	IRManagedClusterName = "ome-irmanaged-kind"
	RawClusterName       = "ome-raw-kind"
)

// KIND timeout tier — tuned for real kubelet latency (image pull, pod schedule,
// container start, readiness probe ≈ 30-60s before any rollout work), NOT
// envtest microseconds. ALL KIND suites use these; they must never reach for
// util's envtest-tuned tier (util.MediumTimeout=30s / util.PollInterval=200ms),
// whose magnitudes differ and would silently under-wait on a real kubelet.
const (
	ShortTimeout  = 30 * time.Second
	MediumTimeout = 2 * time.Minute
	LongTimeout   = 5 * time.Minute
	PollInterval  = 100 * time.Millisecond
)

// OMEScheme builds the scheme every OME KIND suite needs: core k8s + OME
// v1beta1 (kindutil.BaseScheme) plus prometheus-operator (PodMonitor) and KEDA
// (ScaledObject). The ISVC controller emits a PodMonitor on every OMENative
// reconcile and Owns ScaledObject unconditionally (PR #337), so without those
// schemes cache-sync fails and the suite never reaches a spec. Gang scheduling
// (PodGroup) is NOT registered here — see GangScheme.
func OMEScheme() *runtime.Scheme {
	s := kindutil.BaseScheme()
	utilruntime.Must(monitoringv1.AddToScheme(s))
	utilruntime.Must(kedav1.AddToScheme(s))
	return s
}

// GangScheme is OMEScheme plus the scheduler-plugins PodGroup type. Only the
// multi-node gang suite needs it: registering PodGroup makes the controller's
// gang-availability discovery succeed, and the direct path then sets a gang
// schedulerName — which leaves pods Pending on a KIND cluster that has no gang
// scheduler. So the single-pod / direct-path suites use OMEScheme and stay on
// the non-gang path; only the IR-managed gang suite opts into this.
func GangScheme() *runtime.Scheme {
	s := OMEScheme()
	utilruntime.Must(schedulingv1alpha1.AddToScheme(s))
	return s
}

// FakeServingImages is the v1/v2 fake-serving build spec every suite loads. The
// distinct VERSION build-arg gives v2 a different digest from v1, which in-place
// image-bump tests need — kubelet short-circuits the Image-equality check when
// two tags map to the same sha256.
func FakeServingImages() []kindutil.ImageBuildSpec {
	return []kindutil.ImageBuildSpec{
		{ContextDir: "tests/integration/kindutil/fake-serving", Tag: "fake-serving:v1", BuildArgs: map[string]string{"VERSION": "v1"}},
		{ContextDir: "tests/integration/kindutil/fake-serving", Tag: "fake-serving:v2", BuildArgs: map[string]string{"VERSION": "v2"}},
	}
}

// OMECRDPaths is the CRD set every OME KIND suite applies: the full OME CRDs +
// prometheus-operator (PodMonitor) + KEDA (ScaledObject), both hard cluster-side
// dependencies for the ISVC controller (see OMEScheme). The PodGroup CRD is NOT
// here — see GangCRDPaths.
func OMECRDPaths() []string {
	return []string{
		"config/crd/full",
		"tests/integration/dep-crds/prometheus-operator",
		"tests/integration/dep-crds/keda",
	}
}

// GangCRDPaths is OMECRDPaths plus the scheduler-plugins PodGroup CRD. Installing
// it makes the controller's gang-availability discovery succeed, so the IR-managed
// multi-node suite can assert PodGroup creation. Only that suite uses it (see
// GangScheme for why the others stay off the gang path).
func GangCRDPaths() []string {
	return append(OMECRDPaths(), "tests/integration/dep-crds/scheduler-plugins")
}

// SetupOpts parameterizes StandardOMESetup across the in-process paths.
type SetupOpts struct {
	// WireInferenceReplica also starts the inferencereplica controller — the
	// second half of the IR-managed path that turns projected InferenceReplica
	// CRs into pods + status. Required for any OMENative coverage, since
	// OMENative-mode Components are driven exclusively through the IR-managed
	// path.
	WireInferenceReplica bool
}

// StandardOMESetup returns the kindutil.ManagerSetup shared by the in-process
// KIND suites: it seeds inferenceservice-config, installs a fake
// (always-Reachable, llama-shape config.json) model-cache provider, and wires
// the BaseModel + InferenceService reconcilers — plus the optional
// inferencereplica controller per opts. Same controller code as production;
// only the cluster behind the manager differs.
//
//	omenative_coordination_kind: StandardOMESetup(SetupOpts{WireInferenceReplica: true})
//	irmanaged_kind:              StandardOMESetup(SetupOpts{WireInferenceReplica: true})
func StandardOMESetup(opts SetupOpts) kindutil.ManagerSetup {
	return func(ctx context.Context, mgr ctrl.Manager) error {
		// Direct (uncached) client — the manager cache isn't started during
		// Setup, so the seed must not go through mgr.GetClient().
		directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
		if err != nil {
			return err
		}
		if err := util.SeedDefaultConfigMaps(ctx, directClient); err != nil {
			return err
		}

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
		provider := sharded.NewProvider(sharded.ProviderClusterCache, fakeMC, sharded.ProviderConfig{
			Provider: sharded.ProviderClusterCache,
			Endpoint: "http://cluster-cache.clustercache.svc.cluster.local:10090",
		})

		if err := components.SetupBaseModel(ctx, mgr, components.WithModelCacheProvider(provider)); err != nil {
			return err
		}
		if err := components.SetupInferenceService(ctx, mgr, components.WithISVCModelCacheProvider(provider)); err != nil {
			return err
		}

		if opts.WireInferenceReplica {
			return (&inferencereplica.Reconciler{
				Client:    mgr.GetClient(),
				Log:       ctrl.Log.WithName("InferenceReplica"),
				APIReader: mgr.GetAPIReader(),
				Recorder:  mgr.GetEventRecorderFor("InferenceReplica"),
			}).SetupWithManager(mgr)
		}
		return nil
	}
}
