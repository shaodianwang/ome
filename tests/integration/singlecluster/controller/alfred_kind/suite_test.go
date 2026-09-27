// Package alfred_kind gates the contract between the pipeline-built alfred
// image and this tree: the OMENative controller it dispatches migrations to
// and the ome-resources alfred templates it is deployed from. It runs inside a
// real KIND cluster: workers that advertise fake GPUs, node-health and
// maintenance signals patched onto Node objects, the real kind scheduler, real
// fake-serving pods, and alfred rendered from the chart.
package alfred_kind

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// alfredNamespace is where the manager seed, the chart render and the guard's
// ServiceAccount username all place alfred; the three must agree on it.
var alfredNamespace = constants.OMENamespace

// Names alfred and the chart fix; every spec reads them from here.
const (
	alfredDeployment     = "ome-alfred"
	alfredServiceAccount = "ome-alfred"
	alfredConfigMapName  = "alfred-config"
	alfredConfigKey      = "config.yaml"
	alfredRecsConfigMap  = "alfred-recommendations"
	alfredRecsKey        = "last-cycle.json"
	alfredJournalName    = "alfred-dispatch-state"
	alfredJournalKey     = "state.json"
	alfredSimConfigMap   = "alfred-simulation"
	alfredGuardName      = "ome-alfred-migration-writes"
	alfredMetricsPort    = 8080
	alfredPodLabelKey    = "control-plane"
	alfredPodLabelValue  = "ome-alfred"

	gpuResource     = corev1.ResourceName("nvidia.com/gpu")
	gpusPerWorker   = int64(8)
	healthCondition = corev1.NodeConditionType("GpuUnhealthy")

	// Wire contract this suite guards, pinned deliberately rather than imported
	// from either side: a rename in alfred or in the controller fails here.
	migrationAnnoPrefix = "ome.io/migration-request-v1-"
	// movableAnnotation is the per-workload opt-out alfred reads from the ISVC.
	movableAnnotation = "alfred.ome.io/movable"
	// priorityAnnotation is the per-workload ranking value alfred reads from
	// the ISVC: a float in [0, 1], default 0.5, lower is more protected.
	priorityAnnotation = "alfred.ome.io/priority"
	// Defragmentation vocabulary: the policy's name in reports and metrics,
	// the reason its findings and requests carry, and the event an advisory
	// finding is produced under. recommendOnlyAdvisory is the reason the
	// engine stamps on any executable finding in recommend-only mode: the
	// finding is reported with its scheduling diagnosis and never arbitrated.
	defragPolicy          = "defragmentation"
	fragmentationReason   = "Fragmentation"
	fragmentationEvent    = "FragmentationRecommendationProduced"
	recommendOnlyAdvisory = "SimulationRecommendOnly"
	// alfredLeaseName is the leader-election Lease the alfred binary fixes.
	alfredLeaseName = "alfred.ome.io"
	// Recorder components each side stamps on its Events (Source.Component);
	// both record MigrationCompleted against the same InferenceService, so a
	// reason alone does not name the emitter.
	alfredEventSource     = "alfred"
	controllerEventSource = "InferenceReplica"
)

// Vocabulary as the pinned alfred reports it, fixed here rather than imported
// so a rename upstream fails this suite: the node-health policy's name, the
// reason and the advisory reason its findings carry, the outcome and the
// arbiter rejections a cycle record and its events name, the health states of
// a node record, and the journal documents the dispatcher accepts or refuses.
const (
	nodeHealthPolicy = "nodehealth"
	// nodeUnhealthyReason is what a health evacuation's finding, request and
	// migration record carry.
	nodeUnhealthyReason = "NodeUnhealthy"
	// ineligibleAdvisory is stamped on the finding for an OMENative Instance
	// that fails the eligibility baseline: not fully steady (counters at the
	// desired replicas, every member Ready), opted out, paused, under a Never
	// policy, or busy with a transition.
	ineligibleAdvisory = "OMENativeStateIneligible"
	// advisoryOutcome marks a finding that never entered arbitration.
	advisoryOutcome = "advisory"
	// Arbiter rejections: another Instance of the workload was admitted in
	// the same pass; the workload's newest terminal migration is younger than
	// the cooldown that applies to the finding.
	rejectWorkloadBusy = "WorkloadBusy"
	rejectCooldown     = "Cooldown"
	// Health states of a node record.
	nodeHealthClear     = "Clear"
	nodeHealthUnhealthy = "Unhealthy"
	nodeHealthSuspect   = "Suspect"
	// Journal documents: the one the chart pre-creates; one whose entries are
	// not a list, refused by the shape check before any entry is decoded; and
	// one that decodes but carries a version this alfred does not support.
	// While its journal is unreadable the dispatcher withholds every admitted
	// finding as journalUnavailable.
	emptyJournal              = `{"version":"v1","entries":[]}`
	malformedJournal          = `{"version":"v1","entries":"not-a-list"}`
	unsupportedVersionJournal = `{"version":"v9","entries":[]}`
	journalUnavailable        = "JournalUnavailable"
)

// Suite-wide state populated by RunKindSuite + BeforeSuite.
var (
	suiteCtx  context.Context
	k8sClient client.Client
	restCfg   *rest.Config
	clientset kubernetes.Interface
	// workers are the schedulable worker node names, sorted. workers[0] is the
	// node every spec makes unhealthy; workers[1:] are targets. Cleanup removes
	// a raised health condition outright, so the node is Clear at once for the
	// next spec.
	workers []string
	// alfredImage is the reference the alfred Deployment runs.
	alfredImage alfredImageRef
	// guardManifest is the rendered ValidatingAdmissionPolicy + binding, kept
	// so the guard spec can remove and reinstall exactly what the chart ships.
	guardManifest []byte
	// specStart is the watermark that keeps event lookups within the running
	// spec.
	specStart time.Time
)

// The watermark is taken before any spec-level setup or mutation runs.
var _ = ginkgo.BeforeEach(func() { specStart = time.Now() })

// The controller's own migration caps sit well above alfred's so alfred's
// budget specs are the only thing that limits dispatch.
const alfredLifecycleConfig = `{"scaleUpPodBatchSize":100,"scaleDownPodBatchSize":100,"scaleDownRequeueInterval":"5s",` +
	`"requeue":{"operation":"5s","gate":"3s"},` +
	`"gangScheduleTimeout":{"min":"60s","max":"600s"},` +
	`"revisionHistoryLimit":10,"retryBlockHistoryLimit":3,"instanceReadyTimeout":"30m",` +
	`"audit":{"maxInFlightMigrations":10,"maxMigrationsPerWindow":200,"window":"1h"}}`

func setupAlfredManager(ctx context.Context, mgr ctrl.Manager) error {
	directClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		return err
	}
	if err := util.SeedDefaultConfigMaps(ctx, directClient); err != nil {
		return err
	}
	key := client.ObjectKey{Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName}
	configMap := &corev1.ConfigMap{}
	if err := directClient.Get(ctx, key, configMap); err != nil {
		return err
	}
	if configMap.Data == nil {
		configMap.Data = map[string]string{}
	}
	configMap.Data[controllerconfig.LifecycleConfigName] = alfredLifecycleConfig
	if err := directClient.Update(ctx, configMap); err != nil {
		return err
	}
	return kindfix.StandardOMESetup(kindfix.SetupOpts{WireInferenceReplica: true})(ctx, mgr)
}

// TestAlfredKind is the Go entrypoint Ginkgo hooks into.
func TestAlfredKind(t *testing.T) {
	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: kindfix.AlfredClusterName,
		// Three workers: one source, two targets, so target-choice scenarios
		// (spot avoidance, suspicion exclusion) have a real alternative.
		Workers:     3,
		BuildImages: kindfix.FakeServingImages(),
		// alfred lists scheduling.x-k8s.io PodGroups as scheduler snapshot
		// input; installing the CRD keeps that list from failing.
		CRDPaths: kindfix.GangCRDPaths(),
		Scheme:   kindfix.GangScheme(),
		Setup:    setupAlfredManager,
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client
	restCfg = state.RestConfig
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		t.Fatalf("clientset: %v", err)
	}
	clientset = cs

	ginkgo.RunSpecs(t, "Alfred KIND Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	// The suite runs the stock scheduler: its simulation profile is keyed by
	// the default scheduler name, and the fixtures would select the scheduler
	// the environment names otherwise.
	gomega.Expect(kindfix.SchedulerName()).To(gomega.BeEmpty(),
		"the alfred suite runs the stock scheduler; unset %s", kindfix.EnvScheduler)
	workers = schedulableWorkers()
	gomega.Expect(len(workers)).To(gomega.BeNumerically(">=", 3),
		"the alfred suite needs three schedulable workers; got %v", workers)
	resetWorkerSignals()
	for _, w := range workers {
		gomega.Expect(kindutil.SetNodeExtendedResource(suiteCtx, k8sClient, w, gpuResource, gpusPerWorker)).
			To(gomega.Succeed(), "advertise fake GPUs on %s", w)
	}

	img, err := resolveAlfredImage(suiteCtx, kindfix.AlfredClusterName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "resolve the alfred image")
	alfredImage = img

	profiles := bootstrapSimulation(img)
	manifest, guard := renderAlfred(img, profiles)
	guardManifest = guard
	applyManifest(manifest)
	waitAlfredReady()
	waitDecisionCadence()
})
