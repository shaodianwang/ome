// Package omenative_coordination_kind exercises the OMENative cross-Component
// rollout coordination layer end-to-end against a real KIND cluster
// (kubelet + scheduler + containerd). Tests use the fake-serving
// container image built and kind-loaded by RunKindSuite.
package omenative_coordination_kind

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Suite-wide state populated by RunKindSuite + BeforeSuite. Specs
// reference these directly.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

// The suite uses a small destructive-work budget so scale-down behavior is
// exercised in bounded waves without requiring a large Pod population. The
// intervals and caps preserve the production cadence the chart ships, so
// spec timing matches a deployed controller; Pod and EndpointSlice watches
// normally wake the controller sooner.
const kindLifecycleConfig = `{"scaleUpPodBatchSize":100,"scaleDownPodBatchSize":2,"scaleDownRequeueInterval":"5s",` +
	`"requeue":{"operation":"5s","gate":"3s"},` +
	`"gangScheduleTimeout":{"min":"60s","max":"600s"},` +
	`"revisionHistoryLimit":10,"retryBlockHistoryLimit":3,"instanceReadyTimeout":"30m",` +
	`"audit":{"maxInFlightMigrations":3,"maxMigrationsPerWindow":10,"window":"1h"}}`

func setupOMENativeCoordinationManager(ctx context.Context, mgr ctrl.Manager) error {
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
	configMap.Data[controllerconfig.LifecycleConfigName] = kindLifecycleConfig
	if err := directClient.Update(ctx, configMap); err != nil {
		return err
	}
	return kindfix.StandardOMESetup(kindfix.SetupOpts{WireInferenceReplica: true})(ctx, mgr)
}

// TestOMENativeCoordinationKindController is the Go entrypoint Ginkgo
// hooks into. Brings up the KIND cluster + OME reconcilers + fake-serving
// image (scheme, CRDs, images, and manager Setup all from kindfix),
// then runs every Describe in this package.
func TestOMENativeCoordinationKindController(t *testing.T) {
	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: kindfix.CoordClusterName,
		// 2 workers. Migration tests (PD/PDA/PDX migrate + INT-*) move a pod
		// OFF its source node via a required NodeAffinity excluding the source,
		// so the surge pod needs ANOTHER schedulable node. The control-plane
		// node carries the default NoSchedule taint (untolerated), so it does
		// not count — Workers=1 leaves the surge nowhere to land (→ Pending →
		// timeout). Two workers give it a real off-source home.
		Workers:     2,
		BuildImages: kindfix.FakeServingImages(),
		// This suite owns the authoritative multi-pod topology contract, so
		// install and register the optional scheduler-plugins PodGroup API.
		// PodGroup creation is the controller contract under test; which
		// scheduler binds the pods is the scheduler dimension below.
		CRDPaths: kindfix.GangCRDPaths(),
		Scheme:   kindfix.GangScheme(),
		// OMENative is driven through the IR-managed path: the cross-Component
		// coordination this suite exercises (Sequential / BlueGreen /
		// RatioBalanced / soak) runs in the inferencereplica controller at the
		// coordination.EvaluateUpdateGate decision site. WireInferenceReplica
		// starts that controller so projected InferenceReplica CRs turn into
		// pods + status.
		Setup: setupOMENativeCoordinationManager,
		// Scheduler dimension: OME_TEST_SCHEDULER=ome-scheduler installs the gang
		// scheduler and the fixture runtime selects it.
		Scheduler: kindfix.SchedulerFromEnv(),
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "OMENative Coordination KIND Suite")
}
