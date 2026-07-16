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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// Suite-wide state populated by RunKindSuite + BeforeSuite. Specs
// reference these directly.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

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
		// Pods still use KIND's default scheduler; PodGroup creation is the
		// controller contract under test, not coscheduling behavior.
		CRDPaths: kindfix.GangCRDPaths(),
		Scheme:   kindfix.GangScheme(),
		// OMENative is driven exclusively through the IR-managed path now
		// (the legacy omenative-direct path was retired). The cross-Component
		// coordination this suite exercises (Sequential / BlueGreen /
		// RatioBalanced / soak) runs on the IR path: the inferencereplica
		// controller wires the same coordination.EvaluateUpdateGate decision
		// site the direct path used. WireInferenceReplica starts that
		// controller so projected InferenceReplica CRs turn into pods + status.
		Setup: kindfix.StandardOMESetup(kindfix.SetupOpts{WireInferenceReplica: true}),
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "OMENative Coordination KIND Suite")
}
