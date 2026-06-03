// Package irmanaged_kind exercises the IR-MANAGED OMENative path
// end-to-end against a real KIND cluster (kubelet + scheduler +
// containerd). This is the path that is the production default since
// #320 (IsIRManagedComponent → true): the ISVC controller projects
// each OMENative Component to an InferenceReplica CR, and the
// inferencereplica controller reconciles that CR into pods + status.
//
// It is deliberately a SEPARATE suite/cluster from omenative_coordination_kind,
// which disables IR-managed to exercise the legacy direct path. The two
// paths aggregate status differently, so they get different harnesses.
// Tests here use the fake-serving image built and kind-loaded by
// RunKindSuite.
package irmanaged_kind

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// Per-spec timeouts — local aliases of the shared kindfix KIND tier.
const (
	kindShortTimeout  = kindfix.ShortTimeout
	kindMediumTimeout = kindfix.MediumTimeout
	kindLongTimeout   = kindfix.LongTimeout
	kindPollInterval  = kindfix.PollInterval
)

// Suite-wide state populated by RunKindSuite + BeforeSuite.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

// TestIRManagedKindController is the Go entrypoint Ginkgo hooks into.
func TestIRManagedKindController(t *testing.T) {
	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: kindfix.IRManagedClusterName,
		// 1 worker (+ tainted control-plane) = 1 schedulable node. Single-pod
		// surge peaks at 2 pods (old ordinal-0 + surge ordinal-1) with NO
		// anti-node-affinity, so they co-locate on the one worker — fine.
		Workers:     1,
		BuildImages: kindfix.FakeServingImages(),
		// Gang variants install the scheduler-plugins PodGroup CRD + scheme so the
		// multi-node specs exercise real gang wiring (PodGroup creation). The other
		// suites stay on OMEScheme/OMECRDPaths — see kindfix.GangScheme.
		CRDPaths: kindfix.GangCRDPaths(),
		Scheme:   kindfix.GangScheme(),
		// IR-managed ENABLED (production default) + the inferencereplica
		// controller wired in — the second half of the IR-managed path that
		// turns projected IR CRs into pods + status.
		Setup: kindfix.StandardOMESetup(kindfix.SetupOpts{WireInferenceReplica: true}),
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "IR-Managed KIND Suite")
}
