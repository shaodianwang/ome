// Package omenative_perm_kind is a watch-driven, parallel OMENative rollout
// test suite for KIND, modeled on the playground's run.py/batch.py harness.
//
// Design (vs the omenative_coordination_kind contract suite):
//   - WATCH-DRIVEN: each case drives a util.RolloutObserver that reads the pod
//     watch stream, so it can't miss a surge/drain transient (vs 100ms polling).
//   - BUMP ALL MEMBERS: the trigger perturbs every coordination-group member,
//     not engine-only — coordination is a status-only restrictive-gate layer
//     that never initiates a roll on an unchanged Component.
//   - EXTERNAL MANAGER: RunKindSuite is invoked with Setup=nil (no in-process
//     manager), so the OME controller runs OUT of process (`make run-ome-manager`
//     pointed at the KIND cluster). That lets Ginkgo run specs in PARALLEL
//     (ginkgo -p) as pure drivers — the batch.py conc-N model — instead of the
//     serial single-in-process-manager model.
//   - PROD DEFAULT PATH: the manager runs in its default (IR-managed) mode.
//     Cross-Component coordination (Sequential / BlueGreen / RatioBalanced
//     pacing) is gated on this path — the IR-managed coordination gate reads
//     fresh IR status — so the coordination cases here exercise the production
//     default, not a kill-switch fallback.
//   - RICHER CHECKS: peak ceiling, convergence+rotated, negative-control
//     isolation, in-place UID stability, Sequential ordering, RatioBalanced
//     band (see runner_test.go).
//
// RUN (see Makefile target test-kind-perm):
//  1. `make test-kind-perm-up` — brings up KIND + CRDs + fake-serving (no specs)
//  2. `KUBECONFIG=/tmp/kind-ome-coord.kubeconfig make run-ome-manager`
//     against the same cluster, left running in a separate terminal
//  3. `make test-kind-perm` — drives the specs in parallel
package omenative_perm_kind

import (
	"context"
	"os"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// Suite-wide state. The manager runs EXTERNALLY (make run-ome-manager); this
// suite only drives + observes.
var (
	suiteCtx  context.Context
	k8sClient client.Client
	restCfg   *rest.Config
)

// TestOMENativePerm is the Ginkgo entrypoint. RunKindSuite with Setup=nil
// brings up the cluster + fake-serving images + CRDs + a client but does NOT
// start an in-process manager — the controller runs out of process. It REUSES
// the coordination suite's cluster (kindfix.CoordClusterName).
func TestOMENativePerm(t *testing.T) {
	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: kindfix.CoordClusterName,
		Workers:     2,
		Scheme:      kindfix.OMEScheme(),
		BuildImages: kindfix.FakeServingImages(),
		CRDPaths:    kindfix.OMECRDPaths(),
		Setup:       nil, // EXTERNAL manager — no in-process controller.
	})

	suiteCtx = state.Ctx
	k8sClient = state.Client
	restCfg = state.RestConfig

	// PERM_SETUP_ONLY=1 brings up the cluster + CRDs + fake-serving images
	// (done above by RunKindSuite) and returns WITHOUT running specs — so a
	// human can point `make run-ome-manager` at the cluster before the parallel
	// driver run. Used by the `test-kind-perm-up` Makefile target.
	if os.Getenv("PERM_SETUP_ONLY") == "1" {
		t.Log("PERM_SETUP_ONLY=1: cluster + CRDs + images ready; skipping specs")
		return
	}

	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "OMENative Perm KIND Suite")
}

// SynchronizedBeforeSuite applies the shared runtime (CBM + CSR) and labels
// nodes model-Ready exactly once (proc 1), regardless of -p parallelism.
var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	applyRuntime(suiteCtx, gomega.Default, k8sClient)
	return nil
}, func(_ []byte) {})
