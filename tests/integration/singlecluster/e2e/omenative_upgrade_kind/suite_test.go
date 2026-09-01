// Package omenative_upgrade_kind validates the operator-upgrade
// identity-stability contract against a real, chart-deployed OME manager:
// replacing the running manager Pod with a build of the CURRENT commit must
// not disturb pods, node assignments, or revision lineage that a PREVIOUS
// commit's manager already brought to steady state.
//
// Unlike omenative_coordination_kind's in-process manager
// (tests/integration/kindutil.StartManager), this suite installs a real
// ome-controller-manager Deployment via deploy-ome-kind.sh — the same
// script omenative_eval_kind uses — so "the manager Pod was replaced" is a
// literal Pod UID check, not an in-process proxy.
package omenative_upgrade_kind

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// upgradeFromImageEnv names the CI-supplied previous-main ome-manager image
// this suite installs before upgrading to the current build. Absent both
// locally and in default CI, so the suite Skips before paying for a KIND
// cluster and chart install it would have nothing to compare against.
const upgradeFromImageEnv = "OME_UPGRADE_FROM_IMAGE"

// upgradeNamespace holds the shared runtime fixtures deploy-ome-kind.sh
// seeds (a PVC-backed ClusterBaseModel + its seed Job): the PVC and the
// model's storageUri are both namespace-bound, so the install and every
// ISVC this suite creates must agree on one namespace.
const upgradeNamespace = "ome-upgrade-identity"

// omeNamespace is where the chart installs the controller-manager
// Deployment — deploy-ome-kind.sh's OME_NAMESPACE default.
const omeNamespace = "ome"

// Per-spec timeouts — local aliases of the shared kindfix KIND tier.
const (
	kindMediumTimeout    = kindfix.MediumTimeout
	kindLongTimeout      = kindfix.LongTimeout
	kindPollInterval     = kindfix.PollInterval
	upgradeDeployTimeout = 30 * time.Minute
)

// Suite-wide state populated by RunKindSuite.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

func TestOMENativeUpgradeIdentity(t *testing.T) {
	fromImage := os.Getenv(upgradeFromImageEnv)
	if fromImage == "" {
		t.Skipf("%s is unset; the operator-upgrade identity gate runs only "+
			"from the dedicated CI step that resolves the previous main manager image",
			upgradeFromImageEnv)
	}
	suiteConfig, _ := ginkgo.GinkgoConfiguration()
	if suiteConfig.ParallelTotal != 1 {
		t.Fatalf("OMENative upgrade-identity gate requires one Ginkgo process per KIND cluster; got %d", suiteConfig.ParallelTotal)
	}

	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: "ome-upgrade-kind",
		Scheme:      kindfix.OMEScheme(),
		// Dependencies only; the chart owns the OME CRDs.
		CRDPaths: []string{
			"tests/integration/dep-crds/prometheus-operator",
			"tests/integration/dep-crds/keda",
		},
		// No Setup: this suite tests the deployed controller's own restart
		// behavior across a real image change, not an in-process stand-in.
		Deployed: &kindutil.DeployedOME{
			Script: "hack/omenative-testing/deploy-ome-kind-from-image.sh",
			Env: []string{
				"OME_TEST_NS=" + upgradeNamespace,
				"OME_NAMESPACE=" + omeNamespace,
				// This gate does not assert on metrics; skipping Prometheus
				// drops one more baked-image dependency from an already
				// heavy chart install.
				"OME_KIND_PROMETHEUS_ENABLED=false",
			},
			Timeout: upgradeDeployTimeout,
		},
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "OMENative Upgrade Identity KIND Suite")
}
