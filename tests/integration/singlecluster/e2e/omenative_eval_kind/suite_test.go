// Package omenative_eval_kind validates YAML eval contracts against an OME
// installation in KIND.
//
// The chart-installed operator provides the Service, certificate, and webhook
// configurations needed to test admission decisions. The deployment script
// also installs the runtime fixtures referenced by the migrated cases.
package omenative_eval_kind

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

// Per-spec timeouts — local aliases of the shared kindfix KIND tier.
const (
	kindMediumTimeout = kindfix.MediumTimeout
	kindLongTimeout   = kindfix.LongTimeout
	kindPollInterval  = kindfix.PollInterval
	evalDeployTimeout = 30 * time.Minute
)

// The runtime fixtures the deploy script applies, which the specs reference
// rather than minting their own: a spec that created its own would exercise a
// different admission path than the eval it is replacing.
const (
	fixtureModel   = "ome-coordtest-noop"
	fixtureRuntime = "ome-coordtest-cpu"
)

// evalNamespace contains the shared runtime fixtures. The model is PVC-backed,
// so every migrated ISVC must use this namespace and must not delete it.
func evalNamespace() string {
	return os.Getenv("OME_TEST_NS")
}

// Suite-wide state populated by RunKindSuite.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

func TestOMENativeEval(t *testing.T) {
	if evalNamespace() == "" {
		t.Fatal("OMENative eval requires OME_TEST_NS")
	}
	suiteConfig, _ := ginkgo.GinkgoConfiguration()
	if suiteConfig.ParallelTotal != 1 {
		t.Fatalf("OMENative eval requires one Ginkgo process per KIND cluster; got %d", suiteConfig.ParallelTotal)
	}

	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: "ome-eval-kind",
		// Single node: these specs assert admission decisions and
		// single-Component rollout, neither of which needs somewhere to
		// spread. Installing OME already costs minutes; extra nodes add more.
		Workers: 0,
		Scheme:  kindfix.OMEScheme(),
		// Dependencies only. The chart owns the OME CRDs, so applying them
		// here would contend with Helm for ownership of the same objects.
		CRDPaths: []string{
			"tests/integration/dep-crds/prometheus-operator",
			"tests/integration/dep-crds/keda",
		},
		// No Setup: the deployed controller is the one under test.
		Deployed: &kindutil.DeployedOME{
			Script:  "hack/omenative-testing/deploy-ome-kind.sh",
			Timeout: evalDeployTimeout,
		},
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "OMENative Eval KIND Suite")
}
