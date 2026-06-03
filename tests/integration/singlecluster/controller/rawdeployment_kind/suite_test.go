// Package rawdeployment_kind exercises RawDeployment-mode InferenceServices
// end-to-end on a real KIND cluster. The envtest suite already covers that the
// controller emits the right Deployment/Service objects; what it cannot cover —
// and what this suite adds — is that a real kubelet schedules and runs the pods
// and the Deployment reports them Ready. Uses the fake-serving image built and
// kind-loaded by RunKindSuite.
package rawdeployment_kind

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// Per-spec timeouts — the shared KIND tier.
const (
	kindMediumTimeout = kindfix.MediumTimeout
	kindLongTimeout   = kindfix.LongTimeout
	kindPollInterval  = kindfix.PollInterval
)

// Suite-wide state populated by RunKindSuite + BeforeSuite.
var (
	suiteCtx  context.Context
	k8sClient client.Client
)

// TestRawDeploymentKindController is the Go entrypoint Ginkgo hooks into.
func TestRawDeploymentKindController(t *testing.T) {
	state := kindutil.RunKindSuite(t, kindutil.SuiteOpts{
		ClusterName: kindfix.RawClusterName,
		Workers:     1,
		BuildImages: kindfix.FakeServingImages(),
		CRDPaths:    kindfix.OMECRDPaths(),
		Scheme:      kindfix.OMEScheme(),
		// RawDeployment does not use the OMENative / IR-managed path, so the IR
		// toggle is irrelevant; the default StandardOMESetup wires the BaseModel
		// + InferenceService reconcilers.
		Setup: kindfix.StandardOMESetup(kindfix.SetupOpts{}),
	})

	gomega.RegisterFailHandler(ginkgo.Fail)
	suiteCtx = state.Ctx
	k8sClient = state.Client

	ginkgo.RunSpecs(t, "RawDeployment KIND Suite")
}
