// Package inferenceservice_defaulted hosts integration tests for the
// values an InferenceService may leave unset. The reconciler fills them
// after the ServingRuntime merge, so a value authored on the
// InferenceService wins, then one authored on the runtime, then the
// deploy block of inferenceservice-config; the stored object is never
// written. The controller/inferenceservice suite pins reconciler behavior
// for explicit specs; this suite pins the contract for unset fields.
package inferenceservice_defaulted

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestInferenceServiceDefaulted is the Go entrypoint Ginkgo hooks into.
func TestInferenceServiceDefaulted(t *testing.T) {
	util.RunSuite(t, "InferenceService Reconcile-Time Defaults Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{
			// The ISVC RawDeployment path unconditionally reconciles a
			// PodMonitor — without this the SetControllerReference
			// call fails with "no kind is registered for the type v1.PodMonitor".
			framework.PrometheusOperator,
			// KEDA keeps the autoscaler dispatch's scheme complete so
			// per-class cleanup lookups resolve.
			framework.KEDA,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// The reconciler reads the deploy block of inferenceservice-config in
	// the OME namespace; the seed carries the chart-equivalent values. The
	// chart's termination grace is added here rather than in the shared seed
	// because the KIND suites share that seed and a real kubelet would hold
	// every pod deletion for the full period.
	ginkgo.By("seeding inferenceservice-config for the reconciler")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())
	gomega.Expect(setDeployGracePeriod(suiteCtx, k8sClient, configuredGracePeriodSeconds)).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// BaseModel reconciler drives the CBM fixtures to Ready so the
		// ISVC reconciler's model resolution succeeds.
		if err := components.SetupBaseModel(ctx, mgr); err != nil {
			return err
		}
		return components.SetupInferenceService(ctx, mgr)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})

// configuredGracePeriodSeconds is the deploy.terminationGracePeriodSeconds
// this suite configures, the value the chart ships.
const configuredGracePeriodSeconds = int64(600)

// setDeployGracePeriod adds terminationGracePeriodSeconds to the deploy
// block of the seeded inferenceservice-config before the manager starts.
func setDeployGracePeriod(ctx context.Context, c client.Client, seconds int64) error {
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName}
	if err := c.Get(ctx, key, cm); err != nil {
		return err
	}
	deploy := map[string]any{}
	if err := json.Unmarshal([]byte(cm.Data["deploy"]), &deploy); err != nil {
		return err
	}
	deploy["terminationGracePeriodSeconds"] = seconds
	raw, err := json.Marshal(deploy)
	if err != nil {
		return err
	}
	cm.Data["deploy"] = string(raw)
	return c.Update(ctx, cm)
}
