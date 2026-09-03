// Package rolloutpolicy hosts integration tests for the RolloutPolicy
// status controller: Ready/InUse conditions, the portable spec digest, and
// the attached-group count, driven end-to-end through a real apiserver with
// the consumer-side watch fan-out doing the re-enqueueing.
//
// Only the status controller runs here. The run layer a consuming
// InferenceService opens over a referenced policy lives in the
// InferenceService reconciler and is covered by that package's unit tests;
// booting the full reconciler in this leaf would drag in its entire
// dependency surface to test none of it.
package rolloutpolicy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	policycontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/rolloutpolicy"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestRolloutPolicyStatusController(t *testing.T) {
	util.RunSuite(t, "RolloutPolicy Status Controller Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

// boundProvider is the one logical metric-provider name the suite's
// ProviderBindings seam binds; policies referencing anything else report
// ProviderUnbound.
const boundProvider = "llm-metrics"

// suiteBindings is the suite's ProviderBindings seam: a fixed binding set
// with no periodic recheck (live reads make the cadence unnecessary here;
// the requeue sizing is unit-tested on the reconciler).
type suiteBindings struct{}

func (suiteBindings) Providers() (map[string]struct{}, error) {
	return map[string]struct{}{boundProvider: {}}, nil
}

func (suiteBindings) RecheckInterval() time.Duration { return 0 }

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// The consumers-by-policy field index is owned by the manager wiring
		// in production; with only the status controller booted, the suite
		// registers the controller's own extractor.
		if err := mgr.GetFieldIndexer().IndexField(ctx, &v1beta1.InferenceService{},
			policycontroller.PolicyRefIndexField, policycontroller.PolicyRefIndexer); err != nil {
			return fmt.Errorf("register rollout policy-ref index: %w", err)
		}
		return policycontroller.SetupWithBindings(mgr, suiteBindings{})
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

// newTestNamespace gives each spec its own namespace so consumer counts
// never leak across specs (attachment is namespace-scoped by design).
func newTestNamespace() string {
	ginkgo.GinkgoHelper()
	ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "rpolicy-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return ns.Name
}

// canarySpec is a minimal valid canary body whose metrics source names the
// given provider; empty omits the source entirely.
func canarySpec(provider string) v1beta1.RolloutPolicySpec {
	c := &v1beta1.GroupCanary{
		Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		},
	}
	if provider != "" {
		c.Prometheus = &v1beta1.AnalysisPrometheus{
			ProviderRef: &v1beta1.MetricProviderRef{Name: provider},
		}
	}
	return v1beta1.RolloutPolicySpec{Canary: c}
}

func mkPolicy(namespace, name string, spec v1beta1.RolloutPolicySpec) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       spec,
	}
}

// refGroup builds one single-Component rollout group referencing the named
// policy. No webhook runs in this suite, so the consumer needs nothing
// beyond the rollout block the ref lives on.
func refGroup(policyName string, comp v1beta1.ComponentType) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{comp},
		PolicyRef: &v1beta1.RolloutPolicyRef{
			Name:        policyName,
			Progression: v1beta1.RolloutProgressionCanary,
		},
	}
}

func mkConsumer(namespace, name string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
	}
	if len(groups) > 0 {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: groups}
	}
	return isvc
}
