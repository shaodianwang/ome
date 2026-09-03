// Package rolloutpolicy hosts integration tests for the RolloutPolicy
// validating webhook and the InferenceService-side rollout policy-ref
// validation.
//
// These drive real admission through a real apiserver, which the unit tests
// cannot: those call Handle directly with a fake client, so they prove the
// handler's logic but not that the apiserver ever reaches it. Everything
// between — the rule set (CREATE/UPDATE/DELETE), the matchConditions CEL
// guard that must not break DELETE reviews, the CRD's own one-of progression
// CEL rejecting a write before the webhook is consulted, and the in-use
// DELETE and kind-freeze checks reading live cluster state — only exists
// here.
package rolloutpolicy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	isvcwebhook "github.com/ome-projects/ome/pkg/webhook/admission/isvc"
	policywebhook "github.com/ome-projects/ome/pkg/webhook/admission/rolloutpolicy"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// testNamespace hosts every spec's policies and consumer ISVCs. Specs stay
// independent through uniquely named objects, not per-spec namespaces: the
// in-use DELETE check only sees consumers that name the policy under test.
const testNamespace = "rpolicy-webhook"

// suiteMaxPlanBytes is the rendered-plan size cap wired into BOTH validators
// (the policy webhook's MaxPlanBytes and the ISVC validator's
// RolloutMaxPlanBytes — production threads one operator knob into both).
// Small enough for a deliberately padded body to exceed, large enough that
// every ordinary fixture in this suite stays well under it.
const suiteMaxPlanBytes = 2048

// providerName is the logical metric-provider name the valid canary fixture
// declares. Admission never resolves it — binding is a per-cluster,
// reconcile-time concern — so no operator config backs it here.
const providerName = "llm-metrics"

func TestRolloutPolicyWebhook(t *testing.T) {
	util.RunSuite(t, "RolloutPolicy Webhook Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
	// suiteCfg lets specs build purpose-specific clients (e.g. one whose
	// rest.Config captures admission warning headers).
	suiteCfg *rest.Config
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		WebhookPath: "tests/integration/singlecluster/webhook/rolloutpolicy/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCfg = s.Cfg
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	mgrSetup := func(_ context.Context, mgr ctrl.Manager) error {
		server := mgr.GetWebhookServer()
		if server == nil {
			return fmt.Errorf("manager has no webhook server (set Framework.WebhookPath)")
		}
		// Client is the manager's APIReader, not its cache: a spec denies a
		// DELETE, edits the consumer, and retries within milliseconds — the
		// in-use check must observe the write it just made.
		server.Register("/validate-ome-io-v1beta1-rolloutpolicy", &webhook.Admission{
			Handler: &policywebhook.Validator{
				Client:       mgr.GetAPIReader(),
				Decoder:      admission.NewDecoder(mgr.GetScheme()),
				MaxPlanBytes: suiteMaxPlanBytes,
			},
		})
		// The ISVC validator runs with the RolloutPolicy feature enabled so
		// a rejected ref fails on its own rule (reserved kind, shape,
		// size), not on the blanket feature-disabled rejection.
		return ctrl.NewWebhookManagedBy(mgr, &v1beta1.InferenceService{}).
			WithValidator(&isvcwebhook.InferenceServiceValidator{
				Client:               mgr.GetClient(),
				RuntimeSelector:      runtimeselector.New(mgr.GetClient()),
				RolloutPolicyEnabled: true,
				RolloutMaxPlanBytes:  suiteMaxPlanBytes,
			}).
			Complete()
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())

	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
	})).To(gomega.Succeed())
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

var nameSeq int

// uniqueName keeps specs independent: policies and consumers share one
// namespace, and the in-use checks match consumers by policy name.
func uniqueName(prefix string) string {
	nameSeq++
	return fmt.Sprintf("%s-%d", prefix, nameSeq)
}

// analysisStep is one metric-gated canary step at 50% capacity / 50%
// traffic. The query is a parameter so the plan-size specs can pad it past
// the suite cap while every other property stays valid.
func analysisStep(query string) v1beta1.RolloutGroupStep {
	return v1beta1.RolloutGroupStep{
		Capacity: intstr.FromString("50%"),
		Traffic:  50,
		Analysis: &v1beta1.RolloutAnalysis{
			Interval:     metav1.Duration{Duration: 30 * time.Second},
			FailureLimit: 1,
			Metrics: []v1beta1.AnalysisMetric{{
				Name:      "error-rate",
				Query:     query,
				Operator:  v1beta1.ComparisonLTE,
				Threshold: "0.05",
			}},
		},
	}
}

// validCanarySpec is the suite's baseline valid policy body: percent-only
// step capacities, a providerRef-bound metrics source, one analysis gate,
// and a final 100%/100 step. Specs break exactly one property of it per
// rejection case.
func validCanarySpec() v1beta1.RolloutPolicySpec {
	return v1beta1.RolloutPolicySpec{
		Canary: &v1beta1.GroupCanary{
			Prometheus: &v1beta1.AnalysisPrometheus{
				ProviderRef: &v1beta1.MetricProviderRef{Name: providerName},
			},
			Steps: []v1beta1.RolloutGroupStep{
				analysisStep(`sum(rate(request_errors_total[1m]))`),
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
	}
}

func mkPolicy(name string, spec v1beta1.RolloutPolicySpec) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:       spec,
	}
}

// omeNativeEngine returns an EngineSpec annotated for OMENative dispatch —
// the mode every rollout-group member must declare.
func omeNativeEngine() *v1beta1.EngineSpec {
	return &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		PodSpec: v1beta1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "llm-serving:v1"}},
		},
	}
}

func omeNativeDecoder() *v1beta1.DecoderSpec {
	return &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		PodSpec: v1beta1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "llm-serving:v1"}},
		},
	}
}

func omeNativeRouter() *v1beta1.RouterSpec {
	return &v1beta1.RouterSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		PodSpec: v1beta1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "llm-router:v1"}},
		},
	}
}

// refGroup builds a rollout group whose progression comes from a policyRef
// with the given declared kind.
func refGroup(policyName string, progression v1beta1.RolloutProgressionKind, comps ...v1beta1.ComponentType) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components: comps,
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: policyName, Progression: progression},
	}
}

// mkConsumer builds an InferenceService the ISVC validator admits without
// any model or runtime object existing: runtime-only specs referencing an
// absent runtime pass availability (absence is not an admission error), so
// the suite needs no BaseModel/ServingRuntime fixtures. The engine is
// OMENative so it can join canary-kind rollout groups.
func mkConsumer(name string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceService {
	kind := "ClusterServingRuntime"
	apiGroup := "ome.io"
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "llm-serving", Kind: &kind, APIGroup: &apiGroup},
			Engine:  omeNativeEngine(),
		},
	}
	if len(groups) > 0 {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: groups}
	}
	return isvc
}

// mustCreate creates objects and registers child-first cleanup so a
// referenced policy is deleted only after its consumers are gone.
func mustCreate(objs ...client.Object) {
	ginkgo.GinkgoHelper()
	for _, obj := range objs {
		gomega.Expect(k8sClient.Create(suiteCtx, obj)).To(gomega.Succeed())
	}
	ginkgo.DeferCleanup(func() {
		for i := len(objs) - 1; i >= 0; i-- {
			gomega.Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, objs[i]))).To(gomega.Succeed())
		}
	})
}

// denial returns the apiserver's message for a rejected write, and fails
// the spec if the write was admitted.
func denial(err error) string {
	ginkgo.GinkgoHelper()
	gomega.Expect(err).To(gomega.HaveOccurred(), "the write should have been rejected")
	return err.Error()
}
