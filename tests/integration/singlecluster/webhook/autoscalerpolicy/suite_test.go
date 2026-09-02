// Package autoscalerpolicy hosts integration tests for the AutoscalerPolicy
// validating webhook and the InferenceService-side policy-ref validation.
//
// These drive real admission through a real apiserver, which the unit tests
// cannot: those call Handle directly with a fake client, so they prove the
// handler's logic but not that the apiserver ever reaches it. Everything
// between — the rule set (CREATE/UPDATE/DELETE), the matchConditions CEL
// guard that must not break DELETE reviews, the CRD's own class/template
// CEL rejecting a write before the webhook is consulted, and the in-use
// DELETE check reading live cluster state — only exists here.
package autoscalerpolicy

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	policywebhook "github.com/ome-projects/ome/pkg/webhook/admission/autoscalerpolicy"
	isvcwebhook "github.com/ome-projects/ome/pkg/webhook/admission/isvc"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// testNamespace hosts every spec's policies and consumer ISVCs. Specs stay
// independent through uniquely named objects, not per-spec namespaces: the
// in-use DELETE check only sees consumers that name the policy under test.
const testNamespace = "aspolicy-webhook"

// disabledNamespace routes its ISVCs through a second validator instance
// registered with AutoscalerPolicyEnabled=false: the namespaceSelector in
// testdata/webhooks.yaml matches disabledSelectorLabel, so the
// feature-disabled rejection is exercised end-to-end without disturbing
// the suite's default (feature-enabled) validator.
const (
	disabledNamespace          = "aspolicy-webhook-disabled"
	disabledSelectorLabel      = "ome-test-autoscaler-policy"
	disabledSelectorLabelValue = "disabled"
)

func TestAutoscalerPolicyWebhook(t *testing.T) {
	util.RunSuite(t, "AutoscalerPolicy Webhook Integration Suite")
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
		WebhookPath: "tests/integration/singlecluster/webhook/autoscalerpolicy/testdata",
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
		server.Register("/validate-ome-io-v1beta1-autoscalerpolicy", &webhook.Admission{
			Handler: &policywebhook.Validator{
				Client:  mgr.GetAPIReader(),
				Decoder: admission.NewDecoder(mgr.GetScheme()),
			},
		})
		// A second ISVC validator instance runs with the feature disabled;
		// the namespaceSelector on its webhook config scopes it to
		// disabledNamespace so the blanket rejection is testable end-to-end.
		server.Register("/validate-inferenceservice-policy-disabled", admission.WithCustomValidator(
			mgr.GetScheme(), &v1beta1.InferenceService{},
			&isvcwebhook.InferenceServiceValidator{
				Client:                  mgr.GetClient(),
				RuntimeSelector:         runtimeselector.New(mgr.GetClient()),
				AutoscalerPolicyEnabled: false,
			}))
		// The ISVC validator runs with the AutoscalerPolicy feature enabled
		// so a rejected ref fails on its reserved Kind, not on the blanket
		// feature-disabled rejection.
		return ctrl.NewWebhookManagedBy(mgr).
			For(&v1beta1.InferenceService{}).
			WithValidator(&isvcwebhook.InferenceServiceValidator{
				Client:                  mgr.GetClient(),
				RuntimeSelector:         runtimeselector.New(mgr.GetClient()),
				AutoscalerPolicyEnabled: true,
			}).
			Complete()
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())

	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
	})).To(gomega.Succeed())
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   disabledNamespace,
			Labels: map[string]string{disabledSelectorLabel: disabledSelectorLabelValue},
		},
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
// namespace, and the in-use DELETE check matches consumers by policy name.
func uniqueName(prefix string) string {
	nameSeq++
	return fmt.Sprintf("%s-%d", prefix, nameSeq)
}

// validKedaSpec is the suite's baseline valid policy: one prometheus
// trigger with a bound-by-name provider, a templated query over the
// allowed variable set, and explicit ignoreNullValues. Specs break exactly
// one property of it per rejection case.
func validKedaSpec() v1beta1.AutoscalerPolicySpec {
	return v1beta1.AutoscalerPolicySpec{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaPolicyTemplate{
			Triggers: []v1beta1.KedaTriggerTemplate{{
				Type:        "prometheus",
				ProviderRef: &v1beta1.MetricProviderRef{Name: "llm-metrics"},
				Metadata: map[string]string{
					"query":            `sum(rate(http_requests_total{namespace="{{ .Namespace }}"}[1m]))`,
					"threshold":        "10",
					"ignoreNullValues": "false",
				},
			}},
		},
	}
}

func mkPolicy(name string, spec v1beta1.AutoscalerPolicySpec) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:       spec,
	}
}

// mkConsumer builds an InferenceService the ISVC validator admits without
// any model or runtime object existing: runtime-only specs referencing an
// absent runtime pass availability (absence is not an admission error), so
// the suite needs no BaseModel/ServingRuntime fixtures.
func mkConsumer(name, policyName string) *v1beta1.InferenceService {
	kind := "ClusterServingRuntime"
	apiGroup := "ome.io"
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "llm-serving", Kind: &kind, APIGroup: &apiGroup},
			Engine: &v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "llm-serving:v1"}},
				},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: policyName},
				},
			},
		},
	}
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
