// Package acceleratorquota_cert hosts integration tests for ome-quota-manager's
// internal webhook certificate management (OEP-0024).
//
// The claim under test is an installation claim — "the admission path works with
// no cert-manager in the cluster" — and it can only be checked against a real
// apiserver: the rotator generates a CA, stores it in a Secret, and injects the
// caBundle into a live ValidatingWebhookConfiguration. A fake client would let
// all three of those pass while the real thing served a cert the apiserver
// refused.
//
// Separate from the admission suites beside it, and it cannot be merged into
// one: those set Framework.WebhookPath, which has envtest generate its own
// serving cert and rewrite every installed webhook's caBundle. That is the exact
// job the rotator is here to do, so the two would overwrite each other. This
// suite therefore installs an inert webhook configuration of its own and asserts
// on the certificate, never on an admission decision.
package acceleratorquota_cert

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	quotacert "github.com/ome-projects/ome/pkg/quota/cert"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// The names the chart derives from the release. Nothing in the binary defaults
// them, so the suite has to supply them exactly as a real install would.
const (
	certNamespace     = "default"
	secretName        = "ome-quota-manager-webhook-cert"
	serviceName       = "ome-quota-manager-webhook"
	webhookConfigName = "ome-quota-manager.ome.io"
	caName            = "ome-quota-manager-ca"
	caOrganization    = "ome"

	// What the chart renders before anything has been injected. The apiserver
	// requires a syntactically valid caBundle, so the field cannot simply be
	// left out.
	placeholderCABundle = "Cg=="
)

func TestQuotaCert(t *testing.T) {
	util.RunSuite(t, "Quota Webhook Certificate Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client

	// certDir stands in for the Deployment's read-only Secret mount. Nothing in
	// the process writes it; see startSecretProjection for what fills it.
	certDir string

	certsReady chan struct{}
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// The write-conflict gate catches an OME reconciler that lacks conflict-aware
	// retry. It does not apply here: the rotator refreshes from two paths at
	// once — its own startup runnable and the Secret watch — so the loser of
	// that race logs one optimistic-lock rejection at error level, retries with
	// backoff, and succeeds. Every spec below depends on that success, so a
	// regression to a genuinely stuck rotator fails them rather than passing
	// quietly behind the skip.
	s, err := framework.NewSuite(&framework.Framework{SkipWriteConflictCheck: true})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	certDir, err = os.MkdirTemp("", "quota-webhook-certs")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	startSecretProjection()

	// The rotator only ever Updates the Secret, so the chart creating it empty
	// is load-bearing. Seeding it here is not test convenience — it is the
	// contract the chart satisfies.
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: certNamespace, Name: secretName},
	})).To(gomega.Succeed())

	// failurePolicy=Ignore and a resource nothing in this suite writes: the
	// object exists to be injected into, and a fail-closed rule pointed at a
	// Service with no endpoints would wedge every other write instead.
	gomega.Expect(k8sClient.Create(suiteCtx, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: webhookConfigName},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "acceleratorquota.ome-quota-manager.validator",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				CABundle: []byte(placeholderCABundle),
				Service: &admissionregistrationv1.ServiceReference{
					Namespace: certNamespace,
					Name:      serviceName,
					Path:      ptr.To("/validate-ome-io-v1beta1-acceleratorquota"),
				},
			},
			FailurePolicy:           ptr.To(admissionregistrationv1.Ignore),
			SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
			AdmissionReviewVersions: []string{"v1"},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{"ome.io"},
					APIVersions: []string{"v1beta1"},
					Resources:   []string{"acceleratorquotas"},
				},
			}},
		}},
	})).To(gomega.Succeed())

	certsReady = make(chan struct{})
	mgrSetup := func(_ context.Context, mgr ctrl.Manager) error {
		return quotacert.Manage(mgr, quotacert.Options{
			Namespace:         certNamespace,
			SecretName:        secretName,
			ServiceName:       serviceName,
			WebhookConfigName: webhookConfigName,
			CertDir:           certDir,
			CAName:            caName,
			CAOrganization:    caOrganization,
		}, certsReady)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

// startSecretProjection stands in for the kubelet.
//
// The rotator never writes the cert directory — it writes the Secret, and in a
// real pod the kubelet re-projects that Secret into the mounted volume. envtest
// has no kubelet, so without this the certificate would sit in the API and never
// reach disk, and the readiness path under test would never complete. Modelling
// it explicitly is also what makes the chart's read-only Secret mount legible:
// this loop is the thing the volume replaces.
func startSecretProjection() {
	go func() {
		defer ginkgo.GinkgoRecover()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-suiteCtx.Done():
				return
			case <-ticker.C:
			}
			secret := &corev1.Secret{}
			if err := k8sClient.Get(suiteCtx,
				client.ObjectKey{Namespace: certNamespace, Name: secretName}, secret); err != nil {
				continue
			}
			for name, data := range secret.Data {
				if len(data) == 0 {
					continue
				}
				// Best effort, exactly like a sync loop: a failed write is
				// retried on the next tick rather than failing the suite from a
				// goroutine.
				_ = os.WriteFile(filepath.Join(certDir, name), data, 0o600)
			}
		}
	}()
}

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
	if certDir != "" {
		_ = os.RemoveAll(certDir)
	}
})
