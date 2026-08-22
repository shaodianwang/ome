package acceleratorquota_cert

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	timeout  = "90s"
	interval = "200ms"
)

var _ = ginkgo.Describe("Internal webhook certificate management", func() {
	ginkgo.It("signals readiness once the certificate is usable", func() {
		// Webhook handlers register only after this closes, and the component's
		// readyz gate is wired to it. It is a composite signal — cert on disk
		// AND caBundle injected — so a pod that reports ready is one the
		// apiserver can actually complete a handshake with.
		gomega.Eventually(func() bool {
			select {
			case <-certsReady:
				return true
			default:
				return false
			}
		}, timeout, interval).Should(gomega.BeTrue(),
			"the rotator never reported the certificate ready")

		// The two names controller-runtime's webhook server loads.
		for _, name := range []string{"tls.crt", "tls.key"} {
			_, err := os.Stat(filepath.Join(certDir, name))
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "%s is not on disk", name)
		}
	})

	ginkgo.It("persists the material in the Secret so a restart reuses the same CA", func() {
		// The Secret is the only durable copy: the cert directory is a
		// projection of it. Without these keys a restart would mint a new CA and
		// admission would break until the new bundle was injected.
		secret := &corev1.Secret{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(suiteCtx,
				client.ObjectKey{Namespace: certNamespace, Name: secretName}, secret)).To(gomega.Succeed())
			for _, key := range []string{"ca.crt", "ca.key", "tls.crt", "tls.key"} {
				g.Expect(secret.Data).To(gomega.HaveKey(key))
			}
		}, timeout, interval).Should(gomega.Succeed())
	})

	ginkgo.It("injects a caBundle that verifies what the webhook serves", func() {
		// The end-to-end claim, and the only assertion that catches a CA and a
		// serving cert that are each well-formed but unrelated: the apiserver
		// dials the Service and validates the served chain against this
		// caBundle, so a mismatch is a hard admission failure.
		var caBundle []byte
		gomega.Eventually(func(g gomega.Gomega) {
			cfg := &admissionregistrationv1.ValidatingWebhookConfiguration{}
			g.Expect(k8sClient.Get(suiteCtx,
				client.ObjectKey{Name: webhookConfigName}, cfg)).To(gomega.Succeed())
			g.Expect(cfg.Webhooks).To(gomega.HaveLen(1))
			caBundle = cfg.Webhooks[0].ClientConfig.CABundle
			g.Expect(caBundle).NotTo(gomega.BeEmpty())
			g.Expect(string(caBundle)).NotTo(gomega.Equal(placeholderCABundle),
				"the placeholder caBundle was never replaced")
		}, timeout, interval).Should(gomega.Succeed())

		roots := x509.NewCertPool()
		gomega.Expect(roots.AppendCertsFromPEM(caBundle)).To(gomega.BeTrue(),
			"the injected caBundle is not parseable PEM")

		servingPEM, err := os.ReadFile(filepath.Join(certDir, "tls.crt"))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		block, _ := pem.Decode(servingPEM)
		gomega.Expect(block).NotTo(gomega.BeNil(), "the serving certificate is not PEM")
		serving, err := x509.ParseCertificate(block.Bytes)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// The name the apiserver dials comes from the Service reference, so a
		// cert issued for anything else fails the handshake even with a valid
		// chain.
		_, err = serving.Verify(x509.VerifyOptions{
			Roots:     roots,
			DNSName:   serviceName + "." + certNamespace + ".svc",
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"the served certificate does not verify against the injected caBundle")
	})

	ginkgo.It("restores the caBundle after it is clobbered", func() {
		// A re-apply of the chart or a GitOps sync resets the bundle to the
		// placeholder. Recovery has to be automatic: with failurePolicy=Fail a
		// stale bundle blocks every quota write until someone notices.
		cfg := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		gomega.Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Name: webhookConfigName}, cfg)).To(gomega.Succeed())
		injected := cfg.Webhooks[0].ClientConfig.CABundle
		gomega.Expect(injected).NotTo(gomega.BeEmpty())

		cfg.Webhooks[0].ClientConfig.CABundle = []byte(placeholderCABundle)
		gomega.Expect(k8sClient.Update(suiteCtx, cfg)).To(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			current := &admissionregistrationv1.ValidatingWebhookConfiguration{}
			g.Expect(k8sClient.Get(suiteCtx,
				client.ObjectKey{Name: webhookConfigName}, current)).To(gomega.Succeed())
			g.Expect(current.Webhooks[0].ClientConfig.CABundle).To(gomega.Equal(injected))
		}, timeout, interval).Should(gomega.Succeed())
	})
})
