// Package cert self-manages the webhook serving certificate.
//
// The component generates its own CA and serving cert, stores them in a Secret,
// and injects the caBundle into its own ValidatingWebhookConfiguration — so the
// admission path works out of the box with no cert-manager in the cluster and no
// Issuer or Certificate in the chart. Rotation is handled on a timer thereafter.
// This mirrors what Kueue does by default, which matters because the quota plane
// is usually installed alongside Kueue and an operator should not need a second
// cert story for it.
//
// Delivery to the webhook server is via the kubelet, not this process: the
// Secret is mounted at CertDir, so a generated cert becomes servable only once
// the kubelet re-projects it. Readiness is gated on that, so a fresh pod is
// unready until the projection lands — seconds where the kubelet watches Secret
// changes, a full sync period where it polls.
//
// The trade is a real one and worth naming: self-injection requires write access
// to the ValidatingWebhookConfiguration this component owns. Options scopes that
// as narrowly as the library allows — one named webhook config, no mutating
// webhooks — but list/watch on the kind cannot be narrowed, because the rotator
// builds an unfiltered informer over it.
package cert

import (
	"fmt"

	"github.com/go-logr/logr"
	rotator "github.com/open-policy-agent/cert-controller/pkg/rotator"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// Options names the objects the rotator touches. Every one is supplied by the
// caller rather than defaulted here: the chart derives them from the release
// name, so a compiled-in default would silently disagree with any release not
// called what this package guessed.
type Options struct {
	// Namespace holds the Secret and the webhook Service. Required — the
	// rotator cannot build a Secret key without it.
	Namespace string
	// SecretName is the Secret the CA and serving cert are stored in. The chart
	// must create it empty: the rotator updates, and never creates, so a missing
	// Secret is a permanent startup failure rather than something it recovers
	// from.
	SecretName string
	// ServiceName is the webhook Service, used to derive the cert's DNS name.
	ServiceName string
	// WebhookConfigName is the ValidatingWebhookConfiguration to inject the
	// caBundle into.
	WebhookConfigName string
	// CertDir is where the webhook server reads the serving cert. Nothing in
	// this process writes it: the rotator writes the Secret and the kubelet
	// projects it back down, so this must be the Secret's mount path, and the
	// component is not ready until that projection lands.
	CertDir string
	// CAName and CAOrganization identify the generated CA in its subject.
	CAName         string
	CAOrganization string
}

func (o Options) validate() error {
	for _, f := range []struct{ name, value string }{
		{"namespace", o.Namespace},
		{"secret name", o.SecretName},
		{"service name", o.ServiceName},
		{"webhook config name", o.WebhookConfigName},
		{"cert directory", o.CertDir},
		{"CA name", o.CAName},
		{"CA organization", o.CAOrganization},
	} {
		if f.value == "" {
			return fmt.Errorf("internal cert management: %s is required", f.name)
		}
	}
	return nil
}

// Manage registers the certificate rotator with the manager.
//
// certsReady is closed once the cert is on disk and the caBundle is injected.
// The caller MUST wait on it before registering webhook handlers: the
// ValidatingWebhookConfiguration is failurePolicy=Fail, so serving before the
// apiserver can verify the cert turns every quota write into a TLS error rather
// than a clean rejection.
//
// On a fresh install that wait spans the kubelet's Secret projection, because
// the generated cert reaches CertDir only once the kubelet writes it there.
func Manage(mgr ctrl.Manager, opts Options, certsReady chan struct{}) error {
	if err := opts.validate(); err != nil {
		return err
	}
	return rotator.AddRotator(mgr, &rotator.CertRotator{
		SecretKey: types.NamespacedName{
			Namespace: opts.Namespace,
			Name:      opts.SecretName,
		},
		CertDir:        opts.CertDir,
		CAName:         opts.CAName,
		CAOrganization: opts.CAOrganization,
		DNSName:        fmt.Sprintf("%s.%s.svc", opts.ServiceName, opts.Namespace),
		ExtraDNSNames: []string{
			fmt.Sprintf("%s.%s.svc.cluster.local", opts.ServiceName, opts.Namespace),
		},
		IsReady: certsReady,
		Webhooks: []rotator.WebhookInfo{{
			Type: rotator.Validating,
			Name: opts.WebhookConfigName,
		}},
		// Every replica serves admission, so every replica needs the cert on
		// disk — gating rotation on leadership would leave the followers unable
		// to answer the apiserver.
		RequireLeaderElection: false,
	})
}

// WaitForReady blocks until the rotator reports the cert is usable.
func WaitForReady(log logr.Logger, certsReady chan struct{}) {
	log.Info("waiting for the webhook certificate to be generated and injected")
	<-certsReady
	log.Info("webhook certificate ready")
}
