package canary

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// analysisQueryTimeout bounds one analysis sampling pass (all metric queries).
// It is deliberately short: the query runs SYNCHRONOUSLY inside Reconcile and the
// InferenceService controller runs at MaxConcurrentReconciles=1, so a hung
// Prometheus would otherwise stall EVERY InferenceService's reconcile for the
// whole timeout, not just this canary's. An exceeded timeout surfaces as an
// inconclusive sample (hold), never a failure. A deeper fix — raising reconcile
// concurrency or moving sampling off the reconcile goroutine — is tracked
// separately.
const analysisQueryTimeout = 5 * time.Second

// AnalysisSampleFunc evaluates the canary's analysis metrics once. The controller
// wires PrometheusSample; tests inject a fake to keep Reconcile hermetic. It is
// nil under Manual/Auto promotion and never called there.
type AnalysisSampleFunc func(ctx context.Context, in ReconcileInputs, a *v1beta1.RolloutAnalysis) analysis.Result

// PrometheusSample is the production AnalysisSampleFunc. It resolves the
// Prometheus address (explicit ServerAddress, else the bundled Service in the
// ISVC namespace) and an optional bearer token, builds a Querier, and evaluates
// the metrics against the rollout's revision context. Any setup failure (missing
// secret, bad client) becomes an INCONCLUSIVE result so the caller holds rather
// than mistaking a wiring problem for a metric breach.
func PrometheusSample(ctx context.Context, in ReconcileInputs, a *v1beta1.RolloutAnalysis) analysis.Result {
	addr := analysis.DefaultBundledServerAddress(in.ISVC.Namespace)
	var token string
	var headers map[string]string
	if src := in.Prometheus; src != nil { // canary-level shared source
		if src.ServerAddress != "" {
			addr = src.ServerAddress
		}
		headers = src.Headers
		if src.AuthRef != nil {
			t, err := resolveBearerToken(ctx, in.Client, in.ISVC.Namespace, src.AuthRef)
			if err != nil {
				return inconclusiveResult("auth", fmt.Sprintf("resolve bearer token: %v", err))
			}
			token = t
		}
	}
	q, err := analysis.NewQuerier(addr, token, headers)
	if err != nil {
		return inconclusiveResult("prometheus", fmt.Sprintf("build client for %q: %v", addr, err))
	}
	tc := analysis.TemplateContext{
		Namespace:      in.ISVC.Namespace,
		ISVCName:       in.ISVC.Name,
		Component:      string(in.Component),
		CanaryService:  coordination.PerRevisionServiceName(in.ISVC.Name, in.Component, in.CanaryRevisionHash),
		StableService:  coordination.PerRevisionServiceName(in.ISVC.Name, in.Component, in.StableRevisionHash),
		CanaryRevision: in.CanaryRevisionHash,
		StableRevision: in.StableRevisionHash,
	}
	qctx, cancel := context.WithTimeout(ctx, analysisQueryTimeout)
	defer cancel()
	return analysis.Evaluate(qctx, q, a, tc)
}

// resolveBearerToken reads the token value from the referenced Secret key in the
// ISVC namespace. SecretKeySelector is namespace-local by design — analysis auth
// never crosses namespaces.
func resolveBearerToken(ctx context.Context, c client.Client, namespace string, ref *corev1.SecretKeySelector) (string, error) {
	if c == nil {
		return "", fmt.Errorf("no client to read secret %q", ref.Name)
	}
	sec := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, sec); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("secret %s/%s not found", namespace, ref.Name)
		}
		return "", err
	}
	b, ok := sec.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", namespace, ref.Name, ref.Key)
	}
	return string(b), nil
}

// inconclusiveResult builds a single-metric inconclusive Result for a setup
// failure that occurs before any metric query runs.
func inconclusiveResult(name, msg string) analysis.Result {
	return analysis.Result{
		Outcome: analysis.Inconclusive,
		Metrics: []analysis.MetricResult{{Name: name, Message: msg}},
	}
}
