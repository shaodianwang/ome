package canary

import (
	"context"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// buildSampleRequest snapshots everything the background eval needs to query one
// step's analysis WITHOUT touching the live InferenceService — the reconcile owns
// it, so a goroutine reading it would race. It resolves the source address, bearer
// token, headers, and template context up front; the eval then only performs the
// HTTP query. The address is the canary's own ServerAddress when set, else the
// operator-configured bundled address; an empty address means "no source" and the
// eval's query reads inconclusive.
func buildSampleRequest(ctx context.Context, in ReconcileInputs, a *v1beta1.RolloutAnalysis, step int32) (SampleRequest, error) {
	addr := in.BundledPrometheusAddress
	var token string
	var headers map[string]string
	if src := in.Prometheus; src != nil { // canary-level shared source
		if src.ServerAddress != "" {
			addr = src.ServerAddress
		}
		// Clone the map: src.Headers points into the live ISVC spec, and the result
		// is read by the background eval goroutine. A reference copy would hand a
		// spec-owned map to another goroutine, breaking the snapshot invariant.
		headers = maps.Clone(src.Headers)
		if src.AuthRef != nil {
			t, err := resolveBearerToken(ctx, in.Client, in.ISVC.Namespace, src.AuthRef)
			if err != nil {
				return SampleRequest{}, fmt.Errorf("resolve bearer token: %w", err)
			}
			token = t
		}
	}
	return SampleRequest{
		Key: SampleKey{
			Namespace: in.ISVC.Namespace,
			ISVCName:  in.ISVC.Name,
			Component: string(in.Component),
			Revision:  in.CanaryRevisionHash,
			Step:      step,
		},
		ServerAddress: addr,
		BearerToken:   token,
		Headers:       headers,
		TemplateContext: analysis.TemplateContext{
			Namespace:      in.ISVC.Namespace,
			ISVCName:       in.ISVC.Name,
			Component:      string(in.Component),
			CanaryService:  coordination.PerRevisionServiceName(in.ISVC.Name, in.Component, in.CanaryRevisionHash),
			StableService:  coordination.PerRevisionServiceName(in.ISVC.Name, in.Component, in.StableRevisionHash),
			CanaryRevision: in.CanaryRevisionHash,
			StableRevision: in.StableRevisionHash,
		},
		// DeepCopy so the background eval never reads through a pointer into the live
		// spec (the reconcile owns it).
		Analysis:     a.DeepCopy(),
		QueryTimeout: in.QueryTimeout,
	}, nil
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
