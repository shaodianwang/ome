package framework

import (
	apiruntime "k8s.io/apimachinery/pkg/runtime"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

// Pre-defined OptionalCRDBundle constants for OME's optional dependencies.
// Each suite picks the bundles it needs via Framework.OptionalCRDs:
//
//	fwk = &framework.Framework{
//	    OptionalCRDs: []framework.OptionalCRDBundle{framework.Knative},
//	}
//
// CRDDir is resolved against ProjectRoot/tests/integration/dep-crds/<dir>.
// Tests that bump a vendored CRD version run `make <bundle>-crds`
// (which copies CRDs from the corresponding go.mod module) and commit
// the result. Procedure: tests/integration/dep-crds/README.md.

// Knative bundle: serving.knative.dev/v1 Service + Revision + Route +
// Configuration + DomainMapping. Required for InferenceService
// Serverless mode (engine.MinReplicas=0).
var Knative = OptionalCRDBundle{
	Name:   "knative-serving",
	CRDDir: "knative-serving",
	Schemes: []func(*apiruntime.Scheme) error{
		knservingv1.AddToScheme,
	},
}

// LWS bundle: leaderworkerset.x-k8s.io/v1.LeaderWorkerSet. Required
// for InferenceService MultiNode mode (engine.Leader/Worker present).
var LWS = OptionalCRDBundle{
	Name:   "lws",
	CRDDir: "lws",
	Schemes: []func(*apiruntime.Scheme) error{
		lwsv1.AddToScheme,
	},
}

// KEDA bundle: keda.sh/v1alpha1.{ScaledObject,ScaledJob,
// TriggerAuthentication,ClusterTriggerAuthentication}. Required for
// InferenceService autoscalerClass=keda (or default-when-no-class-set
// per shouldCreateScaledObject in pkg/controller/.../keda/keda_reconciler.go).
// The ISVC reconciler creates ScaledObject; the other 3 CRDs come
// along because keda's `make manifests` emits them as one bundle.
var KEDA = OptionalCRDBundle{
	Name:   "keda",
	CRDDir: "keda",
	Schemes: []func(*apiruntime.Scheme) error{
		kedav1.AddToScheme,
	},
}

// EnvoyGateway bundle: gateway.envoyproxy.io/v1alpha1.BackendTrafficPolicy.
// Required for the OEP-0011 traffic translator (Envoy Gateway path)
// integration tests. The translator emits unstructured BTPs so the
// bundle has no schemes — the apiserver-side CRD is the only thing
// envtest needs.
//
// The CRD YAML in dep-crds/envoy-gateway/ is hand-authored (Envoy
// Gateway has no Go module distribution we can pull from go.mod); see
// that directory's SOURCE file for the upstream reference and the
// refresh procedure.
var EnvoyGateway = OptionalCRDBundle{
	Name:    "envoy-gateway",
	CRDDir:  "envoy-gateway",
	Schemes: nil,
}

// GatewayAPI bundle: gateway.networking.k8s.io/v1.HTTPRoute (plus
// supporting types). Required for integration tests that exercise the
// HTTPRoute builder's weighted-backendRef path against a real
// apiserver. The full Gateway API standard channel includes Gateway,
// GatewayClass, HTTPRoute, ReferenceGrant, GRPCRoute — we vendor just
// HTTPRoute since that's what OME emits and what the traffic-test
// suite needs.
//
// The CRD YAML in dep-crds/gateway-api/ comes from upstream
// sigs.k8s.io/gateway-api; see that directory's SOURCE file for the
// pinned version and refresh procedure. The Go type registration
// (gatewayapiv1.Install) mirrors what cmd/manager/main.go does at
// runtime when ingressConfig.EnableGatewayAPI is set.
var GatewayAPI = OptionalCRDBundle{
	Name:   "gateway-api",
	CRDDir: "gateway-api",
	Schemes: []func(*apiruntime.Scheme) error{
		gatewayapiv1.Install,
	},
}
