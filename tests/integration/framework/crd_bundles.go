package framework

import (
	apiruntime "k8s.io/apimachinery/pkg/runtime"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

// Pre-defined OptionalCRDBundle constants for OME's optional dependencies.
// Each suite picks the bundles it needs via Framework.OptionalCRDs:
//
//	fwk = &framework.Framework{
//	    OptionalCRDs: []framework.OptionalCRDBundle{framework.LWS, framework.KEDA},
//	}
//
// CRDDir is resolved against ProjectRoot/tests/integration/dep-crds/<dir>.
// Tests that bump a vendored CRD version run `make <bundle>-crds`
// (which copies CRDs from the corresponding go.mod module) and commit
// the result. Procedure: tests/integration/dep-crds/README.md.

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
// Required for the traffic translator (Envoy Gateway path)
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

// PrometheusOperator bundle: monitoring.coreos.com/v1.PodMonitor.
// Required for the ISVC RawDeployment path, which
// always reconciles a PodMonitor via owner-ref + Create. The
// prometheus-operator Go module ships the types but not the CRD
// YAMLs, so the YAML in dep-crds/prometheus-operator/ is hand-vendored
// (same pattern as envoy-gateway). See that directory's SOURCE file.
var PrometheusOperator = OptionalCRDBundle{
	Name:   "prometheus-operator",
	CRDDir: "prometheus-operator",
	Schemes: []func(*apiruntime.Scheme) error{
		monitoringv1.AddToScheme,
	},
}

// GKESlice bundle: accelerator.gke.io/v1beta1.Slice, the cluster-scoped
// object that binds a TPU partition on a node pool that provisions slices
// on demand. Required for the InferenceReplica TPU slice provisioning
// integration tests: the IR reconciler provisions slices only when the
// CRD is served at setup. Slices are read and written unstructured
// (pkg/tpuslice/gke), so the bundle has no schemes.
//
// The CRD YAML in dep-crds/gke-slice/ is a hand-authored stub; see that
// directory's SOURCE file for what it models and the refresh procedure.
var GKESlice = OptionalCRDBundle{
	Name:    "gke-slice",
	CRDDir:  "gke-slice",
	Schemes: nil,
}

// Kueue bundle: kueue.x-k8s.io/v1beta2.{ResourceFlavor,ClusterQueue,
// LocalQueue,Workload,Cohort,AdmissionCheck,Topology,…}. v1beta2 is the
// storage version and the one Kueue's own controllers speak, so the
// harness registers it alone. Registering the served v1beta1 too would be
// a trap: Kueue's conversion webhook is applied by a kustomize patch that
// dep-crds does not vendor, so the bases carry no conversion strategy and
// the apiserver would hand back a bare apiVersion rewrite — dropping the
// fields v1beta2 renamed, cohortName among them. Required for the
// real-Kueue placement validation harness, which runs Kueue's full
// admission stack (StartKueue) in-process against envtest and
// gates→admits→ungates queue-labeled pods with no kubelet.
//
// The CRD YAMLs in dep-crds/kueue/ are module-sourced from
// sigs.k8s.io/kueue's config/components/crd/bases (vendored by
// `make kueue-crds`, gitignored). The Kueue pod webhook — needed to
// actually gate pods — is NOT installed by this bundle; StartKueue's
// callers wire the webhook server, and the webhook manifests live in
// the module's config/components/webhook (see framework/kueue.go).
var Kueue = OptionalCRDBundle{
	Name:   "kueue",
	CRDDir: "kueue",
	Schemes: []func(*apiruntime.Scheme) error{
		kueue.AddToScheme,
	},
}
