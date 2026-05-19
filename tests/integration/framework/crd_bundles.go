package framework

import (
	apiruntime "k8s.io/apimachinery/pkg/runtime"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
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
