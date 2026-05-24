package omenative

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// fixtureOptions is the functional-options shape for newOMENativeFixture.
// Defaults are tuned to "minimum viable OMENative ISVC that the
// controller can reconcile to Ready" — single replica, single safetensors
// container, LlamaForCausalLM architecture matching the suite's CBM
// inspection fake.
type fixtureOptions struct {
	cbmName            string
	csrName            string
	isvcName           string
	image              string
	minReplicas        int
	updateStrategy     v1beta1.OMENativeUpdateStrategyType
	restartPolicy      v1beta1.OMENativeRestartPolicy
	migrationMode      v1beta1.OMENativeMigrationPolicyMode
	engineAnnots       map[string]string
	extraContainers    []corev1.Container
	engineAffinity     *corev1.Affinity
	withDecoder        bool
	decoderImage       string
	decoderMinReplicas int
	decoderRawMode     bool
	withRouter         bool
	routerImage        string
	routerMinReplicas  int
	routerRawMode      bool
}

// FixtureOption mutates fixtureOptions before the fixture is built.
type FixtureOption func(*fixtureOptions)

// WithCBMName overrides the ClusterBaseModel name.
func WithCBMName(n string) FixtureOption { return func(o *fixtureOptions) { o.cbmName = n } }

// WithCSRName overrides the ClusterServingRuntime name.
func WithCSRName(n string) FixtureOption { return func(o *fixtureOptions) { o.csrName = n } }

// WithISVCName overrides the InferenceService name. Pods derive their
// stable names from this, so tests that assert on pod names should
// set it deterministically.
func WithISVCName(n string) FixtureOption { return func(o *fixtureOptions) { o.isvcName = n } }

// WithImage overrides the engine container image. Defaults to a
// throwaway test image; tests asserting on image updates can set
// this to track a deliberate version.
func WithImage(img string) FixtureOption { return func(o *fixtureOptions) { o.image = img } }

// WithMinReplicas sets engine.MinReplicas. Defaults to 1.
func WithMinReplicas(n int) FixtureOption { return func(o *fixtureOptions) { o.minReplicas = n } }

// WithUpdateStrategy sets engine.OMENative.UpdateStrategy.Type.
// Defaults to InPlaceIfPossible (matches OEP defaulter behavior).
func WithUpdateStrategy(t v1beta1.OMENativeUpdateStrategyType) FixtureOption {
	return func(o *fixtureOptions) { o.updateStrategy = t }
}

// WithRestartPolicy sets engine.OMENative.RestartPolicy.
// Defaults to RecreateInstance.
func WithRestartPolicy(p v1beta1.OMENativeRestartPolicy) FixtureOption {
	return func(o *fixtureOptions) { o.restartPolicy = p }
}

// WithMigrationMode sets engine.OMENative.MigrationPolicy.Mode.
// Defaults to Auto. Set to Never to disable migration request
// processing for tests that want to assert non-migration behavior.
func WithMigrationMode(m v1beta1.OMENativeMigrationPolicyMode) FixtureOption {
	return func(o *fixtureOptions) { o.migrationMode = m }
}

// WithEngineAnnotation adds a key/value to
// engine.ComponentExtensionSpec.Annotations. Sticky across calls.
func WithEngineAnnotation(k, v string) FixtureOption {
	return func(o *fixtureOptions) {
		if o.engineAnnots == nil {
			o.engineAnnots = map[string]string{}
		}
		o.engineAnnots[k] = v
	}
}

// WithExtraContainer adds an additional container to the engine
// PodSpec. Useful for tests asserting on multi-container behavior.
func WithExtraContainer(c corev1.Container) FixtureOption {
	return func(o *fixtureOptions) { o.extraContainers = append(o.extraContainers, c) }
}

// WithEngineAffinity sets engine.PodSpec.Affinity. Used by migration
// specs to construct a PodSpec whose hard NodeAffinity collides with
// the overlay's NotIn[FromNode].
func WithEngineAffinity(a *corev1.Affinity) FixtureOption {
	return func(o *fixtureOptions) { o.engineAffinity = a }
}

// WithDecoder enables a Decoder component on the ISVC, opted into
// OMENative via per-component annotation. Used for PD-disaggregated
// integration coverage (OEP-0007.1 foundation).
func WithDecoder() FixtureOption {
	return func(o *fixtureOptions) { o.withDecoder = true }
}

// WithDecoderImage overrides the decoder container image when
// WithDecoder is set. Defaults to the engine's image so a
// Decoder-aware test can still bump the engine in isolation.
func WithDecoderImage(img string) FixtureOption {
	return func(o *fixtureOptions) { o.decoderImage = img }
}

// WithDecoderMinReplicas sets decoder.MinReplicas (only meaningful
// with WithDecoder). Defaults to 1.
func WithDecoderMinReplicas(n int) FixtureOption {
	return func(o *fixtureOptions) { o.decoderMinReplicas = n }
}

// WithDecoderRawMode flips the Decoder annotation from OMENative to
// RawDeployment. Used by mixed-mode tests asserting per-component
// dispatch isolation.
func WithDecoderRawMode() FixtureOption {
	return func(o *fixtureOptions) { o.decoderRawMode = true }
}

// WithRouter enables a Router component on the ISVC, opted into
// OMENative via per-component annotation. Used for three-way PD
// integration coverage.
func WithRouter() FixtureOption {
	return func(o *fixtureOptions) { o.withRouter = true }
}

// WithRouterImage overrides the router container image. Defaults to
// the engine's image when WithRouter is set.
func WithRouterImage(img string) FixtureOption {
	return func(o *fixtureOptions) { o.routerImage = img }
}

// WithRouterMinReplicas sets router.MinReplicas. Defaults to 1.
func WithRouterMinReplicas(n int) FixtureOption {
	return func(o *fixtureOptions) { o.routerMinReplicas = n }
}

// WithRouterRawMode flips the Router annotation from OMENative to
// RawDeployment.
func WithRouterRawMode() FixtureOption {
	return func(o *fixtureOptions) { o.routerRawMode = true }
}

// omenativeFixture bundles the three objects every OMENative test
// has to create. Methods (none yet) can layer scenario-specific
// mutations on top.
type omenativeFixture struct {
	CBM  *v1beta1.ClusterBaseModel
	CSR  *v1beta1.ClusterServingRuntime
	ISVC *v1beta1.InferenceService
}

// newOMENativeFixture builds the three objects required to drive an
// OMENative ISVC through the reconcile loop. Does NOT apply them to
// the apiserver — caller chains Create + DeferCleanup so each spec
// can decide the apply order and any pre-create mutations.
//
// Defaults produce a minimum-viable spec the suite's stock
// CBM-inspection fake (in suite_test.go) accepts: format=safetensors,
// architecture=LlamaForCausalLM, framework=Transformers.
//
// The engine annotation ome.io/deploymentMode=OMENative is set here
// rather than via the ISVC-level wrapper helper because the
// integration suite doesn't register the defaulting webhook that
// propagates the ISVC-level annotation down (see suite_test.go
// comments). Setting on the engine spec directly is the
// integration-test contract.
//
// Pure constructor — no t.Helper() / Ginkgo coupling so it's callable
// from any test framework variant in the suite (regular testing.T
// or GinkgoT) and stays trivially mockable.
func newOMENativeFixture(namespace string, opts ...FixtureOption) *omenativeFixture {
	o := &fixtureOptions{
		cbmName:            "fixture-llama-7b",
		csrName:            "fixture-runtime",
		isvcName:           "fixture-isvc",
		image:              "ghcr.io/test/runtime:test",
		minReplicas:        1,
		updateStrategy:     v1beta1.OMENativeUpdateStrategyInPlaceIfPossible,
		restartPolicy:      v1beta1.OMENativeRestartPolicyRecreateInstance,
		migrationMode:      v1beta1.OMENativeMigrationPolicyModeAuto,
		decoderMinReplicas: 1,
		routerMinReplicas:  1,
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.decoderImage == "" {
		o.decoderImage = o.image
	}
	if o.routerImage == "" {
		o.routerImage = o.image
	}

	cbm := v1beta1testing.MakeClusterBaseModel(o.cbmName).
		StorageURI("oci://n/test/b/models/o/llama-7b").
		ModelFormat("safetensors").
		ModelType("llama").
		ModelArchitecture("LlamaForCausalLM").
		ModelFramework("Transformers").
		Obj()

	csr := v1beta1testing.MakeClusterServingRuntime(o.csrName).
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		Obj()

	containers := []corev1.Container{{
		Name:  "ome-container",
		Image: o.image,
	}}
	containers = append(containers, o.extraContainers...)

	// Engine annotations: OMENative deployment mode + any caller-
	// supplied additions.
	engineAnnots := map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	for k, v := range o.engineAnnots {
		engineAnnots[k] = v
	}

	mr := o.minReplicas
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: engineAnnots,
			MinReplicas: &mr,
			OMENative: &v1beta1.OMENativeComponentSpec{
				UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{Type: o.updateStrategy},
				RestartPolicy:  &o.restartPolicy,
				MigrationPolicy: &v1beta1.OMENativeMigrationPolicy{
					Mode: o.migrationMode,
				},
			},
		},
		PodSpec: v1beta1.PodSpec{Containers: containers, Affinity: o.engineAffinity},
	}

	wrapper := v1beta1testing.MakeInferenceService(o.isvcName, namespace).
		Model(o.cbmName).
		Runtime(o.csrName).
		DeploymentMode(constants.OMENative).
		Engine(engine)

	if o.withDecoder {
		dmr := o.decoderMinReplicas
		mode := string(constants.OMENative)
		if o.decoderRawMode {
			mode = string(constants.RawDeployment)
		}
		decoderExt := v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: mode},
			MinReplicas: &dmr,
		}
		if !o.decoderRawMode {
			decoderExt.OMENative = &v1beta1.OMENativeComponentSpec{
				UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{Type: o.updateStrategy},
				RestartPolicy:  &o.restartPolicy,
				MigrationPolicy: &v1beta1.OMENativeMigrationPolicy{
					Mode: o.migrationMode,
				},
			}
		}
		decoder := &v1beta1.DecoderSpec{
			ComponentExtensionSpec: decoderExt,
			PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
				Name:  "ome-container",
				Image: o.decoderImage,
			}}},
		}
		wrapper = wrapper.Decoder(decoder)
	}

	if o.withRouter {
		rmr := o.routerMinReplicas
		mode := string(constants.OMENative)
		if o.routerRawMode {
			mode = string(constants.RawDeployment)
		}
		routerExt := v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: mode},
			MinReplicas: &rmr,
		}
		if !o.routerRawMode {
			routerExt.OMENative = &v1beta1.OMENativeComponentSpec{
				UpdateStrategy: &v1beta1.OMENativeUpdateStrategy{Type: o.updateStrategy},
				RestartPolicy:  &o.restartPolicy,
				MigrationPolicy: &v1beta1.OMENativeMigrationPolicy{
					Mode: o.migrationMode,
				},
			}
		}
		router := &v1beta1.RouterSpec{
			ComponentExtensionSpec: routerExt,
			PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
				Name:  "ome-container",
				Image: o.routerImage,
			}}},
		}
		wrapper = wrapper.Router(router)
	}

	isvc := wrapper.Obj()
	return &omenativeFixture{CBM: cbm, CSR: csr, ISVC: isvc}
}
