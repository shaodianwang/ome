package coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// fixtureOptions is the functional-options shape for newCoordFixture.
// Defaults are tuned to "minimum viable OMENative ISVC the controller
// can reconcile to Ready" — single-replica engine + decoder, no router,
// no rolloutCoordination block.
//
// Mirrors the omenative suite's fixtureOptions but adds coordination
// knobs: a slice of RolloutCoordinationGroup the test wants applied to
// spec.rolloutCoordination.groups[], plus per-Component options for
// PD-style 3-way scenarios.
type fixtureOptions struct {
	cbmName  string
	csrName  string
	isvcName string

	// Engine knobs.
	engineImage       string
	engineMinReplicas int
	engineAnnots      map[string]string

	// Decoder knobs (always present by default; opt out with WithoutDecoder).
	withoutDecoder     bool
	decoderImage       string
	decoderMinReplicas int
	decoderRawMode     bool

	// Router knobs.
	withRouter        bool
	routerImage       string
	routerMinReplicas int
	routerRawMode     bool

	// Coordination knobs.
	coordGroups   []v1beta1.RolloutCoordinationGroup
	pausedGlobal  bool
	extraISVCAnns map[string]string
}

// FixtureOption mutates fixtureOptions before the fixture is built.
type FixtureOption func(*fixtureOptions)

// --- Naming overrides -----------------------------------------------------

func WithCBMName(n string) FixtureOption  { return func(o *fixtureOptions) { o.cbmName = n } }
func WithCSRName(n string) FixtureOption  { return func(o *fixtureOptions) { o.csrName = n } }
func WithISVCName(n string) FixtureOption { return func(o *fixtureOptions) { o.isvcName = n } }

// --- Engine knobs ---------------------------------------------------------

// WithEngineImage overrides the engine container image. Defaults to a
// throwaway test image; tests asserting on image updates can set this
// to track a deliberate version.
func WithEngineImage(img string) FixtureOption {
	return func(o *fixtureOptions) { o.engineImage = img }
}

// WithEngineReplicas sets engine.MinReplicas. Defaults to 1.
func WithEngineReplicas(n int) FixtureOption {
	return func(o *fixtureOptions) { o.engineMinReplicas = n }
}

// WithEngineAnnotation adds a key/value to engine.Annotations.
// Sticky across calls.
func WithEngineAnnotation(k, v string) FixtureOption {
	return func(o *fixtureOptions) {
		if o.engineAnnots == nil {
			o.engineAnnots = map[string]string{}
		}
		o.engineAnnots[k] = v
	}
}

// --- Decoder knobs --------------------------------------------------------

// WithoutDecoder drops the decoder Component. Useful for single-Component
// coordination groups (degenerate but allowed).
func WithoutDecoder() FixtureOption {
	return func(o *fixtureOptions) { o.withoutDecoder = true }
}

// WithDecoderImage overrides the decoder container image.
func WithDecoderImage(img string) FixtureOption {
	return func(o *fixtureOptions) { o.decoderImage = img }
}

// WithDecoderReplicas sets decoder.MinReplicas.
func WithDecoderReplicas(n int) FixtureOption {
	return func(o *fixtureOptions) { o.decoderMinReplicas = n }
}

// WithDecoderRawMode flips the decoder to RawDeployment mode. Used by
// mixed-mode coordination scenarios that need a non-OMENative peer.
func WithDecoderRawMode() FixtureOption {
	return func(o *fixtureOptions) { o.decoderRawMode = true }
}

// --- Router knobs ---------------------------------------------------------

// WithRouter adds a router Component. Defaults to OMENative; combine
// with WithRouterRawMode for mixed mode.
func WithRouter() FixtureOption {
	return func(o *fixtureOptions) { o.withRouter = true }
}

// WithRouterImage overrides the router container image.
func WithRouterImage(img string) FixtureOption {
	return func(o *fixtureOptions) {
		o.withRouter = true
		o.routerImage = img
	}
}

// WithRouterReplicas sets router.MinReplicas.
func WithRouterReplicas(n int) FixtureOption {
	return func(o *fixtureOptions) {
		o.withRouter = true
		o.routerMinReplicas = n
	}
}

// WithRouterRawMode flips the router to RawDeployment mode.
func WithRouterRawMode() FixtureOption {
	return func(o *fixtureOptions) {
		o.withRouter = true
		o.routerRawMode = true
	}
}

// --- Coordination knobs ---------------------------------------------------

// WithCoordinationGroup appends one RolloutCoordinationGroup to
// spec.rolloutCoordination.groups[]. Combine multiple calls for tests
// covering 2+ groups in a single ISVC.
func WithCoordinationGroup(g v1beta1.RolloutCoordinationGroup) FixtureOption {
	return func(o *fixtureOptions) { o.coordGroups = append(o.coordGroups, g) }
}

// WithBlueGreenGroup is sugar for WithCoordinationGroup with a fully-formed
// BlueGreen group over the given Components. Pacing defaults apply.
func WithBlueGreenGroup(components ...v1beta1.ComponentType) FixtureOption {
	return WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
		Components: components,
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
	})
}

// WithIndependentGroup is sugar for an explicit Independent group.
func WithIndependentGroup(components ...v1beta1.ComponentType) FixtureOption {
	return WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
		Components: components,
		Policy:     v1beta1.CoordinationPolicyIndependent,
	})
}

// WithRollingUpdateGroup is sugar for a RollingUpdate group.
func WithRollingUpdateGroup(components ...v1beta1.ComponentType) FixtureOption {
	return WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
		Components: components,
		Policy:     v1beta1.CoordinationPolicyRollingUpdate,
	})
}

// WithSequentialGroup is sugar for a Sequential group with order
// matching the listed components.
func WithSequentialGroup(order ...v1beta1.ComponentType) FixtureOption {
	return WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
		Components: order,
		Policy:     v1beta1.CoordinationPolicySequential,
		Order:      order,
	})
}

// WithRolloutPaused adds the ome.io/rollout-paused=true annotation to
// the ISVC so the coordination layer routes every group to Phase=Paused.
func WithRolloutPaused() FixtureOption {
	return func(o *fixtureOptions) { o.pausedGlobal = true }
}

// WithISVCAnnotation adds a key/value to the ISVC's top-level
// Annotations map. Used to test ome.io/* annotation handling at the
// ISVC level (vs. per-Component annotations like deploymentMode).
func WithISVCAnnotation(k, v string) FixtureOption {
	return func(o *fixtureOptions) {
		if o.extraISVCAnns == nil {
			o.extraISVCAnns = map[string]string{}
		}
		o.extraISVCAnns[k] = v
	}
}

// --- Fixture builder ------------------------------------------------------

// coordFixture bundles the three objects every coordination test has to
// create. Methods (none yet) can layer scenario-specific mutations on top.
type coordFixture struct {
	CBM  *v1beta1.ClusterBaseModel
	CSR  *v1beta1.ClusterServingRuntime
	ISVC *v1beta1.InferenceService
}

// newCoordFixture builds the three objects required to drive a
// coordination-aware OMENative ISVC through the reconcile loop. Does
// NOT apply them to the apiserver — caller chains Create + DeferCleanup
// so each spec can decide the apply order and any pre-create mutations.
//
// Defaults produce a 2-Component spec (engine + decoder, both
// OMENative) the suite's stock CBM-inspection fake accepts.
func newCoordFixture(namespace string, opts ...FixtureOption) *coordFixture {
	o := &fixtureOptions{
		isvcName:           "fixture-isvc",
		engineImage:        "fake-serving:v1",
		engineMinReplicas:  1,
		decoderMinReplicas: 1,
		routerMinReplicas:  1,
	}
	for _, opt := range opts {
		opt(o)
	}
	// Default CBM/CSR names derive from the ISVC name so each test
	// gets unique cluster-scoped resources by default. Sharing the
	// same default across Ordered tests previously caused DeferCleanup
	// races where Test2 saw Test1's mid-delete CBM and reconcile
	// surfaced "ClusterBaseModel not found". Tests that want explicit
	// names still use WithCBMName / WithCSRName.
	if o.cbmName == "" {
		o.cbmName = o.isvcName + "-cbm"
	}
	if o.csrName == "" {
		o.csrName = o.isvcName + "-csr"
	}
	if o.decoderImage == "" {
		o.decoderImage = o.engineImage
	}
	if o.routerImage == "" {
		o.routerImage = o.engineImage
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

	// Engine. Matches the working pattern in
	// tests/integration/singlecluster/controller/omenative/lifecycle_test.go:
	// minimum-viable spec with deploymentMode annotation only — no
	// OMENative subspec and no explicit MinReplicas. The defaulter
	// fills in OMENative defaults during reconciliation; an explicit
	// subspec in the fixture appeared to interfere with runtime
	// resolution in early experiments. Tests that need a non-default
	// strategy/policy can set it via WithEngineAnnotation or by
	// editing the returned ISVC before Create.
	engineAnnots := map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	for k, v := range o.engineAnnots {
		engineAnnots[k] = v
	}
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: engineAnnots,
		},
		PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
			Name:  "ome-container",
			Image: o.engineImage,
		}}},
	}
	if o.engineMinReplicas != 1 {
		mr := o.engineMinReplicas
		engine.ComponentExtensionSpec.MinReplicas = &mr
	}

	wrapper := v1beta1testing.MakeInferenceService(o.isvcName, namespace).
		Model(o.cbmName).
		Runtime(o.csrName).
		DeploymentMode(constants.OMENative).
		Engine(engine)

	// ISVC-level annotations: pause + caller-supplied extras.
	if o.pausedGlobal {
		wrapper = wrapper.Annotation("ome.io/rollout-paused", "true")
	}
	for k, v := range o.extraISVCAnns {
		wrapper = wrapper.Annotation(k, v)
	}

	if !o.withoutDecoder {
		mode := string(constants.OMENative)
		if o.decoderRawMode {
			mode = string(constants.RawDeployment)
		}
		decoderExt := v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: mode},
		}
		if o.decoderMinReplicas != 1 {
			dmr := o.decoderMinReplicas
			decoderExt.MinReplicas = &dmr
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
		mode := string(constants.OMENative)
		if o.routerRawMode {
			mode = string(constants.RawDeployment)
		}
		routerExt := v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: mode},
		}
		if o.routerMinReplicas != 1 {
			rmr := o.routerMinReplicas
			routerExt.MinReplicas = &rmr
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
	if len(o.coordGroups) > 0 {
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: append([]v1beta1.RolloutCoordinationGroup(nil), o.coordGroups...),
		}
	}

	return &coordFixture{CBM: cbm, CSR: csr, ISVC: isvc}
}

// deferFixtureCleanup registers a single DeferCleanup that tears the
// coord fixture down in the right order. The naive
//
//	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
//	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
//	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })
//
// pattern triggers a controller-side retry storm on slower CI runners:
// DeferCleanup is LIFO so CBM is deleted before the ISVC has been
// fully purged from the apiserver. The InferenceService reconciler
// then runs once more, sees the ISVC still present (deletion is
// async), and falls through to modelConfigReconciler which fails
// with "ClusterBaseModel not found" — controller-runtime retries with
// exponential backoff, the work queue saturates, and the NEXT test's
// reconciles starve. On buildkite envtest this manifests as C-1+C-4
// and H timing out even though the rollouts themselves are fine.
//
// This helper deletes the ISVC first and waits for the apiserver to
// acknowledge full deletion (after the finalizer drop), then deletes
// CBM and CSR — no race window.
func deferFixtureCleanup(fix *coordFixture) {
	ginkgo.DeferCleanup(func() {
		if fix == nil {
			return
		}
		if fix.ISVC != nil {
			_ = k8sClient.Delete(suiteCtx, fix.ISVC)
			isvcKey := client.ObjectKeyFromObject(fix.ISVC)
			Eventually(func() bool {
				fresh := &v1beta1.InferenceService{}
				err := k8sClient.Get(suiteCtx, isvcKey, fresh)
				return apierrors.IsNotFound(err)
			}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue(),
				"ISVC %s/%s should be fully deleted before CBM/CSR cleanup", fix.ISVC.Namespace, fix.ISVC.Name)
		}
		if fix.CBM != nil {
			_ = k8sClient.Delete(suiteCtx, fix.CBM)
		}
		if fix.CSR != nil {
			_ = k8sClient.Delete(suiteCtx, fix.CSR)
		}
	})
}
