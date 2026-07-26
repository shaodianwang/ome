package omenative_coordination_kind

import (
	"github.com/onsi/ginkgo/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindfix"
)

// translateCoordGroupToV2 maps a single v1 RolloutCoordinationGroup (the
// operator-facing shape the specs in this suite still author for readability)
// into the v2 RolloutGroup list the controller now consumes. The v1
// types still exist as spec/status helpers, so the test literals keep
// compiling; this is the one central seam that retargets them to
// spec.rollout.groups[].
//
// The mapping is per-Policy and is intentionally lossy:
//   - BlueGreen     -> one v2 group with BlueGreen progression (Order, Soak carried).
//   - RollingUpdate -> one v2 group with RollingUpdate progression; pacing MaxSurge/
//     MaxUnavailable carry into GroupRollingUpdate, and RatioBalanced pacing becomes
//     a MaintainRatio guard (Tolerance from RatioTolerancePercent, default 5).
//   - Sequential    -> one single-Component v2 group per component in Order, each a
//     BlueGreen progression. The controller collapses that fan-out back to Sequential
//     semantics (group N completes before group N+1 begins).
//   - Independent   -> NO v2 group. A Component in no group rolls independently, which
//     is the v2 spelling of the old Independent policy.
func translateCoordGroupToV2(g v1beta1.RolloutCoordinationGroup) []v1beta1.RolloutGroup {
	switch g.Policy {
	case v1beta1.CoordinationPolicyBlueGreen:
		return []v1beta1.RolloutGroup{{
			Components: g.Components,
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			Order:      g.Order,
			Soak:       g.Soak,
		}}

	case v1beta1.CoordinationPolicyRollingUpdate:
		ru := &v1beta1.GroupRollingUpdate{}
		var maintain *v1beta1.MaintainRatio
		if g.Pacing != nil {
			ru.MaxSurge = g.Pacing.MaxSurge
			ru.MaxUnavailable = g.Pacing.MaxUnavailable
			if g.Pacing.Type == v1beta1.CoordinationPacingRatioBalanced {
				tol := int32(5)
				if g.Pacing.RatioTolerancePercent != nil {
					tol = *g.Pacing.RatioTolerancePercent
				}
				maintain = &v1beta1.MaintainRatio{Tolerance: tol}
			}
		}
		return []v1beta1.RolloutGroup{{
			Components:    g.Components,
			RollingUpdate: ru,
			Order:         g.Order,
			Soak:          g.Soak,
			MaintainRatio: maintain,
		}}

	case v1beta1.CoordinationPolicySequential:
		// Fan out into one single-Component group per component, in Order.
		// The controller collapses this back to Sequential semantics. Use
		// Order when present (Sequential requires it); fall back to
		// Components so a malformed-but-historically-accepted literal still
		// produces deterministic groups.
		order := g.Order
		if len(order) == 0 {
			order = g.Components
		}
		out := make([]v1beta1.RolloutGroup, 0, len(order))
		for _, c := range order {
			out = append(out, v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{c},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
				Soak:       g.Soak,
			})
		}
		return out

	case v1beta1.CoordinationPolicyIndependent:
		// Independent has no v2 equivalent: a Component in no group rolls
		// independently. Emit nothing.
		return nil

	default:
		return nil
	}
}

// fixtureOptions is the functional-options shape for newCoordFixture.
// Defaults are tuned to "minimum viable OMENative ISVC the controller
// can reconcile to Ready" — single-replica engine + decoder, no router,
// no rolloutCoordination block. Specs opt into coordination groups,
// pacing, PD 3-way, etc. via the With* options.
type fixtureOptions struct {
	cbmName  string
	csrName  string
	isvcName string

	// Engine knobs.
	engineImage       string
	engineMinReplicas int
	engineAnnots      map[string]string
	engineTopologyKey string
	// engineWorkerSize > 0 promotes the engine to a multi-pod gang
	// Instance: a Leader + N workers per Instance (total = N+1 pods).
	// Zero leaves the engine as single-pod (default).
	engineWorkerSize     int
	engineUpdateStrategy v1beta1.UpdateStrategyType

	// Decoder knobs (always present by default; opt out with WithoutDecoder).
	withoutDecoder        bool
	decoderImage          string
	decoderMinReplicas    int
	decoderRawMode        bool
	decoderWorkerSize     int
	decoderUpdateStrategy v1beta1.UpdateStrategyType

	// Router knobs.
	withRouter           bool
	routerImage          string
	routerMinReplicas    int
	routerRawMode        bool
	routerUpdateStrategy v1beta1.UpdateStrategyType

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

// WithEngineWorkerSize promotes the engine to multi-pod by declaring
// a Leader + Worker pair with Worker.Size=n. Each Instance materializes
// n+1 pods (1 leader at ordinal=0, n workers at ordinals 1..n). Used
// by tests that exercise multi-pod behaviour like rank-0 Service
// selector tightening.
func WithEngineWorkerSize(n int) FixtureOption {
	return func(o *fixtureOptions) { o.engineWorkerSize = n }
}

// WithEngineTopologyKey configures the topology domain used by multi-pod
// worker affinity and the corresponding PodGroup annotation.
func WithEngineTopologyKey(key string) FixtureOption {
	return func(o *fixtureOptions) { o.engineTopologyKey = key }
}

// WithEngineUpdateStrategy sets Spec.Engine.Lifecycle.UpdateStrategy.Type.
// Use this to opt the engine into RecreatePod, InPlaceIfPossible, or
// InPlaceOnly — the default SurgeThenDrain leaves Lifecycle unset.
func WithEngineUpdateStrategy(t v1beta1.UpdateStrategyType) FixtureOption {
	return func(o *fixtureOptions) { o.engineUpdateStrategy = t }
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

// WithDecoderWorkerSize promotes the decoder to multi-pod. Mirrors
// WithEngineWorkerSize.
func WithDecoderWorkerSize(n int) FixtureOption {
	return func(o *fixtureOptions) { o.decoderWorkerSize = n }
}

// WithDecoderUpdateStrategy sets Spec.Decoder.Lifecycle.UpdateStrategy.Type.
func WithDecoderUpdateStrategy(t v1beta1.UpdateStrategyType) FixtureOption {
	return func(o *fixtureOptions) { o.decoderUpdateStrategy = t }
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

// WithRouterUpdateStrategy sets Spec.Router.Lifecycle.UpdateStrategy.Type.
// Implies WithRouter.
func WithRouterUpdateStrategy(t v1beta1.UpdateStrategyType) FixtureOption {
	return func(o *fixtureOptions) {
		o.withRouter = true
		o.routerUpdateStrategy = t
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

	cbm := kindfix.StandardClusterBaseModel(o.cbmName, kindfix.StandardModelStorageURI)
	csr := kindfix.StandardClusterServingRuntime(o.csrName)

	// Engine. Minimum-viable spec with deploymentMode annotation only
	// — no OMENative subspec and no explicit MinReplicas. The defaulter
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
	if o.engineTopologyKey != "" {
		topologyKey := o.engineTopologyKey
		engine.TopologyKey = &topologyKey
	}
	if o.engineMinReplicas != 1 {
		mr := o.engineMinReplicas
		engine.ComponentExtensionSpec.MinReplicas = &mr
	}
	if o.engineWorkerSize > 0 {
		// Multi-pod gang. Leader + N workers per Instance — the
		// admission defaulter wires both halves of the pairing and
		// the OMENative reconciler stamps `ome.io/pod-ordinal` on
		// every pod (leader=0, workers=1..Size).
		size := o.engineWorkerSize
		engine.Leader = &v1beta1.LeaderSpec{
			PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
				Name:  "ome-container",
				Image: o.engineImage,
			}}},
		}
		engine.Worker = &v1beta1.WorkerSpec{
			Size: &size,
			PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
				Name:  "ome-container",
				Image: o.engineImage,
			}}},
		}
	}
	if o.engineUpdateStrategy != "" {
		engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{Type: o.engineUpdateStrategy},
		}
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
		if o.decoderWorkerSize > 0 {
			size := o.decoderWorkerSize
			decoder.Leader = &v1beta1.LeaderSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
					Name:  "ome-container",
					Image: o.decoderImage,
				}}},
			}
			decoder.Worker = &v1beta1.WorkerSpec{
				Size: &size,
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
					Name:  "ome-container",
					Image: o.decoderImage,
				}}},
			}
		}
		if o.decoderUpdateStrategy != "" {
			decoder.Lifecycle = &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{Type: o.decoderUpdateStrategy},
			}
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
		if o.routerUpdateStrategy != "" {
			router.Lifecycle = &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{Type: o.routerUpdateStrategy},
			}
		}
		wrapper = wrapper.Router(router)
	}

	isvc := wrapper.Obj()
	if len(o.coordGroups) > 0 {
		// Translate the operator-facing v1 coordination groups the specs
		// author into v2 spec.rollout.groups[]. Independent groups
		// translate to nothing (a Component in no group rolls independently),
		// so the resulting v2 list may be shorter than coordGroups — or empty,
		// in which case Spec.Rollout stays nil (no rollout block at all).
		var v2Groups []v1beta1.RolloutGroup
		for _, g := range o.coordGroups {
			v2Groups = append(v2Groups, translateCoordGroupToV2(g)...)
		}
		if len(v2Groups) > 0 {
			isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: v2Groups}
		}
	}

	return &coordFixture{CBM: cbm, CSR: csr, ISVC: isvc}
}

// deferFixtureCleanup registers a single DeferCleanup that tears the coord
// fixture down ISVC-first via kindfix.CleanupModelRuntime (see there for why
// the order matters).
func deferFixtureCleanup(fix *coordFixture) {
	ginkgo.DeferCleanup(func() {
		if fix == nil {
			return
		}
		kindfix.CleanupModelRuntime(suiteCtx, k8sClient, fix.CBM, fix.CSR, fix.ISVC)
	})
}
