package inferenceservice

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// Ordered-group stopgap boundary, sequential spelling.
//
// spec.rollout.groups[] is documented as ordered, but the engine sequences
// only a pure run of single-Component blueGreen groups (the sequential
// spelling); every other multi-group shape — and any use of
// groups[i].order — would run concurrently, so admission rejects those
// shapes rather than accept an ordering it does not enforce.
//
// These specs drive both sides of that boundary through the real
// apiserver + webhook: the enforced spelling (including the 3-Component
// run and its soak carry) must be admitted, and each unenforced escape
// hatch — canary-in-list and multi-Component-in-list on create, order
// added or a group widened on update — must be rejected with its
// documented reason. The pure-function halves live in
// pkg/validation/coordination_test.go; here the assertion is that the
// webhook wiring surfaces the same decisions on live Create/Update calls.
var _ = ginkgo.Describe("InferenceService webhook validation — ordered-group stopgap boundary", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(setupCoordCBMandCSR)

	// singleBG builds the one-Component blueGreen group the enforced
	// sequential spelling is made of.
	singleBG := func(c v1beta1.ComponentType) v1beta1.RolloutGroup {
		return v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{c},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		}
	}

	// threeComponentISVC builds an engine+decoder+router OMENative ISVC
	// carrying the given groups — the shapes below need a third
	// Component to form their group lists.
	threeComponentISVC := func(name string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceService {
		isvc := v1beta1testing.MakeInferenceService(name, "isvc-webhook").
			Model(coordCBMName).
			Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Decoder(omeNativeDecoder()).
			Router(omeNativeRouter()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: groups}
		return isvc
	}

	// SQ-1: the enforced spelling at N=3. The 2-group run is admitted by
	// W-24; this pins that adding a third single-Component blueGreen
	// group (router in the chain) stays inside the enforced boundary.
	ginkgo.It("SQ-1 accepts a three-group single-Component blueGreen run", func() {
		isvc := threeComponentISVC("sequenti-sq1",
			singleBG(v1beta1.RouterComponent),
			singleBG(v1beta1.DecoderComponent),
			singleBG(v1beta1.EngineComponent),
		)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	// SQ-2: soak is honored only between sequenced groups. On a
	// non-final group of the enforced run it is carried into the
	// sequencer, so admission must accept it.
	ginkgo.It("SQ-2 accepts soak on a non-final group of the enforced run", func() {
		soaked := singleBG(v1beta1.DecoderComponent)
		soaked.Soak = &metav1.Duration{Duration: 30 * time.Second}
		isvc := coordISVC("sequenti-sq2",
			soaked,
			singleBG(v1beta1.EngineComponent),
		)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	// SQ-3: a canary group inside a multi-group list would run
	// concurrently with its neighbors while the list promises order.
	ginkgo.It("SQ-3 rejects a canary group inside a multi-group list → GroupOrderingNotHonored", func() {
		// The canary group carries the engine: canary's own structural
		// rule requires the entrypoint Component, and this spec needs the
		// ordering rejection to be the one that fires.
		isvc := coordISVC("sequenti-sq3",
			v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 50},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			},
			singleBG(v1beta1.DecoderComponent),
		)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonGroupOrderingNotHonored))
	})

	// SQ-4: a multi-Component group inside a multi-group list breaks the
	// collapse even when every progression is blueGreen — the two groups
	// would roll concurrently.
	ginkgo.It("SQ-4 rejects a multi-Component blueGreen group inside a multi-group list → GroupOrderingNotHonored", func() {
		isvc := threeComponentISVC("sequenti-sq4",
			v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
			singleBG(v1beta1.RouterComponent),
		)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonGroupOrderingNotHonored))
	})

	// SQ-5: order rejection ratchets onto the update path — a stored
	// enforced run cannot grow an order field, even a structurally valid
	// single-entry one, because no progression applies it.
	ginkgo.It("SQ-5 rejects an update that adds order to an enforced run → OrderNotHonored", func() {
		got := createBaselineISVC("sequenti-sq5",
			singleBG(v1beta1.DecoderComponent),
			singleBG(v1beta1.EngineComponent),
		)
		got.Spec.Rollout.Groups[0].Order = []v1beta1.ComponentType{v1beta1.DecoderComponent}
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonOrderNotHonored))
	})

	// SQ-6: widening a group of a stored multi-group run to two
	// Components is rejected on update — the same widening is accepted
	// for a single-group list (WU-7), so this pins that the multi-group
	// context is what makes it an unenforced shape.
	ginkgo.It("SQ-6 rejects an update that widens a group of an enforced run → GroupOrderingNotHonored", func() {
		isvc := threeComponentISVC("sequenti-sq6",
			singleBG(v1beta1.DecoderComponent),
			singleBG(v1beta1.EngineComponent),
		)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: "isvc-webhook", Name: "sequenti-sq6"}, got)).To(Succeed())
		got.Spec.Rollout.Groups[1].Components = append(
			got.Spec.Rollout.Groups[1].Components, v1beta1.RouterComponent)
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonGroupOrderingNotHonored))
	})
})
