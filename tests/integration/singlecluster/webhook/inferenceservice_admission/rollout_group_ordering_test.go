package inferenceservice_admission

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// spec.rollout.groupOrdering through the real admission chain.
//
// This shape needs an admission test specifically: the controller-side
// per-unit canary suite runs the reconciler without the validating webhook,
// so it happily drove two concurrent canary groups that no apiserver would
// have accepted. Everything below therefore goes through k8sClient.Create /
// Update rather than calling the validator directly.
var _ = ginkgo.Describe("ISVC admission matrix: rollout group ordering", func() {

	ginkgo.BeforeEach(ensureNamespace)

	// The rollout rules read the mode off each Component, so the ISVC-level
	// annotation alone leaves every member resolving to "" and the group is
	// rejected as non-OMENative before any ordering rule is reached.
	omeNative := map[string]string{constants.DeploymentMode: string(constants.OMENative)}

	// routerEngineISVC is the shape the field exists for: a router and an
	// engine that want to roll as unrelated canaries.
	routerEngineISVC := func(name string) *v1beta1.InferenceService {
		engine := admissionEngine()
		engine.Annotations = omeNative
		router := admissionRouter()
		router.Annotations = omeNative
		router.MinReplicas, router.MaxReplicas = ptr.To(1), 1
		return v1beta1testing.MakeInferenceService(name, "admissio-matrix").
			Runtime("admissio-runtime").
			DeploymentMode(constants.OMENative).
			Engine(engine).
			Router(router).Obj()
	}

	// A canary group with no steps is rejected by the canary validator before
	// the ordering rule runs, so these carry a real one-step ladder.
	canaryLadder := func() *v1beta1.GroupCanary {
		return &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}}
	}

	independentCanaryGroups := []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: canaryLadder()},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryLadder()},
	}

	ginkgo.It("rejects two canary groups when the ordering promise is left at its default", func() {
		isvc := routerEngineISVC("admissio-order-default")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: independentCanaryGroups}

		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GroupOrderingNotHonored"))
		Expect(err.Error()).To(ContainSubstring("groupOrdering: Concurrent"),
			"the rejection must name the escape hatch, or it is undiscoverable from the error")
	})

	ginkgo.It("admits two canary groups declared Concurrent, and stores the field", func() {
		isvc := routerEngineISVC("admissio-order-concurrent")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingConcurrent),
			Groups:        independentCanaryGroups,
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-order-concurrent")
		Expect(got.Spec.Rollout.GroupOrdering).To(HaveValue(Equal(v1beta1.RolloutGroupOrderingConcurrent)))
		Expect(got.Spec.Rollout.Groups).To(HaveLen(2))
	})

	ginkgo.It("treats an explicit Sequential exactly like an absent field", func() {
		isvc := routerEngineISVC("admissio-order-sequential")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingSequential),
			Groups:        independentCanaryGroups,
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GroupOrderingNotHonored"))
	})

	ginkgo.It("rejects a value outside the enum at the schema layer", func() {
		isvc := routerEngineISVC("admissio-order-bogus")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			GroupOrdering: ptr.To(v1beta1.RolloutGroupOrdering("Whenever")),
			Groups:        independentCanaryGroups,
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Unsupported value"),
			"the CRD enum, not the webhook, should be what rejects a typo")
	})

	ginkgo.It("lets a stored Sequential rollout be converted to Concurrent in place", func() {
		isvc := routerEngineISVC("admissio-order-convert")
		// Admitted under Sequential: a pure run of single-Component
		// blueGreen groups is the one shape the engine really sequences.
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		}}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		ginkgo.By("switching to independent canary groups requires declaring Concurrent")
		got := mustGet("admissio-order-convert")
		got.Spec.Rollout.Groups = independentCanaryGroups
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GroupOrderingNotHonored"))

		got = mustGet("admissio-order-convert")
		got.Spec.Rollout.GroupOrdering = ptr.To(v1beta1.RolloutGroupOrderingConcurrent)
		got.Spec.Rollout.Groups = independentCanaryGroups
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		ginkgo.By("and clearing it again is rejected, so the relaxation cannot be lost silently")
		got = mustGet("admissio-order-convert")
		got.Spec.Rollout.GroupOrdering = nil
		err = k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GroupOrderingNotHonored"))
	})

	ginkgo.It("does not excuse the rules that are not about sequencing", func() {
		ginkgo.By("order is dead configuration under Concurrent too")
		isvc := routerEngineISVC("admissio-order-deadcfg")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingConcurrent),
			Groups: []v1beta1.RolloutGroup{
				{
					Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
					Order:      []v1beta1.ComponentType{v1beta1.RouterComponent},
					Canary:     canaryLadder(),
				},
				{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryLadder()},
			},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("OrderNotHonored"))

		ginkgo.By("and one unit still may not be driven by two canary groups")
		// Concurrent says the groups are independent. Two groups on the SAME
		// unit are not, whatever the declaration claims: they would contend
		// for one revision, step counter and traffic entry.
		dup := routerEngineISVC("admissio-order-dup")
		dup.Spec.Rollout = &v1beta1.RolloutSpec{
			GroupOrdering: ptr.To(v1beta1.RolloutGroupOrderingConcurrent),
			Groups: []v1beta1.RolloutGroup{
				{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryLadder()},
				{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryLadder()},
			},
		}
		err = k8sClient.Create(suiteCtx, dup)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("MultipleCanaryGroups"))
	})
})
