package rolloutpolicy

import (
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// inlineCanary is a minimal valid inline canary plan — the coexistence and
// size-cap specs mutate it.
func inlineCanary() *v1beta1.GroupCanary {
	return &v1beta1.GroupCanary{
		Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		},
	}
}

var _ = ginkgo.Describe("InferenceService rollout policy-ref admission", func() {

	// The referenced policy does not need to exist for the ref to be
	// admitted: resolution is a run-open concern (fail-closed park +
	// condition), not an admission one, so GitOps can apply the pair in any
	// order.
	ginkgo.It("admits a group with a policyRef, referenced policy existing or not", func() {
		mustCreate(mkConsumer(uniqueName("llm-a"),
			refGroup(uniqueName("canary-std"), v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent)))
	})

	ginkgo.It("rejects a ref with the reserved ClusterRolloutPolicy kind", func() {
		group := refGroup(uniqueName("canary-std"), v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent)
		group.PolicyRef.Kind = "ClusterRolloutPolicy"
		msg := denial(k8sClient.Create(suiteCtx, mkConsumer(uniqueName("llm-a"), group)))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonRolloutPolicyRefInvalid))
		gomega.Expect(msg).To(gomega.ContainSubstring("reserved"))
	})

	// progression is a required enum on the CRD, so an empty declaration is
	// rejected at the schema layer before the webhook is consulted.
	ginkgo.It("rejects a ref missing its declared progression at the CRD schema", func() {
		group := refGroup(uniqueName("canary-std"), "", v1beta1.EngineComponent)
		msg := denial(k8sClient.Create(suiteCtx, mkConsumer(uniqueName("llm-a"), group)))
		gomega.Expect(msg).To(gomega.ContainSubstring("progression"))
	})

	// The shape rules evaluate over the ref's DECLARED kind without
	// dereferencing the policy: a declared-canary group on a router-fronted
	// ISVC must contain the router, or the stepped traffic would only move
	// on an internal Service.
	ginkgo.It("rejects a declared-canary group omitting the router entrypoint", func() {
		isvc := mkConsumer(uniqueName("llm-a"),
			refGroup(uniqueName("canary-std"), v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent))
		isvc.Spec.Router = omeNativeRouter()
		msg := denial(k8sClient.Create(suiteCtx, isvc))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonCanaryInvalid))
		gomega.Expect(msg).To(gomega.ContainSubstring("entrypoint"))
	})

	ginkgo.It("rejects two declared-canary groups", func() {
		isvc := mkConsumer(uniqueName("llm-a"),
			refGroup(uniqueName("canary-std"), v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent),
			refGroup(uniqueName("canary-alt"), v1beta1.RolloutProgressionCanary, v1beta1.DecoderComponent))
		isvc.Spec.Decoder = omeNativeDecoder()
		msg := denial(k8sClient.Create(suiteCtx, isvc))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonMultipleCanaryGroups))
	})

	// Soak is never honored on a canary group, and the kind here is only
	// declared by the ref — the rejection proves the soak rule resolves
	// through the declaration.
	ginkgo.It("rejects soak on a declared-canary ref group", func() {
		group := refGroup(uniqueName("canary-std"), v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent)
		group.Soak = &metav1.Duration{Duration: 5 * time.Minute}
		msg := denial(k8sClient.Create(suiteCtx, mkConsumer(uniqueName("llm-a"), group)))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonSoakNotHonored))
	})

	// Ref + inline coexistence is the preview/rollback mechanism: the inline
	// block wins at resolution, so admission must accept the pair rather
	// than force a two-step migration.
	ginkgo.It("admits a ref coexisting with an inline canary block", func() {
		group := refGroup(uniqueName("canary-std"), v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent)
		group.Canary = inlineCanary()
		mustCreate(mkConsumer(uniqueName("llm-a"), group))
	})

	// Inline plans are pinned into status at run open exactly like
	// policy-sourced ones, so the same operator cap bounds both authoring
	// paths.
	ginkgo.It("rejects an inline canary over the configured plan-size cap", func() {
		plan := inlineCanary()
		plan.Steps[0] = analysisStep(strings.Repeat("m", suiteMaxPlanBytes))
		plan.Steps[0].Traffic = 10
		isvc := mkConsumer(uniqueName("llm-a"), v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary:     plan,
		})
		msg := denial(k8sClient.Create(suiteCtx, isvc))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonRolloutPlanTooLarge))
		gomega.Expect(msg).To(gomega.ContainSubstring("spec.rollout.groups[0].canary"))
	})
})
