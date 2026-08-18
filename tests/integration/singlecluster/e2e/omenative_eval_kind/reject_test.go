package omenative_eval_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// Soak is honored only where a rollout collapses to Sequential, and that needs
// two or more single-Component blueGreen groups. Declared on a single group it
// cannot be honored, so admission rejects p322.
var _ = ginkgo.Describe("admission — soak on a non-collapsing rollout", func() {
	ginkgo.It("is denied by the validating webhook", func() {
		prepareEvalCase("p322")
		isvc := p322InferenceService(evalNamespace())

		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"a soak on a single blueGreen group cannot be honored, so admission must refuse it")
		Expect(err.Error()).To(ContainSubstring("admission webhook"),
			"the refusal must come from the webhook rather than schema validation: %v", err)
		// Match the rule so an unrelated webhook denial cannot satisfy the case.
		Expect(err.Error()).To(ContainSubstring("SoakNotHonored"),
			"denial must cite the soak rule: %v", err)
	})
})

func p322InferenceService(namespace string) *v1beta1.InferenceService {
	four := 4
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			MinReplicas: &four,
			MaxReplicas: four,
			Annotations: map[string]string{
				constants.DeploymentMode: string(constants.OMENative),
			},
		},
	}
	isvc := v1beta1testing.MakeInferenceService("p322", namespace).
		Model(fixtureModel).
		Runtime(fixtureRuntime).
		AutoSync(true).
		Engine(engine).
		Obj()
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			Soak:       &metav1.Duration{Duration: 30 * time.Second},
		}},
	}
	return isvc
}
