package autoscalerpolicy

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/autoscalerpolicy/render"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/validation"
)

var _ = ginkgo.Describe("AutoscalerPolicy write admission", func() {

	ginkgo.It("admits a valid standard policy and rejects an update that breaks it", func() {
		policy := mkPolicy(uniqueName("request-activity"), validKedaSpec())
		mustCreate(policy)

		// UPDATE runs the same spec validation as CREATE: a stored-valid
		// policy cannot be edited into a reserved shape.
		policy.Spec.Enforcement = v1beta1.PolicyEnforcementRequired
		msg := denial(k8sClient.Update(suiteCtx, policy))
		gomega.Expect(msg).To(gomega.ContainSubstring(render.ReasonEnforcementReserved))
	})

	// Class/template coherence is a CRD CEL rule, so the apiserver rejects
	// the write before the webhook is ever consulted — the assertion pins
	// the generated CRD's message, not the handler's.
	ginkgo.It("rejects a class/template mismatch at the CRD itself", func() {
		spec := validKedaSpec()
		spec.HPA = &v1beta1.HPAAutoscaler{}
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("both-templates"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("class KEDA requires keda and forbids hpa"))

		hpaWithKeda := validKedaSpec()
		hpaWithKeda.Class = v1beta1.AutoscalerHPA
		msg = denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("hpa-with-keda"), hpaWithKeda)))
		gomega.Expect(msg).To(gomega.ContainSubstring("class HPA forbids keda"))
	})

	ginkgo.It("rejects enforcement Required as a reserved shape", func() {
		spec := validKedaSpec()
		spec.Enforcement = v1beta1.PolicyEnforcementRequired
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("required"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring(render.ReasonEnforcementReserved))
		gomega.Expect(msg).To(gomega.ContainSubstring("reserved shape"))
	})

	ginkgo.It("rejects a forbidden template construct in trigger metadata", func() {
		spec := validKedaSpec()
		spec.Keda.Triggers[0].Metadata["query"] = `{{ printf .Namespace }}`
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("function-call"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("forbidden template construct"))
	})

	ginkgo.It("rejects the provider-owned metadata keys serverAddress and authModes", func() {
		spec := validKedaSpec()
		spec.Keda.Triggers[0].Metadata["serverAddress"] = "http://prometheus.example.com:9090"
		spec.Keda.Triggers[0].Metadata["authModes"] = "bearer"
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("endpoint-smuggle"), spec)))
		// Both findings arrive in one denial: admission joins every issue
		// so the author sees the full list from a single rejected write.
		gomega.Expect(msg).To(gomega.ContainSubstring(v1beta1.AutoscalerPolicyReasonForbiddenMetadataKey))
		gomega.Expect(msg).To(gomega.ContainSubstring(`"serverAddress"`))
		gomega.Expect(msg).To(gomega.ContainSubstring(`"authModes"`))
	})

	ginkgo.It("rejects a prometheus trigger without a providerRef", func() {
		spec := validKedaSpec()
		spec.Keda.Triggers[0].ProviderRef = nil
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("unbound"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring(render.ReasonProviderRefRequired))
	})

	ginkgo.It("requires metricType AverageValue when the query returns desired replicas", func() {
		spec := validKedaSpec()
		spec.Keda.Triggers[0].QueryReturnsDesiredReplicas = true
		spec.Keda.Triggers[0].MetricType = autoscalingv2.ValueMetricType
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("desired-value"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring(render.ReasonMetricTypeForced))

		spec.Keda.Triggers[0].MetricType = autoscalingv2.AverageValueMetricType
		mustCreate(mkPolicy(uniqueName("desired-avg"), spec))
	})

	ginkgo.It("requires ignoreNullValues to be explicit on prometheus triggers", func() {
		spec := validKedaSpec()
		delete(spec.Keda.Triggers[0].Metadata, "ignoreNullValues")
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("implicit-null"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring(render.ReasonExplicitNullValues))
	})
})

var _ = ginkgo.Describe("InferenceService policy-ref admission", func() {

	ginkgo.It("rejects a ref with the reserved ClusterAutoscalerPolicy kind", func() {
		isvc := mkConsumer(uniqueName("consumer"), uniqueName("request-activity"))
		isvc.Spec.Engine.AutoscalerPolicyRef.Kind = "ClusterAutoscalerPolicy"
		msg := denial(k8sClient.Create(suiteCtx, isvc))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonAutoscalerPolicyRefKindReserved))
	})

	// The referenced policy does not need to exist for the ref to be
	// admitted: resolution is a reconcile-time concern (fail-closed hold +
	// condition), not an admission one, so GitOps can apply the pair in
	// any order.
	ginkgo.It("admits a ref with the namespaced kind, existing or not", func() {
		explicit := mkConsumer(uniqueName("consumer"), uniqueName("request-activity"))
		explicit.Spec.Engine.AutoscalerPolicyRef.Kind = "AutoscalerPolicy"
		defaulted := mkConsumer(uniqueName("consumer"), uniqueName("request-activity"))
		mustCreate(explicit, defaulted)
	})
})

var _ = ginkgo.Describe("AutoscalerPolicy delete admission", func() {

	ginkgo.It("denies deletion while a component references the policy, then admits at zero refs", func() {
		policy := mkPolicy(uniqueName("request-activity"), validKedaSpec())
		consumer := mkConsumer(uniqueName("consumer"), policy.Name)
		mustCreate(policy, consumer)

		msg := denial(k8sClient.Delete(suiteCtx, policy))
		gomega.Expect(msg).To(gomega.ContainSubstring(consumer.Name + "/engine"))
		gomega.Expect(msg).To(gomega.ContainSubstring(constants.AutoscalerPolicyAllowInUseDelete),
			"the denial must teach the break-glass path")

		consumer.Spec.Engine.AutoscalerPolicyRef = nil
		gomega.Expect(k8sClient.Update(suiteCtx, consumer)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Delete(suiteCtx, policy)).To(gomega.Succeed())
	})

	ginkgo.It("admits deleting a policy no component references", func() {
		policy := mkPolicy(uniqueName("request-activity"), validKedaSpec())
		mustCreate(policy)
		gomega.Expect(k8sClient.Delete(suiteCtx, policy)).To(gomega.Succeed())
	})

	ginkgo.It("admits an in-use deletion carrying the break-glass annotation", func() {
		policy := mkPolicy(uniqueName("request-activity"), validKedaSpec())
		policy.Annotations = map[string]string{constants.AutoscalerPolicyAllowInUseDelete: "true"}
		consumer := mkConsumer(uniqueName("consumer"), policy.Name)
		mustCreate(policy, consumer)

		gomega.Expect(k8sClient.Delete(suiteCtx, policy)).To(gomega.Succeed())
	})
})
