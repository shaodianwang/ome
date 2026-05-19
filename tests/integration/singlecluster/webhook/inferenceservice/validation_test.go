package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// ensureNamespace creates the "isvc-webhook" namespace used by every
// spec's namespaced ISVC. Shared by every Describe via BeforeEach so
// each Describe's specs work in isolation under -ginkgo.focus.
func ensureNamespace() {
	ginkgo.GinkgoHelper()
	_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc-webhook"},
	})
}

// Webhook envtest specs for the InferenceService validating webhook.
// Each spec sends a Create (or Update) through the apiserver and
// asserts the admission decision — same logic the unit tests in
// pkg/webhook/admission/isvc/ cover, but verified end-to-end with
// the real apiserver invoking the webhook over TLS, with real
// BaseModel/ClusterBaseModel lookups against the API server.
var _ = ginkgo.Describe("InferenceService webhook validation", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("rejects ISVC with invalid name (uppercase / dots)", func() {
		// IsvcRegexp enforces DNS-style: lowercase alphanumeric +
		// hyphens, no dots. Uppercase is the most common operator
		// mistake.
		isvc := v1beta1testing.MakeInferenceService("Bad-ISVC-Name", "isvc-webhook").
			Model("any-model").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Bad-ISVC-Name"),
			"webhook must reject ISVC names that violate IsvcRegexp")
	})

	ginkgo.It("rejects ISVC with Decoder set but Engine nil", func() {
		// validateEngineDecoderConfiguration: a Decoder is meaningless
		// without an Engine to disaggregate from. Most operators who
		// hit this set the wrong field by mistake.
		isvc := v1beta1testing.MakeInferenceService("decoder-only", "isvc-webhook").
			Model("any-model").
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("decoder cannot be specified without engine"))
	})

	ginkgo.It("rejects ISVC with unknown autoscalerClass annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("bad-as-class", "isvc-webhook").
			Model("any-model").
			Annotation(constants.AutoscalerClass, "made-up-class").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not a supported autoscaler class type"))
	})

	ginkgo.It("rejects ISVC with HPA autoscaler + invalid metric annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("bad-hpa-metric", "isvc-webhook").
			Model("any-model").
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassHPA)).
			Annotation(constants.AutoscalerMetrics, "fictional-metric").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("is not a supported metric"))
	})

	ginkgo.It("rejects ISVC with KEDA autoscaler + invalid scaling operator annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("bad-keda-op", "isvc-webhook").
			Model("any-model").
			Annotation(constants.AutoscalerClass, string(constants.AutoscalerClassKEDA)).
			Annotation(constants.KedaScalingOperator, "EqualToButNotReally").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid KEDA scaling operator"))
	})

	ginkgo.It("rejects ISVC referencing a model that doesn't exist", func() {
		// validateModelExists: the BaseModel/ClusterBaseModel lookup
		// hits the apiserver, so this is the most-valuable
		// integration check (unit tests use mocks).
		isvc := v1beta1testing.MakeInferenceService("missing-model", "isvc-webhook").
			Model("does-not-exist-anywhere").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("referenced model"))
		Expect(err.Error()).To(ContainSubstring("not found"))
	})

	ginkgo.It("accepts ISVC referencing an existing ClusterBaseModel + matching ClusterServingRuntime", func() {
		// Symmetric positive case for the model-existence check.
		// validateRuntimeAndModelResolution additionally requires
		// either a runtime that supports the model's format OR an
		// engine with hasFullRunnerConfig — we provide the runtime,
		// matching the realistic operator setup.
		cbm := v1beta1testing.MakeClusterBaseModel("ok-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("ok-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("ok-isvc", "isvc-webhook").
			Model("ok-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	ginkgo.It("rejects ISVC referencing CBM whose PVC URI points at a different namespace", func() {
		// validateClusterBaseModelPVCNamespace: a pod can't mount a
		// PVC from another namespace, so an ISVC + CBM combo where
		// the CBM's PVC URI specifies a different namespace from the
		// ISVC would silently break at scheduling time without this
		// check.
		cbm := v1beta1testing.MakeClusterBaseModel("cross-ns-cbm").
			StorageURI("pvc://elsewhere:my-pvc/models/llama").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		isvc := v1beta1testing.MakeInferenceService("cross-ns-isvc", "isvc-webhook").
			Model("cross-ns-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("references PVC in namespace"))
		Expect(err.Error()).To(ContainSubstring("isvc-webhook"))
	})

	ginkgo.It("validates Updates too: changing model to a missing one is rejected", func() {
		// The webhook is registered for verbs=create;update so the
		// existing-model check must also fire on Updates. CBM + CSR
		// satisfy the runtime-resolution check on the initial Create.
		cbm := v1beta1testing.MakeClusterBaseModel("update-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("update-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		isvc := v1beta1testing.MakeInferenceService("update-isvc", "isvc-webhook").
			Model("update-cbm").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		isvc.Spec.Model.Name = "definitely-not-here"
		err := k8sClient.Update(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("referenced model"))
		Expect(err.Error()).To(ContainSubstring("not found"))
	})

	// ---- OEP-0011 spec.traffic typed-core validation ----

	// trafficBaseISVC builds a minimal ISVC that passes every other
	// validation rule, so each spec only exercises the spec.traffic
	// rules. CRD enum validation catches malformed enum values before
	// the webhook ever runs; specs below exercise the cross-field rules
	// the webhook owns.
	trafficBaseISVC := func(name string, traffic *v1beta1.TrafficSpec) *v1beta1.InferenceService {
		return v1beta1testing.MakeInferenceService(name, "isvc-webhook").
			Model("any-model").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).
			Traffic(traffic).
			Obj()
	}

	ginkgo.It("rejects ISVC with algorithm=ConsistentHash but no consistentHash block", func() {
		ch := v1beta1.LoadBalancingTypeConsistentHash
		isvc := trafficBaseISVC("traffic-missing-ch", &v1beta1.TrafficSpec{Algorithm: &ch})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("MissingConsistentHashSpec"))
	})

	ginkgo.It("rejects ISVC with consistentHash block but algorithm != ConsistentHash", func() {
		lr := v1beta1.LoadBalancingTypeLeastRequest
		isvc := trafficBaseISVC("traffic-unexpected-ch", &v1beta1.TrafficSpec{
			Algorithm: &lr,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("UnexpectedConsistentHashSpec"))
	})

	ginkgo.It("rejects ISVC with consistentHash.type=Header and no headers", func() {
		ch := v1beta1.LoadBalancingTypeConsistentHash
		isvc := trafficBaseISVC("traffic-missing-hash-headers", &v1beta1.TrafficSpec{
			Algorithm: &ch,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type: v1beta1.HashTypeHeader,
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("MissingHashKey"))
	})

	ginkgo.It("rejects ISVC with both headers and cookie set on consistentHash", func() {
		ch := v1beta1.LoadBalancingTypeConsistentHash
		isvc := trafficBaseISVC("traffic-multi-hash", &v1beta1.TrafficSpec{
			Algorithm: &ch,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
				Cookie:  &v1beta1.HashCookie{Name: "ome-session"},
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("MultipleHashKeys"))
	})

	ginkgo.It("rejects ISVC with endpointOverride.type=Header and no headers", func() {
		isvc := trafficBaseISVC("traffic-missing-eo-headers", &v1beta1.TrafficSpec{
			EndpointOverride: &v1beta1.EndpointOverrideSpec{
				Type: v1beta1.EndpointOverrideTypeHeader,
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("MissingEndpointOverrideKey"))
	})

	ginkgo.It("accepts ISVC with valid consistent-hash on header", func() {
		ch := v1beta1.LoadBalancingTypeConsistentHash
		isvc := trafficBaseISVC("traffic-ok-ch-header", &v1beta1.TrafficSpec{
			Algorithm: &ch,
			ConsistentHash: &v1beta1.ConsistentHashSpec{
				Type:    v1beta1.HashTypeHeader,
				Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
			},
		})
		// The ISVC references a non-existent model; the model lookup
		// runs AFTER traffic validation, so this Create call will
		// reach the model check. The traffic-validation phase is
		// considered passed if the error (if any) is NOT a traffic
		// error code.
		err := k8sClient.Create(suiteCtx, isvc)
		if err != nil {
			Expect(err.Error()).NotTo(ContainSubstring("MissingConsistentHashSpec"))
			Expect(err.Error()).NotTo(ContainSubstring("MissingHashKey"))
			Expect(err.Error()).NotTo(ContainSubstring("MultipleHashKeys"))
			Expect(err.Error()).NotTo(ContainSubstring("UnexpectedConsistentHashSpec"))
		} else {
			ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		}
	})

	ginkgo.It("accepts ISVC with no spec.traffic at all (backward compatible)", func() {
		isvc := trafficBaseISVC("traffic-absent", nil)
		err := k8sClient.Create(suiteCtx, isvc)
		// Same logic as above — model lookup may fail; traffic
		// validation must not.
		if err != nil {
			Expect(err.Error()).NotTo(ContainSubstring("MissingConsistentHashSpec"))
			Expect(err.Error()).NotTo(ContainSubstring("MissingHashKey"))
		} else {
			ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		}
	})

	// ---- OEP-0011 ome.io/* annotation validation ----

	ginkgo.It("rejects ISVC with malformed int annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("ann-bad-int", "isvc-webhook").
			Model("any-model").
			Annotation(constants.CircuitBreakerMaxConnectionsAnnotation, "notanumber").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("InvalidIntValue"))
	})

	ginkgo.It("rejects ISVC with malformed duration annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("ann-bad-duration", "isvc-webhook").
			Model("any-model").
			Annotation(constants.TimeoutIdleAnnotation, "ten-seconds").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("InvalidDuration"))
	})

	ginkgo.It("rejects ISVC with unknown ome.io traffic annotation (Did-you-mean)", func() {
		isvc := v1beta1testing.MakeInferenceService("ann-typo", "isvc-webhook").
			Model("any-model").
			Annotation(constants.OMEAPIGroupName+"/circut-breaker-max-connections", "1024"). // typo
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("UnknownTrafficAnnotation"))
		Expect(err.Error()).To(ContainSubstring(constants.CircuitBreakerMaxConnectionsAnnotation))
	})

	ginkgo.It("accepts ISVC with ome.io/btp.* passthrough annotation", func() {
		isvc := v1beta1testing.MakeInferenceService("ann-btp-passthrough", "isvc-webhook").
			Model("any-model").
			Annotation(constants.PassthroughEnvoyGatewayPrefix+"loadBalancer.slowStart.window", "30s").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		// Model lookup may fail; traffic-annotation validation must not.
		if err != nil {
			Expect(err.Error()).NotTo(ContainSubstring("UnknownTrafficAnnotation"))
			Expect(err.Error()).NotTo(ContainSubstring("InvalidIntValue"))
			Expect(err.Error()).NotTo(ContainSubstring("InvalidDuration"))
		} else {
			ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		}
	})

	ginkgo.It("rejects ISVC with retry-attempts > 0 but no retry-on", func() {
		isvc := v1beta1testing.MakeInferenceService("ann-missing-retry-on", "isvc-webhook").
			Model("any-model").
			Annotation(constants.RetryAttemptsAnnotation, "3").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("MissingRetryOn"))
	})

	// ---- OEP-0011 spec.traffic.rollout validation ----

	ginkgo.It("rejects step-based rollout whose final step doesn't reach 100%", func() {
		isvc := v1beta1testing.MakeInferenceService("rollout-incomplete", "isvc-webhook").
			Model("any-model").
			Traffic(&v1beta1.TrafficSpec{
				Rollout: &v1beta1.RolloutSpec{
					Steps: []v1beta1.RolloutStep{
						{NewCapacity: intstr.FromString("50%"), TrafficWeight: 50},
						{NewCapacity: intstr.FromString("75%"), TrafficWeight: 75},
					},
				},
			}).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("IncompleteRolloutPlan"))
	})

	ginkgo.It("rejects step capacity > 200%", func() {
		isvc := v1beta1testing.MakeInferenceService("rollout-capacity-too-big", "isvc-webhook").
			Model("any-model").
			Traffic(&v1beta1.TrafficSpec{
				Rollout: &v1beta1.RolloutSpec{
					Steps: []v1beta1.RolloutStep{
						{NewCapacity: intstr.FromString("300%"), TrafficWeight: 100},
					},
				},
			}).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("InvalidStepCapacity"))
	})

	ginkgo.It("rejects mixed rollout forms (step-based + engine.canaryTrafficPercent)", func() {
		canary := int64(10)
		isvc := v1beta1testing.MakeInferenceService("rollout-mixed", "isvc-webhook").
			Model("any-model").
			Traffic(&v1beta1.TrafficSpec{
				Rollout: &v1beta1.RolloutSpec{
					Steps: []v1beta1.RolloutStep{
						{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
					},
				},
			}).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					CanaryTrafficPercent: &canary,
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("MixedRolloutForms"))
	})

	ginkgo.It("accepts step-based rollout that reaches 100% in one step", func() {
		isvc := v1beta1testing.MakeInferenceService("rollout-single-step", "isvc-webhook").
			Model("any-model").
			Traffic(&v1beta1.TrafficSpec{
				Rollout: &v1beta1.RolloutSpec{
					Steps: []v1beta1.RolloutStep{
						{NewCapacity: intstr.FromString("100%"), TrafficWeight: 100},
					},
				},
			}).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "x"}},
				},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		// Model lookup may fail; rollout validation must not.
		if err != nil {
			Expect(err.Error()).NotTo(ContainSubstring("IncompleteRolloutPlan"))
			Expect(err.Error()).NotTo(ContainSubstring("InvalidStepCapacity"))
			Expect(err.Error()).NotTo(ContainSubstring("MixedRolloutForms"))
		} else {
			ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		}
	})
})
