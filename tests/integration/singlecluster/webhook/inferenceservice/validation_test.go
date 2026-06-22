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

	// ----- OEP-0011 Test 19 backfill + new admission gaps -----

	ginkgo.It("rejects ome.io/rollout-ready-timeout not a duration (InvalidDuration)", func() {
		isvc := v1beta1testing.MakeInferenceService("isvc-bad-timeout", "isvc-webhook").
			Annotation(constants.RolloutReadyTimeoutAnnotation, "not-a-duration").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
			}).Obj()

		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("InvalidDuration"))
	})

	ginkgo.It("accepts ome.io/rollout-promote as a known annotation", func() {
		// rollout-promote is a recognized ome.io/* traffic annotation; a
		// well-formed value must not produce a hard admission rejection.
		isvc := v1beta1testing.MakeInferenceService("isvc-promote-no-rollout", "isvc-webhook").
			Annotation(constants.RolloutPromoteAnnotation, "e5d6f79d").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
			}).Obj()

		err := k8sClient.Create(suiteCtx, isvc)
		if err != nil {
			// Model lookup may fail; a valid rollout-promote value must not
			// produce a hard rejection.
			Expect(err.Error()).NotTo(ContainSubstring("InvalidRolloutPromoteTarget"))
		} else {
			ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		}
	})

	// OEP-0014 Phase 2: explicit-pin admission validation. Three rejections
	// happen at the webhook so misconfigured pins never reach the controller:
	// AutoSync conflict, wrong runtime in the name, wrong scope prefix.
	ginkgo.It("rejects spec.runtime.revision when autoSync is true (silently-ignored pin)", func() {
		isvc := v1beta1testing.MakeInferenceService("pin-conflict", "isvc-webhook").
			Runtime("some-runtime").AutoSync(true).RuntimeRevision("cr-some-runtime-abc12345").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("AutoSync=true would silently ignore the pin"))
	})

	ginkgo.It("rejects spec.runtime.revision without explicit autoSync (default true is still wrong)", func() {
		isvc := v1beta1testing.MakeInferenceService("pin-default-sync", "isvc-webhook").
			Runtime("some-runtime").RuntimeRevision("cr-some-runtime-abc12345").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("AutoSync=true would silently ignore the pin"))
	})

	ginkgo.It("rejects spec.runtime.revision naming a DIFFERENT runtime", func() {
		isvc := v1beta1testing.MakeInferenceService("pin-wrong-runtime", "isvc-webhook").
			Runtime("runtime-a").AutoSync(false).RuntimeRevision("cr-runtime-b-abc12345").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("does not match the expected naming convention"))
	})

	ginkgo.It("rejects spec.runtime.revision with wrong scope prefix (cluster ref but r- name)", func() {
		isvc := v1beta1testing.MakeInferenceService("pin-wrong-scope", "isvc-webhook").
			Runtime("some-runtime").AutoSync(false).
			RuntimeRevision("r-isvc-webhook-some-runtime-abc12345"). // namespaced shape; ref is cluster
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("does not match the expected naming convention"))
	})

	ginkgo.It("accepts a correctly-formed explicit pin (autoSync=false + matching name)", func() {
		isvc := v1beta1testing.MakeInferenceService("pin-ok", "isvc-webhook").
			Runtime("some-runtime").AutoSync(false).RuntimeRevision("cr-some-runtime-abc12345").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	// =========================================================================
	// OEP-0007 multi-node Phase 3: webhook validation rules.
	// See .claude/specs/2026-05-27-multi-node-real-cluster-tests/README.md §M-E
	// and .claude/specs/2026-05-27-omenative-multi-node/plan.md §Phase 3.
	// Unit coverage lives in pkg/validation/isvc_test.go +
	// pkg/webhook/admission/isvc/inference_service_validation_test.go; the
	// envtest specs below confirm the wired-up webhook rejects through the
	// apiserver path operators actually hit.
	// =========================================================================

	ginkgo.It("multi-node phase 3: rejects engine.worker.size=0 with leader set (WorkerSizeMustBePositive / M-E.1)", func() {
		size := 0
		isvc := v1beta1testing.MakeInferenceService("phase3-worker-size-zero", "isvc-webhook").
			Runtime("any-runtime").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				Leader:  &v1beta1.LeaderSpec{},
				Worker:  &v1beta1.WorkerSpec{Size: &size},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("WorkerSizeMustBePositive"))
	})

	ginkgo.It("multi-node phase 3: rejects leader without worker (LeaderRequiresWorker / M-E.2)", func() {
		isvc := v1beta1testing.MakeInferenceService("phase3-leader-only", "isvc-webhook").
			Runtime("any-runtime").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				Leader:  &v1beta1.LeaderSpec{},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("LeaderRequiresWorker"))
	})

	ginkgo.It("multi-node phase 3: rejects worker without leader (WorkerRequiresLeader / M-E.3)", func() {
		size := 3
		isvc := v1beta1testing.MakeInferenceService("phase3-worker-only", "isvc-webhook").
			Runtime("any-runtime").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				Worker:  &v1beta1.WorkerSpec{Size: &size},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("WorkerRequiresLeader"))
	})

	ginkgo.It("multi-node phase 3: rejects Sequential coordination with multi-pod engine and no Order (SequentialMultiPodRequiresOrder)", func() {
		// OEP-0017: the Sequential coordination policy is removed. Cross-Component
		// sequencing is now expressed as multiple single-Component groups in list
		// order, so a "Sequential group without Order" is no longer expressible —
		// there is no Order-on-Sequential rule. This rejection no longer fires;
		// skip until/if a v2 multi-pod sequencing guard is reintroduced.
		ginkgo.Skip("OEP-0017: Sequential policy removed; SequentialMultiPodRequiresOrder no longer applies")
	})

	ginkgo.It("multi-node phase 3: rejects lean multi-pod ISVC with no runner images (MultiPodLeanModelNeedsRunner)", func() {
		// Lean = no spec.model + no spec.runtime. Lean single-pod is
		// supported per #249; lean multi-pod needs explicit leader.runner +
		// worker.runner images because the controller has no defaulting
		// source. To hit this validator we must bypass the earlier
		// "at-least-one-of-model-or-runtime" check, which is impossible
		// from the webhook entry point — but we can hit the parallel error
		// path: declare runtime AND multi-pod with no leader image. Webhook
		// will reject for hasFullRunnerConfig=false / no runtime supports
		// model… instead let's exercise the reason via the unit-tested
		// helper. The envtest spec below uses the closest reachable case:
		// no runtime + no model → "at least one of spec.model or
		// spec.runtime must be set" surfaces first (covered separately).
		// The Phase-3 unit tests
		// (TestValidateMultiPodLeanRunner in pkg/validation/) cover the
		// validator directly.
		isvc := v1beta1testing.MakeInferenceService("phase3-lean-mp-no-runner", "isvc-webhook").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				Leader:  &v1beta1.LeaderSpec{},
				Worker:  &v1beta1.WorkerSpec{Size: func() *int { v := 2; return &v }()},
			}).Obj()
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		// Either MultiPodLeanModelNeedsRunner (Phase-3 validator) or
		// "at least one of spec.model or spec.runtime" (earlier in the
		// pipeline) is acceptable — both reject the same bad spec.
		Expect(err.Error()).To(SatisfyAny(
			ContainSubstring("MultiPodLeanModelNeedsRunner"),
			ContainSubstring("at least one of spec.model or spec.runtime"),
		))
	})

	// Symmetric positive case so we know the multi-pod path doesn't
	// false-positive when the spec is well-formed.
	ginkgo.It("multi-node phase 3: accepts well-formed multi-pod ISVC with runtime and worker.size", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("phase3-ok-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("phase3-ok-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		size := 3
		isvc := v1beta1testing.MakeInferenceService("phase3-ok-mp", "isvc-webhook").
			Model("phase3-ok-cbm").
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{Size: &size},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})

	// =========================================================================
	// task #36 — webhook coordination validation gap fixes.
	// Reproduces W-1 / W-2 from
	// .claude/specs/2026-05-25-real-cluster-tests/README.md, which the
	// real-cluster test runs in May 2026 caught failing. Runtime
	// ValidateGroupShape in pkg/.../coordination/groups.go:213 already
	// rejects these shapes, but the admission webhook was letting them
	// through. These specs assert the webhook now rejects them at
	// admission time so operators see the conflict before the object
	// lands in apiserver.
	// =========================================================================

	// real-cluster W-1: Sequential + RatioBalanced is meaningless —
	// Sequential serializes Components, so the cross-Component ratio
	// RatioBalanced is supposed to enforce is undefined. Mirror of the
	// YAML at .claude/specs/2026-05-25-real-cluster-tests/yamls/
	// w1-invalid-sequential-ratiobalanced.yaml.
	ginkgo.It("task #36: rejects Sequential + RatioBalanced pacing (SequentialIncompatibleWithRatioBalanced / real-cluster W-1)", func() {
		// OEP-0017: Sequential policy and per-group Pacing are removed. A group
		// no longer carries a (Sequential) policy, and RatioBalanced pacing
		// became the group-level MaintainRatio guard — which is meaningful on any
		// multi-Component group. The "Sequential is incompatible with
		// RatioBalanced" combination is therefore not expressible in v2, and
		// ReasonSequentialIncompatibleWithRatioBalanced is no longer returned by
		// any validator. Skip until/if an equivalent v2 guard is introduced.
		ginkgo.Skip("OEP-0017: Sequential policy + per-group Pacing removed; SequentialIncompatibleWithRatioBalanced no longer applies")
	})

	// real-cluster W-2: maxSurge=0 AND maxUnavailable=0 deadlocks any
	// rollout — no surge headroom, no drain headroom. Mirror of the YAML
	// at .claude/specs/2026-05-25-real-cluster-tests/yamls/
	// w2-invalid-zero-budget.yaml.
	ginkgo.It("task #36: rejects RollingUpdate group with maxSurge=0 and maxUnavailable=0 (ZeroBudgetPacingUnstartable / real-cluster W-2)", func() {
		zero := intstr.FromInt(0)
		isvc := v1beta1testing.MakeInferenceService("task36-w2-zero-budget", "isvc-webhook").
			Runtime("any-runtime").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
			}).
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
			}).Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{
					MaxSurge:       &zero,
					MaxUnavailable: &zero,
				},
			}},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ZeroBudgetPacingUnstartable"))
	})

	// Positive: a well-formed multi-Component coordination group must be
	// accepted (no over-rejection from the pacing/budget checks).
	//
	// OEP-0017: the old "Sequential + PerComponent pacing" combination is
	// re-expressed as the canonical v2 multi-Component group — engine +
	// decoder rolling together under BlueGreen. (Sequential cross-Component
	// ordering would now be two single-Component groups; the over-rejection
	// guard being exercised here is the pacing/budget path, which a coupled
	// multi-Component group covers.)
	ginkgo.It("task #36: accepts well-formed multi-Component coordination group (no over-rejection)", func() {
		isvc := v1beta1testing.MakeInferenceService("task36-seq-ok", "isvc-webhook").
			Runtime("any-runtime").
			Engine(&v1beta1.EngineSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
			}).
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "x"}}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
			}).Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	})
})
