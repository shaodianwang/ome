package rolloutpolicy

import (
	"strings"
	"sync"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/validation"
)

var _ = ginkgo.Describe("RolloutPolicy write admission", func() {

	ginkgo.It("admits a valid canary policy", func() {
		mustCreate(mkPolicy(uniqueName("canary-std"), validCanarySpec()))
	})

	// The one-of progression is a CRD CEL rule, so the apiserver rejects the
	// write before the webhook is ever consulted — the assertions pin the
	// generated CRD's message, not the handler's.
	ginkgo.It("rejects two progressions and zero progressions at the CRD itself", func() {
		both := validCanarySpec()
		both.BlueGreen = &v1beta1.GroupBlueGreen{}
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("two-arms"), both)))
		gomega.Expect(msg).To(gomega.ContainSubstring("exactly one of canary"))

		msg = denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("no-arm"), v1beta1.RolloutPolicySpec{})))
		gomega.Expect(msg).To(gomega.ContainSubstring("exactly one of canary"))
	})

	ginkgo.It("rejects an absolute step capacity as service-specific", func() {
		spec := validCanarySpec()
		spec.Canary.Steps[0].Capacity = intstr.FromInt32(3)
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("absolute"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("must be a percentage"))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonRolloutPolicyInvalid))
	})

	ginkgo.It("rejects a raw serverAddress in the body", func() {
		spec := validCanarySpec()
		spec.Canary.Prometheus = &v1beta1.AnalysisPrometheus{
			ServerAddress: "http://prometheus.example.com:9090",
		}
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("raw-address"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("serverAddress is not allowed in a policy body"))
	})

	ginkgo.It("rejects an authRef in the body", func() {
		spec := validCanarySpec()
		spec.Canary.Prometheus.AuthRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "metrics-token"},
			Key:                  "token",
		}
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("auth-smuggle"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("authRef is not allowed in a policy body"))
	})

	ginkgo.It("rejects an analysis step without a providerRef metrics source", func() {
		spec := validCanarySpec()
		spec.Canary.Prometheus = nil
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("unbound-gate"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("providerRef is required when any step declares analysis"))
	})

	ginkgo.It("rejects a non-numeric analysis threshold", func() {
		spec := validCanarySpec()
		spec.Canary.Steps[0].Analysis.Metrics[0].Threshold = "high"
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("bad-threshold"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("must be numeric"))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonAnalysisInvalid))
	})

	// failureLimit carries a CRD Minimum=1 bound, so the schema layer
	// rejects 0 before the webhook is consulted.
	ginkgo.It("rejects analysis failureLimit 0 at the CRD schema", func() {
		spec := validCanarySpec()
		spec.Canary.Steps[0].Analysis.FailureLimit = 0
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("no-limit"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("failureLimit"))
	})

	ginkgo.It("rejects a final step that does not reach 100 traffic", func() {
		spec := validCanarySpec()
		spec.Canary.Steps[1].Traffic = 90
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("partial-cutover"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring("final step traffic must be 100"))
	})

	// The oversized body is otherwise fully valid — the size cap must be its
	// own rejection, not a side effect of a shape rule.
	ginkgo.It("rejects a body over the configured plan-size cap", func() {
		spec := validCanarySpec()
		spec.Canary.Steps[0].Analysis.Metrics[0].Query = strings.Repeat("m", suiteMaxPlanBytes)
		msg := denial(k8sClient.Create(suiteCtx, mkPolicy(uniqueName("oversized"), spec)))
		gomega.Expect(msg).To(gomega.ContainSubstring(validation.ReasonRolloutPlanTooLarge))
		gomega.Expect(msg).To(gomega.ContainSubstring("exceeding the configured cap"))
	})
})

// warningRecorder captures admission warning headers so a spec can assert
// on the exact warnings a write produced.
type warningRecorder struct {
	mu       sync.Mutex
	warnings []string
}

func (w *warningRecorder) HandleWarningHeader(_ int, _ string, text string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warnings = append(w.warnings, text)
}

func (w *warningRecorder) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.warnings...)
}

var _ = ginkgo.Describe("RolloutPolicy update admission", func() {

	// A referenced body edit is inert for pinned runs (each consumer's next
	// run picks it up), so admission warns rather than rejects.
	ginkgo.It("admits a referenced body edit with a warning", func() {
		policy := mkPolicy(uniqueName("canary-std"), validCanarySpec())
		consumer := mkConsumer(uniqueName("llm-a"),
			refGroup(policy.Name, v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent))
		mustCreate(policy, consumer)

		recorder := &warningRecorder{}
		cfg := rest.CopyConfig(suiteCfg)
		cfg.WarningHandler = recorder
		warned, err := client.New(cfg, client.Options{Scheme: k8sClient.Scheme()})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		got := &v1beta1.RolloutPolicy{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(policy), got)).To(gomega.Succeed())
		got.Spec.Canary.Steps[0].Traffic = 60
		gomega.Expect(warned.Update(suiteCtx, got)).To(gomega.Succeed(),
			"a same-kind body edit under live references must be admitted")

		gomega.Expect(recorder.snapshot()).To(gomega.ContainElement(gomega.And(
			gomega.ContainSubstring("reference RolloutPolicy"),
			gomega.ContainSubstring("pinned plan"),
		)), "the webhook response must carry the referenced-edit warning")
	})

	// Consumers' shape rules were admitted against the declared progression
	// kind, so an in-place kind change would invalidate every one of them.
	ginkgo.It("rejects a progression-kind change while referenced", func() {
		policy := mkPolicy(uniqueName("canary-std"), validCanarySpec())
		consumer := mkConsumer(uniqueName("llm-a"),
			refGroup(policy.Name, v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent))
		mustCreate(policy, consumer)

		got := &v1beta1.RolloutPolicy{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(policy), got)).To(gomega.Succeed())
		got.Spec.Canary = nil
		got.Spec.BlueGreen = &v1beta1.GroupBlueGreen{}
		msg := denial(k8sClient.Update(suiteCtx, got))
		gomega.Expect(msg).To(gomega.ContainSubstring("progression kind cannot change"))
	})

	ginkgo.It("admits a progression-kind change with zero refs", func() {
		policy := mkPolicy(uniqueName("canary-std"), validCanarySpec())
		mustCreate(policy)

		got := &v1beta1.RolloutPolicy{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(policy), got)).To(gomega.Succeed())
		got.Spec.Canary = nil
		got.Spec.BlueGreen = &v1beta1.GroupBlueGreen{}
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())
	})
})

var _ = ginkgo.Describe("RolloutPolicy delete admission", func() {

	ginkgo.It("denies deletion while a rollout group references the policy, then admits at zero refs", func() {
		policy := mkPolicy(uniqueName("canary-std"), validCanarySpec())
		consumer := mkConsumer(uniqueName("llm-a"),
			refGroup(policy.Name, v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent))
		mustCreate(policy, consumer)

		msg := denial(k8sClient.Delete(suiteCtx, policy))
		gomega.Expect(msg).To(gomega.ContainSubstring(consumer.Name),
			"the denial must name the referencing InferenceService")
		gomega.Expect(msg).To(gomega.ContainSubstring(constants.AutoscalerPolicyAllowInUseDelete),
			"the denial must teach the break-glass path")

		got := &v1beta1.InferenceService{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(consumer), got)).To(gomega.Succeed())
		got.Spec.Rollout = nil
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Delete(suiteCtx, policy)).To(gomega.Succeed())
	})

	ginkgo.It("admits deleting a policy no rollout group references", func() {
		policy := mkPolicy(uniqueName("canary-std"), validCanarySpec())
		mustCreate(policy)
		gomega.Expect(k8sClient.Delete(suiteCtx, policy)).To(gomega.Succeed())
	})

	ginkgo.It("admits an in-use deletion carrying the break-glass annotation", func() {
		policy := mkPolicy(uniqueName("canary-std"), validCanarySpec())
		policy.Annotations = map[string]string{constants.AutoscalerPolicyAllowInUseDelete: "true"}
		consumer := mkConsumer(uniqueName("llm-a"),
			refGroup(policy.Name, v1beta1.RolloutProgressionCanary, v1beta1.EngineComponent))
		mustCreate(policy, consumer)

		gomega.Expect(k8sClient.Delete(suiteCtx, policy)).To(gomega.Succeed())
	})
})
