package autoscalerpolicy

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/autoscalerpolicy/render"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
)

const (
	settleTimeout = 10 * time.Second
	pollInterval  = 200 * time.Millisecond
	unboundName   = "spike-metrics"
)

// fetch re-reads the policy from the apiserver.
func fetch(policy *v1beta1.AutoscalerPolicy) *v1beta1.AutoscalerPolicy {
	ginkgo.GinkgoHelper()
	got := &v1beta1.AutoscalerPolicy{}
	gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(policy), got)).To(gomega.Succeed())
	return got
}

// hasCondition matches when the named condition reaches the wanted state
// and its per-condition observedGeneration has caught up to the spec.
func hasCondition(policy *v1beta1.AutoscalerPolicy, condType string, status metav1.ConditionStatus, reason string) func() error {
	return func() error {
		got := fetch(policy)
		cond := apimeta.FindStatusCondition(got.Status.Conditions, condType)
		if cond == nil {
			return fmt.Errorf("condition %s not present in %v", condType, got.Status.Conditions)
		}
		if cond.Status != status || cond.Reason != reason {
			return fmt.Errorf("condition %s is %s/%s, want %s/%s (message: %s)",
				condType, cond.Status, cond.Reason, status, reason, cond.Message)
		}
		if cond.ObservedGeneration != got.Generation {
			return fmt.Errorf("condition %s observedGeneration %d lags generation %d",
				condType, cond.ObservedGeneration, got.Generation)
		}
		return nil
	}
}

func hasAttached(policy *v1beta1.AutoscalerPolicy, want int32) func() error {
	return func() error {
		got := fetch(policy)
		if got.Status.AttachedComponents != want {
			return fmt.Errorf("attachedComponents = %d, want %d", got.Status.AttachedComponents, want)
		}
		return nil
	}
}

var _ = ginkgo.Describe("AutoscalerPolicy status controller", func() {

	ginkgo.It("reports Ready with the portable digest for a valid provider-bound policy", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "request-activity", kedaSpec(boundProvider))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.AutoscalerPolicyReasonTemplatesValid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyInUseCondition,
			metav1.ConditionFalse, v1beta1.AutoscalerPolicyReasonNoConsumers),
			settleTimeout, pollInterval).Should(gomega.Succeed())

		got := fetch(policy)
		gomega.Expect(got.Status.ObservedGeneration).To(gomega.Equal(got.Generation))
		gomega.Expect(got.Status.AttachedComponents).To(gomega.BeZero())
		// The digest is a pure function of the stored (API-defaulted) spec:
		// an independent recompute must agree, or cross-cluster comparison
		// against a control plane's copy is meaningless.
		expected, err := render.PortableDigest(&got.Spec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.Status.PortableDigest).To(gomega.Equal(expected))
	})

	ginkgo.It("reports Ready for an HPA-only policy without consulting provider bindings", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "cpu-headroom", v1beta1.AutoscalerPolicySpec{Class: v1beta1.AutoscalerHPA})
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.AutoscalerPolicyReasonTemplatesValid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("flips Ready to False on an invalid spec and recovers, with the digest tracking each edit", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "request-activity", kedaSpec(boundProvider))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.AutoscalerPolicyReasonTemplatesValid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		validDigest := fetch(policy).Status.PortableDigest

		// No webhook runs in this suite, so the reserved shape lands in
		// etcd and the controller must degrade it to a condition — the
		// skew path when a policy is admitted by an older webhook.
		got := fetch(policy)
		got.Spec.Enforcement = v1beta1.PolicyEnforcementRequired
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyReadyCondition,
			metav1.ConditionFalse, render.ReasonEnforcementReserved),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		invalid := fetch(policy)
		gomega.Expect(invalid.Status.ObservedGeneration).To(gomega.Equal(invalid.Generation))
		gomega.Expect(invalid.Status.PortableDigest).NotTo(gomega.Equal(validDigest),
			"the digest must track the spec even while invalid")

		got = fetch(policy)
		got.Spec.Enforcement = v1beta1.PolicyEnforcementDefault
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.AutoscalerPolicyReasonTemplatesValid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Expect(fetch(policy).Status.PortableDigest).To(gomega.Equal(validDigest),
			"reverting the spec must restore the original digest")
	})

	ginkgo.It("reports ProviderUnknown until the operator config binds the provider", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "spike-guard", kedaSpec(unboundName))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyReadyCondition,
			metav1.ConditionFalse, v1beta1.AutoscalerPolicyReasonProviderUnknown),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Expect(apimeta.FindStatusCondition(fetch(policy).Status.Conditions,
			v1beta1.AutoscalerPolicyReadyCondition).Message).To(gomega.ContainSubstring(unboundName))

		bindProvider(unboundName)
		// The controller has no watch on the operator config, so a binding
		// fix takes effect on the next policy event; a benign spec touch
		// stands in for that event here.
		got := fetch(policy)
		got.Spec.Keda.PollingInterval = ptr.To(int32(30))
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.AutoscalerPolicyReasonTemplatesValid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("counts attached component slots and flips InUse across attach and detach", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "request-activity", kedaSpec(boundProvider))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyInUseCondition,
			metav1.ConditionFalse, v1beta1.AutoscalerPolicyReasonNoConsumers),
			settleTimeout, pollInterval).Should(gomega.Succeed())

		// Attachment counts component slots, not InferenceServices: one
		// consumer referencing from engine and decoder contributes two.
		dual := mkConsumer(ns, "dual-consumer", policy.Name, policy.Name)
		single := mkConsumer(ns, "single-consumer", policy.Name, "")
		gomega.Expect(k8sClient.Create(suiteCtx, dual)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, single)).To(gomega.Succeed())

		gomega.Eventually(hasAttached(policy, 3), settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyInUseCondition,
			metav1.ConditionTrue, v1beta1.AutoscalerPolicyReasonAttached),
			settleTimeout, pollInterval).Should(gomega.Succeed())

		// Detach one slot by update: the watch maps the old object too, so
		// the policy losing the ref is enqueued even though the new object
		// no longer names it from that slot.
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(dual), dual)).To(gomega.Succeed())
		dual.Spec.Decoder.AutoscalerPolicyRef = nil
		gomega.Expect(k8sClient.Update(suiteCtx, dual)).To(gomega.Succeed())
		gomega.Eventually(hasAttached(policy, 2), settleTimeout, pollInterval).Should(gomega.Succeed())

		// Detach by consumer deletion.
		gomega.Expect(k8sClient.Delete(suiteCtx, single)).To(gomega.Succeed())
		gomega.Eventually(hasAttached(policy, 1), settleTimeout, pollInterval).Should(gomega.Succeed())

		// Last slot detaches: InUse returns to False.
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(dual), dual)).To(gomega.Succeed())
		dual.Spec.Engine.AutoscalerPolicyRef = nil
		gomega.Expect(k8sClient.Update(suiteCtx, dual)).To(gomega.Succeed())

		gomega.Eventually(hasAttached(policy, 0), settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Eventually(hasCondition(policy, v1beta1.AutoscalerPolicyInUseCondition,
			metav1.ConditionFalse, v1beta1.AutoscalerPolicyReasonNoConsumers),
			settleTimeout, pollInterval).Should(gomega.Succeed())
	})
})

// bindProvider adds a serverAddress binding for the named provider to the
// operator ConfigMap, keeping every existing binding intact.
func bindProvider(name string) {
	ginkgo.GinkgoHelper()
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName}
	gomega.Expect(k8sClient.Get(suiteCtx, key, cm)).To(gomega.Succeed())
	cm.Data[controllerconfig.AutoscalerPolicyConfigName] = fmt.Sprintf(
		`{"metricProviders":{%q:{"serverAddress":"http://prometheus.example.com:9090"},%q:{"serverAddress":"http://prometheus.example.com:9090"}}}`,
		boundProvider, name)
	gomega.Expect(k8sClient.Update(suiteCtx, cm)).To(gomega.Succeed())
}
