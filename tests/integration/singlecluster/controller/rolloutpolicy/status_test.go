package rolloutpolicy

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	policycore "github.com/ome-projects/ome/pkg/rolloutpolicy"
)

const (
	settleTimeout = 10 * time.Second
	pollInterval  = 200 * time.Millisecond
	unboundName   = "spike-metrics"
)

// fetch re-reads the policy from the apiserver.
func fetch(policy *v1beta1.RolloutPolicy) *v1beta1.RolloutPolicy {
	ginkgo.GinkgoHelper()
	got := &v1beta1.RolloutPolicy{}
	gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(policy), got)).To(gomega.Succeed())
	return got
}

// hasCondition matches when the named condition reaches the wanted state
// and its per-condition observedGeneration has caught up to the spec.
func hasCondition(policy *v1beta1.RolloutPolicy, condType string, status metav1.ConditionStatus, reason string) func() error {
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

func hasAttached(policy *v1beta1.RolloutPolicy, want int32) func() error {
	return func() error {
		got := fetch(policy)
		if got.Status.AttachedGroups != want {
			return fmt.Errorf("attachedGroups = %d, want %d", got.Status.AttachedGroups, want)
		}
		return nil
	}
}

var _ = ginkgo.Describe("RolloutPolicy status controller", func() {

	ginkgo.It("reports Ready with the portable digest for a valid provider-bound policy", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "canary-std", canarySpec(boundProvider))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.RolloutPolicyReasonBodyValid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyInUseCondition,
			metav1.ConditionFalse, v1beta1.RolloutPolicyReasonNoConsumers),
			settleTimeout, pollInterval).Should(gomega.Succeed())

		got := fetch(policy)
		gomega.Expect(got.Status.ObservedGeneration).To(gomega.Equal(got.Generation))
		gomega.Expect(got.Status.AttachedGroups).To(gomega.BeZero())
		gomega.Expect(got.Status.PortableDigest).To(gomega.HavePrefix("rp1:"))
		// The digest is a pure function of the stored spec: an independent
		// recompute must agree, or cross-cluster comparison against another
		// cluster's copy is meaningless.
		expected, err := policycore.PortableDigest(&got.Spec)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(got.Status.PortableDigest).To(gomega.Equal(expected))
	})

	// No webhook runs in this suite, so an invalid body lands in etcd and
	// the controller must degrade it to a condition — the skew path when a
	// policy is admitted by an older webhook or a break-glass write.
	ginkgo.It("reports BodyInvalid for a stored body that fails plan validation", func() {
		ns := newTestNamespace()
		spec := v1beta1.RolloutPolicySpec{
			Canary: &v1beta1.GroupCanary{
				Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 50},
				},
			},
		}
		policy := mkPolicy(ns, "partial-cutover", spec)
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyReadyCondition,
			metav1.ConditionFalse, v1beta1.RolloutPolicyReasonBodyInvalid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		got := fetch(policy)
		gomega.Expect(apimeta.FindStatusCondition(got.Status.Conditions,
			v1beta1.RolloutPolicyReadyCondition).Message).To(gomega.ContainSubstring("traffic"))
		gomega.Expect(got.Status.PortableDigest).NotTo(gomega.BeEmpty(),
			"the digest describes the spec, not its validity")
	})

	ginkgo.It("counts attached rollout groups and flips InUse across attach and detach", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "canary-std", canarySpec(boundProvider))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyInUseCondition,
			metav1.ConditionFalse, v1beta1.RolloutPolicyReasonNoConsumers),
			settleTimeout, pollInterval).Should(gomega.Succeed())

		consumer := mkConsumer(ns, "llm-a", refGroup(policy.Name, v1beta1.EngineComponent))
		gomega.Expect(k8sClient.Create(suiteCtx, consumer)).To(gomega.Succeed())

		gomega.Eventually(hasAttached(policy, 1), settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyInUseCondition,
			metav1.ConditionTrue, v1beta1.RolloutPolicyReasonAttached),
			settleTimeout, pollInterval).Should(gomega.Succeed())

		// Attachment counts rollout groups, not InferenceServices: one
		// consumer referencing from two groups contributes two.
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(consumer), consumer)).To(gomega.Succeed())
		consumer.Spec.Rollout.Groups = append(consumer.Spec.Rollout.Groups,
			refGroup(policy.Name, v1beta1.DecoderComponent))
		gomega.Expect(k8sClient.Update(suiteCtx, consumer)).To(gomega.Succeed())
		gomega.Eventually(hasAttached(policy, 2), settleTimeout, pollInterval).Should(gomega.Succeed())

		// Detach by removing the rollout block: the watch maps the old
		// object too, so the policy losing the refs is enqueued even though
		// the new object no longer names it.
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(consumer), consumer)).To(gomega.Succeed())
		consumer.Spec.Rollout = nil
		gomega.Expect(k8sClient.Update(suiteCtx, consumer)).To(gomega.Succeed())

		gomega.Eventually(hasAttached(policy, 0), settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyInUseCondition,
			metav1.ConditionFalse, v1beta1.RolloutPolicyReasonNoConsumers),
			settleTimeout, pollInterval).Should(gomega.Succeed())
	})

	// An unbound provider name is a per-cluster caveat, not a body defect:
	// members of one fleet legitimately bind different provider sets, so
	// Ready stays True and only the reason softens.
	ginkgo.It("keeps Ready True with ProviderUnbound when the provider is not bound here", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "spike-guard", canarySpec(unboundName))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.RolloutPolicyReasonProviderUnbound),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		gomega.Expect(apimeta.FindStatusCondition(fetch(policy).Status.Conditions,
			v1beta1.RolloutPolicyReadyCondition).Message).To(gomega.ContainSubstring(unboundName))
		gomega.Expect(fetch(policy).Status.PortableDigest).NotTo(gomega.BeEmpty())
	})

	ginkgo.It("moves the digest with each body edit and restores it on an identical body", func() {
		ns := newTestNamespace()
		policy := mkPolicy(ns, "canary-std", canarySpec(boundProvider))
		gomega.Expect(k8sClient.Create(suiteCtx, policy)).To(gomega.Succeed())

		gomega.Eventually(hasCondition(policy, v1beta1.RolloutPolicyReadyCondition,
			metav1.ConditionTrue, v1beta1.RolloutPolicyReasonBodyValid),
			settleTimeout, pollInterval).Should(gomega.Succeed())
		originalDigest := fetch(policy).Status.PortableDigest

		got := fetch(policy)
		got.Spec.Canary.ScaleDownDelaySeconds = ptr.To(int32(60))
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())

		gomega.Eventually(func() string {
			return fetch(policy).Status.PortableDigest
		}, settleTimeout, pollInterval).ShouldNot(gomega.Equal(originalDigest),
			"a body edit must move the digest")

		got = fetch(policy)
		got.Spec.Canary.ScaleDownDelaySeconds = nil
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())

		gomega.Eventually(func() string {
			return fetch(policy).Status.PortableDigest
		}, settleTimeout, pollInterval).Should(gomega.Equal(originalDigest),
			"an identical body must restore the identical digest")

		// A metadata-only touch is body-neutral and must leave the digest
		// where it is.
		got = fetch(policy)
		got.Labels = map[string]string{"team": "serving"}
		gomega.Expect(k8sClient.Update(suiteCtx, got)).To(gomega.Succeed())
		gomega.Consistently(func() string {
			return fetch(policy).Status.PortableDigest
		}, time.Second, pollInterval).Should(gomega.Equal(originalDigest))
	})
})
