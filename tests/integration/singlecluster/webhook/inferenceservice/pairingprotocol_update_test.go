package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// pairingBaseline creates the prebuilt ISVC and returns the apiserver's
// freshly-read copy so the caller can chain an Update off the post-admission
// ResourceVersion.
func pairingBaseline(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	ExpectWithOffset(1, k8sClient.Create(suiteCtx, isvc)).To(Succeed(), "baseline create")
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
	return pairingFetch(isvc.Namespace, isvc.Name)
}

func pairingFetch(namespace, name string) *v1beta1.InferenceService {
	got := &v1beta1.InferenceService{}
	ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, got)).To(Succeed())
	return got
}

// UPDATE-path admission for spec.rollout.pairingProtocol: a change between
// two non-empty values is admitted only when engine and decoder roll as one
// blueGreen or canary group; transitions to/from empty are always admitted
// (empty pairs with anything). Runs against the real apiserver + webhook so
// the wiring in ValidateUpdate is what's proven, not just the pure rule.
var _ = ginkgo.Describe("InferenceService webhook validation (UPDATE) — spec.rollout.pairingProtocol", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(setupCoordCBMandCSR)

	ginkgo.BeforeEach(ensureNamespace)

	setProtocol := func(isvc *v1beta1.InferenceService, p string) {
		if isvc.Spec.Rollout == nil {
			isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
		}
		if p == "" {
			isvc.Spec.Rollout.PairingProtocol = nil
			return
		}
		isvc.Spec.Rollout.PairingProtocol = ptr.To(p)
	}

	ginkgo.It("PP-1 admits a protocol change under a shared blueGreen group", func() {
		isvc := coordISVC("pairing-pp1", atomicGroup())
		setProtocol(isvc, "nixl-v1")
		got := pairingBaseline(isvc)
		setProtocol(got, "nixl-v2")
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	ginkgo.It("PP-2 rejects a protocol change under a rollingUpdate group", func() {
		isvc := coordISVC("pairing-pp2", pmGroup())
		setProtocol(isvc, "nixl-v1")
		got := pairingBaseline(isvc)
		setProtocol(got, "nixl-v2")
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonPairingProtocolChangeUncoordinated))
	})

	ginkgo.It("PP-3 rejects a protocol change with no rollout groups", func() {
		isvc := coordISVC("pairing-pp3")
		setProtocol(isvc, "nixl-v1")
		got := pairingBaseline(isvc)
		setProtocol(got, "nixl-v2")
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonPairingProtocolChangeUncoordinated))
	})

	ginkgo.It("PP-4 admits clearing and setting the protocol without groups", func() {
		isvc := coordISVC("pairing-pp4")
		setProtocol(isvc, "nixl-v1")
		got := pairingBaseline(isvc)

		// Clearing: empty pairs with anything.
		setProtocol(got, "")
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		// Setting from unset: also upgrade-neutral.
		refetched := pairingFetch(got.Namespace, got.Name)
		setProtocol(refetched, "nixl-v2")
		Expect(k8sClient.Update(suiteCtx, refetched)).To(Succeed())
	})
})
