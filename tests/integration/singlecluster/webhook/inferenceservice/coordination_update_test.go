package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// Coordination UPDATE admission specs for OEP-0007.1. The webhook's
// ValidateUpdate path runs validation.ValidateCoordinationUpdate
// (PairedMixable lockstep) + validation.ValidateCoordination (the
// CREATE-time structural rules, re-applied so update can't escape
// them). These tests prove both halves wire correctly under
// real apiserver Update calls.
//
// Pattern: create a baseline ISVC via Create, then mutate + Update.
// Each test factors out the create step into a small helper so the
// assertion focuses on the update's accept/reject decision.

// pmGroup builds a PairedMixable group over engine + decoder.
func pmGroup() v1beta1.RolloutCoordinationGroup {
	return v1beta1.RolloutCoordinationGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyPairedMixable,
	}
}

// atomicGroup builds an Atomic group over engine + decoder.
func atomicGroup() v1beta1.RolloutCoordinationGroup {
	return v1beta1.RolloutCoordinationGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyAtomic,
	}
}

// createBaselineISVC creates an ISVC named `name` carrying `groups` and
// returns the apiserver's freshly-read copy. Caller mutates this copy
// then calls k8sClient.Update.
func createBaselineISVC(name string, groups ...v1beta1.RolloutCoordinationGroup) *v1beta1.InferenceService {
	isvc := coordISVC(name, groups...)
	ExpectWithOffset(1, k8sClient.Create(suiteCtx, isvc)).To(Succeed(), "baseline create")
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

	// Re-fetch so the caller has the post-admission ResourceVersion to
	// chain an Update off of.
	got := &v1beta1.InferenceService{}
	ExpectWithOffset(1, k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: isvc.Namespace, Name: isvc.Name},
		got,
	)).To(Succeed())
	return got
}

// withRunner attaches a Runner block carrying the given image. The
// validator's PairedMixable lockstep check reads spec.Engine.Runner.Image
// (not PodSpec.Containers[0].Image), so update tests asserting on
// lockstep must populate the Runner field.
func withEngineRunner(isvc *v1beta1.InferenceService, image string) {
	isvc.Spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{
		Name: "ome-container", Image: image,
	}}
}

func withDecoderRunner(isvc *v1beta1.InferenceService, image string) {
	isvc.Spec.Decoder.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{
		Name: "ome-container", Image: image,
	}}
}

var _ = ginkgo.Describe("InferenceService webhook validation (UPDATE) — rolloutCoordination (OEP-0007.1)", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(setupCoordCBMandCSR)

	ginkgo.BeforeEach(ensureNamespace)

	// WU-1: PairedMixable lockstep is the marquee update rule. Bump
	// engine Runner.Image WITHOUT bumping decoder → rejected with
	// the dedicated lockstep reason. The validator reads
	// spec.Engine.Runner.Image (see componentImages in
	// pkg/validation/coordination.go), so the test seeds Runner on
	// both Components at v1 then mutates only engine to v2.
	ginkgo.It("WU-1 rejects PairedMixable image bump on one Component only → PairedMixableLockstepViolation", func() {
		isvc := coordISVC("coord-wu1", pmGroup())
		withEngineRunner(isvc, "ghcr.io/test/runtime:v1")
		withDecoderRunner(isvc, "ghcr.io/test/runtime:v1")
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: isvc.Namespace, Name: isvc.Name}, got)).To(Succeed())
		got.Spec.Engine.Runner.Image = "ghcr.io/test/runtime:v2"
		// decoder.Runner.Image stays at v1 → asymmetric
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonPairedMixableLockstepViolation))
	})

	// WU-1a: PairedMixable lockstep catches image bumps via either
	// Runner.Image OR PodSpec.Containers[0].Image. componentImages
	// reads Runner first and falls back to PodSpec — both surfaces
	// produce the same lockstep signal so users can't accidentally
	// escape the validator by setting the image via PodSpec.
	//
	// Closes G11.
	ginkgo.It("WU-1a catches PairedMixable bump via PodSpec.Containers[0].Image", func() {
		got := createBaselineISVC("coord-wu1a", pmGroup())
		got.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonPairedMixableLockstepViolation))
	})

	// WU-2: Atomic has no lockstep restriction. Both Components can
	// bump together, one at a time, or stay still — all accepted.
	ginkgo.It("WU-2 accepts Atomic image bump on both Components together", func() {
		got := createBaselineISVC("coord-wu2", atomicGroup())
		got.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		got.Spec.Decoder.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-3: Updating the group's Policy mid-rollout: OEP-0007.1
	// doesn't forbid this — the structural validator just re-runs the
	// shape rules on the new spec. Pin observed behavior.
	ginkgo.It("WU-3 accepts policy change Atomic → Independent mid-rollout", func() {
		got := createBaselineISVC("coord-wu3", atomicGroup())
		got.Spec.RolloutCoordination.Groups[0].Policy = v1beta1.CoordinationPolicyIndependent
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-4: Adding a brand-new group to an existing ISVC is allowed
	// (assuming it passes the structural rules).
	ginkgo.It("WU-4 accepts adding a new group to an existing ISVC", func() {
		got := createBaselineISVC("coord-wu4") // no groups initially
		got.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{atomicGroup()},
		}
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-5: Removing the whole rolloutCoordination block mid-rollout
	// is allowed — coordination is opt-in, so opting out is symmetric.
	ginkgo.It("WU-5 accepts removing the rolloutCoordination block entirely", func() {
		got := createBaselineISVC("coord-wu5", atomicGroup())
		got.Spec.RolloutCoordination = nil
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-6: Modifying Sequential Order mid-rollout. The structural
	// validator re-runs and accepts any valid new shape; runtime
	// behavior at the controller is a separate concern (covered in
	// state_sequential_test.go).
	ginkgo.It("WU-6 accepts Sequential order swap mid-rollout", func() {
		seq := v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		}
		got := createBaselineISVC("coord-wu6", seq)
		got.Spec.RolloutCoordination.Groups[0].Order = []v1beta1.ComponentType{
			v1beta1.DecoderComponent, v1beta1.EngineComponent,
		}
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-7: Adding a Component to an existing group is structurally
	// fine as long as it's declared on the spec.
	ginkgo.It("WU-7 accepts adding a Component to an existing group (with Router on spec)", func() {
		// Build with Router so adding it to the group later is allowed.
		isvc := v1beta1testing.MakeInferenceService("coord-wu7", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Decoder(omeNativeDecoder()).
			Router(omeNativeRouter()).
			Obj()
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{atomicGroup()},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: "isvc-webhook", Name: "coord-wu7"}, got)).To(Succeed())

		got.Spec.RolloutCoordination.Groups[0].Components = append(
			got.Spec.RolloutCoordination.Groups[0].Components, v1beta1.RouterComponent)
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-8: Removing a Component from an existing group accepted; the
	// removed Component then rolls independently.
	ginkgo.It("WU-8 accepts removing a Component from an existing group", func() {
		got := createBaselineISVC("coord-wu8", atomicGroup())
		got.Spec.RolloutCoordination.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
		// 1-Component Atomic is a degenerate but accepted shape (W-21).
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-9: PairedMixable lockstep when Runner field is missing on a
	// Component. componentImages returns "" for both → "" != ""
	// evaluates symmetric, so an update that touches the (also nil)
	// runner side stays accepted as a no-op. Pin so a future
	// refactor that starts treating nil-runner as a distinct value
	// gets caught.
	ginkgo.It("WU-9 PairedMixable with nil Runner on both sides treats image as empty (symmetric)", func() {
		// Build without Runner field (componentImages reads
		// spec.Engine.Runner.Image; PodSpec.Containers[0].Image is a
		// different path). Coordination tests use PodSpec containers,
		// so changing the container image WITHOUT a Runner block is
		// invisible to the PairedMixable lockstep check.
		got := createBaselineISVC("coord-wu9", pmGroup())
		got.Spec.Engine.PodSpec.Containers = append(got.Spec.Engine.PodSpec.Containers, corev1.Container{
			Name: "sidecar", Image: "ghcr.io/test/sidecar:v1",
		})
		// engine.PodSpec changed, but engine.Runner is still nil → empty
		// image, decoder.Runner is also nil → empty image, anyChanged
		// stays false. Webhook accepts.
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-10: Changing a Component's deploymentMode annotation off
	// OMENative while it's still listed in a coordination group is
	// rejected — coordination demands OMENative. The Engine/Decoder
	// symmetry check fires too (covered separately); here we pick the
	// Engine+Router path so CoordinationRequiresOMENative is the
	// distinct rejector.
	ginkgo.It("WU-10 rejects switching Component to RawDeployment while still in group → CoordinationRequiresOMENative", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-wu10", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Router(omeNativeRouter()).
			Obj()
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
				Policy:     v1beta1.CoordinationPolicyAtomic,
			}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: "isvc-webhook", Name: "coord-wu10"}, got)).To(Succeed())
		got.Spec.Router.Annotations[constants.DeploymentMode] = string(constants.RawDeployment)
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonCoordinationRequiresOMENative))
	})
})
