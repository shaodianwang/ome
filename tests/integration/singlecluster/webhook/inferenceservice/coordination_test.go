package inferenceservice

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// Coordination admission specs for the rollout spec
// (spec.rollout.groups[]). Each test sends a Create or Update through
// the apiserver and asserts the validating webhook's decision. The
// validator's rejection-reason constants (validation.Reason*) are
// referenced by name so a future rename of a constant fails the test
// loudly here instead of silently changing the operator-facing error
// string.
//
// Shape of the spec: spec.rollout.groups[] is a presence-based one-of
// progression (blueGreen | rollingUpdate | canary; at most one — a group
// that names none defaults to blueGreen). BlueGreen and RollingUpdate are
// the coordination-style progressions; sequencing across Components is
// expressed as multiple single-Component groups in list order; independence
// is the absence of a group; MaintainRatio is the group-level ratio guard.
// Specs written against admission rules this shape cannot express
// (Sequential-policy and single-Component-group rejections) are skipped
// with a note stating the current rule.
//
// Unset pacing values (MaxSurge/MaxUnavailable/MaintainRatio) are resolved
// by the controller at reconcile time, never at admission.

// Reusable container set: minimal pod with OME's runtime container.
func coordRunner() []corev1.Container {
	return []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:v1"}}
}

// omeNativeEngine returns an EngineSpec annotated for OMENative
// deployment mode. Coordination requires every Component in a group to
// be OMENative; specs that test the CoordinationRequiresOMENative
// rejection swap this for raw / missing.
func omeNativeEngine() *v1beta1.EngineSpec {
	return &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		PodSpec: v1beta1.PodSpec{Containers: coordRunner()},
	}
}

// omeNativeDecoder mirrors omeNativeEngine for the decoder Component.
func omeNativeDecoder() *v1beta1.DecoderSpec {
	return &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		PodSpec: v1beta1.PodSpec{Containers: coordRunner()},
	}
}

func omeNativeRouter() *v1beta1.RouterSpec {
	return &v1beta1.RouterSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		PodSpec: v1beta1.PodSpec{Containers: coordRunner()},
	}
}

// rawDecoder is omeNativeDecoder's RawDeployment counterpart — used by
// the CoordinationRequiresOMENative rejection (W-4) and reserved for
// future cross-mode update specs (WU-10 variants).
//
//nolint:unused // reserved for upcoming cross-mode webhook specs
func rawDecoder() *v1beta1.DecoderSpec {
	return &v1beta1.DecoderSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)},
		},
		PodSpec: v1beta1.PodSpec{Containers: coordRunner()},
	}
}

// coordISVC builds a base ISVC with engine + decoder OMENative,
// pre-populated rollout groups, ready to Create. Spec-specific
// callers swap fields before Create.
// coordCBMName + coordCSRName are the shared model/runtime the
// coordination accept-tests reference. Created once per Describe via
// BeforeAll because validateModelExists hits the apiserver and a
// "model not found" rejection would mask the coordination-validator
// decision the test is actually trying to exercise.
const (
	coordCBMName = "coord-cbm"
	coordCSRName = "coord-runtime"
)

func coordISVC(name string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceService {
	isvc := v1beta1testing.MakeInferenceService(name, "isvc-webhook").
		Model(coordCBMName).
		Runtime(coordCSRName).
		DeploymentMode(constants.OMENative).
		Engine(omeNativeEngine()).
		Decoder(omeNativeDecoder()).
		Obj()
	if len(groups) > 0 {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: groups}
	}
	return isvc
}

// setupCoordCBMandCSR creates the shared CBM + CSR the coordination
// admission specs reference and registers DeferCleanup. Called from
// each Describe block's BeforeAll — Ginkgo BeforeAll scopes are
// per-Describe, so the CREATE and UPDATE Describes each install
// their own pair. Using fresh resources per Describe avoids "model
// already exists" conflicts when both run in the same suite.
func setupCoordCBMandCSR() {
	ginkgo.GinkgoHelper()
	ensureNamespace()
	cbm := v1beta1testing.MakeClusterBaseModel(coordCBMName).
		StorageURI("oci://n/test/b/models/o/llama-7b").
		ModelFormat("safetensors").
		ModelArchitecture("LlamaForCausalLM").
		ModelFramework("Transformers").
		Obj()
	// Idempotent create: the second Describe's BeforeAll will hit
	// AlreadyExists on the first run; treat that as success.
	if err := k8sClient.Create(suiteCtx, cbm); err != nil {
		Expect(client.IgnoreAlreadyExists(err)).To(Succeed())
	}
	csr := v1beta1testing.MakeClusterServingRuntime(coordCSRName).
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		Obj()
	if err := k8sClient.Create(suiteCtx, csr); err != nil {
		Expect(client.IgnoreAlreadyExists(err)).To(Succeed())
	}
}

var _ = ginkgo.Describe("InferenceService webhook validation — spec.rollout.groups[]", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(setupCoordCBMandCSR)

	ginkgo.BeforeEach(ensureNamespace)

	// W-1: zero groups is the no-op shape that historical ISVCs use.
	// The validator must accept it so existing manifests don't break.
	ginkgo.It("W-1 accepts ISVC with empty rollout.groups[]", func() {
		isvc := coordISVC("coord-w1")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{}}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-2: a Component can only appear in one group. Two groups
	// claiming Engine would race on every reconcile — admission
	// catches it up front.
	ginkgo.It("W-2 rejects duplicate Component across groups → DuplicateComponentInCoordinationGroups", func() {
		isvc := coordISVC("coord-w2",
			v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
			v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
		)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonDuplicateComponentInCoordinationGroups))
	})

	// W-3: Component enum is closed to router/engine/decoder. A typo
	// or an aspirational future Component is rejected at admission.
	ginkgo.It("W-3 rejects unknown Component name → InvalidComponentInCoordinationGroup", func() {
		isvc := coordISVC("coord-w3", v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{"predictor"},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonInvalidComponentInCoordinationGroup))
	})

	// W-4: coordination presumes OMENative. A Component declared on
	// the ISVC but in a different deployment mode can't participate.
	// Test uses Engine (OMENative) + Router (Raw) and groups them
	// together — Router's mode mismatch isn't caught by the
	// engine+decoder symmetry check (Router isn't paired), so the
	// CoordinationRequiresOMENative validator is the one that fires.
	ginkgo.It("W-4 rejects Component in group not declared OMENative → CoordinationRequiresOMENative", func() {
		rawRouter := &v1beta1.RouterSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)},
			},
			PodSpec: v1beta1.PodSpec{Containers: coordRunner()},
		}
		isvc := v1beta1testing.MakeInferenceService("coord-w4", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Router(rawRouter).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			}},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonCoordinationRequiresOMENative))
	})

	// W-5: a group is meaningless without at least one Component.
	// CRD MinItems=1 catches this at the schema layer.
	ginkgo.It("W-5 rejects empty components in a group", func() {
		isvc := coordISVC("coord-w5", v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(), "CRD MinItems=1 or webhook should reject")
	})

	// W-6: there is no Sequential policy — cross-Component sequencing is
	// expressed as multiple single-Component groups in list order, so there
	// is no "Sequential requires order" rejection to assert. Skipped.
	ginkgo.It("W-6 rejects Sequential without order → SequentialRequiresOrder", func() {
		ginkgo.Skip("no Sequential policy: sequencing is group list order, so there is no SequentialRequiresOrder check")
	})

	// W-7: a single-Component group is a valid (degenerate) shape; there
	// is no SequentialRequiresMultipleComponents rejection. Skipped.
	ginkgo.It("W-7 rejects Sequential with 1 Component → SequentialRequiresMultipleComponents", func() {
		ginkgo.Skip("a single-Component group is valid; there is no SequentialRequiresMultipleComponents check")
	})

	// W-8: Order is a surge sequence and may be a partial subset of
	// components; there is no SequentialIncompleteOrder rejection. Skipped.
	ginkgo.It("W-8 rejects Sequential order missing one Component → SequentialIncompleteOrder", func() {
		ginkgo.Skip("Order is an optional surge subset; there is no SequentialIncompleteOrder check")
	})

	// W-9: an Order entry not in components[] is a typo. The
	// order⊆components rule (validateOrderSubset) applies to any group with
	// an Order, independent of the progression. It is enforced at BOTH layers — the
	// CRD CEL rule and the webhook (InvalidCoordinationOrder). Against a real
	// apiserver the CEL rule fires first, so the create is rejected with the CEL
	// message; the webhook reason is exercised by the validation unit test.
	ginkgo.It("W-9 rejects order with a Component not in components", func() {
		isvc := coordISVC("coord-w9", v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("order must reference only Components listed in this group"))
	})

	// W-10: the rollingUpdate progression is valid on a single-Component
	// group, so there is no RollingUpdateRequiresMultipleComponents
	// rejection. Skipped.
	ginkgo.It("W-10 rejects RollingUpdate with 1 Component → RollingUpdateRequiresMultipleComponents", func() {
		ginkgo.Skip("single-Component rollingUpdate group is valid, no RollingUpdateRequiresMultipleComponents check")
	})

	// W-11: MaintainRatio.Tolerance above the warning threshold (50%)
	// effectively disables ratio enforcement. The validator allows it
	// but surfaces a CoordinationRatioToleranceWarning the operator
	// can grep on. Exercised via the helper directly (the apiserver's
	// AdmissionResponse warnings aren't surfaced through controller-
	// runtime's typed Create error).
	ginkgo.It("W-11 emits warning when MaintainRatio.Tolerance > 50", func() {
		spec := &v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					BlueGreen:     &v1beta1.GroupBlueGreen{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(75))},
				}},
			},
		}
		warning := validation.CoordinationRatioToleranceWarning(spec)
		Expect(warning).To(ContainSubstring(validation.ReasonRatioToleranceTooHigh))
		Expect(warning).To(ContainSubstring("75"))
	})

	// W-14: enum strings on a ComponentType reject canonical misspells.
	// CRD validation enum is closed; capital "Engine" is not allowed.
	ginkgo.It("W-14 rejects Component name with wrong case (\"Engine\" capital)", func() {
		isvc := coordISVC("coord-w14", v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{"Engine"},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD ComponentType enum is lowercase-only; \"Engine\" must be rejected")
	})

	// W-15: duplicate Order entries — without dedupe,
	// Components=[engine,decoder] + Order=[engine,engine] would let
	// engine surge twice while decoder never surges at all. validator
	// refuses it up front via validateOrderSubset (SequentialDuplicateOrder).
	ginkgo.It("W-15 rejects order with duplicate Component → SequentialDuplicateOrder", func() {
		isvc := coordISVC("coord-w15", v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.EngineComponent},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonSequentialDuplicateOrder),
			"validator must reject duplicate Order entries with SequentialDuplicateOrder")
	})

	// W-16: explicit empty groups[] (not nil) is a valid declaration —
	// the operator told the system they considered coordination and
	// chose none. No-op for the controller.
	ginkgo.It("W-16 accepts rollout block with empty groups", func() {
		isvc := coordISVC("coord-w16")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{}}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-17: CRD validation bounds MaintainRatio.Tolerance to [0, 100]. Below 0
	// is rejected at the schema layer.
	ginkgo.It("W-17 rejects MaintainRatio.Tolerance < 0 (CRD schema)", func() {
		isvc := coordISVC("coord-w17", v1beta1.RolloutGroup{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:     &v1beta1.GroupBlueGreen{},
			MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(-5))},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD Minimum=0 must reject negative maintainRatio.tolerance")
	})

	// W-18: CRD Maximum=100 — anything above is schema-rejected.
	ginkgo.It("W-18 rejects MaintainRatio.Tolerance > 100 (CRD schema)", func() {
		isvc := coordISVC("coord-w18", v1beta1.RolloutGroup{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:     &v1beta1.GroupBlueGreen{},
			MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(150))},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD Maximum=100 must reject over-100 maintainRatio.tolerance")
	})

	// W-19: IntOrString's CRD validation is liberal — a garbage string
	// like "abc" passes the JSON-schema layer, and the runtime resolver
	// would collapse it to a zero budget. The webhook's semantic check
	// rejects it with the exact field path instead.
	ginkgo.It("W-19 rejects malformed MaxSurge string (webhook semantic check)", func() {
		bad := intstr.FromString("abc")
		isvc := coordISVC("coord-w19", v1beta1.RolloutGroup{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{MaxSurge: &bad},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"webhook must reject a maxSurge string the runtime resolver would turn into a zero budget")
		Expect(err.Error()).To(ContainSubstring("InvalidRollingUpdatePercent"))
		Expect(err.Error()).To(ContainSubstring("spec.rollout.groups[0].rollingUpdate.maxSurge"))
	})

	// W-20: a single 3-Component BlueGreen group is the canonical PD
	// shape (router + engine + decoder rolling as one unit).
	ginkgo.It("W-20 accepts BlueGreen group with all 3 Components", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w20", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Decoder(omeNativeDecoder()).
			Router(omeNativeRouter()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{
					v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent,
				},
				BlueGreen: &v1beta1.GroupBlueGreen{},
			}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-21: BlueGreen with 1 Component is degenerate (no peer to gang
	// with) but not explicitly rejected. Pins current behavior — if
	// validation later tightens this, the test changes.
	ginkgo.It("W-21 accepts BlueGreen with 1 Component (degenerate)", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w21", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-22: orphan group — a Component listed in groups[] that isn't
	// declared on the ISVC spec. Operator misclick; reject early.
	ginkgo.It("W-22 rejects group referring to Component not declared on ISVC → OrphanCoordinationGroup", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w22", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			}},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonOrphanCoordinationGroup))
	})

	// W-22a: partial membership — one group member is declared (engine), the
	// other (decoder) is not. The group must be rejected member-by-member, and
	// the error must name the group index and the missing Component so the
	// operator can fix the exact gap.
	ginkgo.It("W-22a rejects group whose members are only partially declared → OrphanCoordinationGroup", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w22a", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			}},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonOrphanCoordinationGroup))
		Expect(err.Error()).To(ContainSubstring("groups[0]"))
		Expect(err.Error()).To(ContainSubstring("decoder"))
	})

	// W-18a: maintainRatio with the tolerance omitted is a valid spelling —
	// the CRD leaves the field optional, and the operator-configured
	// default applies at resolution.
	ginkgo.It("W-18a accepts maintainRatio with tolerance omitted (CRD optional)", func() {
		isvc := coordISVC("coord-w18a", v1beta1.RolloutGroup{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:     &v1beta1.GroupBlueGreen{},
			MaintainRatio: &v1beta1.MaintainRatio{},
		})
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-18b: explicit tolerance 0 is a valid, distinct request (zero drift),
	// not a stand-in for "use the default".
	ginkgo.It("W-18b accepts explicit maintainRatio.tolerance=0", func() {
		isvc := coordISVC("coord-w18b", v1beta1.RolloutGroup{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:     &v1beta1.GroupBlueGreen{},
			MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(0))},
		})
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx,
			client.ObjectKey{Namespace: isvc.Namespace, Name: isvc.Name}, got)).To(Succeed())
		Expect(got.Spec.Rollout.Groups[0].MaintainRatio.Tolerance).NotTo(BeNil(),
			"explicit 0 must round-trip as a set field, distinguishable from omitted")
		Expect(*got.Spec.Rollout.Groups[0].MaintainRatio.Tolerance).To(Equal(int32(0)))
	})

	// W-23: a group with NO progression set (no canary/blueGreen/rollingUpdate)
	// is accepted and means blueGreen — the resolve layer's default. This is the
	// ergonomic spelling for "just roll this Component": the operator omits the
	// progression instead of writing the empty `blueGreen: {}` selector. The
	// progression one-of is at-most-one, not exactly-one.
	ginkgo.It("W-23 accepts a group with no progression (defaults to blueGreen)", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w23", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-24: the canonical "roll decoder, then engine" sequence, spelled as two
	// single-Component groups with NO progression on either. Each defaults to
	// blueGreen and the resolve layer collapses the run into the Sequential state
	// machine — the order reads straight off the list, no progression ceremony.
	ginkgo.It("W-24 accepts no-progression single-Component groups (sequential spelling)", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w24", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Decoder(omeNativeDecoder()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{
				{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
				{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-25: a canary step capacity the runtime resolver cannot parse would
	// silently stage zero new pods while the step sends it traffic. Admission
	// rejects it, naming the exact steps[i].capacity field path.
	ginkgo.It("W-25 rejects a canary step with malformed capacity → CanaryInvalid", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w25", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("abc"), Traffic: 10},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonCanaryInvalid))
		Expect(err.Error()).To(ContainSubstring("spec.rollout.groups[0].canary.steps[0].capacity"))
	})

	// W-26: the well-formed percent canary plan keeps admitting — strict
	// capacity parsing must not reject the forms the resolver handles.
	ginkgo.It("W-26 accepts a canary plan with percent capacities", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w26", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("25%"), Traffic: 10},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-27: a mixed-progression multi-group list promises "decoder completes
	// before engine begins", but only a pure run of single-Component blueGreen
	// groups is sequenced — the mix would run concurrently, so admission
	// rejects it instead of accepting an order it does not enforce.
	ginkgo.It("W-27 rejects mixed-progression multi-group list → GroupOrderingNotHonored", func() {
		isvc := coordISVC("coord-w27",
			v1beta1.RolloutGroup{
				Components:    []v1beta1.ComponentType{v1beta1.DecoderComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{},
			},
			v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
		)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonGroupOrderingNotHonored))
	})

	// W-28: a well-formed (dup-free subset) Order still names a sequence no
	// progression applies — the group's Components advance together — so
	// admission rejects it rather than let the operator believe decoder rolls
	// first.
	ginkgo.It("W-28 rejects group with order set → OrderNotHonored", func() {
		isvc := coordISVC("coord-w28", v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			Order:      []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonOrderNotHonored))
	})
})

// ---------- UPDATE-path specs ----------
//
// The webhook's ValidateUpdate path runs validation.ValidateCoordinationUpdate
// (RollingUpdate lockstep) + validation.ValidateCoordination (the
// CREATE-time structural rules, re-applied so update can't escape
// them). These tests prove both halves wire correctly under real
// apiserver Update calls.
//
// Pattern: create a baseline ISVC via Create, then mutate + Update.
// Each test factors out the create step into a small helper so the
// assertion focuses on the update's accept/reject decision.

// pmGroup builds a RollingUpdate group over engine + decoder.
func pmGroup() v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{},
	}
}

// atomicGroup builds a BlueGreen group over engine + decoder.
func atomicGroup() v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	}
}

// createBaselineISVC creates an ISVC named `name` carrying `groups` and
// returns the apiserver's freshly-read copy. Caller mutates this copy
// then calls k8sClient.Update.
func createBaselineISVC(name string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceService {
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

// withEngineRunner attaches a Runner block carrying the given image. The
// validator's RollingUpdate lockstep check compares the whole
// revision-affecting Component view — Runner, PodSpec, leader/worker
// overrides, pod-template labels/annotations — so a Runner-side image bump
// is one of several surfaces the update tests exercise.
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

var _ = ginkgo.Describe("InferenceService webhook validation (UPDATE) — spec.rollout.groups[]", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(setupCoordCBMandCSR)

	ginkgo.BeforeEach(ensureNamespace)

	// WU-1: RollingUpdate lockstep is the marquee update rule. Bump
	// engine Runner.Image WITHOUT bumping decoder → rejected with
	// the dedicated lockstep reason. The test seeds Runner on both
	// Components at v1 then mutates only engine to v2.
	ginkgo.It("WU-1 rejects RollingUpdate image bump on one Component only → RollingUpdateLockstepViolation", func() {
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
		Expect(err.Error()).To(ContainSubstring(validation.ReasonRollingUpdateLockstepViolation))
	})

	// WU-1a: RollingUpdate lockstep catches image bumps via either
	// Runner.Image OR PodSpec.Containers[0].Image — both surfaces
	// produce the same lockstep signal so users can't accidentally
	// escape the validator by setting the image via PodSpec.
	ginkgo.It("WU-1a catches RollingUpdate bump via PodSpec.Containers[0].Image", func() {
		got := createBaselineISVC("coord-wu1a", pmGroup())
		got.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonRollingUpdateLockstepViolation))
	})

	// WU-1b: lockstep is not an image-string compare — ANY
	// revision-affecting change to one grouped Component (here an
	// env-only bump with both images unchanged) re-renders that
	// Component's pod template and must trip the same rejection.
	ginkgo.It("WU-1b rejects RollingUpdate env-only bump on one Component", func() {
		got := createBaselineISVC("coord-wu1b", pmGroup())
		got.Spec.Engine.PodSpec.Containers[0].Env = append(
			got.Spec.Engine.PodSpec.Containers[0].Env,
			corev1.EnvVar{Name: "CONFIG_VERSION", Value: "2"},
		)
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonRollingUpdateLockstepViolation))
	})

	// WU-1c: the same env bump applied to BOTH grouped Components
	// satisfies lockstep and is admitted.
	ginkgo.It("WU-1c accepts RollingUpdate env bump on both Components", func() {
		got := createBaselineISVC("coord-wu1c", pmGroup())
		got.Spec.Engine.PodSpec.Containers[0].Env = append(
			got.Spec.Engine.PodSpec.Containers[0].Env,
			corev1.EnvVar{Name: "CONFIG_VERSION", Value: "2"},
		)
		got.Spec.Decoder.PodSpec.Containers[0].Env = append(
			got.Spec.Decoder.PodSpec.Containers[0].Env,
			corev1.EnvVar{Name: "CONFIG_VERSION", Value: "2"},
		)
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-2: BlueGreen has no lockstep restriction. Both Components can
	// bump together, one at a time, or stay still — all accepted.
	ginkgo.It("WU-2 accepts BlueGreen image bump on both Components together", func() {
		got := createBaselineISVC("coord-wu2", atomicGroup())
		got.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		got.Spec.Decoder.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-3: the old "switch a group's Policy to Independent
	// mid-rollout" is now "drop the group entirely" — a Component in no
	// group rolls independently. The structural validator just re-runs
	// the shape rules on the new spec (here: no groups, trivially valid).
	// Pin observed behavior.
	ginkgo.It("WU-3 accepts dropping a group (BlueGreen → independent) mid-rollout", func() {
		got := createBaselineISVC("coord-wu3", atomicGroup())
		got.Spec.Rollout.Groups = nil
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-4: Adding a brand-new group to an existing ISVC is allowed
	// (assuming it passes the structural rules).
	ginkgo.It("WU-4 accepts adding a new group to an existing ISVC", func() {
		got := createBaselineISVC("coord-wu4") // no groups initially
		got.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{atomicGroup()},
		}
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-5: Removing the whole rollout block mid-rollout is allowed —
	// rollout grouping is opt-in, so opting out is symmetric.
	ginkgo.It("WU-5 accepts removing the rollout block entirely", func() {
		got := createBaselineISVC("coord-wu5", atomicGroup())
		got.Spec.Rollout = nil
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-6: the old "swap Sequential Order mid-rollout" is
	// now "reorder the single-Component groups in the list" — sequencing
	// is group list order in v2. The structural validator re-runs and
	// accepts any valid new shape; runtime behavior at the controller is
	// a separate concern.
	ginkgo.It("WU-6 accepts reordering single-Component groups mid-rollout", func() {
		engGroup := v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		}
		decGroup := v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		}
		got := createBaselineISVC("coord-wu6", engGroup, decGroup)
		// Swap the group order: decoder rolls before engine now.
		got.Spec.Rollout.Groups = []v1beta1.RolloutGroup{decGroup, engGroup}
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{atomicGroup()},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: "isvc-webhook", Name: "coord-wu7"}, got)).To(Succeed())

		got.Spec.Rollout.Groups[0].Components = append(
			got.Spec.Rollout.Groups[0].Components, v1beta1.RouterComponent)
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-8: Removing a Component from an existing group accepted; the
	// removed Component then rolls independently.
	ginkgo.It("WU-8 accepts removing a Component from an existing group", func() {
		got := createBaselineISVC("coord-wu8", atomicGroup())
		got.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
		// 1-Component BlueGreen is a degenerate but accepted shape (W-21).
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-9: a nil Runner does not exempt a Component from lockstep. The
	// compare covers the whole revision-affecting view, so adding a
	// sidecar container to ONE grouped Component's PodSpec is a one-sided
	// revision bump and is rejected even though no Runner block exists on
	// either side.
	ginkgo.It("WU-9 rejects one-sided PodSpec change with nil Runner on both sides", func() {
		got := createBaselineISVC("coord-wu9", pmGroup())
		got.Spec.Engine.PodSpec.Containers = append(got.Spec.Engine.PodSpec.Containers, corev1.Container{
			Name: "sidecar", Image: "ghcr.io/test/sidecar:v1",
		})
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonRollingUpdateLockstepViolation))
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
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

	// WU-11: the rollout-ordering rules ratchet on update — they fire only
	// when the write changes spec.rollout. An update that rewrites a valid
	// sequenced run into a mixed-progression list is exactly such a write and
	// must be rejected; the sequenced baseline itself was admitted at create.
	// (The grandfathering half — an untouched unenforced rollout passing —
	// is proven at the unit level: admission rejects unenforced shapes at
	// create, so no such stored baseline can be produced in this suite.)
	ginkgo.It("WU-11 rejects update that changes rollout into a mixed-progression list → GroupOrderingNotHonored", func() {
		got := createBaselineISVC("coord-wu11",
			v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
			v1beta1.RolloutGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				BlueGreen:  &v1beta1.GroupBlueGreen{},
			},
		)
		got.Spec.Rollout.Groups[0].RollingUpdate = &v1beta1.GroupRollingUpdate{}
		got.Spec.Rollout.Groups[0].BlueGreen = nil
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonGroupOrderingNotHonored))
	})
})

// Sanity-check the validation reason strings are stable. A constant
// rename without test breakage would silently change the operator-
// facing error grep target.
var _ = ginkgo.Describe("validation reason constant stability", func() {
	ginkgo.It("reason constants render the documented strings", func() {
		Expect(validation.ReasonDuplicateComponentInCoordinationGroups).To(Equal("DuplicateComponentInCoordinationGroups"))
		Expect(validation.ReasonInvalidComponentInCoordinationGroup).To(Equal("InvalidComponentInCoordinationGroup"))
		Expect(validation.ReasonCoordinationRequiresOMENative).To(Equal("CoordinationRequiresOMENative"))
		Expect(validation.ReasonRatioToleranceTooHigh).To(Equal("RatioToleranceTooHigh"))
		Expect(validation.ReasonRatioToleranceOutOfRange).To(Equal("RatioToleranceOutOfRange"))
		Expect(validation.ReasonOrphanCoordinationGroup).To(Equal("OrphanCoordinationGroup"))
		Expect(validation.ReasonInvalidCoordinationOrder).To(Equal("InvalidCoordinationOrder"))
		Expect(validation.ReasonSequentialDuplicateOrder).To(Equal("SequentialDuplicateOrder"))
		Expect(validation.ReasonGroupOrderingNotHonored).To(Equal("GroupOrderingNotHonored"))
		Expect(validation.ReasonOrderNotHonored).To(Equal("OrderNotHonored"))
	})

	// Defensive: the assertion-helper format strings reference the
	// "groups[<idx>]" position so operators can locate the offender.
	// Pin one shape so a refactor doesn't silently drop the index.
	ginkgo.It("error messages include the offending group index", func() {
		spec := &v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{"predictor"},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		}
		err := validation.ValidateCoordination(spec)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("groups[0]"),
			fmt.Sprintf("error should reference groups[0]: got %q", err.Error()))
	})
})
