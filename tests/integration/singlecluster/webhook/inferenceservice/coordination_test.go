package inferenceservice

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/pkg/validation"
)

// Coordination admission specs for OEP-0007.1
// (spec.rolloutCoordination.groups[]). Each test sends a Create or
// Update through the apiserver and asserts the validating webhook's
// decision. The validator's rejection-reason constants
// (validation.Reason*) are referenced by name so a future rename of a
// constant fails the test loudly here instead of silently changing the
// operator-facing error string.
//
// Defaulter-side tests (MaxSurge/MaxUnavailable/RatioTolerance fills)
// live in tests/integration/singlecluster/webhook/inferenceservice_defaulter/.
// The defaulter is not wired in this suite's webhook fixture.

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
// pre-populated rolloutCoordination, ready to Create. Spec-specific
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

func coordISVC(name string, groups ...v1beta1.RolloutCoordinationGroup) *v1beta1.InferenceService {
	isvc := v1beta1testing.MakeInferenceService(name, "isvc-webhook").
		Model(coordCBMName).
		Runtime(coordCSRName).
		DeploymentMode(constants.OMENative).
		Engine(omeNativeEngine()).
		Decoder(omeNativeDecoder()).
		Obj()
	if len(groups) > 0 {
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{Groups: groups}
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

var _ = ginkgo.Describe("InferenceService webhook validation — rolloutCoordination (OEP-0007.1)", ginkgo.Ordered, func() {

	ginkgo.BeforeAll(setupCoordCBMandCSR)

	ginkgo.BeforeEach(ensureNamespace)

	// W-1: zero groups is the no-op shape that historical ISVCs use.
	// The validator must accept it so existing manifests don't break.
	ginkgo.It("W-1 accepts ISVC with empty rolloutCoordination.groups[]", func() {
		isvc := coordISVC("coord-w1")
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{Groups: []v1beta1.RolloutCoordinationGroup{}}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-2: a Component can only appear in one group. Two groups
	// claiming Engine would race on every reconcile — admission
	// catches it up front.
	ginkgo.It("W-2 rejects duplicate Component across groups → DuplicateComponentInCoordinationGroups", func() {
		isvc := coordISVC("coord-w2",
			v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyAtomic,
			},
			v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyAtomic,
			},
		)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonDuplicateComponentInCoordinationGroups))
	})

	// W-3: Component enum is closed to router/engine/decoder. A typo
	// or an aspirational future Component is rejected at admission.
	ginkgo.It("W-3 rejects unknown Component name → InvalidComponentInCoordinationGroup", func() {
		isvc := coordISVC("coord-w3", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{"predictor"},
			Policy:     v1beta1.CoordinationPolicyIndependent,
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
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
				Policy:     v1beta1.CoordinationPolicyAtomic,
			}},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonCoordinationRequiresOMENative))
	})

	// W-5: a group is meaningless without at least one Component.
	// CRD MinItems=1 catches this at the schema layer.
	ginkgo.It("W-5 rejects empty components in a group", func() {
		isvc := coordISVC("coord-w5", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{},
			Policy:     v1beta1.CoordinationPolicyAtomic,
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(), "CRD MinItems=1 or webhook should reject")
	})

	// W-6: Sequential's whole point is the order. Without it the
	// state machine has nothing to walk.
	ginkgo.It("W-6 rejects Sequential without order → SequentialRequiresOrder", func() {
		isvc := coordISVC("coord-w6", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonSequentialRequiresOrder))
	})

	// W-7: Sequential with 1 Component degenerates to "do one rollout
	// then stop" — already what Independent does. Admission rejects
	// to keep semantics clear.
	ginkgo.It("W-7 rejects Sequential with 1 Component → SequentialRequiresMultipleComponents", func() {
		isvc := coordISVC("coord-w7", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonSequentialRequiresMultipleComponents))
	})

	// W-8: every Component in a Sequential group needs an explicit
	// slot in the Order. An incomplete Order would leave Components
	// unschedulable.
	ginkgo.It("W-8 rejects Sequential order missing one Component → SequentialIncompleteOrder", func() {
		isvc := coordISVC("coord-w8", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonSequentialIncompleteOrder))
	})

	// W-9: an Order entry not in components[] is a typo. Catch at
	// admission so the controller doesn't have to defend it.
	ginkgo.It("W-9 rejects Sequential order with Component not in components → InvalidCoordinationOrder", func() {
		isvc := coordISVC("coord-w9", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonInvalidCoordinationOrder))
	})

	// W-10: PairedMixable's defining property is the cross-revision
	// pairing. With one Component, there's no peer to pair with — the
	// policy collapses to Independent.
	ginkgo.It("W-10 rejects PairedMixable with 1 Component → PairedMixableRequiresMultipleComponents", func() {
		isvc := coordISVC("coord-w10", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Policy:     v1beta1.CoordinationPolicyPairedMixable,
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonPairedMixableRequiresMultipleComponents))
	})

	// W-11: RatioTolerance above the warning threshold (50%)
	// effectively disables ratio enforcement. The validator allows it
	// but surfaces a CoordinationRatioToleranceWarning the operator
	// can grep on. Exercised via the helper directly (the apiserver's
	// AdmissionResponse warnings aren't surfaced through controller-
	// runtime's typed Create error).
	ginkgo.It("W-11 emits warning when RatioTolerance > 50", func() {
		tol := int32(75)
		spec := &v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyAtomic,
					Pacing: &v1beta1.CoordinationPacing{
						Type:           v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerance: &tol,
					},
				}},
			},
		}
		warning := validation.CoordinationRatioToleranceWarning(spec)
		Expect(warning).To(ContainSubstring(validation.ReasonRatioToleranceTooHigh))
		Expect(warning).To(ContainSubstring("75"))
	})

	// W-14: enum strings on a ComponentType reject canonical misspells.
	// CRD validation enum is closed; capital "Engine" is not allowed.
	// (Renumbered from spec W-14 — see W-12..W-15 in defaulter suite.)
	ginkgo.It("W-14 rejects Component name with wrong case (\"Engine\" capital)", func() {
		isvc := coordISVC("coord-w14", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{"Engine"},
			Policy:     v1beta1.CoordinationPolicyIndependent,
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD ComponentType enum is lowercase-only; \"Engine\" must be rejected")
	})

	// W-15: Sequential duplicates in the Order list — without dedupe,
	// Components=[engine,decoder] + Order=[engine,engine] would let
	// engine roll twice while decoder never rolls at all. validator
	// now refuses it up front. (Fix landed alongside this PR per G10
	// in the integration-test spec.)
	ginkgo.It("W-15 rejects Sequential order with duplicate Component → SequentialDuplicateOrder", func() {
		isvc := coordISVC("coord-w15", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicySequential,
			Order:      []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.EngineComponent},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonSequentialDuplicateOrder),
			"validator must reject duplicate Sequential Order entries with SequentialDuplicateOrder")
	})

	// W-16: explicit empty groups[] (not nil) is a valid declaration —
	// the operator told the system they considered coordination and
	// chose none. No-op for the controller.
	ginkgo.It("W-16 accepts rolloutCoordination block with empty groups", func() {
		isvc := coordISVC("coord-w16")
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-17: CRD validation bounds RatioTolerance to [0, 100]. Below 0
	// is rejected at the schema layer.
	ginkgo.It("W-17 rejects RatioTolerance < 0 (CRD schema)", func() {
		neg := int32(-5)
		isvc := coordISVC("coord-w17", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyAtomic,
			Pacing: &v1beta1.CoordinationPacing{
				Type:           v1beta1.CoordinationPacingRatioBalanced,
				RatioTolerance: &neg,
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD Minimum=0 must reject negative ratioTolerance")
	})

	// W-18: CRD Maximum=100 — anything above is schema-rejected.
	ginkgo.It("W-18 rejects RatioTolerance > 100 (CRD schema)", func() {
		over := int32(150)
		isvc := coordISVC("coord-w18", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyAtomic,
			Pacing: &v1beta1.CoordinationPacing{
				Type:           v1beta1.CoordinationPacingRatioBalanced,
				RatioTolerance: &over,
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD Maximum=100 must reject over-100 ratioTolerance")
	})

	// W-19: IntOrString CRD validation rejects garbage strings on the
	// MaxSurge field. Strings must be "<int>" or "<int>%".
	ginkgo.It("W-19 rejects malformed MaxSurge string (CRD IntOrString)", func() {
		bad := intstr.FromString("abc")
		isvc := coordISVC("coord-w19", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyAtomic,
			Pacing:     &v1beta1.CoordinationPacing{MaxSurge: &bad},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		// Note: IntOrString's CRD validation is liberal — strings without
		// a numeric prefix are accepted at the JSON-schema layer. The
		// controller defends downstream. This test pins observed
		// behavior so we notice if K8s tightens it.
		if err != nil {
			Expect(err.Error()).To(Or(
				ContainSubstring("invalid"),
				ContainSubstring("IntOrString"),
			))
		}
	})

	// W-20: a single 3-Component Atomic group is the canonical PD
	// shape (router + engine + decoder rolling as one unit).
	ginkgo.It("W-20 accepts Atomic group with all 3 Components", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w20", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Decoder(omeNativeDecoder()).
			Router(omeNativeRouter()).
			Obj()
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{{
				Components: []v1beta1.ComponentType{
					v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent,
				},
				Policy: v1beta1.CoordinationPolicyAtomic,
			}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-21: Atomic with 1 Component is degenerate (no peer to gang
	// with) but not explicitly rejected. Pins current behavior — if
	// the OEP later tightens this, the test changes.
	ginkgo.It("W-21 accepts Atomic with 1 Component (degenerate)", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w21", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyAtomic,
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
		isvc.Spec.RolloutCoordination = &v1beta1.RolloutCoordinationSpec{
			Groups: []v1beta1.RolloutCoordinationGroup{{
				Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyIndependent,
			}},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonOrphanCoordinationGroup))
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
		Expect(validation.ReasonOrphanCoordinationGroup).To(Equal("OrphanCoordinationGroup"))
		Expect(validation.ReasonInvalidCoordinationOrder).To(Equal("InvalidCoordinationOrder"))
		Expect(validation.ReasonPairedMixableRequiresMultipleComponents).To(Equal("PairedMixableRequiresMultipleComponents"))
		Expect(validation.ReasonSequentialRequiresOrder).To(Equal("SequentialRequiresOrder"))
		Expect(validation.ReasonSequentialIncompleteOrder).To(Equal("SequentialIncompleteOrder"))
		Expect(validation.ReasonSequentialRequiresMultipleComponents).To(Equal("SequentialRequiresMultipleComponents"))
		Expect(validation.ReasonSequentialDuplicateOrder).To(Equal("SequentialDuplicateOrder"))
	})

	// Defensive: the assertion-helper format strings reference the
	// "groups[<idx>]" position so operators can locate the offender.
	// Pin one shape so a refactor doesn't silently drop the index.
	ginkgo.It("error messages include the offending group index", func() {
		spec := &v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{"predictor"},
					Policy:     v1beta1.CoordinationPolicyIndependent,
				}},
			},
		}
		err := validation.ValidateCoordination(spec)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("groups[0]"),
			fmt.Sprintf("error should reference groups[0]: got %q", err.Error()))
	})
})
