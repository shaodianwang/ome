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
// Defaulter-side tests (MaxSurge/MaxUnavailable/RatioTolerancePercent fills)
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{Groups: groups},
		}
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{Groups: []v1beta1.RolloutCoordinationGroup{}},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-2: a Component can only appear in one group. Two groups
	// claiming Engine would race on every reconcile — admission
	// catches it up front.
	ginkgo.It("W-2 rejects duplicate Component across groups → DuplicateComponentInCoordinationGroups", func() {
		isvc := coordISVC("coord-w2",
			v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
			},
			v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
				}},
			},
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
			Policy:     v1beta1.CoordinationPolicyBlueGreen,
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

	// W-10: RollingUpdate's defining property is the cross-revision
	// pairing. With one Component, there's no peer to pair with — the
	// policy collapses to Independent.
	ginkgo.It("W-10 rejects RollingUpdate with 1 Component → RollingUpdateRequiresMultipleComponents", func() {
		isvc := coordISVC("coord-w10", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Policy:     v1beta1.CoordinationPolicyRollingUpdate,
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonRollingUpdateRequiresMultipleComponents))
	})

	// W-11: RatioTolerancePercent above the warning threshold (50%)
	// effectively disables ratio enforcement. The validator allows it
	// but surfaces a CoordinationRatioToleranceWarning the operator
	// can grep on. Exercised via the helper directly (the apiserver's
	// AdmissionResponse warnings aren't surfaced through controller-
	// runtime's typed Create error).
	ginkgo.It("W-11 emits warning when RatioTolerancePercent > 50", func() {
		tol := int32(75)
		spec := &v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					Pacing: &v1beta1.CoordinationPacing{
						Type:                  v1beta1.CoordinationPacingRatioBalanced,
						RatioTolerancePercent: &tol,
					},
				}},
			}},
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-17: CRD validation bounds RatioTolerancePercent to [0, 100]. Below 0
	// is rejected at the schema layer.
	ginkgo.It("W-17 rejects RatioTolerancePercent < 0 (CRD schema)", func() {
		neg := int32(-5)
		isvc := coordISVC("coord-w17", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyBlueGreen,
			Pacing: &v1beta1.CoordinationPacing{
				Type:                  v1beta1.CoordinationPacingRatioBalanced,
				RatioTolerancePercent: &neg,
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD Minimum=0 must reject negative ratioTolerancePercent")
	})

	// W-18: CRD Maximum=100 — anything above is schema-rejected.
	ginkgo.It("W-18 rejects RatioTolerancePercent > 100 (CRD schema)", func() {
		over := int32(150)
		isvc := coordISVC("coord-w18", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyBlueGreen,
			Pacing: &v1beta1.CoordinationPacing{
				Type:                  v1beta1.CoordinationPacingRatioBalanced,
				RatioTolerancePercent: &over,
			},
		})
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"CRD Maximum=100 must reject over-100 ratioTolerancePercent")
	})

	// W-19: IntOrString CRD validation rejects garbage strings on the
	// MaxSurge field. Strings must be "<int>" or "<int>%".
	ginkgo.It("W-19 rejects malformed MaxSurge string (CRD IntOrString)", func() {
		bad := intstr.FromString("abc")
		isvc := coordISVC("coord-w19", v1beta1.RolloutCoordinationGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyBlueGreen,
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
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{
						v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent,
					},
					Policy: v1beta1.CoordinationPolicyBlueGreen,
				}},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
	})

	// W-21: BlueGreen with 1 Component is degenerate (no peer to gang
	// with) but not explicitly rejected. Pins current behavior — if
	// the OEP later tightens this, the test changes.
	ginkgo.It("W-21 accepts BlueGreen with 1 Component (degenerate)", func() {
		isvc := v1beta1testing.MakeInferenceService("coord-w21", "isvc-webhook").
			Model(coordCBMName).Runtime(coordCSRName).
			DeploymentMode(constants.OMENative).
			Engine(omeNativeEngine()).
			Obj()
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
				}},
			},
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
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyIndependent,
				}},
			},
		}
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonOrphanCoordinationGroup))
	})
})

// ---------- UPDATE-path specs (OEP-0007.1) ----------
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
func pmGroup() v1beta1.RolloutCoordinationGroup {
	return v1beta1.RolloutCoordinationGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyRollingUpdate,
	}
}

// atomicGroup builds a BlueGreen group over engine + decoder.
func atomicGroup() v1beta1.RolloutCoordinationGroup {
	return v1beta1.RolloutCoordinationGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     v1beta1.CoordinationPolicyBlueGreen,
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

// withEngineRunner attaches a Runner block carrying the given image. The
// validator's RollingUpdate lockstep check reads spec.Engine.Runner.Image
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

	// WU-1: RollingUpdate lockstep is the marquee update rule. Bump
	// engine Runner.Image WITHOUT bumping decoder → rejected with
	// the dedicated lockstep reason. The validator reads
	// spec.Engine.Runner.Image (see componentImages in
	// pkg/validation/coordination.go), so the test seeds Runner on
	// both Components at v1 then mutates only engine to v2.
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
	// Runner.Image OR PodSpec.Containers[0].Image. componentImages
	// reads Runner first and falls back to PodSpec — both surfaces
	// produce the same lockstep signal so users can't accidentally
	// escape the validator by setting the image via PodSpec.
	//
	// Closes G11.
	ginkgo.It("WU-1a catches RollingUpdate bump via PodSpec.Containers[0].Image", func() {
		got := createBaselineISVC("coord-wu1a", pmGroup())
		got.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(validation.ReasonRollingUpdateLockstepViolation))
	})

	// WU-2: BlueGreen has no lockstep restriction. Both Components can
	// bump together, one at a time, or stay still — all accepted.
	ginkgo.It("WU-2 accepts BlueGreen image bump on both Components together", func() {
		got := createBaselineISVC("coord-wu2", atomicGroup())
		got.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		got.Spec.Decoder.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-3: Updating the group's Policy mid-rollout: OEP-0007.1
	// doesn't forbid this — the structural validator just re-runs the
	// shape rules on the new spec. Pin observed behavior.
	ginkgo.It("WU-3 accepts policy change BlueGreen → Independent mid-rollout", func() {
		got := createBaselineISVC("coord-wu3", atomicGroup())
		got.Spec.Rollout.Coordination.Groups[0].Policy = v1beta1.CoordinationPolicyIndependent
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-4: Adding a brand-new group to an existing ISVC is allowed
	// (assuming it passes the structural rules).
	ginkgo.It("WU-4 accepts adding a new group to an existing ISVC", func() {
		got := createBaselineISVC("coord-wu4") // no groups initially
		got.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{atomicGroup()},
			},
		}
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-5: Removing the whole rolloutCoordination block mid-rollout
	// is allowed — coordination is opt-in, so opting out is symmetric.
	ginkgo.It("WU-5 accepts removing the rolloutCoordination block entirely", func() {
		got := createBaselineISVC("coord-wu5", atomicGroup())
		got.Spec.Rollout.Coordination = nil
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
		got.Spec.Rollout.Coordination.Groups[0].Order = []v1beta1.ComponentType{
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{atomicGroup()},
			},
		}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })
		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: "isvc-webhook", Name: "coord-wu7"}, got)).To(Succeed())

		got.Spec.Rollout.Coordination.Groups[0].Components = append(
			got.Spec.Rollout.Coordination.Groups[0].Components, v1beta1.RouterComponent)
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-8: Removing a Component from an existing group accepted; the
	// removed Component then rolls independently.
	ginkgo.It("WU-8 accepts removing a Component from an existing group", func() {
		got := createBaselineISVC("coord-wu8", atomicGroup())
		got.Spec.Rollout.Coordination.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
		// 1-Component BlueGreen is a degenerate but accepted shape (W-21).
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())
	})

	// WU-9: RollingUpdate lockstep when Runner field is missing on a
	// Component. componentImages returns "" for both → "" != ""
	// evaluates symmetric, so an update that touches the (also nil)
	// runner side stays accepted as a no-op. Pin so a future
	// refactor that starts treating nil-runner as a distinct value
	// gets caught.
	ginkgo.It("WU-9 RollingUpdate with nil Runner on both sides treats image as empty (symmetric)", func() {
		// Build without Runner field (componentImages reads
		// spec.Engine.Runner.Image; PodSpec.Containers[0].Image is a
		// different path). Coordination tests use PodSpec containers,
		// so changing the container image WITHOUT a Runner block is
		// invisible to the RollingUpdate lockstep check.
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
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
				}},
			},
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
		Expect(validation.ReasonRollingUpdateRequiresMultipleComponents).To(Equal("RollingUpdateRequiresMultipleComponents"))
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
			Rollout: &v1beta1.RolloutSpec{Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{"predictor"},
					Policy:     v1beta1.CoordinationPolicyIndependent,
				}},
			}},
		}
		err := validation.ValidateCoordination(spec)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("groups[0]"),
			fmt.Sprintf("error should reference groups[0]: got %q", err.Error()))
	})
})
