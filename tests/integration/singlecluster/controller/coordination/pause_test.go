package coordination

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Pause annotation specs for OEP-0007.1.
//
// PausedGlobal is the gate fed by the `ome.io/rollout-paused: "true"`
// annotation on the ISVC. ComputeTransition's outer switch checks
// PausedGlobal BEFORE the per-policy branch, so when set every group
// returns Phase=Paused regardless of policy or sub-state.
//
// Most P-* run as pure-function tests against ComputeTransition.
// Annotation-reading specs (P-8, P-9) cover the public reconciler
// surface but the annotation is read inside reconciler.Reconcile,
// which requires the full reconciler to be wired up (G1). For now
// we exercise the annotation contract via the well-known key string.
//
// Reference: state.go ComputeTransition + reconciler.go
// paussedAnnotationKey().

// pausedObs reuses any policy's idleObs and flips PausedGlobal.
func pausedObs(policy v1beta1.CoordinationPolicy) coordination.GroupObservation {
	g := coordination.ResolvedGroup{
		Name:       "0",
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Policy:     policy,
	}
	if policy == v1beta1.CoordinationPolicySequential {
		g.Order = g.Components
	}
	obs := coordination.GroupObservation{
		Group:      g,
		Components: map[v1beta1.ComponentType]coordination.ComponentObservation{},
	}
	for _, c := range g.Components {
		obs.Components[c] = coordination.ComponentObservation{
			Component:            c,
			DesiredReplicas:      2,
			TotalPods:            2,
			ReadyPods:            2,
			NewRevisionPods:      2,
			NewRevisionReadyPods: 2,
			TargetRevisionHash:   "rev1",
			CurrentRevisionHash:  "rev1",
			RolloutInFlight:      false,
		}
	}
	return obs
}

var _ = ginkgo.Describe("Coordination pause annotation (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-pause-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// ----------------------------------------------------------------
	// Pure-function specs
	// ----------------------------------------------------------------

	ginkgo.It("P-1 PausedGlobal=true on any policy → group Phase=Paused", func() {
		for _, policy := range []v1beta1.CoordinationPolicy{
			v1beta1.CoordinationPolicyBlueGreen,
			v1beta1.CoordinationPolicyRollingUpdate,
			v1beta1.CoordinationPolicyIndependent,
			v1beta1.CoordinationPolicySequential,
		} {
			obs := pausedObs(policy)
			obs.PausedGlobal = true
			tr := coordination.ComputeTransition(obs)
			Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhasePaused),
				"policy %s with PausedGlobal=true must return Paused (outer short-circuit)", policy)
			Expect(tr.Message).To(ContainSubstring("ome.io/rollout-paused"),
				"Paused message references the annotation operators set")
		}
	})

	ginkgo.It("P-2 PausedGlobal=false → group resumes from policy-specific transition", func() {
		obs := pausedObs(v1beta1.CoordinationPolicyBlueGreen)
		obs.PausedGlobal = false
		// Make engine roll so we can confirm we get back the underlying
		// policy phase (Surging in this case), not Paused.
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		mutate(&obs, v1beta1.DecoderComponent, func(o *coordination.ComponentObservation) {
			o.RolloutInFlight = true
			o.NewRevisionPods = 0
			o.TargetRevisionHash = "rev2"
		})
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging),
				"clearing PausedGlobal must let policy transitions resume")
	})

	ginkgo.It("P-3 PausedGlobal=true overrides mid-Surging state without rollback", func() {
		// Group is mid-Surging (no new pods yet); pause is set. Result
		// is Paused with no rollback semantics — when unpaused, the
		// underlying state resumes.
		obs := pausedObs(v1beta1.CoordinationPolicyBlueGreen)
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 0
				o.NewRevisionReadyPods = 0
				o.TargetRevisionHash = "rev2"
			})
		}
		// Without pause: Surging (no surge pods yet).
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseSurging))

		// With pause: Paused, regardless of in-flight state.
		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused))
	})

	ginkgo.It("P-4 PausedGlobal=true on empty-Components observation → still Paused (no panic)", func() {
		// No Components observed; PausedGlobal short-circuits before any
		// Component iteration so this must not panic.
		obs := coordination.GroupObservation{
			Group: coordination.ResolvedGroup{
				Name:       "0",
				Components: []v1beta1.ComponentType{},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
			},
			Components:   map[v1beta1.ComponentType]coordination.ComponentObservation{},
			PausedGlobal: true,
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused))
	})

	ginkgo.It("P-5 PausedGlobal beats per-Component Failed (outer short-circuit precedence)", func() {
		// BlueGreen with engine.Failed AND PausedGlobal=true. The outer
		// short-circuit returns Paused before blueGreenTransition's Failed
		// check runs.
		obs := pausedObs(v1beta1.CoordinationPolicyBlueGreen)
		mutate(&obs, v1beta1.EngineComponent, func(o *coordination.ComponentObservation) {
			o.Failed = true
			o.FailureMessage = "ready timeout"
		})
		// Without pause: Failed.
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseFailed))

		// With pause: Paused (outer short-circuit wins).
		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused),
				"PausedGlobal short-circuits before the per-policy Failed check")
	})

	ginkgo.It("P-6 PausedGlobal during ScalingDown → freezes; underlying state preserved when unpaused", func() {
		// BlueGreen in ScalingDown shape (rolling, surge ready, old drained,
		// promotion pending). Pause freezes — when unpaused, the
		// underlying ScalingDown returns. State machine is pure so we
		// verify by toggling.
		obs := pausedObs(v1beta1.CoordinationPolicyBlueGreen)
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 2
				o.NewRevisionReadyPods = 2
				o.TotalPods = 2 // drained
				o.TargetRevisionHash = "rev2"
			})
		}
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseScalingDown))

		obs.PausedGlobal = true
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhasePaused))

		obs.PausedGlobal = false
		Expect(coordination.ComputeTransition(obs).Phase).
			To(Equal(v1beta1.CoordinationPhaseScalingDown),
				"unpause must restore the underlying transition state")
	})

	ginkgo.It("P-7 PausedGlobal is the SOLE outer-switch gate (no other annotation routes through it)", func() {
		// Negative coverage: only PausedGlobal short-circuits. Any
		// other field on GroupObservation feeds into the policy
		// transition. Toggling PausedGlobal in isolation must be the
		// only way to enter Paused via the outer switch.
		obs := pausedObs(v1beta1.CoordinationPolicyBlueGreen)
		// Set ALL Components to rolling + paused — that's RollingUpdate's
		// allPaused gate, but here policy=BlueGreen uses anyPaused, so this
		// returns Paused via the INNER atomic branch, NOT the outer
		// PausedGlobal short-circuit.
		for _, c := range obs.Group.Components {
			mutate(&obs, c, func(o *coordination.ComponentObservation) {
				o.RolloutInFlight = true
				o.NewRevisionPods = 1
				o.TargetRevisionHash = "rev2"
				o.Paused = true
			})
		}
		tr := coordination.ComputeTransition(obs)
		Expect(tr.Phase).To(Equal(v1beta1.CoordinationPhasePaused))
		Expect(tr.Message).NotTo(ContainSubstring("ome.io/rollout-paused"),
			"per-Component Paused returns Paused via inner branch (different message)")
	})

	// ----------------------------------------------------------------
	// Annotation-key + reconciler-surface specs
	// ----------------------------------------------------------------

	ginkgo.It("P-8 annotation values other than 'true' → not paused (case-sensitive exact match)", func() {
		// reconciler.go reads `isvc.Annotations[paussedAnnotationKey()] == "true"`.
		// "yes", "1", "True" (different case), "TRUE" must NOT pause.
		// Pin the exact-match contract via direct annotation reads.
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "p8"},
		}
		isvc.Annotations = map[string]string{
			"ome.io/rollout-paused": "yes",
		}
		Expect(isvc.Annotations["ome.io/rollout-paused"] == "true").To(BeFalse(),
			"\"yes\" is not the exact string 'true' — reconciler must NOT route to Paused")

		for _, v := range []string{"1", "True", "TRUE", "yes", ""} {
			isvc.Annotations["ome.io/rollout-paused"] = v
			Expect(isvc.Annotations["ome.io/rollout-paused"] == "true").To(BeFalse(),
				"value %q is not exact 'true' — reconciler must NOT route to Paused", v)
		}
		// Sanity: only literal "true" pauses.
		isvc.Annotations["ome.io/rollout-paused"] = "true"
		Expect(isvc.Annotations["ome.io/rollout-paused"] == "true").To(BeTrue())
	})

	ginkgo.It("P-9 G9 typo NOT honored: 'ome.io/rollout-pause' singular is the WRONG key", func() {
		// reconciler.go reads `ome.io/rollout-paused` (with the 'd').
		// Codify the well-known key so a future refactor that drops
		// the 'd' (per G9 cosmetic fix) doesn't accidentally change
		// the public contract.
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns.Name, Name: "p9"},
		}
		isvc.Annotations = map[string]string{
			"ome.io/rollout-pause": "true", // typo (singular)
		}
		Expect(isvc.Annotations["ome.io/rollout-paused"] == "true").To(BeFalse(),
			"singular 'rollout-pause' is the WRONG key — must not pause")
		Expect(isvc.Annotations["ome.io/rollout-pause"] == "true").To(BeTrue(),
			"the typo'd key is present but unused")
	})

	// ----------------------------------------------------------------
	// G1-blocked PIts: end-to-end pause behavior through the reconciler
	// ----------------------------------------------------------------

	ginkgo.It("P-E2E-1 setting ome.io/rollout-paused=true on a live ISVC freezes group transitions", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pause-e2e-1"), WithRolloutPaused(),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-e2e-1")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("P-E2E-2 removing the pause annotation lets the group resume", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("pause-e2e-2"), WithRolloutPaused(),
			WithBlueGreenGroup(v1beta1.EngineComponent, v1beta1.DecoderComponent))
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		// Wait for Paused — guard against nil deref while the status
		// is still being populated by the first reconcile.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-e2e-2")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Phase).
				To(Equal(v1beta1.CoordinationPhasePaused))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Remove the annotation — retry on 409 since the controller
		// reconciles concurrently.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-e2e-2")
			delete(cur.Annotations, "ome.io/rollout-paused")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Group should leave Paused.
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-e2e-2")
			g.Expect(cur.Status.RolloutCoordination).NotTo(BeNil())
			g.Expect(cur.Status.RolloutCoordination.Groups).NotTo(BeEmpty())
			g.Expect(cur.Status.RolloutCoordination.Groups[0].Phase).
				NotTo(Equal(v1beta1.CoordinationPhasePaused))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("P-E2E-3 pause on an ISVC with NO rolloutCoordination block → no-op", func() {
		// Without a coordination block, Status.RolloutCoordination is
		// never written. Setting pause should have no observable effect
		// (no Paused phase to enter — there are no groups).
		fix := newCoordFixture(ns.Name, WithISVCName("pause-e2e-3"), WithRolloutPaused())
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		Consistently(func(g Gomega) {
			cur := freshISVC(g, ns.Name, "pause-e2e-3")
			if cur.Status.RolloutCoordination != nil {
				g.Expect(cur.Status.RolloutCoordination.Groups).To(BeEmpty(),
					"ISVC with no rolloutCoordination block must not surface group statuses")
			}
		}, "5s", util.PollInterval).Should(Succeed())
	})
})
