package omenative_perm_kind

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

func parseDur(s string) (*metav1.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return nil, err
	}
	return &metav1.Duration{Duration: d}, nil
}

func inPlace(t v1beta1.UpdateStrategyType) bool {
	return t == v1beta1.UpdateStrategyInPlaceIfPossible || t == v1beta1.UpdateStrategyInPlaceOnly
}

// runPerm drives one permutation: apply → wait Ready → MarkBaseline → bump
// EVERY group member → watch to convergence → assert the rich checks. Watch-
// driven via util.RolloutObserver (no polling).
func runPerm(ns string, pc permCase) {
	isvc := buildPermISVC(ns, pc)
	gomega.Expect(k8sClient.Create(suiteCtx, isvc)).To(gomega.Succeed(), "create ISVC")
	ginkgo.DeferCleanup(func() {
		_ = k8sClient.Delete(suiteCtx, isvc)
	})

	obsCtx, cancel := context.WithCancel(suiteCtx)
	defer cancel()
	obs, err := util.NewRolloutObserver(restCfg, ns, pc.id)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	obs.Start(obsCtx)

	configured := pc.configured()
	want := map[string]int{}
	modes := map[string]string{}
	for _, c := range configured {
		want[string(c)] = pc.replicas(c)
		if inPlace(pc.cfg(c).strat) {
			modes[string(c)] = "inplace"
		} else {
			modes[string(c)] = "surge"
		}
	}
	// A pairing trigger detects updated pods by revision hash for EVERY
	// strategy: an in-place op restamps the revision-hash (and pairing)
	// label on the LIVE pod, so hash-vs-baseline works uniformly. The
	// in-place contract is still asserted through UID stability below.
	if pc.pairTrigger {
		for c := range modes {
			modes[c] = "surge"
		}
	}
	// Bumped = every coordination-group member; engine-only when no group.
	// Mirrors run.py: bumped = members ∩ configured, else {engine}.
	// A pairing trigger overrides both: the protocol rides every
	// engine+decoder revision, so the patch rolls exactly those Components.
	bumped := map[string]bool{}
	if pc.pairTrigger {
		for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			if pc.replicas(c) > 0 {
				bumped[string(c)] = true
			}
		}
	} else {
		for _, c := range pc.members {
			if pc.replicas(c) > 0 {
				bumped[string(c)] = true
			}
		}
		if len(bumped) == 0 {
			bumped[string(v1beta1.EngineComponent)] = true
		}
	}
	controls := map[string]bool{}
	for _, c := range configured {
		if !bumped[string(c)] {
			controls[string(c)] = true
		}
	}

	sumN, sumBumped := 0, 0
	for _, c := range configured {
		sumN += pc.replicas(c)
		if bumped[string(c)] {
			sumBumped += pc.replicas(c)
		}
	}
	soakS := 0
	if d, err := parseDur(pc.soak); pc.soak != "" && err == nil {
		soakS = int(d.Duration.Seconds())
	}
	initBudget := time.Duration(120+25*sumN) * time.Second
	rotateBudget := time.Duration(120+60*sumBumped+soakS) * time.Second
	// A mid-transition scale-up grows the fleet the rotation must cover.
	for c, t := range pc.pairScale {
		if extra := int(t) - pc.replicas(c); extra > 0 {
			rotateBudget += time.Duration(60*extra) * time.Second
		}
	}

	// Phase 1: initial Ready.
	ginkgo.By("waiting for initial Ready")
	gomega.Eventually(func() bool {
		cnt := obs.Counts()
		for _, c := range configured {
			if cnt[string(c)][1] != want[string(c)] {
				return false
			}
		}
		return true
	}).WithTimeout(initBudget).WithPolling(time.Second).Should(gomega.BeTrue(), "initial Ready")

	// Phase 2: baseline + trigger (bump every member / patch the protocol).
	obs.MarkBaseline(want, bumped, modes)
	ne, nd := pc.replicas(v1beta1.EngineComponent), pc.replicas(v1beta1.DecoderComponent)
	if pc.pacingType == v1beta1.CoordinationPacingRatioBalanced && ne > 0 && nd > 0 {
		obs.SetRatioPair(string(v1beta1.EngineComponent), string(v1beta1.DecoderComponent),
			float64(ne)/float64(nd))
	}
	// Pair-alive arms exactly when the gate does: a non-empty→non-empty
	// transition with both pairing Components configured.
	pairAlive := pc.pairTrigger && pc.pairFrom != "" && pc.pairTo != "" &&
		bumped[string(v1beta1.EngineComponent)] && bumped[string(v1beta1.DecoderComponent)]
	if pairAlive {
		obs.EnablePairAlive()
	}
	if pc.pairTrigger {
		ginkgo.By(fmt.Sprintf("patching pairingProtocol %q -> %q", pc.pairFrom, pc.pairTo))
	} else {
		ginkgo.By("bumping trigger on all members")
	}
	gomega.Eventually(func(g gomega.Gomega) {
		cur := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), cur)).To(gomega.Succeed())
		if pc.pairTrigger {
			setPairingProtocol(cur, pc.pairTo)
		} else {
			for c := range bumped {
				setTrigger(cur, v1beta1.ComponentType(c))
			}
		}
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(gomega.Succeed())
	}).WithTimeout(2*time.Minute).WithPolling(time.Second).Should(gomega.Succeed(), "bump trigger")

	// Phase 3: converge — every configured at N alive+ready, every bumped fully
	// rotated. Pairing cases piggyback two side channels on the poll: record
	// every rolloutHold gate reported mid-transition, and fire the one-shot
	// mid-transition scale once the roll has visibly started.
	ginkgo.By("watching rotation to convergence")
	holdGates := map[string]bool{}
	scaled := len(pc.pairScale) == 0
	gomega.Eventually(func() bool {
		cnt := obs.Counts()
		if pc.pairTrigger {
			cur := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), cur); err == nil {
				for c := range bumped {
					lf := cur.Status.Components[v1beta1.ComponentType(c)].Lifecycle
					if lf != nil && lf.RolloutHold != nil {
						holdGates[string(lf.RolloutHold.Gate)] = true
					}
				}
				if !scaled {
					started := false
					for c := range bumped {
						if cnt[c][2] > 0 {
							started = true
						}
					}
					if started {
						for c, t := range pc.pairScale {
							n := int(t)
							switch c {
							case v1beta1.EngineComponent:
								cur.Spec.Engine.MinReplicas = &n
								cur.Spec.Engine.MaxReplicas = n
							case v1beta1.DecoderComponent:
								cur.Spec.Decoder.MinReplicas = &n
								cur.Spec.Decoder.MaxReplicas = n
							}
						}
						if k8sClient.Update(suiteCtx, cur) == nil {
							for c, t := range pc.pairScale {
								want[string(c)] = int(t)
							}
							scaled = true
						}
					}
				}
			}
		}
		if !scaled {
			return false // the mid-transition scale must fire before convergence can latch
		}
		for _, c := range configured {
			v := cnt[string(c)]
			if v[0] != want[string(c)] || v[1] != want[string(c)] {
				return false
			}
			if bumped[string(c)] && v[2] != want[string(c)] {
				return false
			}
		}
		return true
	}).WithTimeout(rotateBudget).WithPolling(time.Second).Should(gomega.BeTrue(),
		"converge: all configured at N ready, all bumped fully rotated [%s]", pc.desc)

	// --- Verdict summary (run.py-style) — emitted to GinkgoWriter (-ginkgo.v). ---
	final := obs.Counts()
	started, doneTS := obs.Timing()
	fmt.Fprintf(ginkgo.GinkgoWriter, "\n  VERDICT [%s] %s\n", pc.id, pc.desc)
	for _, c := range configured {
		fmt.Fprintf(ginkgo.GinkgoWriter,
			"    %-8s N=%d peak=%d ceil=%d minReady=%d floor=%d final(alive/ready/updated)=%v bumped=%v started=%.0fs done=%.0fs\n",
			c, pc.replicas(c), obs.Peak(string(c)), pc.ceil(c), obs.MinReady(string(c)), pc.servingFloor(c),
			final[string(c)], bumped[string(c)], started[string(c)], doneTS[string(c)])
	}
	if pc.pacingType == v1beta1.CoordinationPacingRatioBalanced {
		dev, ra, rb := obs.RatioWorst()
		idev, ia, ib := obs.RatioInstant()
		fmt.Fprintf(ginkgo.GinkgoWriter, "    ratio sustained dev=%.0f%% at %d:%d (instantaneous worst %.0f%% at %d:%d)\n",
			dev*100, ra, rb, idev*100, ia, ib)
	}

	// --- Checks (port of run.py's verdict) ---

	// 1. Peak alive ≤ ceiling per configured Component (authored upper bound).
	for _, c := range configured {
		ceil := pc.ceil(c)
		gomega.Expect(obs.Peak(string(c))).To(gomega.BeNumerically("<=", ceil),
			"PEAK: %s peak %d must stay ≤ ceiling %d [%s]", c, obs.Peak(string(c)), ceil, pc.desc)
	}

	// 1b. Serving floor — ready count never drops below N - maxUnavailable.
	for _, c := range configured {
		floor := pc.servingFloor(c)
		gomega.Expect(obs.MinReady(string(c))).To(gomega.BeNumerically(">=", floor),
			"FLOOR: %s ready dropped to %d, below floor %d [%s]", c, obs.MinReady(string(c)), floor, pc.desc)
	}

	// 1c. Surge actually fired — a surge-strategy roll must create an extra pod,
	//     so a no-op or recreate-fallback can't pass the upper peak ceiling.
	for _, c := range configured {
		if bumped[string(c)] && pc.surgeExpected(c) {
			gomega.Expect(obs.Peak(string(c))).To(gomega.BeNumerically(">", pc.replicas(c)),
				"SURGE: %s peak %d never exceeded N=%d — surge did not fire [%s]", c, obs.Peak(string(c)), pc.replicas(c), pc.desc)
		}
	}

	// 2. Negative control — non-bumped Components must NOT surge or rotate.
	for c := range controls {
		gomega.Expect(obs.Peak(c)).To(gomega.BeNumerically("<=", want[c]),
			"ISOLATION: %s (not bumped) surged (peak %d > N %d) [%s]", c, obs.Peak(c), want[c], pc.desc)
		gomega.Expect(final[c][2]).To(gomega.Equal(0),
			"ISOLATION: %s (not bumped) rotated %d pods [%s]", c, final[c][2], pc.desc)
	}

	// 3. In-place strategies must update in place — no UID rotation. Keyed on
	// the case's declared strategy, not the observer mode: a pairing trigger
	// runs the observer hash-based ("surge") for every strategy, but the
	// in-place contract still holds.
	for c := range bumped {
		l := pc.cfg(v1beta1.ComponentType(c))
		if l == nil || !inPlace(l.strat) {
			continue
		}
		base := obs.BaselineUIDs(c)
		for u := range obs.CurrentUIDs(c) {
			gomega.Expect(base).To(gomega.HaveKey(u),
				"IN-PLACE: %s pod UID %s rotated — recreated instead of patched [%s]", c, u, pc.desc)
		}
	}

	// 4. Sequential ordering — the next member must not start before the previous finished.
	if pc.policy == v1beta1.CoordinationPolicySequential && len(pc.order) >= 2 {
		for i := 1; i < len(pc.order); i++ {
			a, b := string(pc.order[i-1]), string(pc.order[i])
			if !bumped[a] || !bumped[b] {
				continue
			}
			ta, okA := doneTS[a]
			tb, okB := started[b]
			if okA && okB {
				gomega.Expect(tb).To(gomega.BeNumerically(">=", ta-6.0),
					"SEQUENTIAL: %s started (t=%.0fs) before %s finished (t=%.0fs) [%s]", b, tb, a, ta, pc.desc)
			}
		}
	}

	// 5. RatioBalanced band — serving ratio held within tolerance throughout.
	if pc.pacingType == v1beta1.CoordinationPacingRatioBalanced && pc.ratioTol != nil {
		dev, ra, rb := obs.RatioWorst()
		tol := float64(*pc.ratioTol)/100.0 + 0.10 // band + sampling grace
		gomega.Expect(dev).To(gomega.BeNumerically("<=", tol),
			"RATIO: serving %d:%d dev %.0f%% exceeded tol %d%% [%s]", ra, rb, dev*100, *pc.ratioTol, pc.desc)
	}

	// 6. Pairing-protocol verdicts.
	if pc.pairTrigger {
		// 6a. Pair-alive: no armed unpaired engine/decoder window past the
		// sustain threshold (the gate's no-pair and 1×1-wall escapes are
		// already exempted inside the tracker).
		if pairAlive {
			worst, detail := obs.PairAliveWorst()
			gomega.Expect(worst).To(gomega.BeNumerically("<", util.PairAliveSustainWindow),
				"PAIR-ALIVE: unpaired serving window of %.1fs (%s) during %q→%q [%s]",
				worst, detail, pc.pairFrom, pc.pairTo, pc.desc)
		}
		// 6b. The pair floor visibly held a step where the shape guarantees a
		// denial window.
		if pc.pairExpectHold {
			gomega.Expect(holdGates).To(gomega.HaveKey("Pairing"),
				"HOLD: no rolloutHold gate=Pairing observed mid-transition (gates seen: %v) [%s]", holdGates, pc.desc)
		}
		// 6c. Surface convergence: pod labels, traffic protocols, Service
		// metadata labels (never selectors) all on the target cohort.
		assertPairingSurfaces(ns, pc, bumped)
	}
}

// setPairingProtocol sets (or, for "", removes) spec.rollout.pairingProtocol —
// the pairing-transition rollout trigger.
func setPairingProtocol(isvc *v1beta1.InferenceService, to string) {
	if isvc.Spec.Rollout == nil {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
	}
	if to == "" {
		isvc.Spec.Rollout.PairingProtocol = nil
		return
	}
	isvc.Spec.Rollout.PairingProtocol = &to
}

// assertPairingSurfaces checks the three protocol surfaces converged on the
// case's target cohort: every engine/decoder pod label, every live traffic
// target on the bumped Components, and every routing Service named by live
// traffic (metadata label only — the selector must NEVER carry it). Status
// lags pod convergence, so the whole block polls.
func assertPairingSurfaces(ns string, pc permCase, bumped map[string]bool) {
	target := pc.pairTo
	gomega.Eventually(func(g gomega.Gomega) {
		pods := &corev1.PodList{}
		g.Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(ns),
			client.MatchingLabels{constants.InferenceServicePodLabelKey: pc.id})).To(gomega.Succeed())
		for i := range pods.Items {
			pod := &pods.Items[i]
			comp := pod.Labels[constants.OMEComponentLabel]
			if pod.DeletionTimestamp != nil ||
				(comp != string(v1beta1.EngineComponent) && comp != string(v1beta1.DecoderComponent)) {
				continue
			}
			g.Expect(pod.Labels[util.RolloutPairingProtocolLabel]).To(gomega.Equal(target),
				"pod %s must carry its revision cohort", pod.Name)
		}

		svcs := &corev1.ServiceList{}
		g.Expect(k8sClient.List(suiteCtx, svcs, client.InNamespace(ns),
			client.MatchingLabels{constants.InferenceServicePodLabelKey: pc.id})).To(gomega.Succeed())
		byName := map[string]*corev1.Service{}
		for i := range svcs.Items {
			s := &svcs.Items[i]
			byName[s.Name] = s
			g.Expect(s.Spec.Selector).NotTo(gomega.HaveKey(util.RolloutPairingProtocolLabel),
				"the pairing protocol must never enter Service %s's selector", s.Name)
		}

		cur := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns, Name: pc.id}, cur)).To(gomega.Succeed())
		for c := range bumped {
			targets := cur.Status.Components[v1beta1.ComponentType(c)].Traffic
			g.Expect(targets).NotTo(gomega.BeEmpty(), "%s must publish traffic targets", c)
			for _, t := range targets {
				if t.Percent <= 0 {
					continue
				}
				g.Expect(t.PairingProtocol).To(gomega.Equal(target),
					"%s target %s protocol", c, t.RevisionName)
				svc := byName[t.RevisionName]
				g.Expect(svc).NotTo(gomega.BeNil(), "routing Service %s named by live traffic", t.RevisionName)
				if svc != nil {
					g.Expect(svc.Labels[util.RolloutPairingProtocolLabel]).To(gomega.Equal(target),
						"routing Service %s metadata cohort label", t.RevisionName)
				}
			}
		}
	}).WithTimeout(2*time.Minute).WithPolling(time.Second).Should(gomega.Succeed(),
		"pairing surfaces must converge on %q [%s]", target, pc.desc)
}

// setTrigger stamps the trigger annotation on a Component's ComponentExtensionSpec.
func setTrigger(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) {
	var ext *v1beta1.ComponentExtensionSpec
	switch c {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			ext = &isvc.Spec.Engine.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			ext = &isvc.Spec.Decoder.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if isvc.Spec.Router != nil {
			ext = &isvc.Spec.Router.ComponentExtensionSpec
		}
	}
	if ext == nil {
		return
	}
	if ext.Annotations == nil {
		ext.Annotations = map[string]string{}
	}
	ext.Annotations[util.RolloutTriggerAnnotation] = util.RolloutTriggerValue
}
