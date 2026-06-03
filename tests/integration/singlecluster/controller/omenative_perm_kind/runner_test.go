package omenative_perm_kind

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
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
	// Bumped = every coordination-group member; engine-only when no group.
	// Mirrors run.py: bumped = members ∩ configured, else {engine}.
	bumped := map[string]bool{}
	for _, c := range pc.members {
		if pc.replicas(c) > 0 {
			bumped[string(c)] = true
		}
	}
	if len(bumped) == 0 {
		bumped[string(v1beta1.EngineComponent)] = true
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

	// Phase 2: baseline + trigger (bump every member).
	obs.MarkBaseline(want, bumped, modes)
	ne, nd := pc.replicas(v1beta1.EngineComponent), pc.replicas(v1beta1.DecoderComponent)
	if pc.pacingType == v1beta1.CoordinationPacingRatioBalanced && ne > 0 && nd > 0 {
		obs.SetRatioPair(string(v1beta1.EngineComponent), string(v1beta1.DecoderComponent),
			float64(ne)/float64(nd))
	}
	ginkgo.By("bumping trigger on all members")
	gomega.Eventually(func(g gomega.Gomega) {
		cur := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), cur)).To(gomega.Succeed())
		for c := range bumped {
			setTrigger(cur, v1beta1.ComponentType(c))
		}
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(gomega.Succeed())
	}).WithTimeout(2*time.Minute).WithPolling(time.Second).Should(gomega.Succeed(), "bump trigger")

	// Phase 3: converge — every configured at N alive+ready, every bumped fully rotated.
	ginkgo.By("watching rotation to convergence")
	gomega.Eventually(func() bool {
		cnt := obs.Counts()
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

	// 3. In-place strategies must update in place — no UID rotation.
	for c := range bumped {
		if modes[c] != "inplace" {
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
