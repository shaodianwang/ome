package util

import "testing"

// pairObs builds an observer with pair-alive armed and a synthetic pod set.
func pairObs() *RolloutObserver {
	o := &RolloutObserver{pods: map[string]*obsPod{}, peak: map[string]int{}}
	o.pairTrack = true
	o.pairBadSince = -1
	return o
}

func servingPod(comp, proto string) *obsPod {
	return &obsPod{comp: comp, alive: true, ready: true, serving: true, pairing: proto}
}

func TestPairAliveTracker(t *testing.T) {
	t.Run("pairable arms and never violates", func(t *testing.T) {
		o := pairObs()
		o.pods["e0"] = servingPod("engine", "v1")
		o.pods["d0"] = servingPod("decoder", "v1")
		o.samplePairAliveLocked(0)
		if !o.pairArmed {
			t.Fatal("pairable sample must arm the tracker")
		}
		if worst, _ := o.PairAliveWorst(); worst != 0 {
			t.Fatalf("no violation expected, got %.1fs", worst)
		}
	})

	t.Run("armed unpaired span grows past the window", func(t *testing.T) {
		o := pairObs()
		o.pods["e0"] = servingPod("engine", "v1")
		o.pods["e1"] = servingPod("engine", "v1")
		o.pods["d0"] = servingPod("decoder", "v1")
		o.samplePairAliveLocked(0)
		o.pods["d0"] = servingPod("decoder", "v2") // pair destroyed, not a wall (engine has 2)
		o.samplePairAliveLocked(1)
		o.samplePairAliveLocked(1 + PairAliveSustainWindow)
		if worst, detail := o.PairAliveWorst(); worst < PairAliveSustainWindow {
			t.Fatalf("sustained unpaired span must register, got %.1fs (%s)", worst, detail)
		}
	})

	t.Run("1x1 wall is exempt", func(t *testing.T) {
		o := pairObs()
		o.pods["e0"] = servingPod("engine", "v1")
		o.pods["d0"] = servingPod("decoder", "v1")
		o.samplePairAliveLocked(0)
		o.pods["d0"] = servingPod("decoder", "v2")
		o.samplePairAliveLocked(1)
		o.samplePairAliveLocked(30)
		if worst, _ := o.PairAliveWorst(); worst != 0 {
			t.Fatalf("wall window must be exempt, got %.1fs", worst)
		}
	})

	t.Run("no-pair escape disarms until a pair re-forms", func(t *testing.T) {
		o := pairObs()
		o.pods["e0"] = servingPod("engine", "v1")
		o.pods["e1"] = servingPod("engine", "v1")
		o.pods["d0"] = servingPod("decoder", "v1")
		o.samplePairAliveLocked(0)
		delete(o.pods, "d0") // externally destroyed (kill): decoder side empty
		o.samplePairAliveLocked(1)
		o.pods["d1"] = servingPod("decoder", "v2") // recovery arrives on the new cohort
		o.samplePairAliveLocked(2)
		o.samplePairAliveLocked(2 + 2*PairAliveSustainWindow)
		if worst, _ := o.PairAliveWorst(); worst != 0 {
			t.Fatalf("disarmed recovery window must not violate, got %.1fs", worst)
		}
		o.pods["e0"] = servingPod("engine", "v2")
		o.samplePairAliveLocked(20)
		if !o.pairArmed {
			t.Fatal("a recovered pair must re-arm the tracker")
		}
	})

	t.Run("empty protocol pairs with anything", func(t *testing.T) {
		o := pairObs()
		o.pairArmed = true
		o.pods["e0"] = servingPod("engine", "")
		o.pods["e1"] = servingPod("engine", "v1")
		o.pods["d0"] = servingPod("decoder", "v2")
		o.samplePairAliveLocked(0)
		o.samplePairAliveLocked(30)
		if worst, _ := o.PairAliveWorst(); worst != 0 {
			t.Fatalf("empty protocol must pair with anything, got %.1fs", worst)
		}
	})
}
