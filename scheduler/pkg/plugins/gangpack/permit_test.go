package gangpack

import "testing"

// TestGateDecision: the gang releases when the members already waiting plus the
// arriving one reach minMember — not before.
func TestGateDecision(t *testing.T) {
	cases := []struct {
		waiting, minMember int
		release            bool
	}{
		{0, 1, true},  // singleton gang: the first (only) member completes it
		{0, 3, false}, // 1st of 3 -> wait
		{1, 3, false}, // 2nd of 3 -> wait
		{2, 3, true},  // 3rd of 3 -> release all
		{5, 3, true},  // already over (flexible/extra) -> release
	}
	for _, c := range cases {
		if got := gateDecision(c.waiting, c.minMember); got != c.release {
			t.Fatalf("gateDecision(%d,%d) = %v, want %v", c.waiting, c.minMember, got, c.release)
		}
	}
}

func TestSameGang(t *testing.T) {
	if !sameGang(gangPod("team", "pf"), "team/pf") {
		t.Fatal("pod in team/pf should match gang key team/pf")
	}
	if sameGang(gangPod("team", "pf"), "team/decode") {
		t.Fatal("pod in team/pf must not match a different gang")
	}
	if sameGang(gangPod("other", "pf"), "team/pf") {
		t.Fatal("namespace must be part of the gang key")
	}
	if sameGang(gangPod("team", ""), "team/pf") {
		t.Fatal("non-gang pod matches nothing")
	}
}
