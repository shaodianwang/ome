package coordination

import (
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// fleetPods builds n routable pods of one Component and cohort, one per
// Instance index starting at firstIdx, on surge ordinal `ordinal`.
func fleetPods(comp v1beta1.ComponentType, firstIdx, n int, ordinal int, proto string, ready bool) []client.Object {
	out := make([]client.Object, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("llama-%s-%d-default-%d", comp, firstIdx+i, ordinal)
		out = append(out, drainPod("llama", comp, name, "default", proto, ready))
	}
	return out
}

func requireHeld(t *testing.T, allowed bool, gate v1beta1.RolloutHoldGate, reason, why string) {
	t.Helper()
	if allowed {
		t.Fatalf("%s: drain allowed (gate=%q reason=%q)", why, gate, reason)
	}
	if gate != v1beta1.RolloutHoldGatePairing {
		t.Errorf("gate = %q, want Pairing", gate)
	}
	if !strings.Contains(reason, "cannot pair") {
		t.Errorf("reason must say the replacement cannot pair yet: %s", reason)
	}
}

// 6 prefill + 2 decode, protocol v1 -> v2, rolled 3P+1D at a time. The new
// decoder is Ready while all three new engines still load: it can pair with
// no serving engine, so retiring a v1 decoder would trade serving capacity
// for none. The drain is held until a v2 engine serves.
func TestEvaluatePairingDrain_6P2D_DecoderReadyFirst_HoldsTheOldDecoder(t *testing.T) {
	isvc := pairingISVC("v2")
	var objs []client.Object
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", false)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, objs...)
	requireHeld(t, allowed, gate, reason, "old decoder with no v2 engine serving")
}

// The mirror: three new engines Ready first, the new decoder still loading.
func TestEvaluatePairingDrain_6P2D_EnginesReadyFirst_HoldsTheOldEngines(t *testing.T) {
	isvc := pairingISVC("v2")
	var objs []client.Object
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", false)...)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{"llama-engine-0-default-0"}, objs...)
	requireHeld(t, allowed, gate, reason, "old engine with no v2 decoder serving")
}

// A larger decode side: two old decoders already replaced and a third
// replacement Ready, still with no v2 engine serving. The third old decoder
// stays too; the floor does not thin out to one old pair.
func TestEvaluatePairingDrain_6P4D_HoldsEveryOldDecoderUntilAnEngineServes(t *testing.T) {
	isvc := pairingISVC("v2")
	var objs []client.Object
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", false)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 1, 1, "v2", true)...)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-2-default-0"}, objs...)
	requireHeld(t, allowed, gate, reason, "third old decoder with no v2 engine serving")
}

// Once one v2 engine serves, the v2 decoders are real capacity and the old
// decoder's drain proceeds; the old engines' drains proceed as well.
func TestEvaluatePairingDrain_6P2D_FirstPeerServing_ReleasesTheDrains(t *testing.T) {
	isvc := pairingISVC("v2")
	var objs []client.Object
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 1, 2, 1, "v2", false)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)

	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, objs...); !allowed {
		t.Fatalf("with a v2 engine serving the v2 decoder pairs; the drain must proceed: %s", reason)
	}
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{"llama-engine-0-default-0"}, objs...); !allowed {
		t.Fatalf("with a v2 decoder serving the v2 engine pairs; the drain must proceed: %s", reason)
	}
}

// A source that cannot pair either is not protected: a v2 decoder replaced
// by another v2 decoder while no v2 engine serves trades nothing away, and
// the old pair stays intact behind it.
func TestEvaluatePairingDrain_UnpairableSourceIsNotHeld(t *testing.T) {
	isvc := pairingISVC("v2")
	var objs []client.Object
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 6, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 1, 0, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 2, 1, 1, "v2", true)...)

	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-2-default-0"}, objs...); !allowed {
		t.Fatalf("a v2 source pairs with nothing today; its drain must proceed: %s", reason)
	}
}

// Engines that predate pairing carry the empty protocol and pair with
// anything, so a v2 decoder replacement is capacity from the moment it
// serves and the old decoder's drain proceeds.
func TestEvaluatePairingDrain_EmptyProtocolPeerPairsWithTheReplacement(t *testing.T) {
	isvc := pairingISVC("v2")
	var objs []client.Object
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 2, 0, "", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 2, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)

	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{"llama-decoder-0-default-0"}, objs...); !allowed {
		t.Fatalf("an unlabelled engine pairs with the v2 decoder; the drain must proceed: %s", reason)
	}
}
