package gangpack

import (
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/scheduler/pkg/placement"
)

// fakeGangPodLister stands in for the informer-backed live-gang lister.
type fakeGangPodLister struct {
	keys map[string]bool
	err  error
}

func (f fakeGangPodLister) liveGangKeys() (map[string]bool, error) { return f.keys, f.err }

// TestGCPinsReleasesDeadGangs: gcPins releases pins for gangs with no live pods
// and keeps those that still have some.
func TestGCPinsReleasesDeadGangs(t *testing.T) {
	pins := placement.New()
	pins.Set("ns/live", "a")
	pins.Set("ns/dead", "b")
	g := &GangPack{pins: pins, podLister: fakeGangPodLister{keys: map[string]bool{"ns/live": true}}}

	g.gcPins()

	if _, ok := pins.Get("ns/dead"); ok {
		t.Fatal("pin for a gang with no live pods should be GC'd")
	}
	if _, ok := pins.Get("ns/live"); !ok {
		t.Fatal("pin for a gang that still has pods must be kept")
	}
}

// TestGCPinsSkipsOnListerError: a transient lister error must NOT release
// anything (a partial view could reap live gangs) — the next tick retries.
func TestGCPinsSkipsOnListerError(t *testing.T) {
	pins := placement.New()
	pins.Set("ns/x", "a")
	g := &GangPack{pins: pins, podLister: fakeGangPodLister{err: errors.New("cache not synced")}}

	g.gcPins()

	if _, ok := pins.Get("ns/x"); !ok {
		t.Fatal("must not release pins on a lister error")
	}
}

// TestLiveGangKeys: the informer lister maps pods to gang keys, deduping members
// and ignoring pods with no pod-group label.
func TestLiveGangKeys(t *testing.T) {
	l := &informerPodLister{
		list: func() ([]*v1.Pod, error) {
			return []*v1.Pod{
				gangPod("team", "pf"),     // member
				gangPod("team", "pf"),     // sibling -> same key
				gangPod("team", "decode"), // other gang
				gangPod("team", ""),       // no label -> ignored
			}, nil
		},
	}
	keys, err := l.liveGangKeys()
	if err != nil {
		t.Fatalf("liveGangKeys: %v", err)
	}
	if len(keys) != 2 || !keys["team/pf"] || !keys["team/decode"] {
		t.Fatalf("keys = %v, want {team/pf, team/decode}", keys)
	}
}
