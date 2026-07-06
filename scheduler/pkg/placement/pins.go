// Package placement holds ome-scheduler's stateful placement decisions layered
// on the pure accounting in package topology. Its first piece is the pin store:
// which domain each placement group's gangs are committed to.
package placement

import (
	"sync"

	"github.com/ome-projects/ome/scheduler/pkg/topology"
)

// Pins records, per placement group, the domain that group's gangs are pinned
// to. The first pod of a group chooses a domain via best-fit; every later
// member — and the partner gang, which shares the placement group — follows the
// pin. That is what turns the framework's one-pod-at-a-time scheduling into a
// group decision (OEP-0022 § Placement / Pinning).
//
// The scheduler runs scheduling cycles single-threaded, but plugin state is also
// touched from event handlers (failover rebuild, release on gang teardown), so
// the store guards itself with a mutex.
type Pins struct {
	mu      sync.Mutex
	byGroup map[string]string // placement group -> domain
}

// New returns an empty pin store.
func New() *Pins {
	return &Pins{byGroup: make(map[string]string)}
}

// Choose returns the domain the group is committed to. If the group is already
// pinned, that domain is returned as-is — regardless of current free counts,
// because members are already reserved there; if the gang can no longer complete
// there the caller unwinds via Release and calls Choose again. If the group is
// not yet pinned, Choose runs best-fit over free, pins the winner, and returns
// it. ok is false only when the group is unpinned and no domain currently fits
// the gang (the gang waits, still unpinned).
func (p *Pins) Choose(group string, free topology.FreeByDomain, gangSize int) (domain string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if d, pinned := p.byGroup[group]; pinned {
		return d, true
	}
	d, fit := topology.BestFit(free, gangSize)
	if !fit {
		return "", false
	}
	p.byGroup[group] = d
	return d, true
}

// Get returns the pinned domain for a group, or ("", false) if unpinned.
func (p *Pins) Get(group string) (domain string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	d, ok := p.byGroup[group]
	return d, ok
}

// Set pins a group to a domain without best-fit. Used on leader failover to
// rebuild the store from where bound pods already sit (OEP-0022 § failure modes:
// "the domain is simply where those pods are").
func (p *Pins) Set(group, domain string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byGroup[group] = domain
}

// Release drops a group's pin — on gang completion, on gate-timeout unwind, or on
// teardown — so the next Choose re-runs best-fit and may pick a different domain.
func (p *Pins) Release(group string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.byGroup, group)
}
