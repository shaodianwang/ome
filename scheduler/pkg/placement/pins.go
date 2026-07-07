// Package placement holds ome-scheduler's stateful placement decisions layered
// on the pure accounting in package topology. Its first piece is the pin store:
// which domain each placement group's gangs are committed to, and how much whole-
// node capacity each has reserved there but not yet filled.
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
// A pin also carries a capacity reservation: the whole nodes the gang has
// committed to its domain but not yet filled. A gang's members arrive one
// scheduling cycle at a time, so between pinning a domain and completing there is
// a window where the gang's future members' capacity is unclaimed; without a
// reservation a second gang can best-fit into it and strand the first (which then
// times out and re-plans). The reservation closes that window; it drains to zero
// via Place as members become real occupancy, so the live snapshot's own
// accounting is never double-counted.
//
// The scheduler runs scheduling cycles single-threaded, but plugin state is also
// touched from event handlers (failover rebuild, release on gang teardown), so
// the store guards itself with a mutex.
type Pins struct {
	mu      sync.Mutex
	byGroup map[string]*commitment
	// reserved is the running per-domain sum of every group's outstanding
	// reservation (Σ commitment.remaining). It is a denormalized aggregate so
	// Choose subtracts reservations in O(domains) instead of O(groups) — the
	// difference between O(gangs) and O(1) per pin at fleet scale. Kept exactly in
	// step with byGroup by every mutator.
	reserved map[string]int
}

// commitment is a group's placement decision: the pinned domain plus the count
// of whole nodes still reserved there for members not yet placed. remaining==0
// means the gang is fully committed — every reserved slot has been placed (or the
// pin was rebuilt from already-bound members on failover) — which is the signal
// the gate uses to know it must also count bound siblings, not just waiting ones.
type commitment struct {
	domain    string
	remaining int
}

// New returns an empty pin store.
func New() *Pins {
	return &Pins{
		byGroup:  make(map[string]*commitment),
		reserved: make(map[string]int),
	}
}

// addReservation adjusts the per-domain aggregate by delta, dropping the key when
// it reaches zero so the map stays bounded by live domains. Caller holds the lock.
func (p *Pins) addReservation(domain string, delta int) {
	p.reserved[domain] += delta
	if p.reserved[domain] <= 0 {
		delete(p.reserved, domain)
	}
}

// Choose returns the domain the group is committed to. If the group is already
// pinned, that domain is returned as-is — regardless of current free counts,
// because members are already reserved there; if the gang can no longer complete
// there the caller unwinds via Release and calls Choose again. If the group is
// not yet pinned, Choose best-fits over the EFFECTIVE free counts — the given raw
// free minus every other group's outstanding reservation — so it never picks
// capacity another gang has already committed to. It then pins the winner and
// reserves gangSize whole nodes there. ok is false only when the group is
// unpinned and no domain currently fits the gang (the gang waits, still
// unpinned).
func (p *Pins) Choose(group string, rawFree topology.FreeByDomain, gangSize int) (domain string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, pinned := p.byGroup[group]; pinned {
		return c.domain, true
	}

	// Effective free = raw free minus the outstanding reservation on each domain
	// (this group is not reserved yet). Subtract from the per-domain aggregate in
	// O(domains) rather than scanning every pinned group.
	eff := make(topology.FreeByDomain, len(rawFree))
	for d, f := range rawFree {
		eff[d] = f - p.reserved[d]
	}

	d, fit := topology.BestFit(eff, gangSize)
	if !fit {
		return "", false
	}
	p.byGroup[group] = &commitment{domain: d, remaining: gangSize}
	p.addReservation(d, gangSize)
	return d, true
}

// Place records that one of a group's members has landed on a real node in its
// pinned domain, shrinking the reservation by one whole node — the member is now
// counted by the live snapshot instead, so draining the reservation in step keeps
// the two from double-counting the same node. No-op for an unpinned group or a
// reservation already at zero.
func (p *Pins) Place(group string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.byGroup[group]; ok && c.remaining > 0 {
		c.remaining--
		p.addReservation(c.domain, -1)
	}
}

// Len returns the number of groups currently pinned (with an outstanding
// commitment) — used to surface a gauge of active placements.
func (p *Pins) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byGroup)
}

// Get returns the pinned domain for a group, or ("", false) if unpinned.
func (p *Pins) Get(group string) (domain string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.byGroup[group]; ok {
		return c.domain, true
	}
	return "", false
}

// Committed reports whether a group is pinned with no outstanding reservation
// (remaining==0): every reserved slot has been placed, or the pin was rebuilt
// from already-bound members on failover. Either way its members are (being)
// bound, so the gate must also count bound siblings, not just waiting ones. A
// still-forming gang has remaining>0, so this stays false and the gate never
// pays the bound-sibling scan while a gang is filling normally.
func (p *Pins) Committed(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	return ok && c.remaining == 0
}

// Set pins a group to a domain without best-fit. Used on leader failover to
// rebuild the store from where bound pods already sit (OEP-0022 § failure modes:
// "the domain is simply where those pods are"). Those pods are real occupancy in
// the snapshot, so the rebuilt commitment holds no outstanding reservation.
func (p *Pins) Set(group, domain string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Drop any prior commitment's reservation before overwriting.
	if old, ok := p.byGroup[group]; ok {
		p.addReservation(old.domain, -old.remaining)
	}
	p.byGroup[group] = &commitment{domain: domain, remaining: 0}
}

// RetainOnly releases every pinned group NOT present in live, returning how many
// were released. It is the garbage collector's hook: a gang whose pods are all
// gone (completed and torn down, deleted, or abandoned before Reserve) leaves a
// pin no failure path reclaims, so byGroup would grow unbounded and a reused
// PodGroup name could inherit a stale domain. Reconciling against the set of
// gangs that still have live pods bounds byGroup to live gangs.
func (p *Pins) RetainOnly(live map[string]bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	released := 0
	for group, c := range p.byGroup {
		if !live[group] {
			delete(p.byGroup, group)
			p.addReservation(c.domain, -c.remaining)
			released++
		}
	}
	return released
}

// Release drops a group's pin and its reservation — on gang completion, on
// gate-timeout unwind, or on teardown — so the next Choose re-runs best-fit and
// may pick a different domain. It reports whether a pin was actually present, so
// callers can act once per gang even though the unwind re-enters per member.
func (p *Pins) Release(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byGroup[group]
	if !ok {
		return false
	}
	delete(p.byGroup, group)
	p.addReservation(c.domain, -c.remaining)
	return true
}
