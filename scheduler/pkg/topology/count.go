package topology

// Node is the framework-free view of a schedulable node the accounting needs:
// which domain it is in (the value of the configured topology label) and whether
// it is free (available for a whole-node gang pod). The plugin maps the live
// scheduler NodeInfo onto this tiny struct so the accounting stays pure and
// unit-testable.
type Node struct {
	Domain string
	Free   bool
}

// CountFree tallies free whole nodes per domain. A node with no domain label
// (empty Domain) belongs to no domain and is ignored. A domain with nodes but
// none free still appears with a count of 0 — "exists but full" must be
// distinguishable from "does not exist" (the reservation and "largest gang that
// fits" logic depend on knowing the full domain set).
func CountFree(nodes []Node) FreeByDomain {
	out := FreeByDomain{}
	for _, n := range nodes {
		if n.Domain == "" {
			continue
		}
		out[n.Domain] += 0 // ensure the domain key exists even if all-occupied
		if n.Free {
			out[n.Domain]++
		}
	}
	return out
}
