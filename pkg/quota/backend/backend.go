// Package backend is the seam between the AcceleratorQuota tree and whatever
// admission system enforces it. The tree is the declaration; a backend is what
// makes that declaration true on one cluster.
//
// The seam exists so the enforcement system stays swappable and, more
// immediately, so the mapping from tree to enforcement objects can be tested
// as a pure function. Rendering decides shape; the backend performs I/O.
package backend

import (
	"context"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/ome-projects/ome/pkg/quota/tree"
)

// Plan is what a backend is asked to make true on this cluster.
//
// The split between Write and Retain is the freeze rule: a node whose subtree
// violates a computed invariant keeps whatever was last written for it, and the
// orphan sweep must be able to tell that apart from a node that has genuinely
// gone away. Both sets are needed because "absent from Write" alone is
// ambiguous between the two.
type Plan struct {
	// Write are the nodes whose objects must match their current budget.
	Write []*tree.Node

	// Retain names nodes whose objects must be left exactly as they are.
	// A sweep that treats these as orphans would delete a tenant's quota
	// because its parent was misauthored.
	Retain sets.Set[string]
}

// NodeOutcome is what materialization did for one node, in terms the caller can
// put on that node's status without knowing anything about the backend.
type NodeOutcome struct {
	// Reason is empty when the node materialized cleanly. Otherwise it is a
	// condition reason from the v1beta1 AcceleratorQuota vocabulary, so the
	// caller never invents one.
	Reason string

	// Message is operator-facing and names the specific object or budget at
	// fault, since a reason alone does not say which flavor went missing.
	Message string
}

// Result reports what one Materialize pass did. Nodes carries an entry only for
// nodes with something to report, so an all-clean pass allocates nothing.
type Result struct {
	// Applied counts objects written this pass, whether or not the write
	// changed anything.
	Applied int

	Nodes map[string]NodeOutcome
}

// Note records a problem against a node, keeping the first one seen.
//
// First rather than last: rendering reports a cause — a budget naming a flavor
// this cluster does not have — before applying reports its symptom, and the
// cause is the half an operator can act on.
func (r *Result) Note(node, reason, message string) {
	if r.Nodes == nil {
		r.Nodes = map[string]NodeOutcome{}
	}
	if _, seen := r.Nodes[node]; seen {
		return
	}
	r.Nodes[node] = NodeOutcome{Reason: reason, Message: message}
}

// Backend renders and applies a Plan.
//
// Materialize is expected to be idempotent and to report per-node problems
// through Result rather than failing the whole pass: one leaf naming a flavor
// that does not exist must not stop every other tenant's quota from being
// written. A returned error means the pass could not be completed at all.
type Backend interface {
	// Name identifies the backend in logs and conditions.
	Name() string

	Materialize(ctx context.Context, plan Plan) (Result, error)
}
