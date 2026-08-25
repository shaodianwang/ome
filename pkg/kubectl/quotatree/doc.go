// Package quotatree renders a set of AcceleratorQuota CRs as the quota tree
// they describe. It is the body of the kubectl-qt plugin, kept out of main so
// the rendering is unit-testable against fixtures rather than a live cluster.
//
// Assembly is not done here. pkg/quota/tree is the authority on what the edges
// mean — which node is the root, which parents dangle, which budgets overrun
// their parent — and this package is a view over its output. Duplicating any of
// that would let the plugin disagree with the controller, which is the one thing
// a diagnostic tool must never do.
//
// The view keeps three things that a naive renderer drops. Nodes with no
// position in the tree are rendered separately rather than omitted, because a
// node whose parent was deleted is precisely the node an operator is hunting
// for. Violations are carried alongside the nodes they are attributed to, and
// separately as a list, because a violation can belong to a parent that is
// itself fine. And a node frozen by an ancestor's violation is marked as such,
// so an operator reads one root cause instead of a subtree of symptoms.
package quotatree
